# -*- coding: utf-8 -*-
"""
WorkBuddy 真实用量 · 实时监控服务
=================================
零依赖本地 HTTP 服务：增量扫描 ~/.workbuddy/traces 与 workbuddy.db，
产出「每次调用的真实 token 输入/输出」并按任务(会话UUID)归因，
提供 /api/usage 实时 JSON，并托管前端看板（同源，无 CORS 问题）。

启动：  python wb_usage_server.py            # 默认 http://127.0.0.1:8788
       python wb_usage_server.py --port 9000
数据源（双通道，2026-10-19 起）：
  1) traces/<pid>/trace_*.json —— 5.8.x 之前的调用明细（5.8.2 起 WB 停写，已归档保留）
  2) logs/<日期>/*.log —— 5.8.x 起的会话日志，含 qua.end 遥测行
     （model / prompt_tokens / completion_tokens / ttft_ms / message_id）
     按 message_id 全局去重，仅合并 traces 未覆盖的日期，防止双计。
缓存：.wb_usage_cache.json（version=8，含 trace 缓存与 log 缓存）——
  原子写（tmp + os.replace），旧档转 .bak；主缓存损坏时回退 .bak，
  两者皆不可用且主缓存存在时拒绝保存（宁可报错也不覆盖现场）。
"""
import os, sys, json, re, time, glob, sqlite3, argparse, threading, traceback, shutil, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

ROOT = os.path.expanduser('~/.workbuddy')
DB = os.path.join(ROOT, 'workbuddy.db')
TRACES = os.path.join(ROOT, 'traces')
LOGS = os.path.join(ROOT, 'logs')          # 5.8.x 起 traces 停供，调用记录改存会话日志
PROJECTS = os.path.join(ROOT, 'projects')  # 会话转写：每条 assistant 消息 providerData.rawUsage（含各厂商原生缓存字段）
# 转写只需覆盖 traces 未覆盖的日期（traces 最后写入 2026-10-13），按文件 mtime 粗筛
TR_CUTOFF = time.mktime(time.strptime('2026-10-14', '%Y-%m-%d'))
HERE = os.path.dirname(os.path.abspath(__file__))
HTML = os.path.join(HERE, 'token_workbench_live.html')
CACHE_FILE = os.path.join(HERE, '.wb_usage_cache.json')
PORT = 8788

UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
MODEL_RE = re.compile(r'This conversation is powered by ([^\r\n"\\]{1,48})')
WS_RE = re.compile(r'Workspace Folder:\s*((?:(?!\\r|\\n|").){1,160})')
# 会话日志中的调用遥测行：[2026/10/19 12:03:53.567] [Info] [pid=26800] [ModelProvider] [SseMonitor#xxx] qua.end: {json}
LOG_LINE_RE = re.compile(r'\[(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})\.\d{3}\].*?qua\.end: (\{.*\})\s*$')
LOG_DAY_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

_lock = threading.Lock()
_cache = {}          # path -> {mtime, size, rec}
_log_cache = {}      # path -> {mtime, size, recs:[{id,day,model,in,out,ttft,dur,sid}]}
_tr_cache = {}       # path -> {mtime, size, recs:[{id,day,ts,model,in,out,cache,sid,ws}]} 会话转写源
_archive_trace = {}  # 已从磁盘消失的 trace 文件记录（独立归档文件，永不被版本重置清空）
_archive_log = {}    # 已从磁盘消失的会话日志记录
_cache_load_failed = False   # 主缓存存在却读取失败且备份不可用 → 禁止覆盖保存
_sessions = {}       # id -> {title, model, updated_at}
_last_req = time.time()   # 最近一次请求时间（空闲自动退出用）
_last = {'ts': 0.0, 'scan_sec': 0.0, 'parsed': 0, 'files': 0,
         'log_files': 0, 'log_records': 0, 'log_days': []}
CACHE_VERSION = 8
ARCHIVE_FILE = os.path.join(HERE, '.wb_usage_archive.json')   # 归档：文件被删后仍保留其记录


def _read_json(path):
    """读 JSON；文件缺失或损坏返回 None（损坏与缺失由此区分）。"""
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    except Exception:
        return None


def _safe_replace(tmp, path, retries=6):
    """Windows 下目标文件被占用时 os.replace 会 PermissionError，重试即可。"""
    last = None
    for i in range(retries):
        try:
            os.replace(tmp, path)
            return True
        except Exception as e:
            last = e
            time.sleep(0.2 * (i + 1))
    raise last


def _atomic_write_json(path, obj):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(obj, fh, ensure_ascii=False)
    if os.path.exists(path):
        try:
            shutil.copy2(path, path + '.bak')      # 复制而非移动：主文件始终存在
        except Exception:
            pass
    _safe_replace(tmp, path)


def load_archive():
    """加载归档文件（与 CACHE_VERSION 无关，永远保留）。"""
    global _archive_trace, _archive_log
    d = _read_json(ARCHIVE_FILE)
    if isinstance(d, dict):
        _archive_trace = d.get('trace') or {}
        _archive_log = d.get('log') or {}
    else:
        _archive_trace, _archive_log = {}, {}


def save_archive():
    """归档落盘（合并磁盘版本，防并发覆盖）。"""
    disk = _read_json(ARCHIVE_FILE)
    if isinstance(disk, dict):
        for k, v in (disk.get('trace') or {}).items():
            _archive_trace.setdefault(k, v)
        for k, v in (disk.get('log') or {}).items():
            _archive_log.setdefault(k, v)
    _atomic_write_json(ARCHIVE_FILE, {'trace': _archive_trace, 'log': _archive_log})


def load_cache():
    """加载缓存。主缓存损坏时回退 .bak；两者皆不可用且主缓存文件存在时，
    置 _cache_load_failed=True，使 save_cache() 拒绝覆盖（保护现场，防止空缓存冲掉历史）。"""
    global _cache, _log_cache, _tr_cache, _cache_load_failed
    load_archive()
    _cache_load_failed = False
    d = _read_json(CACHE_FILE)
    if d is not None:
        # 能读出：版本相符即用；版本不符视为主动重置，允许后续覆盖
        if d.get('version') == CACHE_VERSION:
            _cache = d.get('cache', {})
            _log_cache = d.get('log_cache', {})
            _tr_cache = d.get('tr_cache', {})
        else:
            _cache, _log_cache, _tr_cache = {}, {}, {}
        return
    # 主缓存读不出（损坏或不存在）
    existed = os.path.exists(CACHE_FILE)
    b = _read_json(CACHE_FILE + '.bak')
    if b is not None and b.get('version') == CACHE_VERSION:
        _cache = b.get('cache', {})
        _log_cache = b.get('log_cache', {})
        _tr_cache = b.get('tr_cache', {})
        return
    _cache, _log_cache, _tr_cache = {}, {}, {}
    _cache_load_failed = bool(existed)


def save_cache():
    """原子写 + 并发安全。

    1) 合并磁盘上其它进程写入的条目（磁盘里有、本进程没有的，一律保留），
       避免多进程（Stop hook / 生成器 / 实时服务）互相覆盖导致「归档基线」被清空；
    2) 写临时文件后 os.replace 落位；备份用 copy 而非 move——
       move 会让主文件在那一瞬消失，并发进程此刻读缓存会得到空基线（已踩坑：09-19 数据二次丢失）；
    3) _cache_load_failed 时拒绝保存（宁可报错也不覆盖现场）。
    """
    if _cache_load_failed:
        raise RuntimeError('缓存损坏且备份不可用，已拒绝覆盖以保护现场：' + CACHE_FILE)
    # 1) 合并磁盘版本（并发 lost-update 防护）
    disk = _read_json(CACHE_FILE)
    if isinstance(disk, dict) and disk.get('version') == CACHE_VERSION:
        for k, v in (disk.get('cache') or {}).items():
            # 已归档的路径不再捞回（否则会与归档记录重复计数）
            if k not in _cache and k not in _archive_trace:
                _cache[k] = v
        for k, v in (disk.get('log_cache') or {}).items():
            if k not in _log_cache:
                _log_cache[k] = v
        for k, v in (disk.get('tr_cache') or {}).items():
            if k not in _tr_cache:
                _tr_cache[k] = v
    tmp = CACHE_FILE + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as fh:
            json.dump({'version': CACHE_VERSION, 'cache': _cache, 'log_cache': _log_cache,
                       'tr_cache': _tr_cache}, fh, ensure_ascii=False)
        if os.path.exists(CACHE_FILE):
            try:
                shutil.copy2(CACHE_FILE, CACHE_FILE + '.bak')   # 复制而非移动：主文件始终存在
            except Exception:
                pass
        _safe_replace(tmp, CACHE_FILE)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        raise
    try:
        save_archive()
    except Exception:
        pass


# ---------------------------------------------------------------- 价格表
# 单位：USD / 每 1M tokens（in=输入, cache=缓存命中输入, out=输出）
# 来源：各厂商公开价格 / Artificial Analysis 记录（2026-09 前后）。
# kind: official=有公开价 / local=本机自建(无 API 费用) / free=免费额度 / unknown=平台内置或未公开
PRICES = {
    'deepseek-v4-flash':          {'in': 0.44, 'out': 1.32, 'cache': 0.014, 'kind': 'official'},
    'deepseek-v4.1-flash':        {'in': 0.30, 'out': 1.20, 'cache': 0.030, 'kind': 'official'},
    'deepseek-v4-flash-vision-exp':{'in': 0.44, 'out': 1.32, 'cache': 0.014, 'kind': 'official'},
    'qwen3.8-flash-next':         {'in': 0.15, 'out': 0.47, 'cache': 0.015, 'kind': 'official'},
    'qwen3.6-35b-a3b-awq-4bit':   {'in': 0.0,  'out': 0.0,  'cache': 0.0,   'kind': 'local'},
    'qwen3.8-27b-awq-int4':       {'in': 0.0,  'out': 0.0,  'cache': 0.0,   'kind': 'local'},
    'minimax-m3:free':            {'in': 0.0,  'out': 0.0,  'cache': 0.0,   'kind': 'free'},
    'glm-4.7-flash':              {'in': 0.0,  'out': 0.0,  'cache': 0.0,   'kind': 'free'},
}
# 未公开定价模型（Hy3/Hy4/agnes/sensenova/spark/dots3/intern 等）在「情景估算」时使用的中端 flash 价
DEFAULT_SCENARIO = {'in': 0.30, 'out': 1.20, 'cache': 0.030}
USD_CNY = 7.10


def price_of(name):
    key = (name or '').strip().lower()
    if key in PRICES:
        return PRICES[key]
    for k, v in PRICES.items():
        if k in key or key in k:
            return v
    return {'in': DEFAULT_SCENARIO['in'], 'out': DEFAULT_SCENARIO['out'],
            'cache': DEFAULT_SCENARIO['cache'], 'kind': 'unknown'}


def cost_of(p, tin, tcache, tout):
    """返回 (official_cost_usd, scenario_cost_usd)。local/free 两者皆为 0。"""
    net = max(0, tin - tcache)
    usd = (net / 1e6) * p['in'] + (tcache / 1e6) * p['cache'] + (tout / 1e6) * p['out']
    if p.get('kind') in ('local', 'free'):
        return 0.0, 0.0
    if p.get('kind') == 'official':
        return usd, usd
    return 0.0, usd   # unknown：官方价未知 → 仅计入情景估算


def load_sessions():
    """id -> meta (read-only)。

    先把 workbuddy.db（含 -wal/-shm）复制到临时目录再读，避免以任何方式
    写原库（WAL 模式下只读打开也可能被拒/生成 -shm，沙箱环境下直接报错）。
    """
    out = {}
    tmpdir = None
    try:
        import tempfile, shutil
        tmpdir = tempfile.mkdtemp(prefix='wbusage_')
        dst = os.path.join(tmpdir, 'wb.db')
        shutil.copy2(DB, dst)
        for ext in ('-wal', '-shm'):
            if os.path.exists(DB + ext):
                try:
                    shutil.copy2(DB + ext, dst + ext)
                except Exception:
                    pass
        con = sqlite3.connect(dst, timeout=5)
        cur = con.cursor()
        cur.execute('SELECT id,title,custom_title,model,updated_at,deleted_at FROM sessions')
        for sid, title, ctitle, model, upd, dele in cur.fetchall():
            out[sid] = {
                'title': (ctitle or title or '').strip() or '(未命名会话)',
                'model': (model or '').replace('custom-local:', '') or '(未指定)',
                'updated_at': upd or 0,
                'deleted': bool(dele),
            }
        con.close()
    except Exception:
        traceback.print_exc()
    finally:
        if tmpdir:
            try:
                import shutil
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass
    return out


def usages_of_text(text):
    """Extract model-call usage dicts from a trace file's raw text (robust to truncation net)."""
    out = []
    # fast path: locate each messages array via json parse of the file
    return out


def parse_trace(path):
    """Parse one trace file -> rec dict (or None)."""
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
            raw = fh.read()
    except Exception:
        return None
    if 'prompt_tokens' not in raw and 'Workspace Folder' not in raw:
        return {'skip': True}
    rec = {'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0,
           'session': None, 'model': None, 'ws': None, 'startedAt': None,
           'ops': {}, 'models': {}, 'calls_detail': []}
    try:
        d = json.loads(raw)
    except Exception:
        d = None
    if isinstance(d, dict):
        st = (d.get('trace') or {}).get('startedAt')
        rec['startedAt'] = st
        seen_ids = set()
        for s in d.get('spans') or []:
            nm = s.get('name') or '(未知)'
            o = rec['ops'].setdefault(nm, {'calls': 0, 'chars': 0})
            o['calls'] += 1
            to = s.get('toolOutput')
            if isinstance(to, str):
                o['chars'] += len(to)
            # —— 权威口径：解析 API 响应对象（含 model + usage），按响应 id 去重 ——
            for fld in ('toolOutput', 'toolInput'):
                v = s.get(fld)
                if not isinstance(v, str) or 'prompt_tokens' not in v:
                    continue
                try:
                    parsed = json.loads(v)
                except Exception:
                    continue
                objs = parsed if isinstance(parsed, list) else [parsed]
                for obj in objs:
                    if not isinstance(obj, dict):
                        continue
                    mname = obj.get('model')
                    u = obj.get('usage')
                    if not mname or not isinstance(u, dict) or 'prompt_tokens' not in u:
                        continue
                    rid = obj.get('id')
                    if rid:
                        if rid in seen_ids:
                            continue
                        seen_ids.add(rid)
                    tin = u.get('prompt_tokens') or 0
                    tout = u.get('completion_tokens') or 0
                    tt = u.get('total_tokens') or 0
                    tc = (u.get('prompt_tokens_details') or {}).get('cached_tokens') or 0
                    day = None
                    cr = obj.get('created')
                    if isinstance(cr, (int, float)) and cr > 0:
                        try:
                            day = datetime.datetime.fromtimestamp(cr, TZ).strftime('%Y-%m-%d')
                        except Exception:
                            day = None
                    if not day and st:
                        day = str(st)[:10]
                    disp = mname.replace('custom-local:', '') if isinstance(mname, str) else str(mname)
                    mm = rec['models'].setdefault(
                        disp, {'in': 0, 'out': 0, 'cache': 0, 'tot': 0, 'calls': 0, 'daily': {}})
                    for tgt in (mm, rec):
                        tgt['in'] += tin
                        tgt['out'] += tout
                        tgt['cache'] += tc
                        tgt['tot'] += tt
                        tgt['calls'] += 1
                    # 性能明细：模型 / 响应时间戳 / 该 generation span 耗时ms / 输入 / 输出 / 缓存
                    sdur = s.get('duration')
                    rec['calls_detail'].append([
                        disp, obj.get('created') or 0,
                        sdur if isinstance(sdur, (int, float)) else 0,
                        tin, tout, tc])
                    if day:
                        dd = mm['daily'].setdefault(
                            day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
                        dd['in'] += tin
                        dd['out'] += tout
                        dd['cache'] += tc
                        dd['calls'] += 1
                        dd['tot'] += tt
    # model（用于展示的该 trace 主模型：取 token 最多的那个）
    if rec['models']:
        rec['model'] = max(rec['models'].items(), key=lambda kv: kv[1]['tot'])[0]
    else:
        mm2 = MODEL_RE.search(raw)
        if mm2:
            rec['model'] = mm2.group(1).strip()
    # workspace
    wm = WS_RE.search(raw)
    if wm:
        ws = wm.group(1).replace('\\\\', '/').replace('\\', '/').strip()
        while '//' in ws:
            ws = ws.replace('//', '/')
        rec['ws'] = ws
    # session uuid (pick most frequent that exists in sessions table; else most frequent)
    uu = UUID_RE.findall(raw)
    if uu:
        from collections import Counter
        cnt = Counter(uu)
        ordered = [u for u, _ in cnt.most_common()]
        rec['session'] = next((u for u in ordered if u in _sessions), ordered[0])
    return rec


def _median(xs):
    if not xs:
        return 0
    xs = sorted(xs)
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def _p95(xs):
    if not xs:
        return 0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * 0.95))]


def compute_perf(all_calls):
    """按模型计算性能：响应时间、吞吐(tok/s)、峰值 RPM/TPM、缓存命中率。"""
    by = {}
    for m, created, dur, tin, tout, tc in all_calls:
        e = by.setdefault(m, {'n': 0, 'durs': [], 'tps': [], 'tin': 0, 'tout': 0, 'tc': 0, 'items': []})
        e['n'] += 1
        if 0 < dur < 600000:
            e['durs'].append(dur)
            if tout > 0:
                e['tps'].append(tout / (dur / 1000.0))
        e['tin'] += tin
        e['tout'] += tout
        e['tc'] += tc
        if created:
            e['items'].append((created, tin + tout))
    out = {}
    for m, e in by.items():
        peak_rpm = peak_tpm = 0
        items = sorted(e['items'])
        j = 0
        for i in range(len(items)):
            while items[i][0] - items[j][0] > 60:
                j += 1
            cnt = i - j + 1
            tot = sum(x[1] for x in items[j:i + 1])
            if cnt > peak_rpm:
                peak_rpm = cnt
            if tot > peak_tpm:
                peak_tpm = tot
        out[m] = {
            'calls': e['n'],
            'lat_med': round(_median(e['durs'])),
            'lat_p95': round(_p95(e['durs'])),
            'tps_med': round(_median(e['tps']), 1),
            'avg_in': int(e['tin'] / e['n']) if e['n'] else 0,
            'avg_out': int(e['tout'] / e['n']) if e['n'] else 0,
            'cache_rate': round(e['tc'] / e['tin'] * 100, 1) if e['tin'] else 0,
            'peak_rpm': peak_rpm,
            'peak_tpm': peak_tpm,
        }
    return out


def parse_log_file(path):
    """解析一个会话日志文件中的 qua.end 遥测行，返回记录列表。
    每条：{id, day, model, in, out, ttft, dur, sid}（id=message_id 用于全局去重）。"""
    recs = []
    try:
        fh = open(path, 'r', encoding='utf-8', errors='replace')
    except OSError:
        return recs
    with fh:
        for line in fh:
            if 'qua.end' not in line:
                continue
            m = LOG_LINE_RE.search(line.rstrip('\r\n'))
            if not m:
                continue
            try:
                j = json.loads(m.group(7))
            except Exception:
                continue
            try:
                tin = int(j.get('prompt_tokens') or 0)
                tout = int(j.get('completion_tokens') or 0)
            except Exception:
                continue
            model = (j.get('model') or '').replace('custom-local:', '').strip() or '(未识别模型)'
            day = '%s-%s-%s' % (m.group(1), m.group(2), m.group(3))
            try:
                ts = int(time.mktime((int(m.group(1)), int(m.group(2)), int(m.group(3)),
                                      int(m.group(4)), int(m.group(5)), int(m.group(6)), 0, 0, -1)))
            except Exception:
                ts = 0
            recs.append({
                'id': j.get('message_id') or j.get('trace_id') or (path + '|' + day + '|%d' % len(recs)),
                'day': day,
                'model': model,
                'in': tin, 'out': tout,
                'ttft': int(j.get('ttft_ms') or 0),
                'dur': int(j.get('total_cost_ms') or 0),
                'ts': ts,
                'sid': j.get('conversation_id') or '',
            })
    return recs


def scan_logs(force=False):
    """增量扫描 ~/.workbuddy/logs/<日期>/*.log（5.8.x 起 traces 停供后的替代数据源）。
    按 (mtime,size) 增量；缓存结构与 traces 分开存放。"""
    t0 = time.time()
    files = []
    if os.path.isdir(LOGS):
        for name in sorted(os.listdir(LOGS)):
            sub = os.path.join(LOGS, name)
            if not os.path.isdir(sub) or not LOG_DAY_RE.match(name):
                continue
            for fn in sorted(os.listdir(sub)):
                if fn.endswith('.log'):
                    files.append(os.path.join(sub, fn))
    seen = set()
    parsed = 0
    for p in files:
        seen.add(p)
        try:
            stt = os.stat(p)
        except OSError:
            continue
        ent = _log_cache.get(p)
        if ent and not force and ent.get('mtime') == stt.st_mtime and ent.get('size') == stt.st_size:
            continue
        _log_cache[p] = {'mtime': stt.st_mtime, 'size': stt.st_size, 'recs': parse_log_file(p)}
        parsed += 1
    for p in list(_log_cache.keys()):
        if p not in seen and not os.path.exists(p):
            _archive_log[p] = _log_cache.pop(p)    # 日志被清理同样转入归档，不丢
    _last['log_files'] = len(files)
    _last['log_records'] = sum(len(e.get('recs') or []) for e in _log_cache.values())
    _last['log_scan_sec'] = round(time.time() - t0, 2)
    _last['log_parsed'] = parsed
    return parsed


def _hit_of(u, pd_usage=None):
    """从一次调用的 usage 中取缓存命中 token。
    各家字段不同，按优先级取第一个非零：
      prompt_cache_hit_tokens（DeepSeek/Qwen）→ prompt_tokens_details.cached_tokens（OpenAI）
      → cache_read_input_tokens（Anthropic）→ cached_tokens（OpenAI 旧式）
      → providerData.usage.inputTokensDetails[].cached_tokens（camelCase 归一形式）
    """
    if isinstance(u, dict):
        v = u.get('prompt_cache_hit_tokens')
        if isinstance(v, (int, float)) and v > 0:
            return int(v)
        ptd = u.get('prompt_tokens_details')
        if isinstance(ptd, dict):
            v = ptd.get('cached_tokens')
            if isinstance(v, (int, float)) and v > 0:
                return int(v)
        for k in ('cache_read_input_tokens', 'cached_tokens'):
            v = u.get(k)
            if isinstance(v, (int, float)) and v > 0:
                return int(v)
    if isinstance(pd_usage, dict):
        for det in (pd_usage.get('inputTokensDetails') or []):
            if isinstance(det, dict):
                v = det.get('cached_tokens')
                if isinstance(v, (int, float)) and v > 0:
                    return int(v)
    return 0


def parse_transcript_file(path):
    """解析一个会话转写 jsonl，抽取每次调用 usage。
    rec = {id, day, ts, model, in, out, cache, sid, ws}；id=(sessionId|messageId) 用于跨档去重。"""
    recs = []
    try:
        fh = open(path, 'r', encoding='utf-8', errors='replace')
    except OSError:
        return recs
    with fh:
        for line in fh:
            if '"rawUsage"' not in line and 'inputTokensDetails' not in line:
                continue
            try:
                j = json.loads(line)
            except Exception:
                continue
            if not isinstance(j, dict):
                continue
            pd = j.get('providerData')
            if not isinstance(pd, dict):
                continue
            u = pd.get('rawUsage')
            pd_usage = pd.get('usage')
            if not isinstance(u, dict):
                if isinstance(pd_usage, dict):
                    tin = int(pd_usage.get('inputTokens') or 0)
                    tout = int(pd_usage.get('outputTokens') or 0)
                    u = {'prompt_tokens': tin, 'completion_tokens': tout}
                else:
                    continue
            tin = int(u.get('prompt_tokens') or 0)
            tout = int(u.get('completion_tokens') or 0)
            if tin == 0 and tout == 0:
                continue
            hit = _hit_of(u, pd_usage)
            ts = j.get('timestamp'); day = None; tsec = 0
            try:
                x = float(ts)
                if x > 1e12:
                    x /= 1000.0
                tsec = int(x)
                day = datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d')
            except Exception:
                pass
            # 模型：优先 message.model，其次 providerData.model / requestModelName
            model = ''
            m = j.get('message')
            if isinstance(m, dict) and m.get('model'):
                model = m['model']
            if not model:
                model = pd.get('model') or pd.get('requestModelName') or ''
            model = str(model).replace('custom-local:', '').strip() or '(未识别模型)'
            if model in ('auto', 'Auto'):
                rn = pd.get('requestModelName')
                if rn and str(rn) not in ('Auto', 'auto'):
                    model = str(rn).replace('custom-local:', '').strip()
            sid = j.get('sessionId') or ''
            mid = j.get('id') or pd.get('messageId') or (path + '|%d' % len(recs))
            cwd = j.get('cwd') or ''
            ws = str(cwd).replace('\\\\', '/').replace('\\', '/')
            if ws and not ws.endswith('/'):
                ws += '/'
            recs.append({'id': str(sid) + '|' + str(mid), 'day': day, 'ts': tsec,
                         'model': model, 'in': tin, 'out': tout, 'cache': hit,
                         'sid': sid, 'ws': ws,
                         # 日志 qua.end 的 message_id 用 pd.messageId（ULID），与顶层 id 不同。
                         # 单独保留，供 aggregate 从日志按 messageId 精确补调用耗时。
                         'logmid': str(pd.get('messageId') or '')})
    return recs


def scan_transcripts(force=False):
    """增量扫描 ~/.workbuddy/projects/*/*.jsonl（会话转写，含缓存命中）。
    仅扫 mtime >= 2026-10-14 的文件（更早的日期由 traces 覆盖），按 (mtime,size) 增量。"""
    t0 = time.time()
    files = []
    if os.path.isdir(PROJECTS):
        for dp, _dn, fn in os.walk(PROJECTS):
            for f in fn:
                if not f.endswith('.jsonl'):
                    continue
                p = os.path.join(dp, f)
                try:
                    if os.stat(p).st_mtime < TR_CUTOFF:
                        continue
                except OSError:
                    continue
                files.append(p)
    parsed = 0
    for p in files:
        try:
            stt = os.stat(p)
        except OSError:
            continue
        ent = _tr_cache.get(p)
        if ent and not force and ent.get('mtime') == stt.st_mtime and ent.get('size') == stt.st_size:
            continue
        _tr_cache[p] = {'mtime': stt.st_mtime, 'size': stt.st_size,
                        'recs': parse_transcript_file(p)}
        parsed += 1
    for p in list(_tr_cache.keys()):
        if not os.path.exists(p):
            _tr_cache.pop(p, None)
    _last['tr_files'] = len(files)
    _last['tr_records'] = sum(len(e.get('recs') or []) for e in _tr_cache.values())
    _last['tr_parsed'] = parsed
    _last['tr_scan_sec'] = round(time.time() - t0, 2)
    return parsed


def scan(force=False):
    t0 = time.time()
    global _sessions
    _sessions = load_sessions()
    files = glob.glob(os.path.join(TRACES, '*', 'trace_*.json'))
    seen = set()
    parsed = 0
    for p in files:
        seen.add(p)
        try:
            stt = os.stat(p)
        except OSError:
            continue
        ent = _cache.get(p)
        if ent and not force and ent.get('mtime') == stt.st_mtime and ent.get('size') == stt.st_size:
            continue
        rec = parse_trace(p)
        # 兜底：trace 内无模型标识（辅助型调用用了另一套精简系统提示）时，
        # 用该 trace 所属会话在 workbuddy.db 中的主模型来补全。
        if rec and not rec.get('skip') and not rec.get('model'):
            sid = rec.get('session')
            if sid and sid in _sessions:
                rec['model'] = _sessions[sid]['model']
                rec['model_src'] = 'session'
        _cache[p] = {'mtime': stt.st_mtime, 'size': stt.st_size, 'rec': rec}
        parsed += 1
    # —— 归档代替删除 ——
    # WorkBuddy 会清理 traces 目录（实测单日减少数百个文件），
    # 但文件消失不等于这段用量没发生过：记录移入独立归档文件（永不被版本重置清空），
    # 使累计量只增不减。
    archived_new = 0
    for p in list(_cache.keys()):
        if p not in seen:
            ent = _cache.pop(p)
            if not ent.get('archived'):
                ent['archived'] = True
                ent['archived_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                archived_new += 1
            _archive_trace[p] = ent
    _last['ts'] = time.time()
    _last['scan_sec'] = round(time.time() - t0, 2)
    _last['parsed'] = parsed
    _last['files'] = len(files)
    _last['archived_new'] = archived_new
    _last['archived_total'] = sum(1 for e in _cache.values() if e.get('archived'))
    # —— 会话日志源（5.8.x 起 traces 停供的替代源）——
    log_parsed = scan_logs(force=force)
    # —— 会话转写源（含各厂商原生缓存字段，用于补齐 10-14 起的缓存命中）——
    tr_parsed = scan_transcripts(force=force)
    if parsed or archived_new or log_parsed or tr_parsed:
        try:
            save_cache()
        except Exception as e:
            # 缓存写失败（如文件被占用）不应影响出数：记录并继续
            _last['cache_save_error'] = str(e)[:200]


def aggregate():
    per_model = {}
    per_task = {}
    per_day = {}
    ops = {}
    all_calls = []
    tot = {'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0}
    n_traces = 0
    n_with_usage = 0
    n_archived = 0
    archived_in = 0
    for _p, ent in list(_cache.items()) + list(_archive_trace.items()):
        rec = ent.get('rec')
        if not rec or rec.get('skip'):
            continue
        if rec['calls'] == 0 and not rec.get('model'):
            continue
        n_traces += 1
        if ent.get('archived'):
            n_archived += 1
            archived_in += rec.get('in', 0)
        if rec['calls']:
            n_with_usage += 1
        for cd in (rec.get('calls_detail') or []):
            all_calls.append(cd)
        for on, ov in (rec.get('ops') or {}).items():
            go = ops.setdefault(on, {'calls': 0, 'chars': 0})
            go['calls'] += ov['calls']
            go['chars'] += ov['chars']
        for k in tot:
            tot[k] += rec.get(k, 0) if k != 'calls' else rec['calls']
        day = str(rec['startedAt'])[:10] if rec.get('startedAt') else None
        rec_models = rec.get('models') or {}
        if rec_models:
            # 权威口径：每个模型贡献自己的 token 与按天分布
            for mname, mv in rec_models.items():
                m = per_model.setdefault(mname, {'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0, 'traces': 0})
                m['in'] += mv['in']; m['out'] += mv['out']; m['tot'] += mv['tot']
                m['cache'] += mv['cache']; m['calls'] += mv['calls']; m['traces'] += 1
                md_root = m.setdefault('daily', {})
                for dday, dv in (mv.get('daily') or {}).items():
                    md = md_root.setdefault(dday, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
                    md['in'] += dv['in']; md['out'] += dv['out']; md['cache'] += dv['cache']
                    md['calls'] += dv['calls']; md['tot'] += dv['tot']
                    pd = per_day.setdefault(dday, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
                    pd['in'] += dv['in']; pd['out'] += dv['out']; pd['cache'] += dv['cache']
                    pd['calls'] += dv['calls']; pd['tot'] += dv['tot']
        else:
            model = rec.get('model') or '(未识别模型)'
            m = per_model.setdefault(model, {'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0, 'traces': 0})
            m['in'] += rec['in']; m['out'] += rec['out']; m['tot'] += rec['tot']
            m['cache'] += rec['cache']; m['calls'] += rec['calls']; m['traces'] += 1
            if day:
                md = m.setdefault('daily', {}).setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
                md['in'] += rec['in']; md['out'] += rec['out']; md['cache'] += rec['cache']
                md['calls'] += rec['calls']; md['tot'] += rec['tot']
                pd = per_day.setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
                pd['in'] += rec['in']; pd['out'] += rec['out']; pd['cache'] += rec['cache']
                pd['calls'] += rec['calls']; pd['tot'] += rec['tot']
        model = rec.get('model') or '(未识别模型)'
        sid = rec.get('session')
        meta = _sessions.get(sid) if sid else None
        if sid and meta:
            tkey = sid
            name = meta['title']
        else:
            tkey = 'ws:' + (rec.get('ws') or '(未知)')
            name = rec.get('ws') or '(未归属任务)'
        pt = per_task.setdefault(tkey, {'name': name, 'model': None, 'mcount': {},
                                        'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0,
                                        'traces': 0, 'updated': 0, 'session': sid})
        pt['in'] += rec['in']; pt['out'] += rec['out']; pt['tot'] += rec['tot']
        pt['cache'] += rec['cache']; pt['calls'] += rec['calls']; pt['traces'] += 1
        td_root = pt.setdefault('daily', {})
        if rec_models:
            for mv in rec_models.values():
                for dday, dv in (mv.get('daily') or {}).items():
                    td = td_root.setdefault(dday, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
                    td['in'] += dv['in']; td['out'] += dv['out']; td['cache'] += dv['cache']
                    td['calls'] += dv['calls']; td['tot'] += dv['tot']
        elif day:
            td = td_root.setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
            td['in'] += rec['in']; td['out'] += rec['out']; td['cache'] += rec['cache']
            td['calls'] += rec['calls']; td['tot'] += rec['tot']
        # 任务主模型：按 token 加权
        if rec_models:
            for mname, mv in rec_models.items():
                pt['mcount'][mname] = pt['mcount'].get(mname, 0) + mv['tot']
        else:
            pt['mcount'][model] = pt['mcount'].get(model, 0) + rec['tot']
        if meta:
            pt['updated'] = max(pt['updated'], meta['updated_at'])
    # —— 会话日志耗时索引（ qua.end：model / message_id / total_cost_ms ）——
    # 日志与转写是同一次调用的两个视图：token 以转写为准（含各厂商缓存字段），
    # 耗时以日志为准（转写不记录耗时）。按 messageId 精确关联，不按天跳过，
    # 否则近期模型的 lat/tps 全为 0。只取耗时，不取 token，避免双计。
    dur_by_mid = {}
    for _p, ent in list(_log_cache.items()) + list(_archive_log.items()):
        for r in (ent.get('recs') or []):
            mid = r.get('id')
            du = r.get('dur')
            if mid and du and mid not in dur_by_mid:
                dur_by_mid[mid] = du

    # —— 会话转写源合并（10-14 起 traces 停供后，唯一含缓存命中的权威源）——
    # 与 traces 按「天」互斥：traces 已覆盖的日期一律跳过，防止双计。
    trace_days = set(per_day.keys())          # traces 已覆盖的日期快照
    tr_seen = set()
    tr_calls = tr_in = tr_out = tr_cache = 0
    tr_days = set()
    for _p, ent in list(_tr_cache.items()):
        for r in (ent.get('recs') or []):
            rid = r.get('id')
            if rid in tr_seen:
                continue
            tr_seen.add(rid)
            day = r.get('day')
            if not day or day < '2026-10-13' or day in trace_days:
                continue          # 仅补 traces 失效后的空档，且不与 traces 覆盖日重复
            model = r.get('model') or '(未识别模型)'
            tin, tout, tc = r.get('in', 0), r.get('out', 0), r.get('cache', 0)
            tr_days.add(day); tr_calls += 1; tr_in += tin; tr_out += tout; tr_cache += tc
            m = per_model.setdefault(model, {'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0, 'traces': 0})
            m['in'] += tin; m['out'] += tout; m['tot'] += tin + tout
            m['cache'] += tc; m['calls'] += 1
            md = m.setdefault('daily', {}).setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
            md['in'] += tin; md['out'] += tout; md['cache'] += tc; md['calls'] += 1; md['tot'] += tin + tout
            pdd = per_day.setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
            pdd['in'] += tin; pdd['out'] += tout; pdd['cache'] += tc; pdd['calls'] += 1; pdd['tot'] += tin + tout
            tot['in'] += tin; tot['out'] += tout; tot['tot'] += tin + tout
            tot['cache'] += tc; tot['calls'] += 1
            sid = r.get('sid') or ''
            meta = _sessions.get(sid) if sid else None
            if sid and meta:
                tkey, name = sid, meta['title']
            else:
                ws = r.get('ws') or '(未知)'
                tkey, name = 'ws:' + ws, (ws or '(未归属任务)')
            pt = per_task.setdefault(tkey, {'name': name, 'model': None, 'mcount': {},
                                            'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0,
                                            'traces': 0, 'updated': 0, 'session': sid or None})
            pt['in'] += tin; pt['out'] += tout; pt['tot'] += tin + tout
            pt['cache'] += tc; pt['calls'] += 1; pt['traces'] += 1
            td = pt.setdefault('daily', {}).setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
            td['in'] += tin; td['out'] += tout; td['cache'] += tc; td['calls'] += 1; td['tot'] += tin + tout
            pt['mcount'][model] = pt['mcount'].get(model, 0) + (tin + tout)
            if meta:
                pt['updated'] = max(pt['updated'], meta['updated_at'])
            # 耗时从日志按 messageId 精确补齐；无匹配仍为 0（compute_perf 会跳过）。
            dur = dur_by_mid.get(r.get('logmid') or '', 0)
            all_calls.append((model, r.get('ts') or 0, dur, tin, tout, tc))
    # —— 会话日志源合并（仅补 traces 与转写都未覆盖的日期，防止双计）——
    seen_ids = set()
    log_calls = log_in = log_out = 0
    log_days = set()
    for p, ent in list(_log_cache.items()) + list(_archive_log.items()):
        for r in (ent.get('recs') or []):
            rid = r.get('id')
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            day = r.get('day')
            if not day or day in trace_days or day in tr_days:
                continue                      # 已有 traces/转写覆盖，跳过防重复
            model = r.get('model') or '(未识别模型)'
            tin, tout = r.get('in', 0), r.get('out', 0)
            log_days.add(day)
            log_calls += 1; log_in += tin; log_out += tout
            m = per_model.setdefault(model, {'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0, 'traces': 0})
            m['in'] += tin; m['out'] += tout; m['tot'] += tin + tout; m['calls'] += 1
            md = m.setdefault('daily', {}).setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
            md['in'] += tin; md['out'] += tout; md['calls'] += 1; md['tot'] += tin + tout
            pd = per_day.setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
            pd['in'] += tin; pd['out'] += tout; pd['calls'] += 1; pd['tot'] += tin + tout
            tot['in'] += tin; tot['out'] += tout; tot['tot'] += tin + tout; tot['calls'] += 1
            # 任务归因：conversation_id → sessions 标题；无则用日志文件名（会话名）
            sid = r.get('sid') or ''
            meta = _sessions.get(sid) if sid else None
            fbase = os.path.basename(p)
            fname = fbase.split('__')[0] if '__' in fbase else fbase
            if sid and meta:
                tkey, name = sid, meta['title']
            else:
                tkey, name = 'log:' + p, fname or '(未归属任务)'
            pt = per_task.setdefault(tkey, {'name': name, 'model': None, 'mcount': {},
                                            'in': 0, 'out': 0, 'tot': 0, 'cache': 0, 'calls': 0,
                                            'traces': 0, 'updated': 0, 'session': sid or None})
            pt['in'] += tin; pt['out'] += tout; pt['tot'] += tin + tout; pt['calls'] += 1
            td = pt.setdefault('daily', {}).setdefault(day, {'in': 0, 'out': 0, 'cache': 0, 'calls': 0, 'tot': 0})
            td['in'] += tin; td['out'] += tout; td['calls'] += 1; td['tot'] += tin + tout
            pt['mcount'][model] = pt['mcount'].get(model, 0) + (tin + tout)
            if meta:
                pt['updated'] = max(pt['updated'], meta['updated_at'])
            if r.get('dur') or r.get('ts'):
                all_calls.append((model, r.get('ts') or 0, r.get('dur') or 0, tin, tout, 0))
    # finalize
    for tk, pt in per_task.items():
        if pt['mcount']:
            pt['model'] = max(pt['mcount'].items(), key=lambda kv: kv[1])[0]
        pt.pop('mcount', None)
        if not pt['updated'] and pt['session'] in _sessions:
            pt['updated'] = _sessions[pt['session']]['updated_at']
    tasks = sorted(per_task.values(), key=lambda x: -x['tot'])
    models = sorted(
        [dict(name=k, **v) for k, v in per_model.items()],
        key=lambda x: -x['tot'])
    daily = sorted([[d, v['in'], v['out'], v['cache'], v['calls']] for d, v in per_day.items()])

    # —— 性能 ——
    perf = compute_perf(all_calls)
    for m in models:
        m['perf'] = perf.get(m['name'])

    # —— 成本估算 ——
    for m in models:
        p = price_of(m['name'])
        off, sc = cost_of(p, m['in'], m['cache'], m['out'])
        m['cost'] = round(off * USD_CNY, 2)
        m['cost_scenario'] = round(sc * USD_CNY, 2)
        m['price_kind'] = p.get('kind', 'unknown')
        m['price'] = {'in': p['in'], 'out': p['out'], 'cache': p['cache'], 'kind': p.get('kind', 'unknown')}
    cost_official = round(sum(m['cost'] for m in models), 2)
    cost_scenario = round(sum(m['cost_scenario'] for m in models), 2)
    for t in tasks:
        p = price_of(t.get('model'))
        off, sc = cost_of(p, t['in'], t['cache'], t['out'])
        t['cost'] = round(off * USD_CNY, 2)
        t['cost_scenario'] = round(sc * USD_CNY, 2)
        t['price'] = {'in': p['in'], 'out': p['out'], 'cache': p['cache'], 'kind': p.get('kind', 'unknown')}
    ops_list = sorted(
        [{'name': k, 'calls': v['calls'], 'chars': v['chars'],
          'est_tokens': int(v['chars'] / 3.5)} for k, v in ops.items()],
        key=lambda x: -x['est_tokens'])
    return {
        'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'scan': {'sec': _last['scan_sec'], 'parsed_files': _last['parsed'],
                 'total_files': _last['files'], 'traces_used': n_traces,
                 'traces_with_usage': n_with_usage,
                 'archived_traces': n_archived, 'archived_input': archived_in,
                 'archived_new': _last.get('archived_new', 0),
                 'log_files': _last.get('log_files', 0),
                 'log_records': _last.get('log_records', 0),
                 'log_merged_calls': log_calls, 'log_merged_in': log_in,
                 'log_days': sorted(log_days),
                 'tr_files': _last.get('tr_files', 0),
                 'tr_records': _last.get('tr_records', 0),
                 'tr_merged_calls': tr_calls, 'tr_merged_in': tr_in,
                 'tr_merged_cache': tr_cache, 'tr_days': sorted(tr_days)},
        'totals': tot,
        'models': models,
        'tasks': tasks,
        'daily': daily,
        'ops': ops_list,
        'perf': perf,
        'cost': {'official': cost_official, 'scenario': cost_scenario, 'usd_cny': USD_CNY},
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, ctype, body):
        if isinstance(body, str):
            body = body.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        # 允许 file:// 直接打开的静态页跨域拉取本页数据（刷新即查即新）
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Access-Control-Allow-Private-Network', 'true')
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def do_OPTIONS(self):
        """预检：允许 file:// 打开的静态页跨域访问（Chrome 私有网络访问策略需要显式许可）。"""
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Access-Control-Allow-Private-Network', 'true')
        self.send_header('Access-Control-Max-Age', '600')
        self.end_headers()

    def do_GET(self):
        global _last_req
        _last_req = time.time()
        u = urlparse(self.path)
        if u.path in ('/', '/index.html', '/token_workbench_live.html'):
            try:
                with open(HTML, 'r', encoding='utf-8') as fh:
                    self._send(200, 'text/html; charset=utf-8', fh.read())
            except Exception as e:
                self._send(500, 'text/plain; charset=utf-8', 'HTML not found: %s' % e)
            return
        if u.path == '/api/usage':
            q = parse_qs(u.query)
            force = q.get('force', ['0'])[0] == '1'
            try:
                with _lock:
                    scan(force=force)
                    data = aggregate()
                self._send(200, 'application/json; charset=utf-8',
                           json.dumps(data, ensure_ascii=False))
            except Exception as e:
                self._send(500, 'application/json; charset=utf-8',
                           json.dumps({'error': str(e), 'trace': traceback.format_exc()}, ensure_ascii=False))
            return
        self._send(404, 'text/plain', 'not found')


def prewarm():
    try:
        with _lock:
            scan()
        d = aggregate()
        print('[wb-usage] 扫描完成：%d 个 trace（含用量 %d），输入 %s / 输出 %s，任务 %d，模型 %d，用时 %.1fs'
              % (d['scan']['traces_used'], d['scan']['traces_with_usage'],
                 format(d['totals']['in'], ','), format(d['totals']['out'], ','),
                 len(d['tasks']), len(d['models']), d['scan']['sec']), flush=True)
    except Exception:
        traceback.print_exc()


def _idle_watchdog(idle_sec):
    """空闲自动退出：避免本地服务常驻（默认关闭，--idle-exit 启用）。"""
    while True:
        time.sleep(15)
        if idle_sec and (time.time() - _last_req) > idle_sec:
            print('[wb-usage] 空闲 %d 秒无请求，自动退出' % idle_sec, flush=True)
            os._exit(0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--port', type=int, default=PORT)
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--idle-exit', type=int, default=0,
                    help='空闲多少秒无请求后自动退出（0=常驻）')
    args = ap.parse_args()
    load_cache()
    print('[wb-usage] 已加载本地缓存 %d 条，后台增量扫描中…' % len(_cache), flush=True)
    threading.Thread(target=prewarm, daemon=True).start()
    if args.idle_exit:
        threading.Thread(target=_idle_watchdog, args=(args.idle_exit,), daemon=True).start()
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print('[wb-usage] 服务已启动: http://%s:%d   (Ctrl+C 停止)' % (args.host, args.port), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\n[wb-usage] 已停止')


if __name__ == '__main__':
    main()
