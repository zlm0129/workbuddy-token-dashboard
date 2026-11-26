# -*- coding: utf-8 -*-
"""Build a static snapshot of the live token dashboard (real data baked in)
into ./wb_monitor_site/index.html for publishing."""
import os, sys, json, time, traceback
HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(HERE, '_refresh.log')


def _emit(msg):
    """输出到控制台（有的话）并追加到刷新日志。调用可能来自 pythonw.exe（无 stdout）。"""
    try:
        print(msg)
    except Exception:
        pass
    try:
        with open(LOG, 'a', encoding='utf-8') as f:
            f.write('[%s] %s\n' % (time.strftime('%Y-%m-%d %H:%M:%S'), msg))
        if os.path.getsize(LOG) > 200000:      # 简单裁剪，只留最近 400 行
            lines = open(LOG, 'r', encoding='utf-8').read().splitlines()[-400:]
            with open(LOG, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines) + '\n')
    except Exception:
        pass

sys.path.insert(0, HERE)
import wb_usage_server as srv

OUT_DIR = os.path.join(HERE, 'wb_monitor_site')
OUT_SITE = os.path.join(OUT_DIR, 'index.html')
OUT_USER = os.path.join(HERE, 'WB用量看板.html')
TEMPLATE = os.path.join(HERE, 'token_workbench_live.html')

def main(argv=None):
    # 节流：--min-interval=秒。距离上次生成不足该秒数则跳过（供 hook 高频触发时使用）。
    argv = list(sys.argv[1:] if argv is None else argv)
    interval = 0
    for a in argv:
        if a.startswith('--min-interval='):
            try:
                interval = int(a.split('=', 1)[1])
            except ValueError:
                interval = 0
    if interval > 0 and '--force' not in argv and os.path.exists(OUT_USER):
        age = time.time() - os.path.getmtime(OUT_USER)
        if age < interval:
            _emit('SKIP: 距上次生成仅 %.0f 秒（阈值 %d 秒），跳过本次' % (age, interval))
            return

    with srv._lock:
        srv.scan()
    d = srv.aggregate()
    data_json = json.dumps(d, ensure_ascii=False).replace('</', '<\\/')
    html = open(TEMPLATE, 'r', encoding='utf-8').read()
    inject = '<script>window.__DATA__ = %s;</script>\n' % data_json
    idx = html.rfind('<script>')
    html2 = html[:idx] + inject + html[idx:]
    with open(OUT_USER, 'w', encoding='utf-8') as f:
        f.write(html2)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_SITE, 'w', encoding='utf-8') as f:
        f.write(html2)
    _emit('静态看板 -> ' + OUT_USER)
    _emit('发布副本 -> ' + OUT_SITE)
    _emit('  generated_at: %s' % d['generated_at'])
    _emit('  input=%s output=%s cache=%s calls=%s tasks=%d models=%d'
          % (format(d['totals']['in'], ','), format(d['totals']['out'], ','),
             format(d['totals']['cache'], ','), d['totals']['calls'],
             len(d['tasks']), len(d['models'])))
    _emit('  bytes: %d' % os.path.getsize(OUT_USER))

if __name__ == '__main__':
    try:
        main()
    except Exception:
        _emit('ERROR: ' + traceback.format_exc())
        sys.exit(1)
