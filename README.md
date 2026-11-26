# WorkBuddy Token 用量看板

一个零依赖的本地工具，读取 WorkBuddy 落在本机的数据，统计每次模型调用的真实 Token 消耗，按任务归因，生成可交互看板。所有计算在本机完成，不联网、不上传任何数据。

## 它能看什么

- 总输入 / 输出 / 缓存命中 Token、调用次数
- 按任务（会话）的 Token 消耗排行
- 按模型的消耗与费用分布
- 每日趋势、操作类型、耗时分布

## 数据源

只读以下本机路径：

- `~/.workbuddy/workbuddy.db` —— 会话与用量汇总
- `~/.workbuddy/traces/` —— 旧版调用明细（5.8.2 之前）
- `~/.workbuddy/logs/` —— 新版会话日志中的调用遥测行
- `~/.workbuddy/projects/` —— 会话转写中的逐次用量

不读取 API Key、密码等凭证，不修改上述任何文件。

## 两种用法

### 方式一：实时看板（推荐）

Windows 双击 `启动看板.bat`，浏览器自动打开 `http://127.0.0.1:8788`。脚本会拉起本地服务并后台增量扫描数据。

等价命令：

```bash
python -u wb_usage_server.py --port 8788
```

 macOS / Linux 手动运行上面的命令即可，浏览器访问同一地址。

### 方式二：静态快照

双击 `生成静态快照.bat`，生成 `WB用量看板.html`，单文件可离线打开、可直接发给别人看。等价命令：

```bash
python build_snapshot.py
```

看板前端模板在 `token_workbench_live.html`，静态快照会把它和数据一起内联。

## 环境要求

- Python 3.10+
- 本机装有 WorkBuddy 并产生过会话数据
- 前端图表库 Chart.js 通过 CDN 加载（`token_workbench_live.html` 内引用），查看时需联网；数据本身不出本机

## 隐私说明

生成的 HTML 与缓存文件含会话标题、任务名、模型名和用量数字，**可能暴露你的工作内容**。对外分享前请自行检查。本仓库不包含任何真实用量数据，`.gitignore` 已将缓存、归档和生成的看板排除在外。

## 文件说明

| 文件 | 用途 |
|---|---|
| `wb_usage_server.py` | 核心：增量扫描、聚合、本地 HTTP 服务 |
| `token_workbench_live.html` | 前端模板 |
| `build_snapshot.py` | 生成离线静态快照 |
| `启动看板.bat` | Windows 一键启动实时看板 |
| `生成静态快照.bat` | Windows 一键生成离线快照 |

## 许可

MIT
