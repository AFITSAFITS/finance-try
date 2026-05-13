# AI Finance

一个面向 A 股的股票扫描与复盘平台。当前仓库的目标不是“演示页面”，而是一个可运行、可筛选、可沉淀事件、可部署的系统。

## 目标

这个平台需要满足 3 个基本要求：

1. 真正可用
   能批量扫描、输出清晰结果、记录错误、沉淀事件、支持日常复盘。
2. 能筛选特定形态
   当前已支持批量筛选 `水下金叉后水上再次金叉` 的股票。
3. 能部署
   当前仓库提供 `Dockerfile` 和 `docker-compose.yml`，可以把 API、页面、定时任务作为三个服务部署。

## 当前能力

- 实时行情快照
  - 查询当前价格、涨跌幅、成交额
  - 东方财富优先，腾讯兜底
  - 标记行情数据是否正常
  - 给出盘中观察提示
  - 多股票查询会自动分批，适合默认股票池规模
- 资金流查询
  - 通达信 `TdxQuant` 主力净流入
  - `AkShare` 兜底
- 日线信号扫描
  - `MACD金叉`
  - `MACD死叉`
  - `MA5上穿MA20`
  - `MA5下穿MA20`
  - `水下金叉后水上再次金叉`
- 涨停突破候选
  - 读取每日涨停池
  - 结合近期高点、均线、连板、封板稳定性评分
  - 统计同板块涨停数量和热度排名，识别板块共振
  - 按交易日保存候选股票
  - 支持回填 T+1 / T+3 / T+5 后续表现，按评分分层复盘
- 板块轮动监控
  - 支持行业板块与概念板块
  - 结合近期活跃度和 60 日位置打分
  - 标记 `活跃低位`、`活跃偏高`、`低位观察`、`普通观察`
- 默认股票池
  - 支持手工维护
  - 支持一键导入 `沪深300` 成分股
- 事件沉淀
  - 扫描结果写入 `signal_events`
  - 通知结果写入 `notification_deliveries`
  - 复盘结果写入 `review_snapshots`
  - 涨停突破写入 `limit_up_candidates`
  - 涨停候选复盘写入 `limit_up_review_snapshots`
  - 板块轮动写入 `sector_rotation_snapshots`
  - 每日扫描运行记录写入 `scan_runs`
- 页面与接口
  - FastAPI API
  - Streamlit 页面
  - CLI 命令
- 通知与调度
  - `stdout` 通知
  - 飞书机器人 webhook 通知
  - 独立 worker 每天定时执行默认股票池扫描

## 新增形态定义

`水下金叉后水上再次金叉` 当前按下面规则识别：

1. 最新一个交易日刚刚出现 `MACD金叉`
2. 最新这一笔金叉发生时，`DIF > 0` 且 `DEA > 0`
3. 在更早的历史中，出现过至少一次 `DIF < 0` 且 `DEA < 0` 的 `MACD金叉`

这类信号会在扫描结果里出现在 `MACD形态` 字段中，值为：

- `水下金叉后水上再次金叉`

## 项目结构

```text
app/
  api.py                  FastAPI 接口
  ui.py                   Streamlit 页面
  signal_service.py       日线信号计算与批量扫描
  event_service.py        信号事件入库
  watchlist_service.py    默认股票池
  notification_service.py 通知发送与去重
  worker_service.py       定时任务调度
  review_service.py       复盘回填与统计
  limit_up_service.py     涨停突破候选扫描与保存
  sector_rotation_service.py 板块轮动扫描与保存
  db.py                   SQLite 初始化
scripts/
  get_stock_data.py       CLI 查询入口
  run_daily_scan.py       每日扫描任务
  run_scan_worker.py      常驻任务进程
  review_signal_outcomes.py 复盘任务
data/
  app.db                  默认 SQLite 数据库
```

## 环境要求

- Python 3.11+
- macOS / Linux
- 如需 Docker 部署：Docker + Docker Compose v2

## 本地启动

### 1. 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 启动 API

```bash
uvicorn app.api:app --reload --host 0.0.0.0 --port 8000
```

### 3. 启动页面

```bash
API_BASE_URL=http://127.0.0.1:8000 streamlit run app/ui.py
```

### 4. 健康检查

```bash
curl http://127.0.0.1:8000/health
```

## 默认数据库

默认 SQLite 路径：

- `data/app.db`

也可以通过环境变量覆盖：

```bash
export AI_FINANCE_DB_PATH=/your/path/app.db
```

## 飞书通知配置

如果要把新事件推到飞书群，先创建飞书自定义机器人，然后准备下面两个环境变量：

```bash
export AI_FINANCE_NOTIFICATION_CHANNEL=feishu_webhook
export AI_FINANCE_FEISHU_WEBHOOK='https://open.feishu.cn/open-apis/bot/v2/hook/你的地址'
export AI_FINANCE_FEISHU_SECRET='如果启用了签名校验，就填这里'
```

没有配置时，默认走 `stdout`，只在终端里打印消息。

飞书机器人会收到卡片消息，包含股票代码、信号、交易日、收盘价、涨跌幅、评分、级别、观察结论、数据时效、60 日位置、量能比、相对强度、K 线形态、参考止损、参考目标、指标类型和风险提示，比纯文本更适合在群里快速浏览。

## CLI 用法

### 扫描日线信号

```bash
python scripts/get_stock_data.py daily-signals --codes 600519,000001,300502
python scripts/get_stock_data.py daily-signals --codes-file codes.txt --lookback-days 180 --max-workers 8
python scripts/get_stock_data.py daily-signals --codes-file codes.txt --min-score 60
```

扫描结果会带 `信号评分`、`信号方向`、`信号级别`、`观察结论`、`数据时效`、`数据滞后天数`、`评分原因`、`风险提示`、`60日位置`、`量能比`、`20日涨幅`、`60日涨幅`、`相对强度`、`K线形态`、`K线提示`、`参考止损`、`参考目标` 和 `风险收益比`，可以用 `--min-score` 只保留更值得观察的信号。
`观察结论` 会把评分、方向、风险提示和止损距离合成重点观察、谨慎观察、正常观察、暂不参考或风险回避。
`数据时效` 会标出行情是当日数据、最近交易日还是可能滞后；明显滞后的数据会进入风险提示并降低评分。
`相对强度` 会把股票放在本次扫描的股票池里横向比较，偏强的多头信号会加分，明显偏弱的会降权。
`K线形态` 会识别强势收盘、长上影线和弱势收盘；强势收盘会加分，冲高回落或收盘偏弱会提示风险。
偏多信号会给出参考止损和参考目标；止损距离过大时会降低评分并提示风险。
日线信号入库后会同步保存评分信息和观察结论，复盘统计会按评分区间和观察结论汇总，方便观察高分信号、重点观察信号后续表现是否更稳定。
命令行和每日任务会输出 `signal_summary`，快速汇总命中数、最高评分、观察结论分布和数据时效分布。
每日任务会保存扫描运行记录，方便回看每次是否成功运行、命中多少、错误多少、有没有数据滞后；运行记录会给出 `正常`、`部分失败`、`数据滞后`、`无信号` 或 `失败` 状态。
默认会限制单个行情源的最长等待时间，避免某个外部数据源卡住整批扫描。可以用 `AI_FINANCE_PROVIDER_TIMEOUT_SECONDS` 调整，默认 12 秒。
日线扫描会复用当天已抓取的本地 K 线缓存；缓存不足时才重新请求外部行情源。

### 仅筛选“水下金叉后水上再次金叉”

```bash
python scripts/get_stock_data.py daily-signals \
  --codes-file codes.txt \
  --only-secondary-golden-cross
```

### 每日任务

```bash
python scripts/run_daily_scan.py --channel stdout
python scripts/run_daily_scan.py --channel feishu_webhook
python scripts/run_daily_scan.py --min-score 60
python scripts/run_daily_scan.py --review-after-scan --review-summary-horizon T+3
```

每日任务默认只保存和通知评分 60 以上的信号；如果想保留全部信号，可以设置 `--min-score 0`。
同一只股票同一天如果触发多个信号，系统会保存全部事件用于复盘，但通知只发送一条代表信号，避免重复刷屏。
如果希望日常扫描后顺手积累复盘结果，可以加 `--review-after-scan`；需要限制复盘范围时，可以配合 `--review-trade-date`。

### 常驻任务进程

```bash
python scripts/run_scan_worker.py
python scripts/run_scan_worker.py --run-once --channel feishu_webhook
python scripts/run_scan_worker.py --run-once --min-score 60
python scripts/run_scan_worker.py --run-once --review-after-scan
```

常驻任务也支持扫描后复盘；部署时可以设置 `AI_FINANCE_REVIEW_AFTER_SCAN=true`，并用 `AI_FINANCE_REVIEW_HORIZONS` 和 `AI_FINANCE_REVIEW_SUMMARY_HORIZON` 控制复盘周期。

### 实时行情快照

```bash
python scripts/get_stock_data.py realtime-quotes --codes 600519,000001
```

用于快速核对当前价格、涨跌幅和成交额。优先使用东方财富，失败或缺失时自动尝试腾讯，并标记行情数据是否正常，同时给出盘中观察提示。多股票查询会自动分批，适合默认股票池规模。

### 复盘任务

```bash
python scripts/review_signal_outcomes.py
python scripts/review_signal_outcomes.py --trade-date 2026-04-08 --summary-horizon T+3
python scripts/review_signal_outcomes.py --target limit-up --trade-date 2026-05-12 --summary-horizon T+3
python scripts/review_signal_outcomes.py --stats-only --summary-horizon T+3
```

`--stats-only` 只读取已经保存的复盘结果，不会重新请求外部行情源，适合日常快速查看统计。
日线复盘统计会同时按评分区间、信号方向、观察结论、数据时效、风险提示和止损距离分层，并展示平均 60 日位置、平均量能比、平均止损距离、平均风险收益比、止损触发率、目标触发率、止损先到率、目标先到率、策略结论和结论可信度，便于判断哪些信号应该继续保留、观察或降权。

### 涨停突破候选

```bash
python scripts/get_stock_data.py limit-up-breakthroughs --trade-date 2026-05-12
python scripts/get_stock_data.py limit-up-breakthroughs --min-score 60 --max-items 50 --pool-limit 200
```

### 板块轮动

```bash
python scripts/get_stock_data.py sector-rotation --sector-type industry
python scripts/get_stock_data.py sector-rotation --sector-type concept --top-n 50 --max-items 20
```

板块轮动快照保存后，可以在页面里加载趋势图，观察同一批板块的评分变化。

## API 用法

### 导入沪深300默认股票池

```bash
curl -X POST http://127.0.0.1:8000/api/watchlists/default/import-index \
  -H 'Content-Type: application/json' \
  -d '{"index_code":"000300"}'
```

### 初始化默认股票池

会优先导入沪深 300 成分股；如果外部指数源不可用，会使用内置种子股票池。

```bash
curl -X POST http://127.0.0.1:8000/api/watchlists/default/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{"index_code":"000300"}'
```

### 扫描日线信号

```bash
curl -X POST http://127.0.0.1:8000/api/signals/daily \
  -H 'Content-Type: application/json' \
  -d '{
    "codes_text": "600519\n000001\n300502",
    "lookback_days": 180,
    "adjust": "qfq",
    "max_workers": 8,
    "min_score": 60
  }'
```

返回里会包含：

- `count`
- `requested_count`
- `error_count`
- `elapsed_seconds`
- `items`
- `errors`

### 查询实时行情快照

```bash
curl -X POST http://127.0.0.1:8000/api/market/realtime-quotes \
  -H 'Content-Type: application/json' \
  -d '{"codes":["600519","000001"]}'
```

返回里会包含当前价、涨跌幅、成交额、换手率、量比、数据质量、盘中观察提示和数据来源。

也可以直接查询默认股票池：

```bash
curl 'http://127.0.0.1:8000/api/market/realtime-quotes/default'
```

### 仅筛选“水下金叉后水上再次金叉”

```bash
curl -X POST http://127.0.0.1:8000/api/signals/daily \
  -H 'Content-Type: application/json' \
  -d '{
    "codes_text": "600519\n000001\n300502",
    "lookback_days": 180,
    "adjust": "qfq",
    "max_workers": 8,
    "only_secondary_golden_cross": true
  }'
```

### 扫描默认股票池并写入事件库

```bash
curl -X POST http://127.0.0.1:8000/api/signals/scan-default \
  -H 'Content-Type: application/json' \
  -d '{
    "lookback_days": 180,
    "adjust": "qfq",
    "max_workers": 8
  }'
```

### 扫描并保存涨停突破候选

```bash
curl -X POST http://127.0.0.1:8000/api/limit-up/breakthroughs \
  -H 'Content-Type: application/json' \
  -d '{
    "trade_date": "2026-05-12",
    "lookback_days": 120,
    "min_score": 50,
    "max_items": 100,
    "pool_limit": 200
  }'
```

### 回填涨停候选后续表现

```bash
curl -X POST http://127.0.0.1:8000/api/limit-up/reviews/backfill \
  -H 'Content-Type: application/json' \
  -d '{
    "trade_date": "2026-05-12",
    "horizons": [1, 3, 5],
    "adjust": "qfq"
  }'
```

### 查看涨停候选复盘统计

```bash
curl 'http://127.0.0.1:8000/api/limit-up/reviews/stats?trade_date=2026-05-12&horizon=T%2B3'
```

### 扫描并保存板块轮动

```bash
curl -X POST http://127.0.0.1:8000/api/sectors/rotation \
  -H 'Content-Type: application/json' \
  -d '{
    "trade_date": "2026-05-12",
    "sector_type": "industry",
    "top_n": 30,
    "max_items": 20
  }'
```

### 查看板块轮动趋势

```bash
curl 'http://127.0.0.1:8000/api/sectors/rotation/trends?sector_type=industry&start_date=2026-05-01&end_date=2026-05-12'
```

## 页面用法

页面入口：

```bash
streamlit run app/ui.py
```

重点标签页：

- `日线信号扫描`
  支持批量输入股票，支持勾选“仅保留水下金叉后水上再次金叉”
- `涨停突破`
  扫描每日涨停池，保存高评分突破候选，也可以查看历史候选和后续表现复盘
- `板块轮动`
  扫描行业或概念板块，保存活跃低位板块快照
- `今日提醒`
  对默认股票池执行正式扫描并写入事件库，也可以直接选择通知渠道
- `历史事件`
  查看历史信号事件
- `复盘统计`
  查看 `T+1 / T+3 / T+5` 结果
- `股票池`
  支持导入 `沪深300`，也支持一键初始化默认股票池

## 可用性设计

这个仓库当前针对“能不能用”做了几件实际事情：

- 扫描结果返回耗时、请求股票数、错误数
- 页面能区分“没有命中信号”和“行情源连接失败”
- 默认股票池与事件库持久化到 SQLite
- 每日任务发现默认股票池为空时，会先自动初始化股票池，避免部署后空跑
- 支持把扫描结果转成事件与复盘快照
- 支持把涨停突破候选和板块轮动快照按交易日保存
- 涨停候选会保留同板块涨停数量，便于判断是否只是单票异动
- 涨停候选支持按评分分层复盘，观察后续收益、胜率、回撤和策略结论
- 支持 CLI、API、UI 三种入口

## 部署

### Docker 部署

仓库已提供：

- `Dockerfile`
- `docker-compose.yml`
- `.env.example`
- `.dockerignore`

先准备环境变量：

```bash
cp .env.example .env
```

如果要发飞书，把 `.env` 里的这两项填上：

```bash
AI_FINANCE_NOTIFICATION_CHANNEL=feishu_webhook
AI_FINANCE_FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/你的地址
AI_FINANCE_FEISHU_SECRET=如果启用了签名校验就填这里
```

启动方式：

```bash
docker compose up --build -d
```

启动后默认端口：

- API: `8000`
- UI: `8501`

启动后会同时起来 3 个服务：

- `api`
- `ui`
- `worker`

启动后可以跑一次部署烟测：

```bash
python scripts/deployment_smoke.py --require-watchlist
```

这会检查 API 健康状态、默认股票池和页面是否能访问。

### 停止

```bash
docker compose down
```

### 持久化

`docker-compose.yml` 会把本地 `./data` 挂载到容器内 `/app/data`，SQLite 数据不会因为容器重建而丢失。

### 定时任务

现在默认由独立 `worker` 服务负责定时扫描。默认配置是交易日 `15:05` 扫一次。可以在 `.env` 里改：

```bash
AI_FINANCE_WORKER_SCHEDULE_TIME=15:05
AI_FINANCE_TIMEZONE=Asia/Shanghai
AI_FINANCE_WORKER_POLL_SECONDS=30
AI_FINANCE_PROVIDER_TIMEOUT_SECONDS=12
AI_FINANCE_DAILY_MIN_SCORE=60
```

如果你更想用宿主机调度器，也可以关掉 `worker`，自己定时执行：

```bash
python scripts/run_scan_worker.py --run-once --channel feishu_webhook
```

## 生产注意事项

当前项目已经具备部署入口，但要达到稳定生产使用，还需要正视下面几个事实：

- 日线行情当前仍然依赖公网数据源，远端偶发断连时会影响扫描成功率
- 日线 K 线当前会按 东方财富 / AkShare / 腾讯 / 新浪 / BaoStock / Yahoo 的顺序尝试兜底
- 单个行情源默认最多等待 12 秒，超时会继续尝试下一个来源
- 日线扫描会复用当天本地 K 线缓存，但隔天仍会重新刷新数据
- 日线评分会参考价格位置和量能，接近 60 日高位或量能不足时会提示风险
- 涨停池和板块数据同样依赖公网数据源，结果适合做候选和观察，不等同于交易建议
- SQLite 适合单机部署，不适合高并发多实例写入
- 飞书通知依赖你自己提供有效 webhook；如果地址或签名不对，消息不会发出去
- 通达信 `TdxQuant` 仍然依赖本地客户端环境，不适合纯容器化部署

如果要把“真正可用”继续往前推进，建议优先做：

1. 增加更稳定的实时行情源和数据质量校验
2. 扩充复盘样本，持续淘汰胜率不稳定的信号
3. 视部署规模把 SQLite 替换成 PostgreSQL

## 自检

```bash
source .venv/bin/activate
pytest -q
python -m py_compile app/*.py scripts/get_stock_data.py scripts/run_daily_scan.py scripts/run_scan_worker.py scripts/review_signal_outcomes.py scripts/deployment_smoke.py
python scripts/deployment_smoke.py --require-watchlist
```

## 当前验证范围

本仓库当前已覆盖：

- 新信号识别单测
- API 参数透传单测
- CLI 参数透传单测
- 事件入库去重单测
- 信号评分入库与复盘分层统计单测
- 飞书通知发送与重试单测
- worker 定时执行与命令入口单测
- 涨停突破候选扫描与保存单测
- 涨停候选复盘回填与统计单测
- 板块轮动扫描与保存单测

如果你要继续往“可部署、可运维”推进，下一步应该补的是：

- Docker 构建验证
- 一次真实部署 smoke test
- 飞书卡片消息
