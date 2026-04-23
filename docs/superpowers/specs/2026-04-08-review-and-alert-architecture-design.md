# 股票信号提醒与复盘系统设计

日期：2026-04-08

## 目标

把当前以“手动查询”为主的股票数据工具，升级成一个更适合长期使用的“股票池 + 每日扫描 + 事件归档 + 通知 + 复盘”系统。

本设计重点解决两类问题：

1. 如何更及时地知道值得关注的股票信号
2. 如何更方便地回看历史事件并评估信号效果

## 当前现状

当前仓库已经具备：

- CLI、FastAPI、Streamlit 三套入口
- `TDX` 主力净流入查询
- `THSDK` K 线查询
- 基于 `AkShare` 的日线技术信号扫描

当前新增的日线信号能力可以识别：

- `MACD金叉`
- `MACD死叉`
- `MA5上穿MA20`
- `MA5下穿MA20`

但它仍然偏“即时查询”，还缺少：

- 固定股票池
- 事件持久化
- 去重通知
- 历史复盘视图
- 结果统计与效果评估

## 设计原则

### 1. 从“查询工具”升级成“事件系统”

系统的核心产物不应是某次查询结果，而应是“信号事件”。

每次扫描之后，系统需要把新发生的交叉、阈值突破、共振等信号记录为事件。后续通知、复盘、统计都围绕事件展开。

### 2. 数据采集、指标计算、规则判断、存储与通知分层

后续会不断增加指标与提醒规则，因此需要避免继续把逻辑堆进一个大服务文件中。

推荐把能力拆为：

- 数据提供层：负责拉取、清洗、缓存行情
- 指标层：负责计算 MACD、MA、RSI、KDJ 等指标
- 规则层：负责判断金叉、死叉、上穿、下穿、阈值突破等
- 扫描层：对股票池遍历，生成标准事件
- 存储层：负责落库和查询
- 通知层：负责把高优先级事件发送出去
- 复盘层：负责统计事后表现

### 3. 及时信息与低打扰并重

系统不应把所有信号都直接推送给用户，而应支持通知分级：

- 强提醒：高价值事件，建议即时推送
- 普通提醒：纳入每日摘要
- 仅归档：只入库，供后续复盘

### 4. 每个事件都要可复盘

事件不仅要记录“发生了什么”，还要记录“发生时的快照”，以便后续追踪效果并回看当时上下文。

## 目标用户流程

### 日常流程

1. 用户维护一份股票池
2. 系统每天在固定时间扫描股票池
3. 系统根据指标与规则生成当天新事件
4. 去重后输出通知
5. 所有事件进入历史时间线
6. 若干天后系统自动补充事件的事后表现，用于复盘

### 复盘流程

1. 用户打开历史事件页
2. 选择股票、时间范围、信号类型
3. 查看当日触发快照与后续表现
4. 汇总某类信号在一定时间窗内的效果

## 推荐目录结构

```text
app/
  api/
    __init__.py
    routers/
      signals.py
      tdx.py
      thsdk.py
      watchlists.py
      reviews.py
  providers/
    __init__.py
    daily_history.py
    tdx.py
    thsdk.py
  signals/
    __init__.py
    indicators.py
    rules.py
    scanner.py
    schemas.py
  storage/
    __init__.py
    db.py
    repository.py
    migrations.py
  services/
    __init__.py
    watchlist_service.py
    notification_service.py
    review_service.py
    scheduler_service.py
  ui/
    dashboard.py
    sections/
      today_alerts.py
      event_history.py
      review_stats.py
      watchlist_manager.py
scripts/
  run_daily_scan.py
  backfill_daily_bars.py
  review_signal_outcomes.py
data/
  app.db
docs/
  superpowers/specs/
```

## 数据模型

首选使用 SQLite。对于当前阶段，SQLite 足够轻量，也方便本地开发与单机场景。

### 1. `watchlists`

用于定义股票池。

建议字段：

- `id`
- `name`
- `description`
- `is_default`
- `created_at`
- `updated_at`

### 2. `watchlist_items`

用于存储股票池中的股票。

建议字段：

- `id`
- `watchlist_id`
- `code`
- `name`
- `market`
- `enabled`
- `tags`
- `created_at`

建议唯一约束：

- `(watchlist_id, code)`

### 3. `daily_bars`

用于缓存原始日线数据，既减少重复抓取，也支持复盘。

建议字段：

- `code`
- `trade_date`
- `open`
- `close`
- `high`
- `low`
- `volume`
- `amount`
- `pct_change`
- `turnover_rate`
- `adjust`
- `source`
- `fetched_at`

建议唯一约束：

- `(code, trade_date, adjust, source)`

### 4. `signal_events`

这是核心表。每一条记录代表一次新发生的技术信号事件。

建议字段：

- `id`
- `trade_date`
- `code`
- `indicator`
- `event_type`
- `severity`
- `summary`
- `close_price`
- `pct_change`
- `payload_json`
- `created_at`

其中：

- `indicator` 例：`MACD`、`MA`
- `event_type` 例：`golden_cross`、`death_cross`、`ma5_cross_up_ma20`
- `payload_json` 保存触发时的指标快照，如 `DIF`、`DEA`、`MA5`、`MA20`

建议唯一约束：

- `(trade_date, code, indicator, event_type)`

### 5. `notification_deliveries`

记录事件是否已经通知过，以及通过什么渠道发送。

建议字段：

- `id`
- `signal_event_id`
- `channel`
- `status`
- `delivered_at`
- `message_id`
- `error_message`

建议唯一约束：

- `(signal_event_id, channel)`

### 6. `review_snapshots`

用于保存某事件在后续时点的表现，用于复盘统计。

建议字段：

- `id`
- `signal_event_id`
- `horizon`
- `close_price`
- `pct_return`
- `max_drawdown`
- `updated_at`

其中 `horizon` 可取：

- `T+1`
- `T+3`
- `T+5`
- `T+10`

## 标准事件模型

内部推荐统一使用如下事件结构：

```json
{
  "trade_date": "2026-04-08",
  "code": "600519",
  "indicator": "MACD",
  "event_type": "golden_cross",
  "severity": "normal",
  "summary": "MACD金叉",
  "payload": {
    "close": 1530.25,
    "pct_change": 1.82,
    "dif": 1.2034,
    "dea": 1.1028
  }
}
```

这样做的好处是：

- 新增指标时不需要修改数据库主结构
- 通知层只消费统一事件
- UI 可以按事件类型进行分组和筛选
- 后续统计更容易聚合

## 指标与规则设计

### 指标层

指标层只负责对行情表进行计算，不负责判断是否触发提醒。

例如：

- `compute_macd(df)`
- `compute_moving_averages(df, windows=[5, 10, 20, 60])`
- `compute_rsi(df, period=14)`
- `compute_kdj(df)`
- `compute_boll(df, window=20)`

### 规则层

规则层负责把指标结果转成“事件”。

例如：

- `detect_macd_cross(df)`
- `detect_ma_cross(df, fast=5, slow=20)`
- `detect_rsi_threshold(df, low=30, high=70)`
- `detect_multi_signal_confluence(events)`

规则层输出事件列表，而不是直接面向表格或页面。

## 扫描流程设计

### 每日扫描主流程

1. 读取默认股票池
2. 按股票获取最新日线历史
3. 更新 `daily_bars`
4. 计算指标
5. 运行规则生成事件
6. 写入 `signal_events`
7. 根据优先级决定是否通知
8. 记录通知结果

### 去重逻辑

对于“每天过滤一次”的使用方式，建议只把“最新一根交易日相对上一根交易日新发生的事件”写入事件表。

然后通过数据库唯一约束和通知表进一步保证：

- 同一交易日同一事件只入库一次
- 同一事件同一渠道只发送一次

## 通知策略

### 通知分级

推荐初期规则：

- `high`
  - 同一天同一只股票出现两个及以上看多信号
  - 或后续定义的重要组合信号
- `normal`
  - 单一指标触发
- `archive_only`
  - 弱信号或不打扰用户的信号

### 通知渠道

建议分阶段实现：

第一阶段：

- API 输出
- Streamlit 页面展示
- CLI 控制台输出

第二阶段：

- 飞书机器人
- 邮件摘要

### 通知时间

建议默认在 A 股收盘后执行，例如：

- 每个交易日 `15:10` 至 `15:30`

## 复盘设计

### 历史事件页

支持：

- 按股票筛选
- 按时间范围筛选
- 按指标筛选
- 按事件类型筛选

每条事件展示：

- 触发日期
- 股票代码
- 信号摘要
- 触发时快照
- 是否已通知
- `T+1/T+3/T+5` 表现

### 统计页

展示以下指标：

- 某类信号触发次数
- 某类信号后续平均收益
- 某类信号胜率
- 某类信号最大回撤
- 某只股票对某类信号的历史表现

## API 设计建议

建议新增或逐步拆出如下接口：

- `GET /api/watchlists`
- `POST /api/watchlists`
- `POST /api/watchlists/{id}/items`
- `POST /api/signals/scan`
- `GET /api/signals/events`
- `GET /api/signals/events/{id}`
- `GET /api/reviews/summary`
- `POST /api/notifications/test`

现有的 `/api/signals/daily` 可逐步演进为：

- 即时扫描接口
- 或内部扫描器的便捷入口

## 页面设计建议

建议把当前 Streamlit 页拆成四个区域：

### 1. 今日提醒

展示今天新增的信号事件，按优先级排序。

### 2. 历史事件

展示事件时间线，支持筛选。

### 3. 复盘统计

展示各类信号的历史效果。

### 4. 股票池管理

允许直接维护默认关注列表。

## 实施阶段建议

### Phase 1：事件化基础设施

目标：

- 引入 SQLite
- 建立 `watchlists`、`watchlist_items`、`signal_events`
- 扫描结果从“临时返回”变成“入库事件”

交付：

- 默认股票池
- 扫描后写入事件表
- 今日提醒页面读取事件表

### Phase 2：通知与去重

目标：

- 建立 `notification_deliveries`
- 完成每日任务执行入口
- 完成去重通知

交付：

- 每日扫描脚本
- 单事件单渠道只发送一次
- 可扩展到飞书/邮件

### Phase 3：历史行情缓存与复盘

目标：

- 引入 `daily_bars`
- 增加 `review_snapshots`
- 输出复盘统计

交付：

- 历史事件详情
- `T+1/T+3/T+5` 收益统计
- 按信号聚合的复盘报告

### Phase 4：扩展指标体系

目标：

- 新增 RSI、KDJ、BOLL
- 引入多信号共振判断

交付：

- 标准指标插件式扩展
- 多指标事件评分

## 测试策略

### 单元测试

- 指标计算正确性
- 规则判断正确性
- 事件去重逻辑
- 复盘收益计算逻辑

### 集成测试

- 股票池扫描写库
- 事件查询接口
- 通知记录流程

### 回归测试

- 现有 `TDX`、`THSDK`、`Streamlit` 功能不回退

## 风险与取舍

### 风险 1：外部行情源不稳定

`AkShare` 依赖公开数据源，可能偶发失败。

缓解方式：

- 对日线数据做本地缓存
- 保留抓取失败记录
- 单只股票失败不影响整批扫描

### 风险 2：事件模型设计不稳定

如果一开始就把字段写死成很多列，后续新增指标会持续修改结构。

缓解方式：

- 主表保持稳定字段
- 指标细节快照放入 `payload_json`

### 风险 3：通知噪音过大

如果没有分级与去重，用户容易失去信任。

缓解方式：

- 先做严格去重
- 只推送高优先级事件
- 其余只归档

## 默认假设

- 用户当前主要关注 A 股日线级别信号
- 每日扫描一次即可，不做盘中提醒
- 默认时间按 `Asia/Shanghai`
- 当前主数据源仍以 `AkShare` 为日线历史来源
- 先以本地单机部署为主，后续再考虑多用户与远程服务

## 推荐的下一步

下一阶段优先实现 `Phase 1`，即：

- 建立 SQLite
- 增加默认股票池
- 把现有 `/api/signals/daily` 的扫描结果写入 `signal_events`
- 页面增加“今日提醒”和“历史事件”两个读取数据库的视图

这是最小但最关键的一步。完成后，系统会从“临时扫描结果”正式升级成“可通知、可追溯、可复盘”的事件系统。
