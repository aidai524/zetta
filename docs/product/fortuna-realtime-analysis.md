# Fortuna Realtime Data Analysis

本文档记录对 `https://app.fortuna.cc/` 公开页面、前端 bundle 和公开实时连接的观察，用于评估我们自己的 Discovery 实时交易页和行情刷新架构。

分析时间：2026-06-22

## 结论

Fortuna 的“及时性”主要不是靠高频 REST 轮询，而是靠 WebSocket 实时推送：

```text
Polymarket realtime source
        ↓
Fortuna backend enrichment / cache
        ↓
wss://api.fortuna.cc/ws
        ↓
browser Trade Feed
```

它的市场列表、排行榜、用户资料等仍然走 REST API；真正让页面体感“秒级变化”的是 Trade Feed 的 WS 推送。

## 公开证据

### Frontend Stack

`https://app.fortuna.cc/` 返回的是 Next.js 页面，HTTP 响应头包含：

```text
x-powered-by: Next.js
server: cloudflare
```

页面 HTML 中右侧实时区域包含：

```text
Trade Feed
Waiting for trades...
Disconnected
```

说明前端有实时连接状态，而不是只做静态列表刷新。

### Backend API

公开后端域名：

```text
https://api.fortuna.cc
```

健康检查：

```text
GET https://api.fortuna.cc/health
```

示例返回：

```json
{
  "status": "ok",
  "db": "ok",
  "timestamp": "2026-06-22T14:58:46.493Z"
}
```

响应头里有：

```text
x-powered-by: Express
server: cloudflare
ratelimit-policy: 200;w=60
```

这说明它有自己的 Express 后端和数据库，不是纯前端直连 Polymarket。

### Public Trade WebSocket

前端 bundle 中存在公开交易流连接：

```text
wss://api.fortuna.cc/ws
```

前端代码把 `https://api.fortuna.cc` 替换为 `wss://api.fortuna.cc/ws`：

```js
new WebSocket("https://api.fortuna.cc".replace(/^http/, "ws") + "/ws")
```

连接后第一条消息示例：

```json
{
  "type": "connected",
  "message": "Connected to trade feed",
  "polymarketConnected": true
}
```

随后服务端推送 `type=trade` 消息：

```json
{
  "type": "trade",
  "asset_id": "56899336583481809473223911452586130698528887572157743897606593594571591534764",
  "market": "atp-bailly-tiffon-2026-06-22",
  "price": "0.75",
  "side": "BUY",
  "size": "1.333332",
  "timestamp": "1782140385000",
  "hash": "0xf33ebb4a06827f435a89a2f8fd5267db1f9fdc8539da4ae1465d5bdbcf78bd37",
  "maker_address": "0x26Bd0dC53E21C890B1558b1266F87a823d1ca06D",
  "question": "Wimbledon, Qualification ATP: Gilles Arnaud Bailly vs Pol Martin Tiffon",
  "slug": "atp-bailly-tiffon-2026-06-22",
  "outcome": "Pol Martin Tiffon",
  "icon_url": "https://polymarket-upload.s3.us-east-2.amazonaws.com/atp-tour-b4390c4fb8.jpg",
  "market_id": "2630194"
}
```

前端只接收 `type === "trade"` 的消息，并转换为近期活动行：

```js
if ("trade" !== message.type) return;

{
  event_type: "last_trade_price",
  asset_id: message.asset_id || "",
  market: message.market || "",
  price: message.price || "",
  side: message.side || "BUY",
  size: message.size || "0",
  timestamp: message.timestamp || String(Date.now()),
  hash: message.hash,
  maker_address: message.maker_address || "",
  question: message.question,
  outcome: message.outcome,
  slug: message.slug,
  icon_url: message.icon_url,
  market_id: message.market_id
}
```

重连策略：

```text
close 后 2 秒重连；没有订阅者时关闭连接。
```

### Logged-In Trading WebSocket

另一个 `TradingLiveProvider` 也连接同一个 `/ws`，但它处理登录后的钱包状态：

```text
wallet
order
position
```

它会接收服务端推送的 `updates` 数组，并更新：

- wallet balance
- open orders
- live positions
- token prices

如果 WebSocket 不可用，才 fallback 到：

```text
GET /wallet-snapshot
```

并且 fallback 轮询间隔是 5 秒。

### Market REST API

市场列表走 REST：

```text
GET https://api.fortuna.cc/markets/events?limit=5&sort_by=volume_5m&sort_dir=desc
```

示例字段：

```json
{
  "market_id": "2565371",
  "token_id": "34140020473055059017561678467516053150545808058742972951374837012033612418292",
  "top_outcome": "Miami Marlins",
  "slug": "mlb-tex-mia-2026-06-22",
  "question": "Texas Rangers vs. Miami Marlins",
  "top_outcome_price": 0.57,
  "price_5m_ago": 0.54,
  "price_1h_ago": 0.53999996,
  "price_24h_ago": 0.52,
  "price_change_5m": 0.029999971,
  "price_change_1h": 0.030000031,
  "price_change_24h": 0.050000012,
  "volume_5m": 82483.586,
  "volume_1h": 83680.73,
  "volume_24h": 83680.73,
  "liquidity": 45.4693,
  "outcome_count": 2,
  "end_date": "2026-06-29T22:40:00Z"
}
```

响应头显示它允许短缓存：

```text
cache-control: public, max-age=0, s-maxage=30, stale-while-revalidate=120
```

这说明市场列表可以接受 30 秒级边缘缓存；秒级体感主要靠 WebSocket Trade Feed。

### Direct Polymarket CLOB Usage

前端 bundle 中还直接使用 Polymarket CLOB REST：

```text
https://clob.polymarket.com/book?token_id=...
https://clob.polymarket.com/prices-history?...
```

用途包括：

- order book
- estimated buy fill
- estimated sell fill
- price history fallback

这比只依赖 Gamma 或离线 mart 更接近交易系统本身。

### Binance Realtime Prices

Crypto Up/Down 类市场还接 Binance futures aggregate trade stream：

```text
wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade/...
```

前端把价格更新节流到约 500ms 批量刷新 UI。这解释了加密价格类页面为什么会有很强的实时感。

## Fortuna 的实时分层

从公开资源推断，Fortuna 至少分成三层：

### 1. Backend Ingestion

后端连接 Polymarket 实时源，接收成交事件，并补充市场元数据：

- question
- slug
- outcome
- icon_url
- market_id

公开 WS 消息里的这些字段说明服务端已经做了 enrichment，而不是把原始 Polymarket 事件直接原样丢给前端。

### 2. REST Cache

市场列表和排行榜走后端 REST，并支持 Cloudflare 缓存：

- `volume_5m`
- `price_change_5m`
- `volume_1h`
- `volume_24h`
- `top_outcome_price`
- `liquidity`

这些字段通常来自服务端预聚合或近实时表，不会在浏览器里现算。

### 3. Browser WebSocket

浏览器通过 WS 接收新成交，立即插入 Trade Feed。

UI 上的“快”主要来自：

- 无需等下一轮 REST poll
- 服务端主动推送
- 前端只追加新交易，不整表重刷
- 市场元数据已经随消息带上，无需每条再查

## 与我们当前实现的差距

我们当前 Discovery 实时交易页更多是：

```text
collector/frontier task
        ↓
raw files / loaders
        ↓
ClickHouse marts
        ↓
REST API
        ↓
frontend polling
```

这个链路天然会有延迟：

- 抓取任务要排队
- raw load 要等批次
- mart/API 要读数据库
- 前端只能轮询
- 新旧数据容易一起重渲染

Fortuna 的链路是：

```text
Polymarket realtime source
        ↓
backend stream processor
        ↓
WebSocket broadcast
        ↓
frontend append
```

数据库在这个链路里更像异步沉淀层，不挡住前端展示。

## 建议的 Zetta 目标架构

我们要达到类似体感，应该补一条实时旁路：

```text
Polymarket CLOB websocket / chain event listener
        ↓
zetta realtime stream service
        ↓
dedupe + normalize + enrich
        ↓
WebSocket broadcast
        ↓
Discovery live trade feed
        ↓
async write Postgres / ClickHouse / Redpanda
```

### Realtime Event Schema

建议前端 WS 消息先对齐 Fortuna 这种简单结构：

```json
{
  "type": "trade",
  "asset_id": "token_id",
  "market": "market_slug",
  "price": "0.75",
  "side": "BUY",
  "size": "100",
  "timestamp": "1782140385000",
  "hash": "0x...",
  "maker_address": "0x...",
  "question": "Market title",
  "slug": "event-or-market-slug",
  "outcome": "Yes",
  "icon_url": "https://...",
  "market_id": "..."
}
```

### Server Responsibilities

实时服务需要做：

- 连接 Polymarket CLOB WS 或链上事件源
- 事件去重，按交易 hash/log index/order id 建唯一键
- token_id 到 market/outcome 的内存映射
- 消息补充 question/slug/icon/category
- 低延迟推给浏览器 WS
- 异步写入 ClickHouse/Redpanda/Postgres
- 定时刷新 token metadata
- 断线重连和监控

### Frontend Responsibilities

实时交易页面需要：

- 建立 `/trades/stream` WebSocket
- 收到 `trade` 消息后只插入顶部
- 保持本地 ring buffer，例如 500-2000 条
- 不整表闪烁
- 老数据只自然下移
- WS 断开时显示状态，并 fallback 到 REST poll

## 优先级

### P0: Public Trade Feed WS

先做公开成交流：

```text
GET /trades/live
WS  /trades/stream
```

REST 继续作为初始快照，WS 只推增量。

### P1: Metadata Enrichment Cache

需要一个内存/Redis/Postgres 缓存：

```text
token_id -> {
  market_id,
  question,
  slug,
  event_slug,
  outcome,
  icon_url,
  category
}
```

没有 metadata 的交易也可以先推，但前端展示会弱一些；更好的做法是推基础交易，再异步补 metadata 更新。

### P2: Wallet/Position Realtime

等公开成交流稳定后，再做登录态或 tracked wallet 的实时仓位/活动更新。

### P3: Market List Near-Realtime Aggregates

类似 Fortuna 的 `volume_5m`、`price_change_5m`，我们可以用流式聚合或 1 分钟滚动表实现。

## 机器安排

这个任务不应该全部压在 API 主进程里。

建议：

- 当前 API 主机：提供 `/trades/stream` WS 和 REST 初始快照。
- Helper 机器：至少一台跑 realtime collector，连接 Polymarket/链上事件源。
- Postgres/Redis/Redpanda：做广播或缓冲队列。
- ClickHouse：异步落地和历史查询。

如果先快速落地，可以在 API 主机上跑单进程 realtime collector；确认稳定后再拆到 helper。

## 风险点

- Polymarket CLOB WS 事件模型需要确认是否覆盖全部成交。
- 链上事件更权威，但解析、metadata join、reorg/confirmations 更复杂。
- 单条成交可能有 maker/taker 多行，需要去重和用户侧语义转换。
- 只靠 WS 不等于完整历史，仍需落库补偿任务。
- Cloudflare/浏览器代理对长连接需要配置超时和心跳。
- 前端需要明确区分“实时增量”和“REST 快照”，避免重复展示。

## 对我们的直接启发

如果目标是“实时交易页面秒级变化”，继续提高 REST 轮询频率不是最优解。我们应该实现：

```text
REST snapshot + WebSocket delta
```

这会直接解决：

- 页面半天不变
- 新老数据一起闪
- 列表体感像整页刷新
- 数据依赖 loader/mart 批次导致延迟

数据库链路仍然保留，用于历史、筛选、回放和补偿；但前端实时展示不要等数据库链路完成。
