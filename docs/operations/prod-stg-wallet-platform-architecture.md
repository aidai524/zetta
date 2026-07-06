# Production And Staging Wallet Platform Architecture

本文档描述 Discovery / wallet analytics 从当前 stg 环境发布到正式环境时的推荐架构。

核心原则：

- stg 负责重计算、数据修正、回测、链上补全和功能开发。
- prod 负责稳定、快速地展示最终结果，并独立承接实时 trade feed。
- prod 请求时不依赖 stg 在线状态。
- prod 不跑重型 ClickHouse mart、不跑全站回补、不跑高频链上扫描。

## Target Shape

```text
Polymarket / Polygon
        │
        ├── stg: compute, backfill, analysis
        │       ├── ClickHouse warehouse
        │       ├── Postgres task queue
        │       ├── helper wallet workers
        │       └── publish snapshots / publish tables
        │
        └── prod: serving, realtime, cached reads
                ├── Nginx + frontend
                ├── zetta-api
                ├── official-trade-feed
                ├── Redis / small Postgres cache
                └── imported read-only publish data
```

## Environment Roles

### stg

当前主机器和三台 helper 继续作为 stg / compute 环境。

stg 负责：

- Polymarket historical and realtime ingestion.
- Polygon chain enrichment.
- FIFA wallet PnL mart build.
- Polycop wallet signal refresh.
- Smart wallet / whale ranking calculation.
- Unusual betting analysis.
- Wallet workers and historical wallet data refresh.
- Schema and code development.
- Data repair, backfill, and experiments.

stg 可以接受：

- 后台任务重。
- ClickHouse 查询慢。
- 短时间服务抖动。
- 临时暂停和恢复任务。

### prod

prod 是轻量服务环境，只负责用户请求和实时展示。

prod 负责：

- Discovery frontend.
- API serving.
- Official Polymarket RTDS WebSocket feed.
- Realtime wallet activity cache.
- Reading precomputed wallet and FIFA results published from stg.
- Health checks, logs, and alerting.

prod 不负责：

- `chain-frontier`
- `wallet-fifa-24h-pnl` heavy mart rebuild
- `unusual-betting` heavy recompute
- broad `marts`
- old `wallet-rollup`
- full-site historical backfill
- large wallet worker fleet
- high-frequency ClickHouse scans

## Recommended Prod Machine

Minimum cost version:

```text
1 prod VM
8 vCPU
16 GB RAM
200 GB SSD
Ubuntu 24.04
```

Services:

```text
nginx
zetta-api.service
zetta-official-trade-feed.service
redis-server
postgresql or docker postgres small cache
lightweight publish-sync timer
monitoring/log rotation
```

Optional later split:

```text
prod-api-1:
  nginx
  frontend
  zetta-api
  official-trade-feed

prod-cache-1:
  Redis
  Postgres cache
  optional small read-only ClickHouse
```

Do not start with identical stg/prod hardware. Keep software, schema, and API behavior compatible, but use smaller prod hardware because prod should only serve cached and realtime data.

## Data Flow

### Batch / Analytical Data

stg computes authoritative results and publishes immutable snapshots.

```text
stg ClickHouse / Postgres
        ↓
publish tables or JSON snapshots
        ↓
rsync / scp / object storage / internal HTTP endpoint
        ↓
prod Postgres / Redis / local JSON cache
        ↓
prod API
```

prod should not query stg during user requests.

Recommended publish datasets:

```text
prod_wallet_screener_fifa
prod_polycop_fifa_signals
prod_wallet_fifa_summary
prod_wallet_fifa_positions
prod_wallet_fifa_recent_activity
prod_market_metadata
prod_token_metadata
prod_unusual_betting_summary optional
```

Each published dataset should include metadata:

```text
version
generated_at
source_env
row_count
checksum
schema_version
```

### Realtime Data

Realtime follow/copy-trade data must be handled by prod directly.

```text
Polymarket RTDS WebSocket
        ↓
prod official-trade-feed
        ↓
in-memory recent cache + Redis/state fallback
        ↓
prod /api/wallets/detail?...realtime=1
        ↓
frontend / copy-trading systems
```

Goal:

```text
wallet trade成交后 1-3 秒内 prod API 可以返回该 trade
```

ClickHouse insertion can lag by tens of seconds and should not be part of the realtime follow-trade decision path.

## Core API Mapping

### `/api/wallets/polycop-fifa-signals`

prod behavior:

- Read from published prod cache.
- No live Polycop fetch on user request.
- No heavy FIFA join on user request.

stg behavior:

- Fetch Polycop.
- Join FIFA wallet stats.
- Score and segment wallets.
- Publish cache to prod.

Suggested freshness:

```text
5-10 minutes
```

### `/api/wallets/screener?scope=fifa&mode=whale`

prod behavior:

- Read from `prod_wallet_screener_fifa`.
- Return stale-but-valid previous snapshot if new publish fails.
- No heavy ClickHouse mart build on user request.

stg behavior:

- Build `mart_wallet_fifa_24h_pnl`.
- Calculate smart/whale ranking fields.
- Publish compact screener snapshot.

Suggested freshness:

```text
5-15 minutes
```

### `/api/wallets/detail?...live=1&realtime=1`

prod behavior:

- Realtime activity: prod `official-trade-feed` memory/Redis first.
- Wallet live value and live positions: Polymarket live APIs.
- FIFA historical stats: published stg snapshots.
- Local state files only as fallback.

stg behavior:

- Compute historical positions and PnL.
- Publish wallet FIFA summaries and positions.
- Repair or backfill missing data.

Suggested freshness:

```text
realtime activity: 1-3 seconds
historical summary: 5-15 minutes
```

## Publish Model

Use atomic publish on prod.

Recommended process:

```text
1. stg writes snapshot with manifest.
2. prod syncs snapshot into a staging location.
3. prod verifies row_count, checksum, and schema_version.
4. prod loads into *_next tables or next cache keys.
5. prod atomically swaps active pointer.
6. old active version remains available for rollback.
```

Example layout:

```text
/var/lib/zetta/publish/
  wallet_screener_fifa/
    20260706T130000Z/
      manifest.json
      data.jsonl.zst
    current -> 20260706T130000Z
```

Example manifest:

```json
{
  "dataset": "wallet_screener_fifa",
  "version": "20260706T130000Z",
  "source_env": "stg",
  "generated_at": "2026-07-06T13:00:00Z",
  "schema_version": 1,
  "row_count": 1000,
  "checksum": "sha256:..."
}
```

## Recommended Timers

### stg timers

Keep on stg:

```text
zetta-polycop-wallet-signals.timer
zetta-wallet-fifa-24h-pnl.timer
zetta-load-trades-realtime.timer
zetta-live-token-metadata.timer
zetta-active-event-wallets.timer
zetta-frontier.timer
```

Run with caution on stg:

```text
zetta-chain-frontier.timer
zetta-unusual-betting.timer
zetta-unusual-betting-worker.service
zetta-marts.timer
zetta-wallet-rollup.timer
```

These should stay disabled or low-frequency unless explicitly needed, because they can compete with core wallet APIs.

### prod timers

Keep on prod:

```text
zetta-api.service
zetta-official-trade-feed.service
zetta-prod-publish-sync.timer
zetta-api-prewarm.timer
logrotate / metrics timers
```

Do not run on prod:

```text
zetta-chain-frontier.timer
zetta-unusual-betting-worker.service
zetta-marts.timer
zetta-wallet-rollup.timer
large wallet worker fleet
full-site backfill timers
```

## Deployment Flow

Recommended release flow:

```text
1. Develop and test on stg.
2. Run API tests and targeted health checks.
3. Rebuild or validate publish datasets on stg.
4. Publish snapshots from stg.
5. Deploy code to prod.
6. Run lightweight prod migration.
7. Sync snapshots to prod.
8. Restart prod API/feed if needed.
9. Prewarm core prod APIs.
10. Verify core API latency and realtime WS latency.
11. Switch or keep traffic on prod.
```

Core verification:

```bash
curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?limit=100&min_fifa_notional=1000&data_quality=estimate'

curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/screener?scope=fifa&mode=whale&limit=100'

curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/detail?user=0x1fd80277d4cc327a2a1440d144e13d71774e7749&live=1&position_limit=10&activity_limit=100&pnl_points_limit=0&realtime=1'
```

Realtime verification:

```text
Polymarket trade timestamp
        ↓
prod official-trade-feed received_at
        ↓
prod /wallets/detail recent_activity visible time
```

Target:

```text
p99 WS received_at - trade timestamp <= 3 seconds
p99 detail API response time <= 1 second
```

## Cost Policy

Do not make prod and stg identical by default.

Recommended cost policy:

- stg has expensive compute and storage.
- prod has small serving hardware.
- prod only stores compact publish snapshots and realtime cache.
- prod can be scaled only when user traffic requires it.

Upgrade prod when one of these is true:

- API p95 exceeds 1 second under normal traffic.
- official-trade-feed p99 latency exceeds 3 seconds without upstream delay.
- Redis/Postgres cache memory pressure is high.
- frontend/API traffic requires high availability.

## Failure Modes

### stg down

prod should continue serving the last successful publish snapshot.

Impact:

- historical and ranking data becomes stale.
- realtime trade feed remains available if prod feed is healthy.

### prod feed down

Impact:

- realtime detail activity and copy-trade freshness fail.
- cached historical/ranking APIs can still work.

Action:

- restart `zetta-official-trade-feed.service`.
- verify Polymarket upstream connection.
- verify cache write queue and drop count.

### publish fails

prod should keep previous active dataset.

Action:

- reject failed snapshot.
- alert on stale publish age.
- do not clear active prod tables.

## Current Recommendation

For the next production release:

```text
stg:
  keep current master + 3 helpers
  compute all heavy data

prod:
  start with one 8C/16G VM
  run frontend, API, official-trade-feed, Redis/Postgres cache
  import compact publish snapshots from stg
  do not run heavy ClickHouse jobs
```

This gives the lowest practical cost while preserving the critical requirement: realtime wallet trades must be visible to prod users within a few seconds.
