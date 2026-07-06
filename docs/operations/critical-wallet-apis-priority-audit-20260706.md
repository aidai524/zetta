# Critical Wallet APIs Priority Audit - 2026-07-06

本文档整理 2026-07-06 对 Discovery 三个核心钱包接口的审计结论，目标是先保证这三条链路稳定、快速、数据尽量实时，再决定其他任务降级或保留。

## Scope

本次优先保障的接口：

```text
GET /api/wallets/polycop-fifa-signals?limit=100&min_fifa_notional=1000&data_quality=estimate
GET /api/wallets/screener?scope=fifa&mode=whale&limit=100
GET /api/wallets/detail?user=%s&live=1&position_limit=10&activity_limit=100&pnl_points_limit=0&realtime=1
```

对应业务：

- Polycop 聪明钱包信号列表，并叠加 FIFA 相关统计。
- FIFA scope whale / smart wallet screener。
- 单个钱包详情页和跟单系统依赖的 realtime wallet activity。

## Executive Summary

- `wallets/detail` 在健康状态下可以做到几十毫秒级返回；它的实时 activity 主要依赖 `zetta-official-trade-feed.service` 的进程状态缓存，而不是每次直接扫 ClickHouse。
- `polycop-fifa-signals` 和 `screener?scope=fifa` 在 ClickHouse 压力高时会显著变慢，甚至超时。已经给这两个列表类接口加了进程内 stale cache 保护，API 重启后需要预热。
- 三个接口最重要的后台链路是 official trade feed、realtime trade loader、FIFA wallet PnL mart、Polycop signals job、token metadata job，以及 helper 节点的钱包任务 worker。
- 当前最需要降级或治理的是广域、非核心、容易压 ClickHouse 的任务，尤其是高频 `chain-frontier` 和大量非 FIFA `trades` 队列。
- `mart_wallet_screener` 当前偏旧，只应作为补充数据，FIFA 主统计不能依赖它作为核心真实来源。

## Current Health

本次检查中观察到的接口耗时：

| Endpoint | 状态 | 观察耗时 | 说明 |
| --- | --- | ---: | --- |
| `polycop-fifa-signals` | 健康时 | ~0.11s | 直接命中缓存表时很快。 |
| `screener?scope=fifa&mode=whale` | 冷查询 | ~15.0s | 首次查询偏慢，主要受 ClickHouse 查询和缓存未命中影响。 |
| `screener?scope=fifa&mode=whale` | 热查询 | ~1.0s | ClickHouse 状态正常时可接受，但仍比列表缓存慢。 |
| `wallets/detail?...realtime=1` | 健康时 | ~0.07s - 0.08s | 单钱包详情通常很快。 |
| `polycop-fifa-signals` | ClickHouse 压力高时 | ~19.8s | 说明列表接口受后台 CH 压力影响明显。 |
| `screener?scope=fifa&mode=whale` | ClickHouse 压力高时 | timeout / 500, ~32s | 需要缓存保护和后台任务错峰。 |
| `wallets/detail?...realtime=1` | ClickHouse 压力高时 | ~2.1s | 仍可返回，但有抖动。 |

已做的保护：

- 给 `polycop-fifa-signals` 增加关键接口缓存。
- 给 `screener` 在 `scope=fifa` 时增加关键接口缓存。
- API 进程内有可用旧结果时，ClickHouse 短时间异常不会立刻把用户请求打成 500。

预热后的结果：

| Endpoint | 预热后耗时 |
| --- | ---: |
| `polycop-fifa-signals` | ~0.04s |
| `screener?scope=fifa&mode=whale` | ~0.07s |
| `wallets/detail?...realtime=1` | ~0.08s |

注意：当前缓存是 API 进程内缓存。`zetta-api.service` 重启后，第一次请求仍可能走冷查询，所以建议加一个轻量 prewarm timer。

## Dependency Map

### `polycop-fifa-signals`

主要依赖：

- Postgres: `polycop_wallet_signal_cache`
- ClickHouse: `mart_wallet_fifa_24h_pnl`
- Timer: `zetta-polycop-wallet-signals.timer`
- Service: `zetta-polycop-wallet-signals.service`

观察到的缓存状态：

| Field | Value |
| --- | --- |
| `cache_key` | `latest` |
| `status` | `ok` |
| `source` | `polycop` |
| `wallet_count` | 427 |
| `stable_count` | 16 |
| `flow_count` | 18 |
| `burst_count` | 70 |
| `refreshed_at` | `2026-07-06 12:30:20 UTC` |

结论：

- 这个接口应该继续以定时缓存为主，不要在用户请求时现场抓 Polycop。
- 当前每 10 分钟刷新一次合理。
- 该接口叠加 FIFA 指标时会读 `mart_wallet_fifa_24h_pnl`，因此会间接受 FIFA mart 新鲜度影响。

### `screener?scope=fifa&mode=whale`

主要依赖：

- ClickHouse: `mart_wallet_fifa_24h_pnl`
- ClickHouse: `mart_wallet_screener`
- ClickHouse: `fact_wallet_pnl_snapshot`
- ClickHouse: `fact_wallet_portfolio_snapshot`
- ClickHouse upstream: `mart_fifa_trade`
- ClickHouse upstream: `mart_fifa_trade_by_user`
- ClickHouse upstream: `mart_fifa_chain_trade`
- Dimensions: `dim_market`, `dim_event`, `dim_outcome_token`
- Timer: `zetta-wallet-fifa-24h-pnl.timer`
- Service: `zetta-wallet-fifa-24h-pnl.service`

观察到的数据规模和新鲜度：

| Table | Rows | Freshness |
| --- | ---: | --- |
| `mart_wallet_fifa_24h_pnl` | ~246k | `updated_at` 到 `2026-07-06 12:09:41 UTC` |
| `mart_fifa_trade` | ~24.88M | `updated_at` 到 `2026-07-06 12:31:53 UTC` |
| `mart_fifa_trade_by_user` | ~24.88M | `updated_at` 到 `2026-07-06 12:31:53 UTC` |
| `mart_fifa_chain_trade` | ~23.43M | 文件元数据到 `2026-07-06 09:01:17 UTC` |
| `mart_wallet_screener` | ~2.20M | `updated_at` 最大仍是 `2026-06-15 19:18:20 UTC` |

结论：

- FIFA screener 的主数据应该以 `mart_wallet_fifa_24h_pnl` 为准。
- `mart_wallet_screener` 当前偏旧，只能作为名称、历史补充或兜底，不能作为 FIFA ROI / PnL 的主判断依据。
- `zetta-wallet-fifa-24h-pnl.service` 一次构建耗时约 14 分钟，虽然 timer 配置是 5 分钟 inactive cadence，但实际刷新周期会受构建时长影响。

### `wallets/detail?...live=1&realtime=1`

主要依赖：

- Polymarket live wallet API:
  - positions
  - wallet value
  - user pnl
  - PUSD balance
- Realtime state cache from `zetta-official-trade-feed.service`
- ClickHouse FIFA position override: `mart_fifa_trade_by_user`
- Metadata enrichment:
  - `dim_market`
  - `dim_outcome_token`
- API service: `zetta-api.service`

结论：

- 详情接口里的 recent activity 不应该依赖慢速全表查询。
- `realtime=1` 的关键价值是优先从 official trade feed 的状态缓存拿近期成交。
- 跟单系统依赖这个接口时，真正的核心指标是 trade 从 Polymarket WS 到 API 可见的延迟，目标应保持在几秒内。

## Timers And Services

### Core, Keep High Priority

| Unit | Priority | Reason |
| --- | --- | --- |
| `zetta-api.service` | critical | 三个接口都依赖它。 |
| `zetta-official-trade-feed.service` | critical | 钱包 realtime activity 和 official trade feed 的核心入口。 |
| `zetta-load-trades-realtime.timer` | high | 将 RTDS / realtime raw 数据入 ClickHouse，影响后续 mart 和历史查询。 |
| `zetta-wallet-fifa-24h-pnl.timer` | high | 构建 FIFA 钱包 PnL、whale/smart wallet FIFA 指标。 |
| `zetta-polycop-wallet-signals.timer` | high | 构建 Polycop 信号缓存。 |
| `zetta-live-token-metadata.timer` | high | 补齐 token / market metadata，影响详情和 activity 展示字段完整性。 |
| Helper `zetta-worker.service` | high | 三台 helper 处理 wallet activity / pnl / portfolio / trades。 |

### Related But Should Be Controlled

| Unit | Current Issue | Recommendation |
| --- | --- | --- |
| `zetta-chain-frontier.timer` | 20s 级别高频，曾出现 ClickHouse timeout。 | 降到 3-5 分钟，或先暂停直到 timeout 治理完成。 |
| `zetta-active-event-wallets.timer` | 会产生大量非 FIFA `trades` 队列。 | 保留钱包刷新能力，但限制非 FIFA market trades。 |
| `zetta-frontier.timer` | 广域发现任务，不是三条核心接口必要链路。 | 保留为二级任务，避免和 FIFA mart 重叠。 |
| `zetta-unusual-betting.timer` | 与三条接口非强相关。 | 可保留，但降低优先级。 |
| `zetta-unusual-betting-worker.service` | 单独查询 ClickHouse，可能竞争资源。 | 可保留，但不要抢占核心刷新窗口。 |

### Keep Disabled Until Needed

| Unit | Reason |
| --- | --- |
| `zetta-marts.timer` | 之前因大内存查询失败，不是当前三条接口的必要链路。 |
| `zetta-wallet-rollup.timer` | 之前失败，且当前 FIFA scope 已由专用 mart 支撑。 |

## Task Queue Findings

Postgres task queue 里观察到的现象：

- 曾有 `pending` 约 1366，`running` 约 2。
- `trades` pending 约 946，其中 FIFA 约 79，非 FIFA 约 867。
- 钱包类任务包括 `wallet-activity`、`wallet-pnl`、`wallet-portfolio`、`wallet-trades`，这些主要由 helper 节点处理。
- 最近 30 分钟内看到 15 个 worker 节点/进程，包括三台 helper 和 master worker。

结论：

- 当前 master 队列里有不少非 FIFA market trades，它们会占用采集和 ClickHouse 资源，但对三条核心接口不是直接必要。
- Helper 节点对钱包类任务有效，应该保留。
- 对于核心接口稳定性，应该让 FIFA wallet mart、Polycop signals、official trade feed 优先于广域非 FIFA 队列。

## Recommended Priority Policy

建议把任务分三层。

### P0: 必须保持健康

- `zetta-api.service`
- `zetta-official-trade-feed.service`
- `zetta-load-trades-realtime.timer`
- `zetta-wallet-fifa-24h-pnl.timer`
- `zetta-polycop-wallet-signals.timer`
- `zetta-live-token-metadata.timer`
- 三台 helper 的 wallet worker

目标：

- API 详情接口常态低于 1 秒。
- 列表类接口命中缓存后低于 200ms。
- official trade feed 到 API 可见延迟保持几秒级。

### P1: 保留，但不能压住 P0

- `zetta-active-event-wallets.timer`
- `zetta-chain-frontier.timer`
- `zetta-frontier.timer`
- `zetta-unusual-betting.timer`
- `zetta-unusual-betting-worker.service`

调整建议：

- `chain-frontier` 从 20s 降到 3-5min，直到 ClickHouse timeout 消除。
- `active-event-wallets` 保留钱包维度刷新，减少或关闭非 FIFA market trades 入队。
- 避免 `wallet-fifa-24h-pnl`、`load-trades-realtime`、`chain-frontier` 同时跑重查询。

### P2: 暂不恢复

- `zetta-marts.timer`
- `zetta-wallet-rollup.timer`

恢复条件：

- 明确有页面或接口依赖。
- 查询内存和耗时已被限制。
- 不影响 P0 接口。

## Recommended Next Changes

### 1. Add API prewarm timer

原因：两个列表类接口已有进程内缓存，但 API 重启后缓存为空。

建议每 1 分钟或 API restart 后预热：

```text
https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?limit=100&min_fifa_notional=1000&data_quality=estimate
https://discovery.prophet.zone/api/wallets/screener?scope=fifa&mode=whale&limit=100
```

### 2. Stagger heavy jobs

避免这些任务重叠：

- `zetta-wallet-fifa-24h-pnl.service`
- `zetta-load-trades-realtime.service`
- `zetta-chain-frontier.service`
- `zetta-unusual-betting-worker.service`

### 3. Lower chain frontier pressure

当前 `chain-frontier` 高频失败时会持续制造 ClickHouse 压力。建议先降频，再单独查 timeout 根因。

### 4. Reduce non-FIFA market trade backlog

当前 pending `trades` 里非 FIFA 占多数。建议在当前阶段优先处理 FIFA 相关 market trades 和钱包相关任务。

### 5. Persist critical list cache if needed

当前关键接口缓存是 API 进程内缓存。如果需要更强的重启后保护，可以把最终 response 同步落到 Postgres 或本地 state 文件，并在 API 冷启动时加载。

## Open Risks

- `zetta-official-trade-feed.service` 如果出现慢客户端阻塞或进程内存异常，仍可能造成 WS 读取积压，进而让详情接口返回的 recent activity 延迟超过几秒。
- `zetta-load-trades-realtime.service` 曾出现 ClickHouse insert timeout，需要控制 batch、超时、重试和并发。
- `zetta-wallet-fifa-24h-pnl.service` 构建耗时约 14 分钟，刷新频率实际不可能达到 5 分钟一次完整刷新。
- `mart_wallet_screener` 当前偏旧，不能用于判断 FIFA 的实时 ROI / PnL。
- 进程内缓存只能保护 API 进程存活期间，不能跨 API 重启。

## Operational Checklist

日常检查可以优先看这些：

```bash
systemctl status zetta-api.service zetta-official-trade-feed.service --no-pager
systemctl list-timers 'zetta-*' --all --no-pager
journalctl -u zetta-official-trade-feed.service -n 100 --no-pager
journalctl -u zetta-wallet-fifa-24h-pnl.service -n 100 --no-pager
journalctl -u zetta-polycop-wallet-signals.service -n 100 --no-pager
```

关键接口探活：

```bash
curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?limit=100&min_fifa_notional=1000&data_quality=estimate'

curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/screener?scope=fifa&mode=whale&limit=100'

curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/detail?user=0x1fd80277d4cc327a2a1440d144e13d71774e7749&live=1&position_limit=10&activity_limit=100&pnl_points_limit=0&realtime=1'
```

判断是否达标：

- 第一个和第二个列表接口命中缓存后应在 200ms 左右或以内。
- 单钱包详情接口常态应低于 1s。
- 如果列表接口又出现 10s+ 或 500，优先检查 ClickHouse 重任务是否重叠，以及 API 关键缓存是否刚重启未预热。
