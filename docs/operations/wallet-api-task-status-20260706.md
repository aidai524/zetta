# Wallet API Task Status - 2026-07-06

本文档记录 2026-07-06 为保障三个核心钱包接口稳定性而保留和暂停的 systemd 任务状态。

## Protected APIs

当前优先保障的三个接口：

```text
GET /api/wallets/polycop-fifa-signals?limit=100&min_fifa_notional=1000&data_quality=estimate
GET /api/wallets/screener?scope=fifa&mode=whale&limit=100
GET /api/wallets/detail?user=%s&live=1&position_limit=10&activity_limit=100&pnl_points_limit=0&realtime=1
```

目标：

- 列表类接口优先命中缓存，避免被 ClickHouse 重任务拖慢。
- 单钱包详情接口优先保证 realtime activity 几秒级可见。
- 后台任务优先服务 FIFA 钱包 PnL、Polycop signals、official trade feed 和 wallet metadata。

## Kept Running

这些任务当前保留，属于核心链路或必要辅助链路。

| Unit | Current State | Reason |
| --- | --- | --- |
| `zetta-api.service` | enabled / active | 三个核心接口的 API 服务。 |
| `zetta-official-trade-feed.service` | enabled / active | 官方 trade feed 和 realtime wallet activity 的核心来源。 |
| `zetta-load-trades-realtime.timer` | enabled / active | 将 realtime/raw trade 数据持续入库，支撑后续历史查询和 marts。 |
| `zetta-wallet-fifa-24h-pnl.timer` | enabled / active | 构建 FIFA scope 钱包 PnL、whale/smart wallet 指标。 |
| `zetta-polycop-wallet-signals.timer` | enabled / active | 定时刷新 Polycop 聪明钱包信号缓存。 |
| `zetta-live-token-metadata.timer` | enabled / active | 补齐 token、market、outcome metadata，影响 activity 展示完整度。 |
| `zetta-active-event-wallets.timer` | enabled / active | 发现活跃 event 和相关钱包任务；当前保留，但后续应限制非 FIFA market trade 压力。 |
| `zetta-chain-frontier.timer` | enabled / active | 补链上成交和 maker/merge/redeem 相关数据；当前保留，但建议降频或单独治理 timeout。 |
| `zetta-frontier.timer` | enabled / active | 广域 event/market 发现；保留为二级任务，避免压住核心任务。 |
| `zetta-prune-raw.timer` | enabled / active | 原始数据清理维护任务，非业务计算重任务。 |

当前 `systemctl list-timers 'zetta-*' --all` 中仍看到的 zetta timers：

```text
zetta-polycop-wallet-signals.timer
zetta-load-trades-realtime.timer
zetta-active-event-wallets.timer
zetta-frontier.timer
zetta-prune-raw.timer
zetta-chain-frontier.timer
zetta-live-token-metadata.timer
zetta-wallet-fifa-24h-pnl.timer
```

## Paused

这些任务已经暂停，保留 unit、代码和表结构，不删除，便于需要时恢复。

| Unit | Current State | Why Paused |
| --- | --- | --- |
| `zetta-unusual-betting.timer` | disabled / inactive | 比赛大额/异常下注分析，不是三个核心钱包接口的必要链路，会共享 ClickHouse/FIFA 资源。 |
| `zetta-unusual-betting-worker.service` | disabled / inactive | unusual betting 的后台 worker，暂停后不再消费该类任务。 |
| `zetta-unusual-betting.service` | static / inactive | timer 触发的一次性 seed service；timer 已停，所以不会自动触发。 |
| `zetta-marts.timer` | disabled / inactive | 全站通用 mart 构建任务，之前出现过 ClickHouse memory limit，当前三个核心接口不依赖它。 |
| `zetta-marts.service` | static / inactive | mart builder service；旧 failed 标记已清理。 |
| `zetta-wallet-rollup.timer` | disabled / inactive | 旧钱包 rollup 刷新任务；当前 FIFA 钱包列表由 `mart_wallet_fifa_24h_pnl` 支撑。 |
| `zetta-wallet-rollup.service` | static / inactive | wallet rollup service；旧 failed 标记已清理。 |

本次执行的暂停动作：

```bash
systemctl disable --now zetta-unusual-betting.timer zetta-marts.timer zetta-wallet-rollup.timer
systemctl stop zetta-unusual-betting-worker.service
systemctl disable zetta-unusual-betting-worker.service
systemctl reset-failed zetta-marts.service zetta-wallet-rollup.service
```

## Relationship To The Three APIs

### Directly related and kept

- `zetta-api.service`
- `zetta-official-trade-feed.service`
- `zetta-load-trades-realtime.timer`
- `zetta-wallet-fifa-24h-pnl.timer`
- `zetta-polycop-wallet-signals.timer`
- `zetta-live-token-metadata.timer`

这些任务直接影响三个接口的速度、实时 activity、FIFA PnL、钱包列表和字段完整性。

### Indirectly related and kept with caution

- `zetta-active-event-wallets.timer`
- `zetta-chain-frontier.timer`
- `zetta-frontier.timer`

这些任务能补充发现、链上完整性和市场元数据，但也会消耗 ClickHouse 或队列资源。后续如果核心接口延迟升高，优先考虑降低这几项频率或限制非 FIFA 数据。

### Not required by the three APIs and paused

- `zetta-unusual-betting.timer`
- `zetta-unusual-betting-worker.service`
- `zetta-marts.timer`
- `zetta-wallet-rollup.timer`

暂停这些任务不会影响当前三个核心钱包接口的主链路。影响范围主要是：

- 比赛大额/异常下注分析缓存停止刷新。
- 全站通用 mart 不再构建。
- 旧钱包 rollup 不再刷新。

## Restore Commands

如果需要恢复 unusual betting：

```bash
systemctl enable --now zetta-unusual-betting.timer
systemctl enable --now zetta-unusual-betting-worker.service
```

如果需要恢复全站 mart：

```bash
systemctl enable --now zetta-marts.timer
```

如果需要恢复旧钱包 rollup：

```bash
systemctl enable --now zetta-wallet-rollup.timer
```

恢复前建议先确认 ClickHouse 压力和三个核心接口耗时，避免恢复后抢占资源。

## Verification Commands

检查保留和暂停状态：

```bash
systemctl list-timers 'zetta-*' --all --no-pager

systemctl is-enabled \
  zetta-api.service \
  zetta-official-trade-feed.service \
  zetta-load-trades-realtime.timer \
  zetta-wallet-fifa-24h-pnl.timer \
  zetta-polycop-wallet-signals.timer \
  zetta-live-token-metadata.timer \
  zetta-active-event-wallets.timer \
  zetta-chain-frontier.timer \
  zetta-frontier.timer \
  zetta-unusual-betting.timer \
  zetta-unusual-betting-worker.service \
  zetta-marts.timer \
  zetta-wallet-rollup.timer

systemctl is-active \
  zetta-api.service \
  zetta-official-trade-feed.service \
  zetta-load-trades-realtime.timer \
  zetta-wallet-fifa-24h-pnl.timer \
  zetta-polycop-wallet-signals.timer \
  zetta-live-token-metadata.timer \
  zetta-active-event-wallets.timer \
  zetta-chain-frontier.timer \
  zetta-frontier.timer \
  zetta-unusual-betting.timer \
  zetta-unusual-betting-worker.service \
  zetta-marts.timer \
  zetta-wallet-rollup.timer
```

核心接口探活：

```bash
curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/polycop-fifa-signals?limit=100&min_fifa_notional=1000&data_quality=estimate'

curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/screener?scope=fifa&mode=whale&limit=100'

curl -w '\n%{http_code} %{time_total}s\n' -o /dev/null \
  'https://discovery.prophet.zone/api/wallets/detail?user=0x1fd80277d4cc327a2a1440d144e13d71774e7749&live=1&position_limit=10&activity_limit=100&pnl_points_limit=0&realtime=1'
```
