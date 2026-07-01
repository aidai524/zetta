# FIFA Wallet Scope Implementation - 2026-06-27

This document summarizes the FIFA / World Cup wallet scope work completed on
2026-06-27.

## Goals

- Add a FIFA-only wallet detail scope without changing the default whole-site
  wallet detail behavior.
- Add FIFA-only smart and whale wallet lists.
- Add cached all-wallet FIFA 24h PnL rankings.
- Fix FIFA trade overcounting caused by repeated Polymarket Data API replay
  rows.
- Keep the heavy computation on the master / ClickHouse node and expose read
  APIs for the discovery frontend.

## Public API

### Single Wallet Detail

Whole-site wallet detail remains the default:

```text
GET /api/wallets/detail?user={wallet}
```

FIFA-only wallet detail:

```text
GET /api/wallets/detail?user={wallet}&scope=fifa
```

Example:

```text
https://discovery.prophet.zone/api/wallets/detail?user=0xef21da2db1bed42fc7894fa543cb6e91ab38ac34&scope=fifa&activity_limit=20&position_limit=10&pnl_points_limit=0
```

The FIFA response keeps the existing wallet detail shape and adds:

```json
{
  "scope": "fifa",
  "wallet": {
    "scope": "fifa",
    "data_source": "fifa"
  }
}
```

### FIFA 24h PnL Ranking

```text
GET /api/wallets/fifa-24h-pnl
```

Example:

```text
https://discovery.prophet.zone/api/wallets/fifa-24h-pnl?limit=50&sort=pnl_24h&direction=desc
```

### FIFA Smart / Whale Summary

```text
GET /api/wallets/summary?scope=fifa
```

Current production validation after deployment:

```text
total_wallets: 54509
wallets_over_10k: 669
smart_wallets: 5
whale_wallets: 48
```

### FIFA Smart / Whale Lists

```text
GET /api/wallets/screener?scope=fifa&mode=smart&limit=50
GET /api/wallets/screener?scope=fifa&mode=whale&limit=50
```

Default whole-site screener behavior is unchanged when `scope=fifa` is omitted.

For FIFA lists, `range=1d` / `range=24h` filters to wallets with
`trade_count_24h > 0` and orders active flow by `traded_notional_24h` first:

```text
GET /api/wallets/screener?scope=fifa&mode=watch&range=1d&limit=100
```

The FIFA mart currently stores 24h activity fields and 7d PnL / win-rate fields.
Precise `range=7d` and `range=30d` list activity filters require additional
persisted 7d / 30d traded-notional or trade-count fields.

## Scope Definition

FIFA scope includes markets whose market slug or event slug starts with:

```text
fifwc-
```

This includes base match events and related same-match markets such as spread,
totals, halftime, and `more-markets` variants when their slugs are under the
same `fifwc-*` namespace.

## Segment Definitions

Whole-site smart and whale labels still come from `mart_wallet_screener`.
The shared screener segment contract is documented in
`docs/product/wallet-screener-segments.md`.

FIFA scoped labels are recomputed from FIFA-only data:

```text
FIFA smart:
  fifa_buy_notional >= 10000
  and fifa_data_quality = estimate
  and fifa_pnl_roi >= 0.55

FIFA whale:
  fifa_traded_notional >= 1000000
  or fifa_max_single_trade_notional >= 100000

FIFA candidate smart:
  fifa_buy_notional >= 1000
  and fifa_data_quality = estimate
  and fifa_pnl_roi >= 0.10

FIFA watch:
  candidate smart, FIFA whale, fifa_traded_notional >= 10000,
  or fifa_traded_notional_24h >= 1000
```

FIFA smart intentionally excludes `missing_mark_price` and
`needs_chain_balance` rows because those rows can have incomplete mark or
balance data. FIFA whale remains volume-based and exposes `fifa_data_quality`
for display.

## Data Model

New ClickHouse marts:

```text
mart_fifa_trade
mart_wallet_fifa_24h_pnl
mart_wallet_fifa_24h_pnl_next
```

`mart_fifa_trade` is the scoped FIFA trade cache. It is built from
`fact_trade_by_time` and joined to market/event metadata.

`mart_wallet_fifa_24h_pnl` is the read model for all-wallet FIFA 24h PnL and is
also used by:

- `/api/wallets/fifa-24h-pnl`
- `/api/wallets/detail?scope=fifa`
- `/api/wallets/summary?scope=fifa`
- `/api/wallets/screener?scope=fifa`

## Trade Deduplication Fix

Polymarket Data API can replay the same economic fill with different
`trade_id`, `transaction_hash`, or `log_index` values. Using those identifiers
directly overcounted FIFA wallet positions and PnL.

The scoped FIFA cache now deduplicates by a stable economic fingerprint:

```text
timestamp
+ condition_id
+ token_id
+ user_address
+ side
+ rounded price
+ rounded size
+ rounded notional
```

After rebuilding, `mart_fifa_trade` dropped from roughly 3.28M rows to roughly
771K rows, which matches the expected replay deduplication effect.

Example validation for wallet `0xef21da2db1bed42fc7894fa543cb6e91ab38ac34`:

```text
Mexico win buy notional:
  before dedup fix: about 7800
  after dedup fix:  about 780
```

The corrected amount matches the wallet activity scale shown in the product UI.

## Refresh Model

Manual build:

```bash
python -m zetta.cli build wallet-fifa-24h-pnl --window-hours 24
python -m zetta.cli build fifa-trades --window-hours 72
```

Production runner:

```bash
zetta-runner wallet-fifa-24h-pnl
```

Production timer:

```text
zetta-wallet-fifa-24h-pnl.timer
```

Default cadence is every 5 minutes. The timer runs on the master / ClickHouse
node. Helper nodes should not run this mart.

## Runtime Deployment

Deployed files:

- `/opt/zetta/src`
- `/usr/local/bin/zetta-runner`
- `zetta-api.service` restarted
- `zetta-wallet-fifa-24h-pnl.timer` enabled and active

Production validation:

```text
https://discovery.prophet.zone/api/wallets/detail?user=0xef21da2db1bed42fc7894fa543cb6e91ab38ac34&scope=fifa

scope: fifa
data_source: fifa
latest_total_pnl: -2144.273494092
trade_activity_count: 17
positions_available: 11
```

```text
https://discovery.prophet.zone/api/wallets/screener?scope=fifa&mode=smart&limit=5

Returns 5 FIFA smart wallets using FIFA-only smart logic.
```

```text
https://discovery.prophet.zone/api/wallets/screener?scope=fifa&mode=whale&limit=3

Returns FIFA whale wallets using FIFA-only whale logic.
```

## Files Changed

Core API:

- `src/zetta/api.py`

Marts and CLI:

- `src/zetta/loaders/marts.py`
- `src/zetta/cli.py`
- `src/zetta/scheduler/runner.py`
- `src/zetta/scheduler/tasks.py`

Schema and runtime:

- `infra/clickhouse/schema.sql`
- `infra/scripts/zetta-runner`
- `infra/systemd/zetta-wallet-fifa-24h-pnl.service`
- `infra/systemd/zetta-wallet-fifa-24h-pnl.timer`
- `infra/systemd/zetta.env.example`
- `infra/scripts/bootstrap_ubuntu.sh`
- `scripts/configure_wallet_helper.sh`

Tests and docs:

- `tests/test_api.py`
- `tests/test_marts.py`
- `tests/test_tasks.py`
- `docs/product/fifa-wallet-24h-pnl-api.md`
- `docs/product/fifa-wallet-scope-implementation-20260627.md`

## Verification

Completed:

```bash
python3 -m compileall -q src/zetta tests/test_api.py tests/test_marts.py
git diff --check
```

Production API checks were run against `https://discovery.prophet.zone`.

Not completed:

```text
pytest
```

Reason: the current runtime environment does not have the `pytest` module
installed in `/opt/zetta/.venv`.

## Known Limitations

- FIFA PnL is still a Data API based estimate and is only as fresh as upstream
  FIFA trade collection.
- `needs_chain_balance` rows indicate cases where Data API reconstruction may
  miss chain-level split/merge/redeem effects.
- `missing_mark_price` rows indicate at least one positive open token position
  could not be marked.
- The FIFA smart/whale screener currently computes from existing marts at query
  time. If traffic grows, this can be promoted to a dedicated cached mart.
