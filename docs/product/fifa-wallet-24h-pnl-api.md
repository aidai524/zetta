# FIFA Wallet 24h PnL API

This API exposes cached all-wallet FIFA / World Cup 24 hour PnL rankings.

## Refresh Model

The endpoint is read-only and does not recompute on request.

Refresh job:

```bash
zetta-runner wallet-fifa-24h-pnl
```

Manual CLI:

```bash
python -m zetta.cli build wallet-fifa-24h-pnl --window-hours 24
```

Production timer:

```bash
zetta-wallet-fifa-24h-pnl.timer
```

Default cadence is every 5 minutes. The result is stored in ClickHouse:

```sql
mart_wallet_fifa_24h_pnl
```

The refresh first maintains a scoped trade cache:

```sql
mart_fifa_trade
```

The first run backfills FIFA trades from `fact_trade_by_time`. Later runs refresh
only a rolling overlap window before rebuilding the wallet PnL mart from the
small scoped cache.

The helper machines should not run this mart. It should run on the master /
ClickHouse node.

## Calculation Scope

FIFA scope includes markets whose market slug or event slug starts with:

```text
fifwc-
```

This includes the base match event and related same-match markets such as totals,
spread, halftime, exact-score, and `more-markets` variants when their slugs are
under the same `fifwc-*` namespace.

## PnL Definition

The mart uses an equity-delta definition:

```text
fifa_total_pnl = fifa_equity_now
fifa_pnl_24h = fifa_equity_now - fifa_equity_24h_ago
fifa_pnl_7d = fifa_equity_now - fifa_equity_7d_ago
```

Token equity at a cutoff is:

```text
net_cashflow_up_to_cutoff + positive_position_size_up_to_cutoff * mark_price_at_cutoff
```

The current implementation uses deduplicated Data API trades and marks open
positions from:

- Gamma `outcomePrices`, including active markets when current prices are present
- latest orderbook mid, when available
- latest CLOB price history, as fallback
- latest FIFA trade price, as last fallback

Negative token balances are flagged as `needs_chain_balance` because Data API
trades alone can miss chain-level split/merge/redeem balance effects.

Win rate is token-position based:

```text
win_rate = profitable FIFA token positions / non-flat FIFA token positions
win_rate_24h = same calculation, restricted to tokens traded in the last 24h
win_rate_7d = same calculation, restricted to tokens traded in the last 7d
```

This avoids counting split fills as many separate wins or losses.

## Endpoint

```http
GET /api/wallets/fifa-24h-pnl
```

Production example:

```text
https://discovery.prophet.zone/api/wallets/fifa-24h-pnl?limit=50&sort=pnl_24h&direction=desc&min_notional_24h=100
```

Query one wallet exactly:

```text
https://discovery.prophet.zone/api/wallets/fifa-24h-pnl?user=0xb56db5215443706244b0af76b3daaad3066ad621&limit=1
```

The single-wallet response row exposes these PnL and win-rate fields:

- `total_pnl`: FIFA total/current PnL.
- `total_pnl_roi`: FIFA total/current ROI.
- `pnl_24h`: FIFA 24 hour PnL.
- `pnl_roi_24h`: FIFA 24 hour ROI.
- `traded_notional_24h`: FIFA 24 hour traded notional.
- `win_rate_24h`: FIFA 24 hour token-position win rate.
- `pnl_7d`: FIFA 7 day PnL.
- `pnl_roi_7d`: FIFA 7 day ROI.
- `win_rate_7d`: FIFA 7 day token-position win rate.
- `win_rate`: FIFA total token-position win rate.

## Query Parameters

| Name | Type | Default | Notes |
| --- | --- | --- | --- |
| `limit` | integer | `50` | Maximum `500`. |
| `offset` | integer | `0` | Pagination offset. |
| `user` | string | empty | Exact wallet address lookup. |
| `q` | string | empty | Case-insensitive wallet address search. |
| `min_notional_24h` | number | `0` | Minimum FIFA traded notional in the last 24h. |
| `min_trades_24h` | integer | `0` | Minimum FIFA trade count in the last 24h. |
| `active_24h` | boolean | `false` | When `true`, only return wallets with at least one FIFA trade in the last 24h. |
| `data_quality` | string | empty | Filter by `estimate`, `missing_mark_price`, or `needs_chain_balance`. |
| `sort` | string | `pnl_24h` | One of `pnl_24h`, `roi_24h`, `notional_24h`, `volume_24h`, `trades_24h`, `pnl_7d`, `roi_7d`, `win_rate`, `win_rate_24h`, `win_rate_7d`, `total_pnl`, `equity`, `last_trade`, `updated_at`. |
| `direction` | string | `desc` | `desc` or `asc`. |

For 24h sort fields (`pnl_24h`, `roi_24h`, `notional_24h`,
`volume_24h`, `trades_24h`, `win_rate_24h`), list responses prioritize wallets
with `trade_count_24h > 0` before inactive wallets. This prevents inactive zero
PnL rows from hiding active losing wallets when sorting by `pnl_24h desc`.

Only active 24h wallets:

```text
https://discovery.prophet.zone/api/wallets/fifa-24h-pnl?limit=50&sort=pnl_24h&direction=desc&active_24h=1
```

## Response Shape

```json
{
  "scope": "fifa",
  "window_hours": 24,
  "limit": 50,
  "offset": 0,
  "sort": "pnl_24h",
  "direction": "desc",
  "active_24h": false,
  "summary": {
    "total": 1234,
    "active_wallets_24h": 777,
    "nonzero_pnl_wallets_24h": 654,
    "profitable_wallets": 321,
    "losing_wallets": 456,
    "traded_notional_24h": 987654.32,
    "pnl_24h": 12345.67,
    "pnl_7d": 45678.9,
    "total_pnl": 99999.0,
    "avg_win_rate": 0.55,
    "avg_win_rate_24h": 0.5,
    "avg_win_rate_7d": 0.58,
    "updated_at": "2026-06-26 15:40:01.690"
  },
  "wallets": [
    {
      "user_address": "0xabc...",
      "trade_count": 120,
      "buy_count": 70,
      "sell_count": 50,
      "traded_size": 10000.0,
      "traded_notional": 7000.0,
      "buy_notional": 4000.0,
      "sell_notional": 3000.0,
      "trade_count_24h": 8,
      "buy_notional_24h": 1000.0,
      "sell_notional_24h": 1500.0,
      "traded_notional_24h": 2500.0,
      "net_notional_24h": 500.0,
      "event_count": 6,
      "market_count": 20,
      "event_count_24h": 2,
      "market_count_24h": 5,
      "token_count": 25,
      "open_position_count": 3,
      "open_position_value_now": 1800.0,
      "open_position_value_24h_ago": 900.0,
      "open_position_value_7d_ago": 500.0,
      "equity_now": 2800.0,
      "equity_24h_ago": 2100.0,
      "equity_7d_ago": 1200.0,
      "total_pnl": 2800.0,
      "total_pnl_roi": 0.7,
      "pnl_24h": 700.0,
      "pnl_base_24h": 1900.0,
      "pnl_roi_24h": 0.368421,
      "pnl_7d": 1600.0,
      "pnl_base_7d": 2500.0,
      "pnl_roi_7d": 0.64,
      "profitable_token_count": 10,
      "losing_token_count": 5,
      "win_rate": 0.666667,
      "profitable_token_count_24h": 2,
      "losing_token_count_24h": 2,
      "win_rate_24h": 0.5,
      "profitable_token_count_7d": 5,
      "losing_token_count_7d": 2,
      "win_rate_7d": 0.714286,
      "first_trade_at": "2026-06-12 01:00:00.000",
      "last_trade_at": "2026-06-26 15:20:00.000",
      "latest_action": "BUY",
      "missing_mark_count": 0,
      "negative_position_count": 0,
      "data_quality": "estimate",
      "updated_at": "2026-06-26 15:40:01.690",
      "is_whale": true,
      "is_smart": false,
      "all_site_total_pnl": 1234.0,
      "all_site_pnl_roi": 0.12,
      "portfolio_value": 5000.0,
      "max_single_trade_notional": 100000.0,
      "all_site_max_single_trade_notional": 150000.0
    }
  ]
}
```

In this endpoint, `total_pnl`, `pnl_24h`, `pnl_7d`,
`max_single_trade_notional`, and win-rate fields are FIFA scoped.
`all_site_total_pnl`, `all_site_pnl_roi`, `portfolio_value`, and
`all_site_max_single_trade_notional` come from the whole-site wallet screener and
are included only for comparison/display.

## Data Quality

`estimate` means all positive open positions had a mark price.

`missing_mark_price` means at least one positive open token position could not be
marked at `now` or `24h ago`.

`needs_chain_balance` means at least one token-level position is negative from
Data API trade reconstruction. That row is still useful for ranking and screening,
but exact PnL should be treated as lower confidence until chain balance movement
data is incorporated.

## Single Wallet Detail Scope

The regular wallet detail endpoint supports two data scopes:

```http
GET /api/wallets/detail?user={wallet}
GET /api/wallets/detail?user={wallet}&scope=fifa
```

Production examples:

```text
https://discovery.prophet.zone/api/wallets/detail?user=0xef21da2db1bed42fc7894fa543cb6e91ab38ac34&activity_limit=20&position_limit=10&pnl_points_limit=0
https://discovery.prophet.zone/api/wallets/detail?user=0xef21da2db1bed42fc7894fa543cb6e91ab38ac34&scope=fifa&activity_limit=20&position_limit=10&pnl_points_limit=0
```

`scope=all` is the default and keeps the existing whole-site wallet detail
behavior. It reads the full wallet portfolio, PnL snapshot, and wallet activity.

`scope=fifa` returns the same wallet detail response shape, but only includes
World Cup markets whose market or event slug starts with `fifwc-`. The response
has:

```json
{
  "scope": "fifa",
  "wallet": {
    "scope": "fifa",
    "data_source": "fifa"
  }
}
```

FIFA single-wallet detail reads:

- `mart_wallet_fifa_24h_pnl` for wallet-level FIFA summary and 24h PnL.
- `mart_fifa_trade` for FIFA-only positions and recent activity.
- `dim_market`, `dim_event`, and `dim_outcome_token` for market titles, slugs,
  event slugs, and outcomes.
- `fact_orderbook_snapshot` and `fact_price_history` for open-position marks.

`mart_fifa_trade` deduplicates Data API replay rows with a stable economic trade
fingerprint:

```text
timestamp + condition_id + token_id + user_address + side + rounded price + rounded size + rounded notional
```

This is intentional. Polymarket Data API replays can emit the same economic fill
with different `trade_id`, `transaction_hash`, or `log_index` values. Using
chain identifiers directly can overcount FIFA wallet positions and PnL.

## FIFA Smart And Whale Wallet Lists

The wallet summary and screener endpoints also support FIFA scoped wallet
segments:

```http
GET /api/wallets/summary?scope=fifa
GET /api/wallets/screener?scope=fifa&mode=smart
GET /api/wallets/screener?scope=fifa&mode=whale
```

Production examples:

```text
https://discovery.prophet.zone/api/wallets/summary?scope=fifa
https://discovery.prophet.zone/api/wallets/screener?scope=fifa&mode=smart&limit=50
https://discovery.prophet.zone/api/wallets/screener?scope=fifa&mode=whale&limit=50
```

Default thresholds match the all-site screener unless overridden:

| Segment | Default Definition |
| --- | --- |
| FIFA smart | `fifa_buy_notional >= 10000`, `fifa_data_quality = estimate`, and `fifa_pnl_roi >= 0.55` |
| FIFA whale | `fifa_traded_notional >= 1000000` or `fifa_max_single_trade_notional >= 100000` |

Threshold parameters:

| Name | Default |
| --- | --- |
| `min_smart_notional` | `10000` |
| `min_roi` | `0.55` |
| `whale_min_notional` | `1000000` |
| `whale_min_single_trade` | `100000` |

Important distinction:

- Without `scope=fifa`, `is_smart` and `is_whale` are whole-site wallet labels
  from `mart_wallet_screener`.
- With `scope=fifa`, `is_smart` and `is_whale` are recomputed from FIFA-only
  trades and FIFA-only PnL. A wallet can be a whole-site whale but not a FIFA
  whale, or the reverse.
- In `/api/wallets/screener?scope=fifa`, `fifa_total_pnl`, `fifa_pnl_24h`,
  `fifa_pnl_7d`, `fifa_win_rate`, and `fifa_traded_notional` are the FIFA-only
  fields. `total_pnl`, `all_site_total_pnl`, `portfolio_value`, and
  `available_balance` keep the whole-site snapshot meaning when such a snapshot
  exists. `total_pnl_scope` tells whether `total_pnl` came from
  `all_site_snapshot`, `all_site_screener`, or `fifa_fallback`.
- FIFA smart excludes `missing_mark_price` and `needs_chain_balance` rows by
  default because those rows can have incomplete mark or balance data. FIFA whale
  is still based on volume and keeps the data quality flag for display.
