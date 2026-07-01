# Wallet Screener Segments

This document describes the wallet list segmentation used by Discovery.

## Important Distinction

Polymarket is the raw data source for trades, positions, value, activity, and
user PnL. Polymarket does not provide a ready-made official `smart wallet` or
`whale wallet` leaderboard in the current Zetta integration.

Zetta computes wallet segments from collected Polymarket data:

- Volume comes from wallet trade notional.
- Whole-site ROI comes from wallet total PnL divided by traded notional.
- FIFA ROI comes from FIFA scoped equity divided by FIFA buy notional.
- Segment labels are product signals, not official Polymarket labels.

## API

```text
GET /api/wallets/screener
GET /api/wallets/screener?scope=fifa
GET /api/wallets/summary
GET /api/wallets/summary?scope=fifa
```

## Modes

| mode | Meaning | Whole-site default rule | FIFA default rule |
|---|---|---|---|
| `smart` | Backward-compatible strict smart mode | Same as `strict_smart` | Same as `strict_smart` |
| `strict_smart` | High-confidence smart wallet | `traded_notional >= 10000` and `pnl_roi >= 0.55` | `buy_notional >= 10000`, `data_quality = estimate`, and `pnl_roi >= 0.55` |
| `candidate_smart` | Wider positive ROI candidate list | `traded_notional >= 5000`, PnL captured, and `pnl_roi >= 0.10` | `buy_notional >= 1000`, `data_quality = estimate`, and `pnl_roi >= 0.10` |
| `whale` | Large capital wallet | `traded_notional >= 1000000` or `max_single_trade_notional >= 100000` | `traded_notional >= 1000000` or `max_single_trade_notional >= 100000` |
| `watch` | Broad watchlist | candidate, whale, `traded_notional >= 100000`, or 24h notional >= `5000` | candidate, whale, `traded_notional >= 10000`, or 24h notional >= `1000` |
| `active` | Recently active wallets | Sort by latest trade | Sort by latest FIFA trade |

## Query Parameters

| parameter | default | applies to | description |
|---|---:|---|---|
| `mode` | `active` | all | One of `active`, `smart`, `strict_smart`, `candidate_smart`, `whale`, `watch`. |
| `limit` | `50` | all | Max rows, capped at 500. |
| `scope` | `all` | all | Use `fifa` for World Cup scoped calculations. |
| `range` | `all` | whole-site | Optional trade scope filter: `1d`, `7d`, `30d`, `All`. |
| `category` | empty | whole-site | Optional category filter, for example `Sports` or `体育`. |
| `min_smart_notional` | `10000` | strict | Strict smart notional floor. |
| `min_roi` | `0.55` | strict | Strict smart ROI floor. |
| `candidate_min_notional` | `5000` whole-site, `1000` FIFA | candidate | Candidate notional floor. |
| `candidate_min_roi` | `0.10` | candidate | Candidate ROI floor. |
| `whale_min_notional` | `1000000` | whale | Total volume whale floor. |
| `whale_min_single_trade` | `100000` | whale | Single trade whale floor. |
| `watch_min_notional` | `100000` whole-site, `10000` FIFA | watch | Broad watchlist total notional floor. |
| `watch_min_notional_24h` | `5000` whole-site, `1000` FIFA | watch | Broad watchlist recent flow floor. |

## Response Fields

Rows from `/wallets/screener` include the existing wallet fields plus:

```json
{
  "is_smart": true,
  "is_candidate_smart": true,
  "is_whale": false,
  "wallet_segment": "strict_smart",
  "candidate_reason": "strict_smart_roi"
}
```

Possible `wallet_segment` values:

- `strict_smart`
- `candidate_smart`
- `whale`
- `recent_flow`
- `active`

Summary responses now include:

```json
{
  "smart_wallets": 3002,
  "candidate_smart_wallets": 12000,
  "whale_wallets": 2282,
  "watch_wallets": 18000
}
```

## Recommended UI Usage

Discovery Smart Money defaults to:

```text
/api/wallets/screener?mode=candidate_smart&limit=100
```

Users can switch to:

- `strict_smart` for the small high-confidence list.
- `whale` for large wallets.
- `watch` for a broader monitoring pool.
- `active` for latest active wallets.

FIFA pages should use the same modes with `scope=fifa`.
