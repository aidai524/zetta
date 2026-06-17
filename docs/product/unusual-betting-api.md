# Large Direction Signal API

This document describes the API used by browser extensions or other clients to
display large direction signals for Polymarket World Cup events.

## Base URL

Production:

```text
https://discovery.prophet.zone/api
```

Local service:

```text
http://127.0.0.1:8088
```

## Signal Definition

The current production rule is intentionally wallet-aggregate based:

- A wallet is counted as a signal wallet only when its total matched notional on
  signal directions for the match is at least `large_threshold`.
- The default `large_threshold` is `500000` USDC.
- Single fill size is not used to decide whether a wallet is a signal wallet.
- Single fill rows are still returned by the detail API as evidence, because
  they help explain order-book matching, but they are not the primary signal.
- Related World Cup event variants are included by default:
  - base match event, for example `fifwc-esp-cvi-2026-06-15`
  - `-more-markets`
  - `-exact-score`
  - `-halftime-result`

Signal directions currently include:

- low-price buys, default user-side price `<= 25c`
- low-price `No` buys
- selling high-probability outcomes, interpreted from the user's side
- spread-market side buys
- other large aggregate side directions

Polymarket fills are expanded to the user's side:

- `BUY Yes` means long Yes.
- `SELL No` is economically equivalent to long Yes.
- `SELL Yes` is economically equivalent to long No.

For `SELL` rows, the API's signal logic converts token price into user-side
price using `1 - token_price`.

## Endpoints

### Summary

Use this endpoint for extension badges, compact panels, watchlists, and match
overview cards.

```http
GET /events/unusual-betting/summary?slug={event_slug}
```

Example:

```text
https://discovery.prophet.zone/api/events/unusual-betting/summary?slug=fifwc-esp-cvi-2026-06-15
```

Recommended query parameters:

| Parameter | Default | Description |
|---|---:|---|
| `slug` | required | Base event slug or any related variant slug. |
| `include_related_markets` | `true` | Include `-more-markets`, `-exact-score`, and `-halftime-result`. |
| `large_threshold` | `500000` | Wallet aggregate threshold for counting signal wallets. |
| `very_large_threshold` | `1000000` | Wallet aggregate threshold for stronger severity. |
| `extreme_threshold` | `5000000` | Wallet aggregate threshold for extreme severity. |
| `cold_price_threshold` | `0.25` | Low user-side price threshold. |
| `cache_ttl_seconds` | `60` | Server-side cache TTL. Use `0` only when debugging. |

Response fields:

| Field | Type | Description |
|---|---|---|
| `status` | string | `ok`, `missing_event`, or `event_not_found`. |
| `event` | object | Matched base event metadata. |
| `slug` | string | Matched base event slug. |
| `severity` | string | `none`, `low`, `medium`, `high`, or `critical`. |
| `conclusion` | string | Human-readable summary. |
| `abnormal_wallet_count` | number | Count of wallets with signal-direction aggregate >= `large_threshold`. |
| `large_signal_wallet_count` | number | Same threshold bucket as `abnormal_wallet_count`. |
| `very_large_signal_wallet_count` | number | Wallet count >= `very_large_threshold`. |
| `extreme_signal_wallet_count` | number | Wallet count >= `extreme_threshold`. |
| `max_abnormal_wallet_notional` | number | Largest signal-direction wallet aggregate among threshold-qualified wallets. |
| `signal_total_notional` | number | Total matched notional on all signal directions. |
| `signal_wallet_count` | number | Total wallets found on signal directions, including below threshold. |
| `thresholds` | object | Thresholds used by this response. |
| `abnormal_wallets` | array | Top threshold-qualified wallets returned for display. |
| `watch_wallets` | array | Top signal wallets whether or not they cross the threshold. |
| `signal_outcomes` | array | Top signal directions. |
| `detail_url` | string | Relative API URL for the full detail endpoint. |
| `chart_url` | string | Relative UI route for the chart page. |

Example summary response excerpt:

```json
{
  "status": "ok",
  "slug": "fifwc-esp-cvi-2026-06-15",
  "severity": "critical",
  "abnormal_wallet_count": 19,
  "max_abnormal_wallet_notional": 10760881.63,
  "conclusion": "Spain vs. Cabo Verde 发现 19 个异常钱包在异常方向下注，信号级别为百万美金级；最大钱包累计约 $10,760,882。",
  "thresholds": {
    "cold_price_threshold": 0.25,
    "large_threshold": 500000.0,
    "very_large_threshold": 1000000.0,
    "extreme_threshold": 5000000.0
  }
}
```

### Full Detail

Use this endpoint for drill-down views and chart pages.

```http
GET /events/unusual-betting?slug={event_slug}&wallet_limit=100&trade_limit=100
```

Example:

```text
https://discovery.prophet.zone/api/events/unusual-betting?slug=fifwc-esp-cvi-2026-06-15&wallet_limit=100&trade_limit=100
```

Additional response fields:

| Field | Type | Description |
|---|---|---|
| `event_scope` | array | Events included in this analysis scope. |
| `parameters.event_slugs` | array | Slugs included in scope. |
| `markets` | array | Markets included in scope. |
| `tokens` | array | Outcome tokens included in scope. |
| `fill_summary` | object | Raw fill count and notional summary. |
| `outcome_summary` | array | All user-side outcome rows, including non-signal rows. |
| `signal_outcomes` | array | Signal direction rows. |
| `signal_wallet_summary` | object | Wallet counts and aggregate maxima independent of display limits. |
| `signal_wallets` | array | Top wallet/outcome rows, limited by `wallet_limit`. |
| `signal_trades` | array | Top fill rows, limited by `trade_limit`; evidence only, not the primary signal. |
| `analysis` | object | Severity, thresholds, conclusion, and top evidence subsets. |

`signal_wallets` rows are not globally unique by wallet because they show the
wallet's signal rows by market/outcome/side. Use the summary endpoint's
`abnormal_wallets` if a wallet-deduplicated list is needed.

`abnormal_wallets` entries in summary are grouped by wallet and include:

| Field | Type | Description |
|---|---|---|
| `user_address` | string | Proxy wallet address. |
| `total_notional` | number | Aggregate signal-direction matched notional for this wallet. |
| `max_notional` | number | Largest fill for reference only. |
| `fills` | number | Number of fill rows. |
| `first_ts` | string | First fill timestamp in UTC. |
| `last_ts` | string | Last fill timestamp in UTC. |
| `selections` | array | Top market/outcome/side selections for this wallet. |

## Chrome Extension Usage

Required host permission:

```json
{
  "host_permissions": [
    "https://discovery.prophet.zone/*",
    "https://polymarket.com/*"
  ]
}
```

Basic fetch example:

```js
async function loadLargeDirectionSignal(slug) {
  const url = new URL("https://discovery.prophet.zone/api/events/unusual-betting/summary");
  url.searchParams.set("slug", slug);

  const response = await fetch(url.toString(), {
    headers: { "Accept": "application/json" }
  });

  if (!response.ok) {
    throw new Error(`Signal API failed: ${response.status}`);
  }

  return response.json();
}
```

Polymarket profile pages usually expose wallet data as a proxy wallet address.
For event pages, extract the event slug from URLs such as:

```text
https://polymarket.com/event/fifwc-esp-cvi-2026-06-15
https://polymarket.com/zh/event/fifwc-esp-cvi-2026-06-15
```

Then call:

```text
GET https://discovery.prophet.zone/api/events/unusual-betting/summary?slug=fifwc-esp-cvi-2026-06-15
```

## UI Recommendations

Use neutral wording in browser UI:

- `Large direction wallets`
- `Signal direction notional`
- `Max wallet aggregate`
- `Signal strength`

Avoid wording that implies wrongdoing. These signals mean "large wallet
aggregate matched on a specific direction," not proof of insider information.

Suggested severity labels:

| API severity | Suggested label |
|---|---|
| `none` | No signal |
| `low` | Watch |
| `medium` | Moderate signal |
| `high` | Strong signal |
| `critical` | Extreme signal |

## Notes and Limits

- Timestamps are UTC in API responses.
- Data is based on matched fills, not open orders.
- Market matching can split a single intended order into many fills; this is why
  wallet aggregate is the primary signal.
- The summary endpoint should be preferred for extension overlays. Use the
  detail endpoint only when the user opens a detailed panel.
