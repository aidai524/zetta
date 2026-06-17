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
- Wallet totals are aggregated across the base match and related market variants
  before threshold filtering.
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

## Refresh And Caching

Production now uses a persistent refresh cache:

- A scheduler seeds `unusual-betting-refresh` tasks for active World Cup events
  and events with recent matched fills.
- A dedicated single-process `zetta-unusual-betting-worker.service` claims those
  tasks and recomputes the full analysis.
- Results are stored in Postgres `unusual_betting_cache`.
- API requests prefer this persistent cache when it is fresh enough.
- The API still supports forced live recomputation for debugging or urgent
  manual checks.

The active-event wallet monitor can also seed the same refresh task. This is
controlled by `ZETTA_ACTIVE_EVENT_INCLUDE_UNUSUAL_BETTING_REFRESH`; it is off by
default to avoid increasing task volume unexpectedly. When enabled, the monitor
looks at active World Cup matches with recent wallet trading and only links a
match when at least one wallet reaches
`ZETTA_ACTIVE_EVENT_UNUSUAL_BETTING_MIN_NOTIONAL`, default `500000`, in the
lookback window. Wallet monitoring and large-direction signal refresh share the
same `collector_tasks` queue, but the refresh tasks should be claimed only by
the dedicated `zetta-unusual-betting-worker.service`. Do not add
`unusual-betting-refresh` to the normal multi-process worker or helper worker
allowlist; parallel refreshes can overload ClickHouse.

Default API behavior:

- `/events/unusual-betting/summary` reads Postgres cache first.
- `/events/unusual-betting` reads Postgres cache first if the cached detail has
  enough `wallet_limit` and `trade_limit` rows for the request.
- If persistent cache is stale, the API may still serve that stale row rather
  than run an expensive live query; the scheduled worker is responsible for
  keeping rows fresh.
- If persistent cache is missing, the API recomputes from ClickHouse fills and
  stores the new result.
- The API also keeps a short in-process cache to avoid duplicate recomputes.

Useful cache parameters:

| Parameter | Default | Description |
|---|---:|---|
| `use_persisted_cache` | `true` | Read Postgres cache before recomputing. |
| `persisted_cache_ttl_seconds` | `3600` | Maximum accepted age for a fresh persistent-cache hit. Set `0` to accept any persisted row age. |
| `cache_ttl_seconds` | `60` | In-process API cache TTL and stale-persistent fallback switch. Set `0` to skip stale fallback and in-process cache, but fresh persistent cache is still used. |
| `use_persisted_cache=0` | `false` | Skip persistent-cache reads. Use with care; this can trigger ClickHouse queries. |
| `refresh` | `false` | Force recompute and write a fresh persistent cache row. |
| `trigger_reason` | `api` | Recorded in cache metadata when recomputed. |

Manual forced refresh example:

```text
https://discovery.prophet.zone/api/events/unusual-betting/summary?slug=fifwc-esp-cvi-2026-06-15&refresh=1&cache_ttl_seconds=0
```

Responses include cache metadata:

```json
{
  "cache": {
    "source": "postgres_cache",
    "cache_key": "9f7...",
    "refreshed_at": "2026-06-17T10:20:30.123456+00:00",
    "generated_at": "2026-06-17T10:20:29.998000+00:00",
    "age_seconds": 42.15,
    "trigger_reason": "scheduled",
    "error": null
  }
}
```

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
| `persisted_cache_ttl_seconds` | `3600` | Freshness window for cached scheduled results. |
| `cache_ttl_seconds` | `60` | In-process cache TTL and stale fallback switch. Use `0` only when debugging. |

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
