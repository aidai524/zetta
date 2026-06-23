# Realtime Wallet Trade Feed

This document describes the realtime trade feed endpoints used by Discovery pages,
Chrome extensions, and follow-trading clients.

## Base URLs

WebSocket:

```text
wss://discovery.prophet.zone/stream/official-trades
```

HTTPS API:

```text
https://discovery.prophet.zone/api
```

Local services:

```text
ws://127.0.0.1:8091/official-trades
http://127.0.0.1:8088
```

## Data Flow

The realtime feed is server-side proxied and cached:

```text
Polymarket official RTDS WebSocket
  -> zetta-official-trade-feed
  -> in-memory broadcast + local state cache
  -> Discovery WebSocket / HTTPS API
```

Clients do not connect directly to Polymarket. The Zetta server keeps the
official RTDS connection open, normalizes trade messages, writes recent wallet
messages under `state_dir/official_trade_feed/wallets`, and broadcasts matched
messages to connected clients.

## WebSocket: Full Feed

Use this when the page wants all realtime trades and will filter client-side.

```text
wss://discovery.prophet.zone/stream/official-trades
```

Connection behavior:

- Sends one `connected` message after connect.
- Replays recent cached trades with `replay: true`.
- Pushes new trades as they arrive.
- Sends `status` and heartbeat messages for connection state.

## WebSocket: Wallet Filter

Use this for follow-trading or wallet monitoring. Pass a wallet array in the
`wallets` query parameter. The server only sends trades for those wallets.

Recommended array form:

```js
const wallets = [
  "0xef21da2db1bed42fc7894fa543cb6e91ab38ac34",
  "0x1111111111111111111111111111111111111111",
];

const url =
  "wss://discovery.prophet.zone/stream/official-trades?wallets=" +
  encodeURIComponent(JSON.stringify(wallets));

const ws = new WebSocket(url);

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);

  if (msg.type === "connected") {
    console.log("connected", msg.wallet_filter_count, msg.wallet_filter);
    return;
  }

  if (msg.type !== "trade") return;

  const trade = msg.trade || {};
  console.log("matched wallet trade:", {
    wallet: trade.user_address,
    time: trade.timestamp,
    side: trade.side,
    title: trade.question,
    outcome: trade.outcome,
    price: trade.price,
    size: trade.size,
    notional: trade.notional,
    tx: trade.transaction_hash,
    replay: msg.replay === true,
  });
};
```

Comma-separated form is also supported:

```text
wss://discovery.prophet.zone/stream/official-trades?wallets=0xef21da2db1bed42fc7894fa543cb6e91ab38ac34,0x1111111111111111111111111111111111111111
```

Repeated parameter form is supported:

```text
wss://discovery.prophet.zone/stream/official-trades?wallet=0xef21da2db1bed42fc7894fa543cb6e91ab38ac34&wallet=0x1111111111111111111111111111111111111111
```

Supported wallet filter parameter names:

| Parameter | Description |
|---|---|
| `wallets` | JSON array or comma-separated list. |
| `wallet` | Single wallet. Can be repeated. |
| `users` / `user` | Alias. |
| `addresses` / `address` | Alias. |

Limits and behavior:

- Maximum 500 wallet addresses per WebSocket connection.
- Invalid wallet strings are ignored.
- Wallet comparison is case-insensitive.
- Empty filter means full feed.
- Filtered connections still receive `connected`, `status`, and heartbeat
  messages.
- Initial replay is also filtered to the requested wallets.
- For filtered connections with up to 25 wallets, replay is augmented from
  Polymarket Data API `/activity` so a reconnect can recover recent wallet
  trades missed while the upstream RTDS connection was stale.

## WebSocket Message Shapes

### Connected

```json
{
  "type": "connected",
  "source": "polymarket-rtds",
  "server_time": "2026-06-22 17:10:57.123",
  "upstream_connected": true,
  "topic": "activity",
  "feed_type": "trades",
  "wallet_filter": [
    "0xef21da2db1bed42fc7894fa543cb6e91ab38ac34"
  ],
  "wallet_filter_count": 1
}
```

### Trade

```json
{
  "type": "trade",
  "source": "polymarket-rtds",
  "topic": "activity",
  "feed_type": "trades",
  "received_at": "2026-06-22 17:10:57.123",
  "latency_seconds": 0.42,
  "replay": true,
  "trade": {
    "trade_id": "a1b2...",
    "transaction_hash": "0x...",
    "timestamp": "2026-06-22 17:10:56.000",
    "condition_id": "0x...",
    "token_id": "123...",
    "user_address": "0xef21da2db1bed42fc7894fa543cb6e91ab38ac34",
    "side": "BUY",
    "price": 0.56,
    "size": 15.0,
    "notional": 8.4,
    "question": "Tunisia vs. Netherlands: O/U 3.5",
    "market_slug": "fifwc-tun-nld-2026-06-25-total-3pt5",
    "event_slug": "fifwc-tun-nld-2026-06-25-more-markets",
    "outcome": "Under",
    "trader_name": "...",
    "trader_pseudonym": "..."
  }
}
```

Notes:

- `replay: true` means the message came from the server's recent cache after
  connect, not from a new live push.
- `trade.source` can be `polymarket-rtds` for official RTDS pushes or
  `polymarket-data-api` for replay backfill.
- Some official RTDS messages may have empty `question`, `slug`, or
  `condition_id`; the server preserves the official payload instead of guessing.

## HTTPS: Wallet Realtime Snapshot

Use this when a client wants a quick snapshot of the server-side realtime cache
for one wallet without opening a WebSocket.

```http
GET /wallets/detail?user={wallet}&realtime=1&activity_limit=20&position_limit=10&pnl_points_limit=0
```

Full example:

```text
https://discovery.prophet.zone/api/wallets/detail?user=0xef21da2db1bed42fc7894fa543cb6e91ab38ac34&realtime=1&activity_limit=20&position_limit=10&pnl_points_limit=0
```

Query parameters:

| Parameter | Default | Description |
|---|---:|---|
| `user` | required | Wallet address. |
| `realtime` | `0` | Set `1` to read only RTDS wallet cache. |
| `activity_limit` | `50` | Recent realtime activity rows to return. |
| `position_limit` | `50` | Kept for response compatibility. Realtime mode usually has no full positions. |
| `pnl_points_limit` | `180` | Set `0` for a lighter response. |

Response markers:

```json
{
  "wallet": {
    "user_address": "0xef21da2db1bed42fc7894fa543cb6e91ab38ac34",
    "data_source": "realtime",
    "data_status": "ok",
    "realtime_activity_count": 3,
    "realtime_last_activity_at": "2026-06-22 17:10:56.000"
  },
  "realtime": {
    "enabled": true,
    "source": "polymarket-rtds",
    "cache_status": "hit",
    "activity_count": 3,
    "activity_limit": 20,
    "captured_at": "2026-06-22 17:10:57.456",
    "last_activity_at": "2026-06-22 17:10:56.000"
  },
  "recent_activity": []
}
```

`wallet.data_status` values:

| Value | Meaning |
|---|---|
| `ok` | Wallet has recent RTDS cached activity. |
| `no_realtime_activity` | No recent RTDS message for this wallet in local cache. This is not an API error. |

## When To Use Which Endpoint

| Use case | Endpoint |
|---|---|
| Keep monitoring many wallets live | WebSocket with `wallets` filter. |
| Show a single wallet's recent realtime cache on page load | HTTPS `realtime=1`. |
| Show complete wallet positions, PnL, risk metrics, and history | HTTPS `live=1` or default wallet detail. |
| Show all market-wide trades | WebSocket without wallet filters. |
