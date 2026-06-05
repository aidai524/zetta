# Zetta Polymarket Data Architecture

## Goal

Collect durable, replayable Polymarket data across public REST APIs, market WebSockets,
and blockchain data sources, then expose low-latency analytical APIs for research and
user-facing tools.

## Source Strategy

| Source | Use |
| --- | --- |
| Gamma API | Events, markets, tags, series, market metadata, outcomes. |
| Data API | Public trades, user activity, positions, holders, open interest. |
| CLOB REST | Order book snapshots, price history, spread, last trade price. |
| CLOB WebSocket | Real-time book, price, last trade, and market lifecycle events. |
| Polygon chain data | Settlement, redeem, CTF balances, exchange logs, reconciliation. Start with self-indexed Polygon RPC logs; add Goldsky, Dune, or Allium as acceleration layers if needed. |

Private user orders require an authenticated user context and should be treated as an
authorization-scoped dataset, not a public full-site crawl target.

## Storage Layers

1. Raw lake: immutable JSONL/Gzip or Parquet partitions by source, entity, and date.
2. Operational state: Postgres task rows, cursors, leases, run history, and errors.
3. Stream log: Redpanda topics for WebSocket and normalized update events.
4. Analytics: ClickHouse facts, dimensions, snapshots, and marts.
5. Serving indexes: optional OpenSearch or Typesense for text search.

## Worker Model

Workers claim tasks from Postgres using a lease, collect pages or streaming windows,
write raw data first, then emit normalized records. Every job must be idempotent and
safe to retry.

Initial worker roles:

- `gamma-events-backfill`
- `gamma-markets-backfill`
- `data-trades-backfill`
- `clob-price-history-backfill`
- `clob-book-snapshot`
- `clob-market-websocket`
- `chain-exchange-indexer`

## Query Model

ClickHouse is the primary analytical store. Use dimensions for current event and market
metadata, facts for trades and prices, snapshots for order books and positions, and marts
for user-facing aggregates such as market candles, trader PnL, market quality, and alert
signals.

## Deployment

Start with Docker Compose for local development. For multiple machines, use node roles
and Postgres task leases so additional workers can be added without changing ingestion
logic. Kubernetes can come later once the data model and throughput are known.
