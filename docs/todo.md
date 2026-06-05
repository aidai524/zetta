# Zetta Implementation TODO

## Phase 1: Public Gamma Backfill

- [x] Verify live `gamma-api.polymarket.com` connectivity.
- [x] Collect a live Gamma events sample through the Zetta CLI.
- [x] Load live Gamma raw records into ClickHouse.
- [x] Verify live `dim_event`, `dim_market`, and `dim_outcome_token` rows.
- [x] Add `gamma-markets` live sample verification.
- [x] Add full Gamma events backfill command with checkpointed keyset pagination.
- [x] Add full Gamma markets backfill command with checkpointed keyset pagination.
- [x] Normalize tags, series, and event-market relationships into dedicated tables.
- [x] Add dedupe/idempotency tracking for raw files already loaded into ClickHouse.
- [x] Run production-mode Gamma events backfill sample.
- [x] Run production-mode Gamma markets backfill sample.
- [x] Add loader batch sizing for very large raw directories.
- [x] Add ClickHouse query examples for Gamma discovery datasets.
- [x] Run Gamma events backfill until exhaustion. Completed with keyset cursor exhausted (`next_cursor: null`).
- [x] Run Gamma markets backfill until exhaustion. Completed with keyset cursor exhausted (`next_cursor: null`).

## Phase 2: CLOB Market Data

- [x] Discover token IDs from `dim_outcome_token`.
- [x] Backfill CLOB price history for discovered token IDs.
- [x] Normalize price history into `fact_price_history`.
- [x] Collect order book snapshots for active token IDs.
- [x] Normalize snapshots into `fact_orderbook_snapshot`.
- [x] Add market-level 1 minute candles into `mart_market_1m`.

## Phase 3: Public Data API

- [x] Collect public trades from Data API by market/event.
- [x] Normalize trades into `fact_trade`.
- [x] Collect user activity, public positions, holders, and open interest.
- [x] Build initial trader profile mart: trade count, volume, notional, first/last trade.
- [x] Enrich trader profile with realized/current/total PnL from public position snapshots.
- [x] Enrich trader profile with chain-verified realized/unrealized PnL.

## Phase 4: Real-Time Stream

- [x] Implement CLOB market WebSocket client.
- [x] Publish real-time events to Redpanda topics.
- [x] Persist WebSocket raw messages.
- [x] Build order book state reconstruction and REST reconciliation command.
- [x] Add price move, liquidity, and whale trade alerts.
- [x] Run periodic REST reconciliation once `clob.polymarket.com/book` is reachable from this host. Verified with host resolve override; best bid/ask deltas were zero for the test token.

## Phase 5: Chain Reconciliation

- [x] Choose chain data source: self-indexed Polygon RPC first, optional Goldsky/Dune/Allium acceleration later.
- [x] Add raw Polygon log collection and ClickHouse loading MVP.
- [x] Decode CTF Exchange `OrderFilled` logs into `fact_exchange_fill`.
- [x] Decode CTF Exchange `OrdersMatched` and `FeeCharged` logs.
- [x] Decode ERC1155 `TransferSingle`/`TransferBatch` balance movements.
- [x] Decode split, merge, and redeem lifecycle events.
- [x] Build initial Data API trade vs chain fill reconciliation mart.
- [x] Build initial settlement/result audit mart.

## Phase 6: Distributed Operations

- [x] Move local JSON task store to Postgres task leases.
- [x] Add worker roles and node IDs for task leases.
- [x] Add rate limit buckets per API family and endpoint.
- [x] Add run metrics, retry policy, dead-letter records, and dashboards.
- [x] Add Ansible or Nomad deployment for multi-machine workers.

## Phase 7: Product API

- [x] Build market search API.
- [x] Build event timeline API.
- [x] Build trader profile API.
- [x] Build liquidity and slippage API.
- [x] Build alert API.

## Phase 8: Historical Event Deep Backfill

- [ ] Backfill historical trades, price history, order book snapshots, and Polygon exchange fills for every event. This dataset is expected to be large; plan to run it on server infrastructure with partitioned tasks by event, market, token, and block range.
- [x] Add Ubuntu server bootstrap script and systemd worker/websocket/loader services.
- [x] Add `tasks seed-history` to generate partitioned deep-backfill tasks from ClickHouse.
- [ ] Run a small server smoke test with `--event-limit 10` before all-event backfill.
- [ ] Run full all-event deep backfill on server infrastructure.
