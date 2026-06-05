# Zetta

Zetta is a data platform for collecting, normalizing, and serving Polymarket data.

The first milestone focuses on a durable ingestion loop:

- Gamma API discovery for events and markets.
- Data API trade and user-facing public datasets.
- CLOB price history and order book snapshots.
- Raw immutable JSONL storage for replay.
- ClickHouse analytical tables for query performance.
- Postgres task state for distributed workers.

## Quick Start

```bash
PYTHONPATH=src python -m zetta.cli endpoints
PYTHONPATH=src python -m zetta.cli collect gamma-events --page-limit 50 --max-pages 1
PYTHONPATH=src python -m zetta.cli collect gamma-markets --page-limit 50 --max-pages 1
```

By default raw responses are written under `data/raw`.

## Network Resolve Overrides

Some local proxy or TUN setups return fake DNS IPs that break TLS for Polymarket hosts.
When direct requests fail with TLS EOF errors, pass explicit host-to-IP overrides as a
global CLI option before the subcommand:

```bash
PYTHONPATH=src python -m zetta.cli \
  --http-resolve-overrides gamma-api.polymarket.com:128.242.240.125,clob.polymarket.com:185.60.216.50 \
  collect gamma-events --page-limit 50 --max-pages 1
```

The override is applied to the curl fallback path and keeps the original HTTPS host
header/SNI intact.

## Local Tasks

```bash
PYTHONPATH=src python -m zetta.cli tasks seed-basic --page-limit 100 --max-pages 1
PYTHONPATH=src python -m zetta.cli tasks status
PYTHONPATH=src python -m zetta.cli tasks run-once
```

Use Postgres-backed task leases for multi-worker runs:

```bash
PYTHONPATH=src python -m zetta.cli --task-store postgres --node-id worker-a tasks seed-basic
PYTHONPATH=src python -m zetta.cli --task-store postgres --node-id worker-a tasks status
PYTHONPATH=src python -m zetta.cli --task-store postgres --node-id worker-a tasks run-once
PYTHONPATH=src python -m zetta.cli --task-store postgres --node-id worker-a tasks run-loop
```

The Postgres task store uses leases, per-node IDs, retry-to-pending behavior, and
dead-letter records after `max_attempts`. Run history is written to `collector_runs`
for Postgres workers and to `*.runs.jsonl` for local JSON workers. The Postgres task
store uses `psycopg`; install the project dependencies with `pip install -e .` if the
active Python environment does not already include it.

Use `make test` for offline tests. The target disables globally installed pytest plugins
so the project test run is isolated from unrelated Python packages on the host.

## Local Services

Start the local stack:

```bash
docker compose up -d
PYTHONPATH=src python -m zetta.cli db ping
PYTHONPATH=src python -m zetta.cli db migrate
```

Load raw Gamma records into ClickHouse after collection:

```bash
PYTHONPATH=src python -m zetta.cli load gamma-raw
```

## Live Gamma Smoke Test

Global CLI options must appear before the subcommand:

```bash
PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-live-test \
  --state-dir data/state-live-test \
  collect gamma-events --page-limit 2 --max-pages 1

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-live-test \
  load gamma-raw
```

Replay already loaded raw files after a schema expansion:

```bash
PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-live-test \
  load gamma-raw --force
```

## Gamma Backfill

Use `--max-pages 0` to keep paging until the API is exhausted. Keep `--resume` on for
production runs so the collector continues from the saved keyset cursor.

```bash
PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw \
  --state-dir data/state \
  collect gamma-events --page-limit 100 --max-pages 0 --resume --sleep-seconds 0.05

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw \
  --state-dir data/state \
  collect gamma-markets --page-limit 100 --max-pages 0 --resume --sleep-seconds 0.05

PYTHONPATH=src python -m zetta.cli --raw-data-dir data/raw load gamma-raw
```

See `docs/todo.md` for the staged implementation backlog.

## CLOB Price History

Discover active CLOB token IDs from ClickHouse:

```bash
PYTHONPATH=src python -m zetta.cli discover tokens --active-only --limit 10
```

Collect and load price history for discovered tokens:

```bash
PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-clob \
  collect prices-history-batch --active-only --limit 10 --interval all --sleep-seconds 0.1

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-clob \
  load clob-price-history --batch-size 5000
```

`fact_price_history` uses replacing storage, so analytical queries should deduplicate by
`token_id, timestamp` or use `FINAL` when exact latest-row semantics matter.

Collect and load order book snapshots:

```bash
PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-book \
  collect books-batch --active-only --limit 10 --sleep-seconds 0.1

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-book \
  load clob-books
```

Build 1 minute price candles:

```bash
PYTHONPATH=src python -m zetta.cli build market-1m
```

## Data API Trades

Collect and load public trades:

```bash
PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-trades \
  collect trades --page-limit 500 --max-pages 1

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-trades \
  load data-trades
```

Collect additional public Data API surfaces:

```bash
PYTHONPATH=src python -m zetta.cli discover markets --active-only --limit 5
PYTHONPATH=src python -m zetta.cli discover wallets --limit 5

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-data \
  collect holders --market <condition_id> --limit 500

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-data \
  collect market-positions --market <condition_id> --limit 500

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-data \
  collect open-interest --market <condition_id>

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-data \
  collect activity --user <wallet> --page-limit 500 --max-pages 1

PYTHONPATH=src python -m zetta.cli --raw-data-dir data/raw-data load data-holders
PYTHONPATH=src python -m zetta.cli --raw-data-dir data/raw-data load data-market-positions
PYTHONPATH=src python -m zetta.cli --raw-data-dir data/raw-data load data-open-interest
PYTHONPATH=src python -m zetta.cli --raw-data-dir data/raw-data load data-activity
PYTHONPATH=src python -m zetta.cli build trader-profiles
```

`mart_trader_profile` combines trade counts/notional with the latest public position
snapshots and the chain-derived `mart_trader_chain_pnl` fields. The chain PnL fields
include fill count, traded notional, chain balance, current value from latest price
history, net cashflow, and mark-to-market PnL.

## Real-Time Stream

Collect CLOB market WebSocket messages for active tokens, persist raw JSONL, load full
`book` events into ClickHouse, and publish the same raw events to Redpanda:

```bash
PYTHONPATH=src python -m zetta.cli collect ws-market \
  --active-only --limit 2 --max-messages 3 --max-seconds 20

PYTHONPATH=src python -m zetta.cli load clob-ws-market-books

PYTHONPATH=src python -m zetta.cli stream ws-market-raw --max-records 1
```

Rebuild in-memory order book state from persisted WebSocket raw events:

```bash
PYTHONPATH=src python -m zetta.cli realtime rebuild-books --max-records 100
```

Compare one reconstructed WebSocket book with the current CLOB REST book when the REST
host is reachable:

```bash
PYTHONPATH=src python -m zetta.cli realtime reconcile-book --token-id <token_id>
```

Build the initial alert mart for price moves, wide spreads, and large trades:

```bash
PYTHONPATH=src python -m zetta.cli build alerts --since-hours 24
```

## Chain Logs

The Phase 5 MVP uses Polygon RPC self-indexing as the durable source of truth. The
default RPC is `https://polygon-bor-rpc.publicnode.com`; use `--polygon-rpc-url` to
swap in a paid or self-hosted endpoint.

```bash
PYTHONPATH=src python -m zetta.cli --timeout 10 chain block-number

PYTHONPATH=src python -m zetta.cli \
  --raw-data-dir data/raw-chain \
  collect chain-logs --from-block <block> --to-block <block> \
  --address <contract_address>

PYTHONPATH=src python -m zetta.cli --raw-data-dir data/raw-chain load chain-logs
```

Raw logs land in `fact_chain_log`. ABI-specific decoding into fills, splits, merges,
redeems, balances, and settlement audit tables is the next chain phase. `OrderFilled`
logs from the CTF Exchange can already be decoded into `fact_exchange_fill`:

```bash
PYTHONPATH=src python -m zetta.cli load exchange-fills
PYTHONPATH=src python -m zetta.cli load orders-matched
PYTHONPATH=src python -m zetta.cli load fees-charged
PYTHONPATH=src python -m zetta.cli load balance-movements
PYTHONPATH=src python -m zetta.cli load lifecycle-events
PYTHONPATH=src python -m zetta.cli build trader-chain-pnl
PYTHONPATH=src python -m zetta.cli build trade-reconciliation
PYTHONPATH=src python -m zetta.cli build settlement-audit
```

## Operations

Rate limiting is enforced through shared in-process token buckets per API family and
endpoint: Gamma, Data API, CLOB REST, and Polygon RPC each have family-level and
endpoint-specific buckets. For multi-machine workers, Postgres leases coordinate task
claiming while each worker reports runs into `collector_runs`.

Build the collector health mart after Postgres-backed workers have run:

```bash
PYTHONPATH=src python -m zetta.cli build collector-health
```

Deployment templates live under `infra/nomad` and `infra/ansible`. The Nomad job runs a
pool of task workers plus one product API service. The Ansible playbook installs a Python
virtualenv, starts worker systemd units on `zetta_workers`, and starts the product API on
`zetta_api`.

For a single Ubuntu server or a small worker fleet, use the systemd bootstrap flow in
`docs/operations/ubuntu-server-deploy.md`. It installs Docker-backed storage services,
collector workers, continuous WebSocket capture, and loader/mart timers.

## Product API

Start the read-only API against ClickHouse:

```bash
PYTHONPATH=src python -m zetta.cli api serve --host 127.0.0.1 --port 8088
```

Endpoints:

- `GET /markets/search?q=<text>&limit=25`
- `GET /events/timeline?event_id=<event_id>&limit=100`
- `GET /traders/profile?user=<wallet>`
- `GET /markets/liquidity?token_id=<token_id>&limit=25`
- `GET /alerts?type=<alert_type>&token_id=<token_id>&limit=50`

## Network Diagnostics

Gamma full exhaustion and CLOB REST reconciliation have both been verified on this host.
If `gamma-api.polymarket.com` or `clob.polymarket.com/book` fails with TLS EOF errors,
check local DNS or proxy/TUN fake-IP behavior and retry with `--http-resolve-overrides`.
For periodic operations, schedule the Gamma commands with `--max-pages 0 --resume` and
run `realtime reconcile-book` for active token IDs.

## Layout

- `src/zetta`: ingestion, storage, and CLI code.
- `infra`: local database and service definitions.
- `docs`: architecture notes and operating model.
