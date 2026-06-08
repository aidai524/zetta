# Ubuntu Server Deployment

Target system: Ubuntu 24.04 LTS.

This deployment runs storage services with Docker Compose and runs Zetta collectors with
systemd. The same model can scale from one server to many workers by pointing every
worker at the same Postgres task store and ClickHouse cluster.

## Recommended Server

Start with:

- 16 vCPU
- 64 GB RAM
- 2 TB NVMe
- Ubuntu 24.04 LTS

For full historical trades, price history, order book snapshots, and Polygon logs,
storage will grow quickly. Use larger NVMe disks or mount object storage for raw files
before running all-event deep backfills.

## One-Command Bootstrap

Copy this repository to the server, then run:

```bash
sudo bash infra/scripts/bootstrap_ubuntu.sh
```

The script installs Docker, Python, project dependencies, starts Postgres, ClickHouse,
Redpanda, and MinIO, installs systemd units, runs ClickHouse migrations, and enables:

- `zetta-worker.service`: Postgres-backed task worker.
- `zetta-api.service`: read-only product API on port `8088`.
- `zetta-ws-market.service`: CLOB WebSocket raw collector for active tokens.
- `zetta-load.timer`: raw-to-ClickHouse loaders every 5 minutes.
- `zetta-marts.timer`: mart builders every 15 minutes.

Edit runtime configuration in:

```bash
sudo nano /etc/zetta/zetta.env
sudo systemctl restart zetta-worker zetta-api zetta-ws-market
```

Set `ZETTA_POLYGON_RPC_URL` to a paid or self-hosted Polygon RPC before large chain
backfills. Public RPC endpoints are useful for smoke tests but too fragile for full
history.

## Seed Initial Work

Seed Gamma discovery first:

```bash
cd /opt/zetta
.venv/bin/python -m zetta.cli \
  --task-store postgres \
  tasks seed-basic --page-limit 100 --max-pages 0
```

Then keep a small frontier of recent active events complete enough for analysis while
the full backfill runs:

```bash
.venv/bin/python -m zetta.cli \
  --task-store postgres \
  tasks seed-frontier \
  --event-limit 50 \
  --condition-limit 50 \
  --token-limit 100
```

The `zetta-frontier.timer` runs this periodically in production. It prioritizes recent
Gamma event/market refreshes, then bounded trade, price-history, and book tasks for those
events, so ClickHouse receives usable event slices before the full historical queue
finishes.

After Gamma raw data has loaded into ClickHouse, also seed deep historical work. Use
`--active-only` for the first production run, then remove it for all historical events:

```bash
.venv/bin/python -m zetta.cli \
  --task-store postgres \
  tasks seed-history \
  --active-only \
  --chain-from-block <start_block> \
  --chain-to-block <latest_block> \
  --chain-block-step 50000
```

For a small smoke test:

```bash
.venv/bin/python -m zetta.cli \
  --task-store postgres \
  tasks seed-history \
  --event-limit 10 \
  --active-only \
  --no-include-chain-logs
```

For a small chain scanner smoke test, use a tiny known block range first:

```bash
.venv/bin/python -m zetta.cli \
  --task-store postgres \
  tasks seed-history \
  --event-limit 1 \
  --no-include-trades \
  --no-include-price-history \
  --no-include-books \
  --chain-from-block <start_block> \
  --chain-to-block <start_block_plus_1000> \
  --chain-block-step 1000
```

The task store deduplicates identical work, so the seed commands are safe to rerun.
`seed-frontier` intentionally adds a refresh marker to its task params, so each timer run
can enqueue a fresh bounded update for recent events.

## Runtime Model

Historical data:

- `trades` tasks page through Data API market trades until exhausted.
- `prices-history` tasks fetch CLOB price history per token.
- `book` tasks snapshot CLOB order books per token.
- `chain-logs` tasks scan Polygon block ranges.

Future data:

- WebSocket captures active CLOB order book messages continuously and writes raw JSONL.
- REST tasks periodically refresh Gamma, trades, holders, positions, open interest, and
  order book reconciliation.
- Chain logs continue from the latest checkpoint by adding new `chain-logs` block-range
  tasks.

## Operations

Status:

```bash
systemctl status zetta-worker zetta-ws-market zetta-load.timer zetta-marts.timer
journalctl -u zetta-worker -f
journalctl -u zetta-ws-market -f
```

Task queue:

```bash
cd /opt/zetta
.venv/bin/python -m zetta.cli --task-store postgres tasks status
```

ClickHouse health:

```bash
.venv/bin/python -m zetta.cli db ping
curl -sS -u zetta:zetta \
  --data-binary "select table, sum(rows) rows, formatReadableSize(sum(bytes_on_disk)) size from system.parts where database='zetta' and active group by table order by rows desc format PrettyCompact" \
  "http://127.0.0.1:8123/?database=system"
```

Scale out:

1. Bootstrap more Ubuntu worker servers.
2. Point `/etc/zetta/zetta.env` at the shared Postgres and ClickHouse hosts.
3. Keep `ZETTA_NODE_ID` unique per server.
4. Start `zetta-worker.service` on each worker.

For dedicated WebSocket hosts, keep `zetta-ws-market.service` enabled only on those
hosts to avoid duplicate raw streams.
