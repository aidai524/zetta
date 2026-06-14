# Wallet Helper Node: 101.47.176.175

Role: wallet helper.

Node ID: `wallet-helper-3`.

This node has 16 CPU cores and 32 GB RAM. It should only process wallet snapshot
tasks. It should not run API, full-site frontier, chain, mart, web frontend, or
local database services. Use 4 worker processes by default; increase only after
Polymarket API error rates and memory are stable.

## Configure

```bash
cd /root/zetta
git pull --ff-only origin main
sudo bash scripts/configure_wallet_helper.sh wallet-helper-3 101.47.178.69 4
```

## Cleanup Cloned Master Data

Only run this on helper nodes. It removes local services and cloned data that are not
needed for wallet-only processing. Keep `/opt/zetta/.venv`, `/opt/zetta/src`,
`/etc/zetta/zetta.env`, and `/usr/local/bin/zetta-runner`.

```bash
# Stop all local Zetta timers/services first. The configure script will re-enable only
# the wallet-only worker and the wallet raw loader.
sudo systemctl disable --now 'zetta-*' 2>/dev/null || true

# Stop local databases copied from master. Helpers write to master Postgres/ClickHouse.
cd /opt/zetta
sudo docker compose down || true
sudo docker volume rm zetta_postgres-data zetta_clickhouse-data zetta_redpanda-data zetta_minio-data 2>/dev/null || true

# Remove copied full-site raw/state data. New wallet-only raw/state directories are kept.
sudo rm -rf /var/lib/zetta/raw /var/lib/zetta/state /var/lib/zetta/quarantine
sudo mkdir -p /var/lib/zetta/wallet-raw /var/lib/zetta/wallet-state

# Optional: remove frontend dependencies and repo-local generated data; helpers do not
# build or serve the web app.
sudo rm -rf /root/zetta/apps/web/node_modules /opt/zetta/apps/web/node_modules 2>/dev/null || true
sudo find /root/zetta/data /opt/zetta/data -mindepth 1 -maxdepth 1 ! -name exports -exec rm -rf {} + 2>/dev/null || true

sudo systemctl daemon-reload
sudo bash scripts/configure_wallet_helper.sh wallet-helper-3 101.47.178.69 4
```

## Expected Environment

```bash
grep -E 'ZETTA_NODE_ID|ZETTA_POSTGRES_DSN|ZETTA_CLICKHOUSE_HOST|ZETTA_RAW_DIR|ZETTA_STATE_DIR|ZETTA_WORKER_TASK_KINDS|ZETTA_WORKER_PROCESSES' /etc/zetta/zetta.env
```

Expected values:

- `ZETTA_NODE_ID=wallet-helper-3`
- `ZETTA_POSTGRES_DSN=postgresql://zetta:zetta@101.47.178.69:55432/zetta`
- `ZETTA_CLICKHOUSE_HOST=101.47.178.69`
- `ZETTA_RAW_DIR=/var/lib/zetta/wallet-raw`
- `ZETTA_STATE_DIR=/var/lib/zetta/wallet-state`
- `ZETTA_WORKER_TASK_KINDS=wallet-portfolio,wallet-pnl`
- `ZETTA_WORKER_PROCESSES=4`

## Verify

```bash
systemctl status zetta-worker.service --no-pager
systemctl list-timers 'zetta-*' --all --no-pager
journalctl -u zetta-worker.service -n 80 --no-pager
curl -sS 'http://101.47.178.69:8123/?user=zetta&password=zetta&query=select%201'
```
