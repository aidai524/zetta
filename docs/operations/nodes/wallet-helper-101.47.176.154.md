# Wallet Helper Node: 101.47.176.154

Role: wallet helper.

Node ID: `wallet-helper-2`.

This node should only process wallet snapshot tasks. It should not run API, full-site
frontier, chain, mart, or local database services.

## Configure

```bash
cd /root/zetta
git pull --ff-only origin main
sudo bash scripts/configure_wallet_helper.sh wallet-helper-2 101.47.178.69 6
```

## Optional Cleanup

Only run this on helper nodes:

```bash
cd /opt/zetta
sudo docker compose down || true
sudo docker volume rm zetta_postgres-data zetta_clickhouse-data zetta_redpanda-data zetta_minio-data 2>/dev/null || true
sudo rm -rf /var/lib/zetta/raw /var/lib/zetta/state
sudo mkdir -p /var/lib/zetta/wallet-raw /var/lib/zetta/wallet-state
sudo systemctl restart zetta-worker.service
```

## Expected Environment

```bash
grep -E 'ZETTA_NODE_ID|ZETTA_POSTGRES_DSN|ZETTA_CLICKHOUSE_HOST|ZETTA_RAW_DIR|ZETTA_STATE_DIR|ZETTA_WORKER_TASK_KINDS|ZETTA_WORKER_PROCESSES' /etc/zetta/zetta.env
```

Expected values:

- `ZETTA_NODE_ID=wallet-helper-2`
- `ZETTA_POSTGRES_DSN=postgresql://zetta:zetta@101.47.178.69:55432/zetta`
- `ZETTA_CLICKHOUSE_HOST=101.47.178.69`
- `ZETTA_RAW_DIR=/var/lib/zetta/wallet-raw`
- `ZETTA_STATE_DIR=/var/lib/zetta/wallet-state`
- `ZETTA_WORKER_TASK_KINDS=wallet-portfolio,wallet-pnl`
- `ZETTA_WORKER_PROCESSES=6`

## Verify

```bash
systemctl status zetta-worker.service --no-pager
systemctl list-timers 'zetta-*' --all --no-pager
journalctl -u zetta-worker.service -n 80 --no-pager
curl -sS 'http://101.47.178.69:8123/?user=zetta&password=zetta&query=select%201'
```
