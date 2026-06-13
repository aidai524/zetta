# Master Node: 101.47.178.69

Role: master.

Node ID: `zetta-ubuntu-1`.

This node owns the canonical Postgres task queue and ClickHouse warehouse. It should keep
the full Polymarket ingestion and mart-building timers enabled. Helper nodes should write
wallet results back to this node.

## Keep Enabled

- `zetta-api.service`
- `zetta-worker.service`
- `zetta-frontier.timer`
- `zetta-frontier-trades.timer`
- `zetta-chain-frontier.timer`
- `zetta-load.timer`
- `zetta-load-trades-realtime.timer`
- `zetta-marts.timer`
- `zetta-wallet-candidates.timer`
- `zetta-wallet-pnl-candidates.timer`
- `zetta-wallet-rollup.timer`
- `zetta-wallet-screener.timer`

## Disable On Master

Disable realtime watchlist wallet refresh after the helper nodes are active:

```bash
sudo systemctl disable --now zetta-wallet-refresh.timer
```

The master can still seed wallet tasks. The helper nodes will claim `wallet-portfolio`
and `wallet-pnl`.

## Apply Current Code

```bash
cd /root/zetta
git pull --ff-only origin main

sudo install -m 0644 src/zetta/cli.py /opt/zetta/src/zetta/cli.py
sudo install -m 0644 src/zetta/scheduler/tasks.py /opt/zetta/src/zetta/scheduler/tasks.py
sudo install -m 0755 infra/scripts/zetta-runner /usr/local/bin/zetta-runner
```

## Environment

```bash
sudo bash -lc '
ENV=/etc/zetta/zetta.env
set_env(){ if grep -q "^$1=" "$ENV"; then sed -i "s|^$1=.*|$1=$2|" "$ENV"; else echo "$1=$2" >> "$ENV"; fi; }
set_env ZETTA_NODE_ID zetta-ubuntu-1
set_env ZETTA_POSTGRES_DSN postgresql://zetta:zetta@127.0.0.1:55432/zetta
set_env ZETTA_CLICKHOUSE_HOST 127.0.0.1
set_env ZETTA_WORKER_TASK_KINDS gamma-events,gamma-markets,event-refresh,trades,activity,holders,market-positions,positions,open-interest,prices-history,book,chain-logs
set_env ZETTA_WALLET_PNL_CANDIDATE_REFRESH_LIMIT 10000
set_env ZETTA_WALLET_CANDIDATE_REFRESH_LIMIT 2000
'
```

## Start

```bash
sudo systemctl daemon-reload
sudo systemctl restart zetta-worker.service
sudo systemctl enable --now zetta-wallet-candidates.timer zetta-wallet-pnl-candidates.timer zetta-wallet-screener.timer
sudo systemctl start zetta-wallet-candidates.service zetta-wallet-pnl-candidates.service
```

## Verify

```bash
systemctl list-timers 'zetta-*' --all --no-pager
systemctl status zetta-worker.service zetta-api.service --no-pager
docker exec -i zetta-postgres-1 psql -U zetta -d zetta -c "
select lease_owner, task_type, status, count(*)
from collector_tasks
where updated_at >= now() - interval '1 hour'
group by lease_owner, task_type, status
order by lease_owner, task_type, status;"
```
