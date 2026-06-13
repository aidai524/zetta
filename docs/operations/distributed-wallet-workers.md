# Distributed Wallet Workers

This runbook describes the current wallet-processing split:

- Master node keeps the main Postgres task queue, ClickHouse warehouse, API, full-site
  Polymarket trade ingestion, loaders, chain jobs, and marts.
- Helper nodes only claim wallet snapshot tasks from the master's Postgres queue and load
  the wallet raw files they collect into the master's ClickHouse.

## Current Nodes

| Role | Node ID | IP | Responsibilities |
| --- | --- | --- | --- |
| Master | `zetta-ubuntu-1` | `101.47.178.69` | Postgres, ClickHouse, API, full-site trades, Gamma, chain, marts, wallet task seeders. |
| Wallet helper | `wallet-helper-1` | `101.47.179.91` | `wallet-portfolio` and `wallet-pnl` workers only. |
| Wallet helper | `wallet-helper-2` | `101.47.176.154` | `wallet-portfolio` and `wallet-pnl` workers only. |
| Wallet helper | `wallet-helper-3` | `101.47.176.175` | `wallet-portfolio` and `wallet-pnl` workers only. |

## Data Flow

1. Master timers seed wallet tasks into Postgres:
   - `zetta-wallet-candidates.timer`
   - `zetta-wallet-pnl-candidates.timer`
2. Helper workers connect to master Postgres at `101.47.178.69:55432`.
3. Helper workers claim only:
   - `wallet-portfolio`
   - `wallet-pnl`
4. Helper workers write raw wallet JSONL locally under `/var/lib/zetta/wallet-raw`.
5. Helper loader timer loads local wallet raw files into master ClickHouse at
   `101.47.178.69:8123`.
6. Master builds wallet rollups and screener marts from ClickHouse.

The helper nodes use a wallet-only raw directory so cloned historical raw files from the
master image are not replayed into ClickHouse.

## Master Configuration

Run on `101.47.178.69`:

```bash
cd /root/zetta
git pull --ff-only origin main

sudo install -m 0644 src/zetta/cli.py /opt/zetta/src/zetta/cli.py
sudo install -m 0644 src/zetta/scheduler/tasks.py /opt/zetta/src/zetta/scheduler/tasks.py
sudo install -m 0755 infra/scripts/zetta-runner /usr/local/bin/zetta-runner

sudo bash -lc '
ENV=/etc/zetta/zetta.env
set_env(){ if grep -q "^$1=" "$ENV"; then sed -i "s|^$1=.*|$1=$2|" "$ENV"; else echo "$1=$2" >> "$ENV"; fi; }
set_env ZETTA_WORKER_TASK_KINDS gamma-events,gamma-markets,event-refresh,trades,activity,holders,market-positions,positions,open-interest,prices-history,book,chain-logs
set_env ZETTA_WALLET_PNL_CANDIDATE_REFRESH_LIMIT 10000
set_env ZETTA_WALLET_CANDIDATE_REFRESH_LIMIT 2000
'

sudo systemctl daemon-reload
sudo systemctl restart zetta-worker.service
sudo systemctl disable --now zetta-wallet-refresh.timer
sudo systemctl enable --now zetta-wallet-candidates.timer zetta-wallet-pnl-candidates.timer zetta-wallet-screener.timer
sudo systemctl start zetta-wallet-candidates.service zetta-wallet-pnl-candidates.service
```

## Helper Configuration

Run the matching node command from the per-node documents:

- [wallet-helper-1: 101.47.179.91](nodes/wallet-helper-101.47.179.91.md)
- [wallet-helper-2: 101.47.176.154](nodes/wallet-helper-101.47.176.154.md)
- [wallet-helper-3: 101.47.176.175](nodes/wallet-helper-101.47.176.175.md)

The helper setup script updates `/etc/zetta/zetta.env`, installs the current runner,
disables non-wallet services, enables `zetta-worker.service`, and enables
`zetta-load-trades-realtime.timer`.

## Required Network Access

Allow only the three helper IPs to reach these master ports:

- Postgres task queue: `101.47.178.69:55432`
- ClickHouse HTTP: `101.47.178.69:8123`

Do not expose these ports broadly if the cloud firewall supports source allowlists.

## Verification

On any helper:

```bash
systemctl status zetta-worker.service --no-pager
systemctl list-timers 'zetta-*' --all --no-pager
journalctl -u zetta-worker.service -n 80 --no-pager
curl -sS 'http://101.47.178.69:8123/?user=zetta&password=zetta&query=select%201'
```

On master:

```bash
cd /opt/zetta
.venv/bin/python -m zetta.cli --task-store postgres tasks progress --recent-limit 20
docker exec -i zetta-postgres-1 psql -U zetta -d zetta -c "
select lease_owner, task_type, status, count(*)
from collector_tasks
where updated_at >= now() - interval '1 hour'
group by lease_owner, task_type, status
order by lease_owner, task_type, status;"
```

## Rollback

To stop a helper:

```bash
sudo systemctl disable --now zetta-worker.service zetta-load-trades-realtime.timer
```

To let the master process wallet tasks again:

```bash
sudo bash -lc '
ENV=/etc/zetta/zetta.env
sed -i "/^ZETTA_WORKER_TASK_KINDS=/d" "$ENV"
'
sudo systemctl restart zetta-worker.service
sudo systemctl enable --now zetta-wallet-refresh.timer
```
