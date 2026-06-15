# Wallet Helper 2 Status - 2026-06-15

## Machine Identity

- Public IP: `101.47.176.154`
- Private IP: `10.17.12.98`
- Node ID: `wallet-helper-2`
- Role: wallet helper only
- Master private IP: `10.17.12.97`
- Master public IP: `101.47.178.69`

This node should only process wallet snapshot tasks:

- `wallet-portfolio`
- `wallet-pnl`

It should not run API, WebSocket collectors, frontier seeders, chain jobs, marts, local
Postgres, local ClickHouse, Redpanda, MinIO, nginx, or Docker workloads.

## Current Environment

Relevant `/etc/zetta/zetta.env` values:

```text
ZETTA_NODE_ID=wallet-helper-2
ZETTA_POSTGRES_DSN=postgresql://zetta:zetta@10.17.12.97:55432/zetta
ZETTA_CLICKHOUSE_HOST=10.17.12.97
ZETTA_RAW_DIR=/var/lib/zetta/wallet-raw
ZETTA_STATE_DIR=/var/lib/zetta/wallet-state
ZETTA_WORKER_TASK_KINDS=wallet-portfolio,wallet-pnl
ZETTA_WORKER_PROCESSES=4
```

## Current Service State

Enabled and active:

- `zetta-worker.service`
- `zetta-load-trades-realtime.timer`

Present but oneshot/inactive except when timer fires:

- `zetta-load-trades-realtime.service`

Disabled / inactive:

- `docker.service`
- `docker.socket`
- `containerd.service`
- `nginx.service`
- `certbot.timer`

Only SSH is listening publicly on this helper.

## Work Completed On This Helper

1. Pulled latest repo state and identified this machine as `wallet-helper-2`.
2. Changed helper master connectivity from public master IP to private master IP:
   - from `101.47.178.69`
   - to `10.17.12.97`
3. Removed unrelated local Zetta systemd unit files, keeping only:
   - `zetta-worker.service`
   - `zetta-load-trades-realtime.service`
   - `zetta-load-trades-realtime.timer`
4. Disabled non-helper services and processes:
   - Docker/containerd
   - nginx/certbot
   - API/WebSocket/frontier/chain/mart services
5. Removed cloned master data from helper:
   - `/var/lib/zetta/raw`
   - `/var/lib/zetta/state`
   - `/var/lib/zetta/quarantine`
   - local Docker database volumes
6. Cleared already loaded helper raw files from `/var/lib/zetta/wallet-raw`.
7. Fixed and pushed a scheduler bug:
   - commit `8259c39 Fix wallet helper task claiming`
   - fixed `PostgresTaskStore._claim_with_status()` parameter ordering when
     `allowed_kinds` is set.
   - without this, helpers using `--task-kind wallet-portfolio --task-kind wallet-pnl`
     failed with `malformed array literal: "wallet-helper-2-1"`.
8. Updated this node's runbook to use the master private IP:
   - `docs/operations/nodes/wallet-helper-101.47.176.154.md`

## Current Verification Snapshot

Captured on `2026-06-15 18:00:54 CST`.

- Git status: clean, `main...origin/main`
- Latest commit: `8259c39 Fix wallet helper task claiming`
- Master ClickHouse connectivity:
  - `http://10.17.12.97:8123/?query=select%201` returns `1`
- Master ClickHouse disk:
  - free: `1.48 TiB`
  - total: `1.92 TiB`
- Local raw directory:
  - `/var/lib/zetta`: `32K`
  - `/var/lib/zetta/wallet-raw` file count: `0`
- Current wallet queue:
  - no `wallet-portfolio` / `wallet-pnl` pending, running, or failed tasks at the time
    checked
- Recent helper runs:
  - no `wallet-helper-2-*` runs in the last 30 minutes because there were no wallet
    tasks available to claim

The helper is active and waiting. It should claim new `wallet-portfolio` and `wallet-pnl`
tasks automatically once the master seeds pending tasks.

## Master-Side Discussion Points

1. Helper nodes should use private master address inside the VPC:
   - Postgres task queue: `10.17.12.97:55432`
   - ClickHouse HTTP: `10.17.12.97:8123`
2. The public master ports `101.47.178.69:55432` and `101.47.178.69:8123` were not
   reachable from this helper, while private ports worked.
3. The master was previously disk constrained:
   - Postgres reported `No space left on device`
   - ClickHouse reported `NOT_ENOUGH_SPACE`
4. Master has since been expanded and ClickHouse reports `1.48 TiB` free.
5. The wallet queue was empty during the final check, so this helper had no work to claim.
6. If wallet tasks should continue, confirm master seeders are enabled and healthy:
   - `zetta-wallet-candidates.timer`
   - `zetta-wallet-pnl-candidates.timer`

## Useful Commands On This Helper

Check helper services:

```bash
systemctl status zetta-worker.service zetta-load-trades-realtime.timer --no-pager
systemctl list-timers 'zetta-*' --all --no-pager
journalctl -u zetta-worker.service -n 80 --no-pager
journalctl -u zetta-load-trades-realtime.service -n 80 --no-pager
```

Check environment:

```bash
grep -E 'ZETTA_NODE_ID|ZETTA_POSTGRES_DSN|ZETTA_CLICKHOUSE_HOST|ZETTA_RAW_DIR|ZETTA_STATE_DIR|ZETTA_WORKER_TASK_KINDS|ZETTA_WORKER_PROCESSES' /etc/zetta/zetta.env
```

Check master connectivity from helper:

```bash
timeout 5 bash -lc '</dev/tcp/10.17.12.97/55432' && echo postgres-private-open
timeout 5 bash -lc '</dev/tcp/10.17.12.97/8123' && echo clickhouse-private-open
curl -sS 'http://10.17.12.97:8123/?user=zetta&password=zetta&query=select%201'
```

Check local raw cache:

```bash
du -sh /var/lib/zetta /var/lib/zetta/wallet-raw /var/lib/zetta/wallet-state
find /var/lib/zetta/wallet-raw -type f | wc -l
```

Pause helper if master becomes disk constrained again:

```bash
sudo systemctl disable --now zetta-worker.service zetta-load-trades-realtime.timer
```

Resume helper:

```bash
sudo systemctl enable --now zetta-worker.service zetta-load-trades-realtime.timer
```
