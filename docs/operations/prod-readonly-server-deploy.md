# Prod Readonly Server Deployment

This runbook installs a low-cost production serving server for `discovery.prophet.zone`.
It is intentionally different from the stg collector deployment:

- prod serves API responses and frontend assets.
- prod runs the official realtime trade feed service.
- prod reads precomputed publish snapshots from stg.
- prod does not run ClickHouse-heavy builders, chain scanners, marts, or generic workers.

Use the current repo code on `main`. The prod server can be outside the stg LAN as long
as it can receive publish snapshots from stg and reach Polymarket realtime APIs. If the
server is inside the same LAN, snapshot sync can use private IPs.

## Target Machine

Minimum for the readonly prod node:

```text
2-4 vCPU
4-8 GB RAM
40-100 GB disk
Ubuntu 24.04 LTS
```

Use a larger machine only if prod will also build frontend assets or keep larger local
logs. The heavy ClickHouse and data generation work stays on stg.

## Directory Layout

```text
/opt/zetta                  repo checkout and Python venv
/etc/zetta/zetta.env        runtime config
/var/lib/zetta/publish      synced stg publish snapshots
/var/lib/zetta/state        small local service state
/var/www/discovery          discovery frontend static build
/usr/local/bin/zetta-runner systemd entrypoint
```

## 1. Install Base Packages

Run on the new prod server:

```bash
sudo apt-get update
sudo apt-get install -y \
  ca-certificates \
  curl \
  git \
  jq \
  nginx \
  python3 \
  python3-venv \
  rsync
```

Install Node only if the prod server will build the frontend locally:

```bash
sudo apt-get install -y nodejs npm
```

## 2. Create User And Directories

```bash
sudo groupadd --system zetta || true
sudo useradd --system --create-home --gid zetta --shell /usr/sbin/nologin zetta || true

sudo install -d -o zetta -g zetta /opt/zetta
sudo install -d -o zetta -g zetta /var/lib/zetta/raw
sudo install -d -o zetta -g zetta /var/lib/zetta/publish
sudo install -d -o zetta -g zetta /var/lib/zetta/state
sudo install -d -o www-data -g www-data /var/www/discovery
sudo install -d -m 0755 /etc/zetta
```

## 3. Pull Code

Use the repo URL that has access on the server:

```bash
sudo -u zetta git clone git@github.com:aidai524/zetta.git /opt/zetta
cd /opt/zetta
sudo -u zetta git checkout main
```

For redeploys:

```bash
cd /opt/zetta
sudo -u zetta git fetch origin
sudo -u zetta git reset --hard origin/main
```

## 4. Install Python App

```bash
cd /opt/zetta
sudo -u zetta python3 -m venv .venv
sudo -u zetta .venv/bin/pip install --upgrade pip
sudo -u zetta .venv/bin/pip install -e .
```

Smoke test:

```bash
sudo -u zetta env PYTHONPATH=/opt/zetta/src /opt/zetta/.venv/bin/python -m zetta.cli \
  --env prod \
  --serving-mode readonly \
  --disable-heavy-jobs \
  --no-enable-clickhouse-heavy-queries \
  --publish-data-dir /var/lib/zetta/publish \
  endpoints
```

Expected fields:

```json
{
  "env": "prod",
  "serving_mode": "readonly",
  "uses_publish_snapshots": "true",
  "allows_heavy_queries": "false"
}
```

## 5. Configure Runtime Env

Create `/etc/zetta/zetta.env`:

```bash
sudo tee /etc/zetta/zetta.env >/dev/null <<'EOF'
ZETTA_HOME=/opt/zetta
ZETTA_PYTHON=/opt/zetta/.venv/bin/python

ZETTA_ENV=prod
ZETTA_SERVING_MODE=readonly
ZETTA_PUBLISH_DATA_DIR=/var/lib/zetta/publish
ZETTA_DISABLE_HEAVY_JOBS=1
ZETTA_ENABLE_CLICKHOUSE_HEAVY_QUERIES=0

ZETTA_RAW_DIR=/var/lib/zetta/raw
ZETTA_STATE_DIR=/var/lib/zetta/state
ZETTA_NODE_ID=zetta-prod-1

# Kept for CLI compatibility. Prod readonly paths should not rely on these.
ZETTA_POSTGRES_DSN=postgresql://zetta:zetta@127.0.0.1:55432/zetta
ZETTA_CLICKHOUSE_HOST=127.0.0.1
ZETTA_CLICKHOUSE_PORT=8123
ZETTA_CLICKHOUSE_USER=zetta
ZETTA_CLICKHOUSE_PASSWORD=zetta
ZETTA_CLICKHOUSE_DATABASE=zetta
ZETTA_POLYGON_RPC_URL=https://polygon-bor-rpc.publicnode.com
ZETTA_HTTP_RESOLVE_OVERRIDES=

ZETTA_OFFICIAL_TRADE_FEED_PORT=8091
ZETTA_TRADE_STREAM_PORT=8090
EOF

sudo chown root:zetta /etc/zetta/zetta.env
sudo chmod 0640 /etc/zetta/zetta.env
```

## 6. Install Systemd Services

Install only the runner and the required service units:

```bash
sudo install -m 0755 /opt/zetta/infra/scripts/zetta-runner /usr/local/bin/zetta-runner
sudo install -m 0644 /opt/zetta/infra/systemd/zetta-api.service /etc/systemd/system/
sudo install -m 0644 /opt/zetta/infra/systemd/zetta-official-trade-feed.service /etc/systemd/system/
sudo install -m 0644 /opt/zetta/infra/systemd/zetta-trade-stream.service /etc/systemd/system/
sudo systemctl daemon-reload
```

Do not enable these on prod readonly:

```text
zetta-worker.service
zetta-marts.timer
zetta-wallet-rollup.timer
zetta-chain-frontier.timer
zetta-unusual-betting.timer
zetta-load.timer
```

If any were enabled accidentally:

```bash
sudo systemctl disable --now \
  zetta-worker.service \
  zetta-marts.timer \
  zetta-wallet-rollup.timer \
  zetta-chain-frontier.timer \
  zetta-unusual-betting.timer \
  zetta-load.timer || true
```

## 7. Export Snapshots On Stg

Run on the stg/data server:

```bash
cd /opt/zetta
PYTHONPATH=/opt/zetta/src /opt/zetta/.venv/bin/python -m zetta.cli \
  --publish-data-dir /var/lib/zetta/publish \
  publish export-core
```

This currently exports:

```text
wallets_screener_fifa
wallets_polycop_fifa_signals
wallets_fifa_24h_pnl
```

Inspect on stg:

```bash
PYTHONPATH=/opt/zetta/src /opt/zetta/.venv/bin/python -m zetta.cli \
  --publish-data-dir /var/lib/zetta/publish \
  publish inspect --dataset wallets_screener_fifa
```

## 8. Sync Snapshots To Prod

Push from stg to prod:

```bash
rsync -az --delete /var/lib/zetta/publish/ \
  zetta@PROD_HOST:/var/lib/zetta/publish/
```

Or pull from prod:

```bash
sudo -u zetta rsync -az --delete \
  zetta@STG_HOST:/var/lib/zetta/publish/ \
  /var/lib/zetta/publish/
```

After sync, inspect on prod:

```bash
sudo -u zetta env PYTHONPATH=/opt/zetta/src /opt/zetta/.venv/bin/python -m zetta.cli \
  --env prod \
  --serving-mode readonly \
  --publish-data-dir /var/lib/zetta/publish \
  publish inspect --dataset wallets_screener_fifa
```

For automatic refresh, schedule the stg export and rsync every 1-5 minutes. The API
reads the `current` pointer on each request, so no API restart is needed after a
snapshot sync.

Example stg cron:

```cron
* * * * * cd /opt/zetta && PYTHONPATH=/opt/zetta/src /opt/zetta/.venv/bin/python -m zetta.cli --publish-data-dir /var/lib/zetta/publish publish export-core >/var/log/zetta-publish-export.log 2>&1 && rsync -az --delete /var/lib/zetta/publish/ zetta@PROD_HOST:/var/lib/zetta/publish/ >/var/log/zetta-publish-rsync.log 2>&1
```

## 9. Start API And Realtime Feed

```bash
sudo systemctl enable --now zetta-api.service
sudo systemctl enable --now zetta-official-trade-feed.service
```

Start `zetta-trade-stream.service` only if the frontend still uses `/stream/` from the
older polling-backed trade stream:

```bash
sudo systemctl enable --now zetta-trade-stream.service
```

Check status:

```bash
systemctl status zetta-api.service zetta-official-trade-feed.service --no-pager
journalctl -u zetta-api.service -n 100 --no-pager
journalctl -u zetta-official-trade-feed.service -n 100 --no-pager
```

## 10. Verify API Locally

```bash
curl -sS 'http://127.0.0.1:8088/wallets/screener?scope=fifa&mode=whale&limit=3' | jq .
curl -sS 'http://127.0.0.1:8088/wallets/polycop-fifa-signals?limit=3&min_fifa_notional=1000&data_quality=estimate' | jq .
curl -sS 'http://127.0.0.1:8088/wallets/fifa-24h-pnl?limit=3&sort=pnl_24h&direction=desc' | jq .
```

The first two should include:

```json
{
  "publish": {
    "source": "publish_snapshot"
  }
}
```

Wallet live detail is not a publish snapshot endpoint; it calls live Polymarket wallet
APIs plus local realtime feed cache:

```bash
curl -sS 'http://127.0.0.1:8088/wallets/detail?user=0x1fd80277d4cc327a2a1440d144e13d71774e7749&live=1&position_limit=10&activity_limit=20&pnl_points_limit=0&realtime=1' | jq .
```

## 11. Deploy Discovery Frontend

The discovery frontend is `apps/analytics`.

Build on prod:

```bash
cd /opt/zetta/apps/analytics
npm ci
npm run build
sudo rsync -a --delete dist/ /var/www/discovery/
sudo chown -R www-data:www-data /var/www/discovery
```

Alternatively build on stg/CI and rsync only `apps/analytics/dist/` to prod.

## 12. Configure Nginx

```bash
sudo install -m 0644 /opt/zetta/infra/nginx/discovery.prophet.zone.conf \
  /etc/nginx/sites-available/discovery.prophet.zone.conf
sudo ln -sf /etc/nginx/sites-available/discovery.prophet.zone.conf \
  /etc/nginx/sites-enabled/discovery.prophet.zone.conf
sudo nginx -t
sudo systemctl reload nginx
```

HTTP verification:

```bash
curl -sS 'http://127.0.0.1/api/wallets/screener?scope=fifa&mode=whale&limit=1' \
  -H 'Host: discovery.prophet.zone' | jq .
```

After DNS points to the server, issue HTTPS:

```bash
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d discovery.prophet.zone
```

## 13. Release Checklist

Before switching traffic:

```bash
git -C /opt/zetta rev-parse --short HEAD
systemctl is-active zetta-api.service
systemctl is-active zetta-official-trade-feed.service
sudo -u zetta /opt/zetta/.venv/bin/python -m zetta.cli \
  --env prod \
  --serving-mode readonly \
  --publish-data-dir /var/lib/zetta/publish \
  publish inspect --dataset wallets_screener_fifa
curl -sS 'http://127.0.0.1:8088/wallets/screener?scope=fifa&mode=whale&limit=1' | jq '.publish'
```

Expected:

- current git commit matches the approved release.
- API and official feed are active.
- publish snapshot is present and recent.
- core list endpoints show `publish.source = publish_snapshot`.
- no heavy timers are active on prod.

Check no heavy timers:

```bash
systemctl list-timers 'zetta-*' --no-pager
systemctl list-units 'zetta-*' --state=running --no-pager
```

## Rollback

Code rollback:

```bash
cd /opt/zetta
sudo -u zetta git fetch origin
sudo -u zetta git reset --hard <previous_commit>
sudo -u zetta .venv/bin/pip install -e .
sudo systemctl restart zetta-api.service zetta-official-trade-feed.service
```

Snapshot rollback:

```bash
ls -1 /var/lib/zetta/publish/wallets_screener_fifa
echo '<previous_version>' | sudo tee /var/lib/zetta/publish/wallets_screener_fifa/current
sudo systemctl restart zetta-api.service
```

Repeat for the other dataset directories if needed.
