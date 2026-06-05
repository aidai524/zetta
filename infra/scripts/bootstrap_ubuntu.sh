#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root: sudo bash infra/scripts/bootstrap_ubuntu.sh" >&2
  exit 1
fi

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ZETTA_HOME="${ZETTA_HOME:-/opt/zetta}"
ZETTA_USER="${ZETTA_USER:-zetta}"
ZETTA_GROUP="${ZETTA_GROUP:-zetta}"

apt-get update
apt-get install -y ca-certificates curl git python3 python3-venv rsync

if ! command -v docker >/dev/null 2>&1; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

if ! getent group "$ZETTA_GROUP" >/dev/null 2>&1; then
  groupadd --system "$ZETTA_GROUP"
fi

if ! id "$ZETTA_USER" >/dev/null 2>&1; then
  useradd --system --create-home --gid "$ZETTA_GROUP" --shell /usr/sbin/nologin "$ZETTA_USER"
fi

install -d -o "$ZETTA_USER" -g "$ZETTA_GROUP" "$ZETTA_HOME"
rsync -a --delete \
  --exclude '.git' \
  --exclude '.pytest_cache' \
  --exclude '.venv' \
  --exclude 'data/raw' \
  --exclude 'data/state' \
  "$SOURCE_DIR/" "$ZETTA_HOME/"
chown -R "$ZETTA_USER:$ZETTA_GROUP" "$ZETTA_HOME"

install -d -o "$ZETTA_USER" -g "$ZETTA_GROUP" /var/lib/zetta/raw /var/lib/zetta/state
install -d -m 0755 /etc/zetta
if [[ ! -f /etc/zetta/zetta.env ]]; then
  install -m 0640 -o root -g "$ZETTA_GROUP" "$ZETTA_HOME/infra/systemd/zetta.env.example" /etc/zetta/zetta.env
fi

python3 -m venv "$ZETTA_HOME/.venv"
"$ZETTA_HOME/.venv/bin/pip" install --upgrade pip
"$ZETTA_HOME/.venv/bin/pip" install -e "$ZETTA_HOME"
chown -R "$ZETTA_USER:$ZETTA_GROUP" "$ZETTA_HOME/.venv"

docker compose -f "$ZETTA_HOME/docker-compose.yml" up -d

install -m 0755 "$ZETTA_HOME/infra/scripts/zetta-runner" /usr/local/bin/zetta-runner
install -m 0644 "$ZETTA_HOME"/infra/systemd/*.service /etc/systemd/system/
install -m 0644 "$ZETTA_HOME"/infra/systemd/*.timer /etc/systemd/system/

systemctl daemon-reload

"$ZETTA_HOME/.venv/bin/python" -m zetta.cli \
  --clickhouse-host 127.0.0.1 \
  --clickhouse-port 8123 \
  --clickhouse-user zetta \
  --clickhouse-password zetta \
  --clickhouse-database zetta \
  db migrate

systemctl enable --now zetta-worker.service
systemctl enable --now zetta-api.service
systemctl enable --now zetta-ws-market.service
systemctl enable --now zetta-load.timer
systemctl enable --now zetta-marts.timer

cat <<'EOF'
Zetta server bootstrap complete.

Next:
  1. Edit /etc/zetta/zetta.env for production RPC, resolve overrides, and worker sizing.
  2. Seed backfill work with:
     cd /opt/zetta
     .venv/bin/python -m zetta.cli --task-store postgres tasks seed-basic --page-limit 100 --max-pages 0
     .venv/bin/python -m zetta.cli --task-store postgres tasks seed-history --active-only --chain-from-block <block> --chain-to-block <block>
  3. Watch:
     systemctl status zetta-worker zetta-ws-market zetta-load.timer
     journalctl -u zetta-worker -f
EOF
