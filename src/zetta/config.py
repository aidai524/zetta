from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    env: str = os.getenv("ZETTA_ENV", "stg")
    serving_mode: str = os.getenv("ZETTA_SERVING_MODE", "compute")
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    data_base_url: str = "https://data-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    clob_ws_market_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    polygon_rpc_url: str = "https://polygon-bor-rpc.publicnode.com"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "zetta"
    clickhouse_password: str = "zetta"
    clickhouse_database: str = "zetta"
    postgres_dsn: str = "postgresql://zetta:zetta@localhost:55432/zetta"
    raw_data_dir: Path = Path("data/raw")
    state_dir: Path = Path("data/state")
    publish_data_dir: Path = Path(os.getenv("ZETTA_PUBLISH_DATA_DIR", "data/publish"))
    raw_chunk_records: int = 1
    raw_chunk_seconds: float = 60.0
    request_timeout_seconds: float = 30.0
    user_agent: str = "ZettaPolymarketCollector/0.1"
    http_resolve_overrides: str = ""
    disable_heavy_jobs: bool = env_bool("ZETTA_DISABLE_HEAVY_JOBS", False)
    enable_clickhouse_heavy_queries: bool = env_bool("ZETTA_ENABLE_CLICKHOUSE_HEAVY_QUERIES", True)

    def is_prod(self) -> bool:
        return self.env.strip().lower() == "prod"

    def uses_publish_snapshots(self) -> bool:
        return self.serving_mode.strip().lower() in {"prod", "readonly", "serve"} or self.is_prod()

    def allows_heavy_queries(self) -> bool:
        return self.enable_clickhouse_heavy_queries and not self.disable_heavy_jobs


settings = Settings()
