from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
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
    raw_chunk_records: int = 1
    raw_chunk_seconds: float = 60.0
    request_timeout_seconds: float = 30.0
    user_agent: str = "ZettaPolymarketCollector/0.1"
    http_resolve_overrides: str = ""


settings = Settings()
