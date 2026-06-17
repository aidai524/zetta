from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


WALLET_ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")


@dataclass(frozen=True)
class TrackedWallet:
    user_address: str
    name: str
    created_at: str | None = None
    updated_at: str | None = None


def normalize_wallet_address(value: str) -> str:
    address = str(value or "").strip().lower()
    return address if WALLET_ADDRESS_RE.match(address) else ""


class TrackedWalletStore:
    def __init__(self, *, dsn: str) -> None:
        self.dsn = dsn

    def list_wallets(self) -> list[dict[str, Any]]:
        psycopg = import_psycopg()
        self.ensure_schema()
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    select user_address, name, created_at, updated_at
                    from tracked_wallets
                    order by updated_at desc, created_at desc
                    """
                )
                return [
                    wallet_row(user_address, name, created_at, updated_at)
                    for user_address, name, created_at, updated_at in cursor.fetchall()
                ]

    def upsert_wallet(self, *, user_address: str, name: str) -> dict[str, Any]:
        psycopg = import_psycopg()
        wallet = normalize_wallet_address(user_address)
        if not wallet:
            raise ValueError("invalid_wallet_address")
        clean_name = (str(name or "").strip() or short_wallet(wallet))[:120]
        self.ensure_schema()
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into tracked_wallets (user_address, name)
                    values (%s, %s)
                    on conflict (user_address) do update
                    set name = excluded.name,
                        updated_at = now()
                    returning user_address, name, created_at, updated_at
                    """,
                    (wallet, clean_name),
                )
                row = cursor.fetchone()
        return wallet_row(*row)

    def delete_wallet(self, *, user_address: str) -> bool:
        psycopg = import_psycopg()
        wallet = normalize_wallet_address(user_address)
        if not wallet:
            return False
        self.ensure_schema()
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute("delete from tracked_wallets where user_address = %s", (wallet,))
                return cursor.rowcount > 0

    def tracked_addresses(self) -> list[str]:
        return [row["user_address"] for row in self.list_wallets()]

    def ensure_schema(self) -> None:
        psycopg = import_psycopg()
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(TRACKED_WALLETS_SCHEMA_SQL)


TRACKED_WALLETS_SCHEMA_SQL = """
create table if not exists tracked_wallets (
  user_address text primary key,
  name text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_tracked_wallets_updated
  on tracked_wallets (updated_at desc);
"""


def wallet_row(
    user_address: str,
    name: str,
    created_at: Any,
    updated_at: Any,
) -> dict[str, Any]:
    wallet = normalize_wallet_address(user_address)
    return {
        "user_address": wallet,
        "address": wallet,
        "name": str(name or "") or short_wallet(wallet),
        "created_at": iso_or_none(created_at),
        "updated_at": iso_or_none(updated_at),
    }


def iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def short_wallet(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}" if len(address) >= 10 else address


def import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "Tracked wallet store requires psycopg. Install project dependencies with `pip install -e .`."
        ) from exc
    return psycopg
