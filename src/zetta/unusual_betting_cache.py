from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


UNUSUAL_BETTING_CACHE_SCHEMA_SQL = """
create table if not exists unusual_betting_cache (
  cache_key text primary key,
  event_id text not null default '',
  event_slug text not null default '',
  event_title text not null default '',
  status text not null default '',
  severity text not null default 'none',
  abnormal_wallet_count integer not null default 0,
  max_abnormal_wallet_notional double precision not null default 0,
  signal_total_notional double precision not null default 0,
  parameters jsonb not null default '{}'::jsonb,
  summary jsonb not null default '{}'::jsonb,
  detail jsonb not null default '{}'::jsonb,
  trigger_reason text not null default '',
  generated_at timestamptz,
  refreshed_at timestamptz not null default now(),
  error text
);

create index if not exists idx_unusual_betting_cache_refreshed
  on unusual_betting_cache (refreshed_at desc);

create index if not exists idx_unusual_betting_cache_event
  on unusual_betting_cache (event_slug, refreshed_at desc);

create index if not exists idx_unusual_betting_cache_severity
  on unusual_betting_cache (severity, refreshed_at desc);
"""


class UnusualBettingCacheStore:
    def __init__(self, *, dsn: str) -> None:
        self.dsn = dsn

    def ensure_schema(self) -> None:
        psycopg, _Jsonb = import_psycopg()
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(UNUSUAL_BETTING_CACHE_SCHEMA_SQL)

    def get(self, cache_key: str, *, max_age_seconds: int | None = None) -> dict[str, Any] | None:
        psycopg, _Jsonb = import_psycopg()
        self.ensure_schema()
        where = "cache_key = %s"
        params: list[Any] = [cache_key]
        if max_age_seconds is not None and max_age_seconds > 0:
            where += " and refreshed_at >= now() - (%s::text || ' seconds')::interval"
            params.append(max_age_seconds)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    select
                      cache_key,
                      event_id,
                      event_slug,
                      event_title,
                      status,
                      severity,
                      abnormal_wallet_count,
                      max_abnormal_wallet_notional,
                      signal_total_notional,
                      parameters,
                      summary,
                      detail,
                      trigger_reason,
                      generated_at,
                      refreshed_at,
                      error
                    from unusual_betting_cache
                    where {where}
                    limit 1
                    """,
                    params,
                )
                row = cursor.fetchone()
        return cache_row(row) if row else None

    def upsert(
        self,
        *,
        cache_key: str,
        event_id: str,
        event_slug: str,
        event_title: str,
        status: str,
        severity: str,
        abnormal_wallet_count: int,
        max_abnormal_wallet_notional: float,
        signal_total_notional: float,
        parameters: dict[str, Any],
        summary: dict[str, Any],
        detail: dict[str, Any],
        trigger_reason: str,
        generated_at: Any = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        psycopg, Jsonb = import_psycopg()
        self.ensure_schema()
        generated_dt = parse_datetime_or_none(generated_at)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into unusual_betting_cache
                      (
                        cache_key,
                        event_id,
                        event_slug,
                        event_title,
                        status,
                        severity,
                        abnormal_wallet_count,
                        max_abnormal_wallet_notional,
                        signal_total_notional,
                        parameters,
                        summary,
                        detail,
                        trigger_reason,
                        generated_at,
                        refreshed_at,
                        error
                      )
                    values
                      (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                    on conflict (cache_key) do update
                    set event_id = excluded.event_id,
                        event_slug = excluded.event_slug,
                        event_title = excluded.event_title,
                        status = excluded.status,
                        severity = excluded.severity,
                        abnormal_wallet_count = excluded.abnormal_wallet_count,
                        max_abnormal_wallet_notional = excluded.max_abnormal_wallet_notional,
                        signal_total_notional = excluded.signal_total_notional,
                        parameters = excluded.parameters,
                        summary = excluded.summary,
                        detail = excluded.detail,
                        trigger_reason = excluded.trigger_reason,
                        generated_at = excluded.generated_at,
                        refreshed_at = now(),
                        error = excluded.error
                    returning
                      cache_key,
                      event_id,
                      event_slug,
                      event_title,
                      status,
                      severity,
                      abnormal_wallet_count,
                      max_abnormal_wallet_notional,
                      signal_total_notional,
                      parameters,
                      summary,
                      detail,
                      trigger_reason,
                      generated_at,
                      refreshed_at,
                      error
                    """,
                    (
                        cache_key,
                        event_id,
                        event_slug,
                        event_title,
                        status,
                        severity,
                        abnormal_wallet_count,
                        max_abnormal_wallet_notional,
                        signal_total_notional,
                        Jsonb(parameters),
                        Jsonb(summary),
                        Jsonb(detail),
                        trigger_reason,
                        generated_dt,
                        error,
                    ),
                )
                row = cursor.fetchone()
        return cache_row(row)


def cache_row(row: Any) -> dict[str, Any]:
    (
        cache_key,
        event_id,
        event_slug,
        event_title,
        status,
        severity,
        abnormal_wallet_count,
        max_abnormal_wallet_notional,
        signal_total_notional,
        parameters,
        summary,
        detail,
        trigger_reason,
        generated_at,
        refreshed_at,
        error,
    ) = row
    return {
        "cache_key": str(cache_key or ""),
        "event_id": str(event_id or ""),
        "event_slug": str(event_slug or ""),
        "event_title": str(event_title or ""),
        "status": str(status or ""),
        "severity": str(severity or "none"),
        "abnormal_wallet_count": int(abnormal_wallet_count or 0),
        "max_abnormal_wallet_notional": float(max_abnormal_wallet_notional or 0.0),
        "signal_total_notional": float(signal_total_notional or 0.0),
        "parameters": dict(parameters or {}),
        "summary": dict(summary or {}),
        "detail": dict(detail or {}),
        "trigger_reason": str(trigger_reason or ""),
        "generated_at": iso_or_none(generated_at),
        "refreshed_at": iso_or_none(refreshed_at),
        "age_seconds": age_seconds(refreshed_at),
        "error": str(error) if error else None,
    }


def parse_datetime_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed
    return None


def age_seconds(value: Any) -> float | None:
    parsed = parse_datetime_or_none(value)
    if parsed is None:
        return None
    return round((datetime.now(UTC) - parsed.astimezone(UTC)).total_seconds(), 3)


def iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def import_psycopg():
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "Unusual betting cache requires psycopg. Install project dependencies with `pip install -e .`."
        ) from exc
    return psycopg, Jsonb
