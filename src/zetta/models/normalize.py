from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any


def normalize_event(event: dict[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    return {
        "event_id": as_str(event.get("id") or event.get("eventID") or event.get("event_id")),
        "ticker": as_str(event.get("ticker")),
        "slug": as_str(event.get("slug")),
        "title": as_str(event.get("title") or event.get("question")),
        "description": as_str(event.get("description")),
        "category": as_str(event.get("category")),
        "active": as_bool(event.get("active")),
        "closed": as_bool(event.get("closed")),
        "archived": as_bool(event.get("archived")),
        "start_time": parse_dt(event.get("startDate") or event.get("start_time")),
        "end_time": parse_dt(event.get("endDate") or event.get("end_time")),
        "created_at": parse_dt(event.get("createdAt") or event.get("created_at")),
        "updated_at": parse_dt(event.get("updatedAt") or event.get("updated_at")),
        "raw_json": json.dumps(event, ensure_ascii=False, separators=(",", ":")),
        "ingested_at": ingested_at,
    }


def normalize_market(
    market: dict[str, Any],
    *,
    event_id: str | None = None,
    ingested_at: datetime,
) -> dict[str, Any]:
    return {
        "market_id": as_str(market.get("id") or market.get("market_id")),
        "condition_id": as_str(market.get("conditionId") or market.get("condition_id")),
        "question": as_str(market.get("question")),
        "slug": as_str(market.get("slug")),
        "event_id": as_str(event_id or market.get("event_id") or market.get("eventId")),
        "active": as_bool(market.get("active")),
        "closed": as_bool(market.get("closed")),
        "archived": as_bool(market.get("archived")),
        "accepting_orders": as_bool(market.get("acceptingOrders") or market.get("accepting_orders")),
        "volume": as_float(market.get("volume")),
        "liquidity": as_float(market.get("liquidity")),
        "start_time": parse_dt(market.get("startDate") or market.get("start_time")),
        "end_time": parse_dt(market.get("endDate") or market.get("end_time")),
        "created_at": parse_dt(market.get("createdAt") or market.get("created_at")),
        "updated_at": parse_dt(market.get("updatedAt") or market.get("updated_at")),
        "raw_json": json.dumps(market, ensure_ascii=False, separators=(",", ":")),
        "ingested_at": ingested_at,
    }


def normalize_outcome_tokens(
    market: dict[str, Any],
    *,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    market_id = as_str(market.get("id") or market.get("market_id"))
    condition_id = as_str(market.get("conditionId") or market.get("condition_id"))
    outcomes = parse_json_list(market.get("outcomes"))
    token_ids = parse_json_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
    rows = []
    for index, token_id in enumerate(token_ids):
        outcome = outcomes[index] if index < len(outcomes) else ""
        rows.append(
            {
                "token_id": as_str(token_id),
                "market_id": market_id,
                "condition_id": condition_id,
                "outcome": as_str(outcome),
                "outcome_index": index,
                "raw_json": json.dumps(
                    {"market_id": market_id, "token_id": token_id, "outcome": outcome},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "ingested_at": ingested_at,
            }
        )
    return rows


def normalize_series(series: dict[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    return {
        "series_id": as_str(series.get("id") or series.get("series_id")),
        "ticker": as_str(series.get("ticker")),
        "slug": as_str(series.get("slug")),
        "title": as_str(series.get("title")),
        "active": as_bool(series.get("active")),
        "closed": as_bool(series.get("closed")),
        "archived": as_bool(series.get("archived")),
        "raw_json": json.dumps(series, ensure_ascii=False, separators=(",", ":")),
        "ingested_at": ingested_at,
    }


def normalize_tag(tag: dict[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    return {
        "tag_id": as_str(tag.get("id") or tag.get("tag_id")),
        "label": as_str(tag.get("label")),
        "slug": as_str(tag.get("slug")),
        "raw_json": json.dumps(tag, ensure_ascii=False, separators=(",", ":")),
        "ingested_at": ingested_at,
    }


def normalize_event_market_bridge(
    *,
    event_id: str,
    market_id: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "market_id": market_id,
        "ingested_at": ingested_at,
    }


def normalize_event_series_bridge(
    *,
    event_id: str,
    series_id: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "series_id": series_id,
        "ingested_at": ingested_at,
    }


def normalize_event_tag_bridge(
    *,
    event_id: str,
    tag_id: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "tag_id": tag_id,
        "ingested_at": ingested_at,
    }


def extract_items(payload: Any, entity: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    candidate = payload.get("data") or payload.get(entity) or payload.get("events") or payload.get("markets")
    if isinstance(candidate, list):
        return [item for item in candidate if isinstance(item, dict)]
    if isinstance(candidate, dict):
        return [candidate]
    return [payload]


def event_markets(event: dict[str, Any]) -> list[dict[str, Any]]:
    markets = event.get("markets")
    if isinstance(markets, list):
        return [market for market in markets if isinstance(market, dict)]
    return []


def event_series(event: dict[str, Any]) -> list[dict[str, Any]]:
    series = event.get("series")
    if isinstance(series, list):
        return [item for item in series if isinstance(item, dict)]
    return []


def event_tags(event: dict[str, Any]) -> list[dict[str, Any]]:
    tags = event.get("tags")
    if isinstance(tags, list):
        return [item for item in tags if isinstance(item, dict)]
    return []


def parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            value = value / 1000
        return datetime.fromtimestamp(value, UTC)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        if cleaned.isdigit():
            return parse_dt(int(cleaned))
        if cleaned.endswith("Z"):
            cleaned = f"{cleaned[:-1]}+00:00"
        try:
            return datetime.fromisoformat(cleaned).astimezone(UTC)
        except ValueError:
            return None
    return None


def as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def as_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
