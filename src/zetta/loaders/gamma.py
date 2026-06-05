from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from zetta.models.normalize import (
    as_str,
    event_markets,
    event_series,
    event_tags,
    extract_items,
    normalize_event,
    normalize_event_market_bridge,
    normalize_event_series_bridge,
    normalize_event_tag_bridge,
    normalize_market,
    normalize_outcome_tokens,
    normalize_series,
    normalize_tag,
    parse_dt,
)
from zetta.storage.clickhouse import ClickHouseWriter
from zetta.storage.raw_reader import iter_raw_records


@dataclass(frozen=True)
class GammaLoadResult:
    raw_records: int
    skipped_raw_records: int
    events: int
    markets: int
    outcome_tokens: int
    series: int
    tags: int
    event_markets: int
    event_series: int
    event_tags: int
    ingest_logs: int

    def add(self, table: str, count: int) -> None:
        if table == "raw_ingest_log":
            object.__setattr__(self, "ingest_logs", self.ingest_logs + count)
        elif table == "dim_event":
            object.__setattr__(self, "events", self.events + count)
        elif table == "dim_market":
            object.__setattr__(self, "markets", self.markets + count)
        elif table == "dim_outcome_token":
            object.__setattr__(self, "outcome_tokens", self.outcome_tokens + count)
        elif table == "dim_series":
            object.__setattr__(self, "series", self.series + count)
        elif table == "dim_tag":
            object.__setattr__(self, "tags", self.tags + count)
        elif table == "bridge_event_market":
            object.__setattr__(self, "event_markets", self.event_markets + count)
        elif table == "bridge_event_series":
            object.__setattr__(self, "event_series", self.event_series + count)
        elif table == "bridge_event_tag":
            object.__setattr__(self, "event_tags", self.event_tags + count)


class GammaRawLoader:
    def __init__(self, *, clickhouse: ClickHouseWriter) -> None:
        self.clickhouse = clickhouse

    def load(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> GammaLoadResult:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        loaded_hashes = self.loaded_payload_hashes()
        batch = GammaLoadBatch()
        result = GammaLoadResult(
            raw_records=0,
            skipped_raw_records=0,
            events=0,
            markets=0,
            outcome_tokens=0,
            series=0,
            tags=0,
            event_markets=0,
            event_series=0,
            event_tags=0,
            ingest_logs=0,
        )

        for record in iter_raw_records(raw_root, source="gamma"):
            object.__setattr__(result, "raw_records", result.raw_records + 1)
            entity = str(record.get("entity") or "")
            payload = record.get("payload")
            digest = payload_hash(payload)
            already_loaded = digest in loaded_hashes
            if already_loaded and not force:
                object.__setattr__(result, "skipped_raw_records", result.skipped_raw_records + 1)
                continue
            ingested_at = parse_dt(record.get("collected_at")) or datetime.now(UTC)
            items = extract_items(payload, entity)

            if not already_loaded:
                batch.raw_ingest_log.append(
                    {
                        "collected_at": ingested_at,
                        "source": "gamma",
                        "entity": entity,
                        "request_url": str(record.get("request_url") or ""),
                        "raw_path": str(record.get("_raw_path") or ""),
                        "payload_hash": digest,
                        "item_count": len(items),
                    }
                )
                loaded_hashes.add(digest)

            if entity == "events":
                for event in items:
                    event_row = normalize_event(event, ingested_at=ingested_at)
                    batch.dim_event.append(event_row)
                    event_id = event_row["event_id"]
                    for market in event_markets(event):
                        market_id = as_str(market.get("id") or market.get("market_id"))
                        batch.dim_market.append(
                            normalize_market(market, event_id=event_id, ingested_at=ingested_at)
                        )
                        if event_id and market_id:
                            batch.bridge_event_market.append(
                                normalize_event_market_bridge(
                                    event_id=event_id,
                                    market_id=market_id,
                                    ingested_at=ingested_at,
                                )
                            )
                        batch.dim_outcome_token.extend(
                            normalize_outcome_tokens(market, ingested_at=ingested_at)
                        )
                    for series in event_series(event):
                        series_id = as_str(series.get("id") or series.get("series_id"))
                        batch.dim_series.append(normalize_series(series, ingested_at=ingested_at))
                        if event_id and series_id:
                            batch.bridge_event_series.append(
                                normalize_event_series_bridge(
                                    event_id=event_id,
                                    series_id=series_id,
                                    ingested_at=ingested_at,
                                )
                            )
                    for tag in event_tags(event):
                        tag_id = as_str(tag.get("id") or tag.get("tag_id"))
                        batch.dim_tag.append(normalize_tag(tag, ingested_at=ingested_at))
                        if event_id and tag_id:
                            batch.bridge_event_tag.append(
                                normalize_event_tag_bridge(
                                    event_id=event_id,
                                    tag_id=tag_id,
                                    ingested_at=ingested_at,
                                )
                            )
            elif entity == "markets":
                for market in items:
                    batch.dim_market.append(normalize_market(market, ingested_at=ingested_at))
                    batch.dim_outcome_token.extend(
                        normalize_outcome_tokens(market, ingested_at=ingested_at)
                    )

            if batch.row_count >= batch_size:
                batch.flush(self.clickhouse, result)

        batch.flush(self.clickhouse, result)
        return result

    def loaded_payload_hashes(self) -> set[str]:
        try:
            output = self.clickhouse.query_text(
                "SELECT payload_hash FROM raw_ingest_log WHERE source = 'gamma' FORMAT TSV"
            )
        except Exception:
            return set()
        return {line.strip() for line in output.splitlines() if line.strip()}


@dataclass
class GammaLoadBatch:
    raw_ingest_log: list[dict[str, Any]]
    dim_event: list[dict[str, Any]]
    dim_market: list[dict[str, Any]]
    dim_outcome_token: list[dict[str, Any]]
    dim_series: list[dict[str, Any]]
    dim_tag: list[dict[str, Any]]
    bridge_event_market: list[dict[str, Any]]
    bridge_event_series: list[dict[str, Any]]
    bridge_event_tag: list[dict[str, Any]]

    def __init__(self) -> None:
        self.raw_ingest_log = []
        self.dim_event = []
        self.dim_market = []
        self.dim_outcome_token = []
        self.dim_series = []
        self.dim_tag = []
        self.bridge_event_market = []
        self.bridge_event_series = []
        self.bridge_event_tag = []

    @property
    def row_count(self) -> int:
        return sum(len(rows) for _table, rows in self.table_rows())

    def flush(self, clickhouse: ClickHouseWriter, result: GammaLoadResult) -> None:
        for table, rows in self.table_rows():
            if rows:
                clickhouse.insert(table, rows)
                result.add(table, len(rows))
        self.__init__()

    def table_rows(self) -> list[tuple[str, list[dict[str, Any]]]]:
        return [
            ("raw_ingest_log", self.raw_ingest_log),
            ("dim_event", self.dim_event),
            ("dim_market", self.dim_market),
            ("dim_outcome_token", self.dim_outcome_token),
            ("dim_series", self.dim_series),
            ("dim_tag", self.dim_tag),
            ("bridge_event_market", self.bridge_event_market),
            ("bridge_event_series", self.bridge_event_series),
            ("bridge_event_tag", self.bridge_event_tag),
        ]


def payload_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()
