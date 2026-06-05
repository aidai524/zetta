from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from zetta.chain.polymarket import (
    FEE_CHARGED_TOPIC,
    ORDER_FILLED_TOPIC,
    ORDERS_MATCHED_TOPIC,
    PAYOUT_REDEMPTION_TOPIC,
    POSITION_SPLIT_TOPIC,
    POSITIONS_MERGE_TOPIC,
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    balance_movement_rows,
    exchange_fill_row,
    fee_charged_row,
    lifecycle_event_row,
    orders_matched_row,
)
from zetta.loaders.clob import payload_hash
from zetta.models.normalize import as_bool, parse_dt
from zetta.storage.clickhouse import ClickHouseWriter
from zetta.storage.raw_reader import iter_raw_records


@dataclass(frozen=True)
class ChainLoadResult:
    raw_records: int
    skipped_raw_records: int
    chain_logs: int
    exchange_fills: int
    orders_matched: int
    fees_charged: int
    balance_movements: int
    lifecycle_events: int
    ingest_logs: int


class ChainRawLoader:
    def __init__(self, *, clickhouse: ClickHouseWriter) -> None:
        self.clickhouse = clickhouse

    def load_logs(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> ChainLoadResult:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        loaded_hashes = self.loaded_payload_hashes()
        log_rows: list[dict[str, Any]] = []
        ingest_rows: list[dict[str, Any]] = []
        raw_records = 0
        skipped = 0
        chain_logs = 0
        ingest_logs = 0

        for record in iter_raw_records(raw_root, source="polygon", entity="logs"):
            raw_records += 1
            payload = record.get("payload")
            digest = payload_hash(payload)
            already_loaded = digest in loaded_hashes
            if already_loaded and not force:
                skipped += 1
                continue
            ingested_at = parse_dt(record.get("collected_at")) or datetime.now(UTC)
            logs = payload.get("logs") if isinstance(payload, dict) else []
            if not isinstance(logs, list):
                logs = []
            if not already_loaded:
                ingest_rows.append(
                    {
                        "collected_at": ingested_at,
                        "source": "polygon",
                        "entity": "logs",
                        "request_url": str(record.get("request_url") or ""),
                        "raw_path": str(record.get("_raw_path") or ""),
                        "payload_hash": digest,
                        "item_count": len(logs),
                    }
                )
                loaded_hashes.add(digest)

            for item in logs:
                if not isinstance(item, dict):
                    continue
                log_rows.append(chain_log_row(item, ingested_at=ingested_at))

            if len(log_rows) + len(ingest_rows) >= batch_size:
                inserted_logs, inserted_ingest = self.flush(log_rows, ingest_rows)
                chain_logs += inserted_logs
                ingest_logs += inserted_ingest

        inserted_logs, inserted_ingest = self.flush(log_rows, ingest_rows)
        chain_logs += inserted_logs
        ingest_logs += inserted_ingest
        return ChainLoadResult(
            raw_records=raw_records,
            skipped_raw_records=skipped,
            chain_logs=chain_logs,
            exchange_fills=0,
            orders_matched=0,
            fees_charged=0,
            balance_movements=0,
            lifecycle_events=0,
            ingest_logs=ingest_logs,
        )

    def build_exchange_fills(self, *, batch_size: int = 10_000) -> ChainLoadResult:
        total = self.build_decoded_events(
            topic=ORDER_FILLED_TOPIC,
            table="fact_exchange_fill",
            row_builder=exchange_fill_row,
            batch_size=batch_size,
        )
        return ChainLoadResult(
            raw_records=0,
            skipped_raw_records=0,
            chain_logs=0,
            exchange_fills=total,
            orders_matched=0,
            fees_charged=0,
            balance_movements=0,
            lifecycle_events=0,
            ingest_logs=0,
        )

    def build_orders_matched(self, *, batch_size: int = 10_000) -> ChainLoadResult:
        total = self.build_decoded_events(
            topic=ORDERS_MATCHED_TOPIC,
            table="fact_orders_matched",
            row_builder=orders_matched_row,
            batch_size=batch_size,
        )
        return ChainLoadResult(
            raw_records=0,
            skipped_raw_records=0,
            chain_logs=0,
            exchange_fills=0,
            orders_matched=total,
            fees_charged=0,
            balance_movements=0,
            lifecycle_events=0,
            ingest_logs=0,
        )

    def build_fee_charged(self, *, batch_size: int = 10_000) -> ChainLoadResult:
        total = self.build_decoded_events(
            topic=FEE_CHARGED_TOPIC,
            table="fact_fee_charged",
            row_builder=fee_charged_row,
            batch_size=batch_size,
        )
        return ChainLoadResult(
            raw_records=0,
            skipped_raw_records=0,
            chain_logs=0,
            exchange_fills=0,
            orders_matched=0,
            fees_charged=total,
            balance_movements=0,
            lifecycle_events=0,
            ingest_logs=0,
        )

    def build_balance_movements(self, *, batch_size: int = 10_000) -> ChainLoadResult:
        total = 0
        for topic in (TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC):
            total += self.build_decoded_events(
                topic=topic,
                table="fact_ctf_balance_movement",
                row_builder=balance_movement_rows,
                batch_size=batch_size,
            )
        return ChainLoadResult(
            raw_records=0,
            skipped_raw_records=0,
            chain_logs=0,
            exchange_fills=0,
            orders_matched=0,
            fees_charged=0,
            balance_movements=total,
            lifecycle_events=0,
            ingest_logs=0,
        )

    def build_lifecycle_events(self, *, batch_size: int = 10_000) -> ChainLoadResult:
        total = 0
        for topic in (POSITION_SPLIT_TOPIC, POSITIONS_MERGE_TOPIC, PAYOUT_REDEMPTION_TOPIC):
            total += self.build_decoded_events(
                topic=topic,
                table="fact_ctf_lifecycle_event",
                row_builder=lifecycle_event_row,
                batch_size=batch_size,
            )
        return ChainLoadResult(
            raw_records=0,
            skipped_raw_records=0,
            chain_logs=0,
            exchange_fills=0,
            orders_matched=0,
            fees_charged=0,
            balance_movements=0,
            lifecycle_events=total,
            ingest_logs=0,
        )

    def build_decoded_events(
        self,
        *,
        topic: str,
        table: str,
        row_builder,
        batch_size: int,
    ) -> int:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        query = (
            "select chain_id, block_number, block_hash, transaction_hash, log_index, "
            "address, topic0, topics_json, data, removed, raw_json, ingested_at "
            "from fact_chain_log "
            f"where topic0 = '{topic}' "
            "order by block_number, transaction_hash, log_index "
            "format JSONEachRow"
        )
        rows: list[dict[str, Any]] = []
        total = 0
        for line in self.clickhouse.query_text(query).splitlines():
            if not line.strip():
                continue
            log = json.loads(line)
            built = row_builder(log)
            if built is None:
                continue
            if isinstance(built, list):
                rows.extend(built)
            else:
                rows.append(built)
            if len(rows) >= batch_size:
                total += self.clickhouse.insert(table, rows)
                rows.clear()
        total += self.clickhouse.insert(table, rows)
        return total

    def flush(
        self,
        log_rows: list[dict[str, Any]],
        ingest_rows: list[dict[str, Any]],
    ) -> tuple[int, int]:
        log_count = self.clickhouse.insert("fact_chain_log", log_rows)
        ingest_count = self.clickhouse.insert("raw_ingest_log", ingest_rows)
        log_rows.clear()
        ingest_rows.clear()
        return log_count, ingest_count

    def loaded_payload_hashes(self) -> set[str]:
        try:
            output = self.clickhouse.query_text(
                "SELECT payload_hash FROM raw_ingest_log "
                "WHERE source = 'polygon' AND entity = 'logs' FORMAT TSV"
            )
        except Exception:
            return set()
        return {line.strip() for line in output.splitlines() if line.strip()}


def chain_log_row(log: dict[str, Any], *, ingested_at: datetime) -> dict[str, Any]:
    topics = log.get("topics") if isinstance(log.get("topics"), list) else []
    return {
        "chain_id": 137,
        "block_number": hex_int(log.get("blockNumber")),
        "block_hash": str(log.get("blockHash") or ""),
        "transaction_hash": str(log.get("transactionHash") or ""),
        "log_index": hex_int(log.get("logIndex")),
        "address": str(log.get("address") or "").lower(),
        "topic0": str(topics[0] if topics else ""),
        "topics_json": json.dumps(topics, ensure_ascii=False, separators=(",", ":")),
        "data": str(log.get("data") or ""),
        "removed": as_bool(log.get("removed")),
        "raw_json": json.dumps(log, ensure_ascii=False, separators=(",", ":")),
        "ingested_at": ingested_at,
    }


def hex_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.startswith("0x"):
            return int(cleaned, 16)
        if cleaned.isdigit():
            return int(cleaned)
    return 0
