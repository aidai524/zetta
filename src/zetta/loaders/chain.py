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
from zetta.loaders.incremental import (
    clickhouse_state_dir,
    loaded_max_raw_path,
    loaded_payload_hashes,
    loaded_payload_hashes_for_paths,
    loader_checkpoint_raw_path,
    save_loader_checkpoint_raw_path,
    sql_string,
)
from zetta.loaders.parallel import raw_paths
from zetta.models.normalize import as_bool, parse_dt
from zetta.storage.clickhouse import ClickHouseWriter
from zetta.storage.raw_reader import iter_raw_records, iter_raw_records_from_paths
from zetta.storage.state import LocalStateStore


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


@dataclass(frozen=True)
class ChainDecodeCheckpoint:
    block_number: int
    transaction_hash: str
    log_index: int


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
        checkpoint_path = None if force else self.loader_checkpoint_raw_path()
        database_after_path = None if force else self.loaded_max_raw_path()
        after_path = checkpoint_path or database_after_path
        paths = (
            raw_paths(raw_root, source="polygon", entity="logs", after_path=after_path)
            if not force and after_path is not None
            else None
        )
        if not force and checkpoint_path is None and database_after_path is not None:
            self.save_loader_checkpoint_raw_path(database_after_path)
        loaded_hashes = (
            loaded_payload_hashes_for_paths(
                self.clickhouse,
                source="polygon",
                entity="logs",
                paths=paths,
            )
            if paths is not None
            else self.loaded_payload_hashes()
        )
        log_rows: list[dict[str, Any]] = []
        ingest_rows: list[dict[str, Any]] = []
        raw_records = 0
        skipped = 0
        chain_logs = 0
        ingest_logs = 0
        last_raw_path = ""

        records = (
            iter_raw_records_from_paths(paths, after_path=after_path)
            if paths is not None
            else iter_raw_records(
                raw_root,
                source="polygon",
                entity="logs",
                after_path=after_path,
            )
        )
        for record in records:
            raw_records += 1
            last_raw_path = str(record.get("_raw_path") or last_raw_path)
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
        if not force and last_raw_path:
            self.save_loader_checkpoint_raw_path(last_raw_path)
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

    def build_exchange_fills(
        self, *, batch_size: int = 10_000, force: bool = False
    ) -> ChainLoadResult:
        table = "fact_exchange_fill"
        total = self.build_decoded_events(
            topic=ORDER_FILLED_TOPIC,
            table=table,
            row_builder=exchange_fill_row,
            batch_size=batch_size,
            checkpoint=None if force else self.decode_checkpoint(table, ORDER_FILLED_TOPIC),
            force=force,
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

    def build_orders_matched(
        self, *, batch_size: int = 10_000, force: bool = False
    ) -> ChainLoadResult:
        table = "fact_orders_matched"
        total = self.build_decoded_events(
            topic=ORDERS_MATCHED_TOPIC,
            table=table,
            row_builder=orders_matched_row,
            batch_size=batch_size,
            checkpoint=None if force else self.decode_checkpoint(table, ORDERS_MATCHED_TOPIC),
            force=force,
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

    def build_fee_charged(
        self, *, batch_size: int = 10_000, force: bool = False
    ) -> ChainLoadResult:
        table = "fact_fee_charged"
        total = self.build_decoded_events(
            topic=FEE_CHARGED_TOPIC,
            table=table,
            row_builder=fee_charged_row,
            batch_size=batch_size,
            checkpoint=None if force else self.decode_checkpoint(table, FEE_CHARGED_TOPIC),
            force=force,
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

    def build_balance_movements(
        self, *, batch_size: int = 10_000, force: bool = False
    ) -> ChainLoadResult:
        table = "fact_ctf_balance_movement"
        default_checkpoint = None if force else self.table_decode_checkpoint(table)
        total = 0
        for topic in (TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC):
            total += self.build_decoded_events(
                topic=topic,
                table=table,
                row_builder=balance_movement_rows,
                batch_size=batch_size,
                checkpoint=None
                if force
                else self.saved_decode_checkpoint(table, topic) or default_checkpoint,
                force=force,
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

    def build_lifecycle_events(
        self, *, batch_size: int = 10_000, force: bool = False
    ) -> ChainLoadResult:
        table = "fact_ctf_lifecycle_event"
        default_checkpoint = None if force else self.table_decode_checkpoint(table)
        total = 0
        for topic in (POSITION_SPLIT_TOPIC, POSITIONS_MERGE_TOPIC, PAYOUT_REDEMPTION_TOPIC):
            total += self.build_decoded_events(
                topic=topic,
                table=table,
                row_builder=lifecycle_event_row,
                batch_size=batch_size,
                checkpoint=None
                if force
                else self.saved_decode_checkpoint(table, topic) or default_checkpoint,
                force=force,
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
        checkpoint: ChainDecodeCheckpoint | None = None,
        force: bool = False,
    ) -> int:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        where = f"where topic0 = {sql_string(topic)}"
        if checkpoint is not None:
            where += (
                " and (block_number, transaction_hash, log_index) > "
                f"({checkpoint.block_number}, {sql_string(checkpoint.transaction_hash)}, "
                f"{checkpoint.log_index})"
            )
        query = (
            "select chain_id, block_number, block_hash, transaction_hash, log_index, "
            "address, topic0, topics_json, data, removed, raw_json, ingested_at "
            "from fact_chain_log "
            f"{where} "
            "order by block_number, transaction_hash, log_index "
            "format JSONEachRow"
        )
        rows: list[dict[str, Any]] = []
        total = 0
        last_seen: ChainDecodeCheckpoint | None = None
        for line in self.clickhouse.query_text(query).splitlines():
            if not line.strip():
                continue
            log = json.loads(line)
            last_seen = checkpoint_from_log(log)
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
        if not force:
            next_checkpoint = last_seen or checkpoint or self.source_decode_checkpoint()
            if next_checkpoint is not None:
                self.save_decode_checkpoint(table, topic, next_checkpoint)
        return total

    def decode_checkpoint(self, table: str, topic: str) -> ChainDecodeCheckpoint | None:
        return self.saved_decode_checkpoint(table, topic) or self.table_decode_checkpoint(table)

    def saved_decode_checkpoint(
        self, table: str, topic: str
    ) -> ChainDecodeCheckpoint | None:
        state_dir = clickhouse_state_dir(self.clickhouse)
        if state_dir is None:
            return None
        try:
            value = LocalStateStore(state_dir).get(decode_checkpoint_key(table, topic), {})
        except Exception:
            return None
        return checkpoint_from_mapping(value if isinstance(value, dict) else {})

    def save_decode_checkpoint(
        self, table: str, topic: str, checkpoint: ChainDecodeCheckpoint
    ) -> None:
        state_dir = clickhouse_state_dir(self.clickhouse)
        if state_dir is None:
            return
        LocalStateStore(state_dir).set(
            decode_checkpoint_key(table, topic),
            {
                "block_number": checkpoint.block_number,
                "transaction_hash": checkpoint.transaction_hash,
                "log_index": checkpoint.log_index,
            },
        )

    def table_decode_checkpoint(self, table: str) -> ChainDecodeCheckpoint | None:
        query = f"""
            select block_number, transaction_hash, log_index
            from {table}
            where block_number > 0
            order by block_number desc, transaction_hash desc, log_index desc
            limit 1
            format JSONEachRow
        """
        return first_checkpoint(self.clickhouse.query_text(query))

    def source_decode_checkpoint(self) -> ChainDecodeCheckpoint | None:
        query = """
            select block_number, transaction_hash, log_index
            from fact_chain_log
            where block_number > 0
            order by block_number desc, transaction_hash desc, log_index desc
            limit 1
            format JSONEachRow
        """
        return first_checkpoint(self.clickhouse.query_text(query))

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
        return loaded_payload_hashes(self.clickhouse, source="polygon", entity="logs")

    def loaded_max_raw_path(self) -> str | None:
        return loaded_max_raw_path(self.clickhouse, source="polygon", entity="logs")

    def loader_checkpoint_raw_path(self) -> str | None:
        return loader_checkpoint_raw_path(
            clickhouse_state_dir(self.clickhouse),
            source="polygon",
            entity="logs",
        )

    def save_loader_checkpoint_raw_path(self, raw_path: str) -> None:
        save_loader_checkpoint_raw_path(
            clickhouse_state_dir(self.clickhouse),
            source="polygon",
            entity="logs",
            raw_path=raw_path,
        )


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


def decode_checkpoint_key(table: str, topic: str) -> str:
    cleaned_topic = topic.lower().removeprefix("0x")
    return f"chain_decode_checkpoints/{table}_{cleaned_topic}"


def first_checkpoint(text: str) -> ChainDecodeCheckpoint | None:
    for line in text.splitlines():
        if not line.strip():
            continue
        return checkpoint_from_mapping(json.loads(line))
    return None


def checkpoint_from_mapping(row: dict[str, Any]) -> ChainDecodeCheckpoint | None:
    block_number = int(row.get("block_number") or 0)
    transaction_hash = str(row.get("transaction_hash") or "")
    log_index = int(row.get("log_index") or 0)
    if block_number <= 0:
        return None
    return ChainDecodeCheckpoint(
        block_number=block_number,
        transaction_hash=transaction_hash,
        log_index=log_index,
    )


def checkpoint_from_log(log: dict[str, Any]) -> ChainDecodeCheckpoint:
    return ChainDecodeCheckpoint(
        block_number=int(log.get("block_number") or 0),
        transaction_hash=str(log.get("transaction_hash") or ""),
        log_index=int(log.get("log_index") or 0),
    )
