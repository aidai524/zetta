from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from zetta.models.normalize import as_float, as_str, parse_dt
from zetta.storage.clickhouse import ClickHouseWriter
from zetta.storage.raw_reader import iter_raw_records


@dataclass(frozen=True)
class DataLoadResult:
    raw_records: int
    skipped_raw_records: int
    trades: int
    activities: int
    holders: int
    market_positions: int
    open_interest: int
    ingest_logs: int


class DataRawLoader:
    def __init__(self, *, clickhouse: ClickHouseWriter) -> None:
        self.clickhouse = clickhouse

    def load_trades(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> DataLoadResult:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        loaded_hashes = self.loaded_payload_hashes()
        trade_rows: list[dict[str, Any]] = []
        log_rows: list[dict[str, Any]] = []
        raw_records = 0
        skipped = 0
        trades = 0
        logs = 0

        for record in iter_raw_records(raw_root, source="data", entity="trades"):
            raw_records += 1
            payload = record.get("payload")
            digest = payload_hash(payload)
            already_loaded = digest in loaded_hashes
            if already_loaded and not force:
                skipped += 1
                continue
            ingested_at = parse_dt(record.get("collected_at")) or datetime.now(UTC)
            items = payload if isinstance(payload, list) else []
            if not already_loaded:
                log_rows.append(
                    {
                        "collected_at": ingested_at,
                        "source": "data",
                        "entity": "trades",
                        "request_url": str(record.get("request_url") or ""),
                        "raw_path": str(record.get("_raw_path") or ""),
                        "payload_hash": digest,
                        "item_count": len(items),
                    }
                )
                loaded_hashes.add(digest)

            for index, trade in enumerate(items):
                if not isinstance(trade, dict):
                    continue
                timestamp = parse_dt(trade.get("timestamp"))
                if timestamp is None:
                    continue
                transaction_hash = as_str(trade.get("transactionHash"))
                token_id = as_str(trade.get("asset"))
                price = as_float(trade.get("price"))
                size = as_float(trade.get("size"))
                trade_rows.append(
                    {
                        "trade_id": trade_id(transaction_hash, token_id, timestamp, index),
                        "transaction_hash": transaction_hash,
                        "log_index": index,
                        "timestamp": timestamp,
                        "market_id": "",
                        "condition_id": as_str(trade.get("conditionId")),
                        "token_id": token_id,
                        "user_address": as_str(trade.get("proxyWallet")).lower(),
                        "side": as_str(trade.get("side")),
                        "price": price,
                        "size": size,
                        "notional": price * size,
                        "source": "data",
                        "raw_json": json.dumps(trade, ensure_ascii=False, separators=(",", ":")),
                        "ingested_at": ingested_at,
                    }
                )
            if len(trade_rows) + len(log_rows) >= batch_size:
                inserted_trades, inserted_logs = self.flush(trade_rows, log_rows)
                trades += inserted_trades
                logs += inserted_logs

        inserted_trades, inserted_logs = self.flush(trade_rows, log_rows)
        trades += inserted_trades
        logs += inserted_logs
        return DataLoadResult(
            raw_records=raw_records,
            skipped_raw_records=skipped,
            trades=trades,
            activities=0,
            holders=0,
            market_positions=0,
            open_interest=0,
            ingest_logs=logs,
        )

    def load_activity(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> DataLoadResult:
        rows, logs, raw_records, skipped = self._load_rows(
            raw_root=raw_root,
            entity="activity",
            force=force,
            row_builder=activity_rows,
        )
        return self._insert_data_rows(
            table="fact_user_activity",
            rows=rows,
            logs=logs,
            raw_records=raw_records,
            skipped=skipped,
            field="activities",
            batch_size=batch_size,
        )

    def load_holders(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> DataLoadResult:
        rows, logs, raw_records, skipped = self._load_rows(
            raw_root=raw_root,
            entity="holders",
            force=force,
            row_builder=holder_rows,
        )
        return self._insert_data_rows(
            table="fact_market_holder_snapshot",
            rows=rows,
            logs=logs,
            raw_records=raw_records,
            skipped=skipped,
            field="holders",
            batch_size=batch_size,
        )

    def load_market_positions(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> DataLoadResult:
        rows, logs, raw_records, skipped = self._load_rows(
            raw_root=raw_root,
            entity="market_positions",
            force=force,
            row_builder=market_position_rows,
        )
        return self._insert_data_rows(
            table="fact_market_position_snapshot",
            rows=rows,
            logs=logs,
            raw_records=raw_records,
            skipped=skipped,
            field="market_positions",
            batch_size=batch_size,
        )

    def load_open_interest(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> DataLoadResult:
        rows, logs, raw_records, skipped = self._load_rows(
            raw_root=raw_root,
            entity="open_interest",
            force=force,
            row_builder=open_interest_rows,
        )
        return self._insert_data_rows(
            table="fact_open_interest_snapshot",
            rows=rows,
            logs=logs,
            raw_records=raw_records,
            skipped=skipped,
            field="open_interest",
            batch_size=batch_size,
        )

    def flush(
        self,
        trade_rows: list[dict[str, Any]],
        log_rows: list[dict[str, Any]],
    ) -> tuple[int, int]:
        trade_count = self.clickhouse.insert("fact_trade", trade_rows)
        log_count = self.clickhouse.insert("raw_ingest_log", log_rows)
        trade_rows.clear()
        log_rows.clear()
        return trade_count, log_count

    def _load_rows(
        self,
        *,
        raw_root: Path,
        entity: str,
        force: bool,
        row_builder,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int]:
        loaded_hashes = self.loaded_payload_hashes(entity)
        rows: list[dict[str, Any]] = []
        logs: list[dict[str, Any]] = []
        raw_records = 0
        skipped = 0
        for record in iter_raw_records(raw_root, source="data", entity=entity):
            raw_records += 1
            payload = record.get("payload")
            digest = payload_hash(payload)
            already_loaded = digest in loaded_hashes
            if already_loaded and not force:
                skipped += 1
                continue
            ingested_at = parse_dt(record.get("collected_at")) or datetime.now(UTC)
            built_rows = row_builder(payload, ingested_at)
            if not already_loaded:
                logs.append(
                    {
                        "collected_at": ingested_at,
                        "source": "data",
                        "entity": entity,
                        "request_url": str(record.get("request_url") or ""),
                        "raw_path": str(record.get("_raw_path") or ""),
                        "payload_hash": digest,
                        "item_count": len(built_rows),
                    }
                )
                loaded_hashes.add(digest)
            rows.extend(built_rows)
        return rows, logs, raw_records, skipped

    def _insert_data_rows(
        self,
        *,
        table: str,
        rows: list[dict[str, Any]],
        logs: list[dict[str, Any]],
        raw_records: int,
        skipped: int,
        field: str,
        batch_size: int,
    ) -> DataLoadResult:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        row_count = 0
        log_count = 0
        for start in range(0, len(rows), batch_size):
            row_count += self.clickhouse.insert(table, rows[start : start + batch_size])
        for start in range(0, len(logs), batch_size):
            log_count += self.clickhouse.insert("raw_ingest_log", logs[start : start + batch_size])
        values = {
            "raw_records": raw_records,
            "skipped_raw_records": skipped,
            "trades": 0,
            "activities": 0,
            "holders": 0,
            "market_positions": 0,
            "open_interest": 0,
            "ingest_logs": log_count,
        }
        values[field] = row_count
        return DataLoadResult(**values)

    def loaded_payload_hashes(self, entity: str = "trades") -> set[str]:
        try:
            output = self.clickhouse.query_text(
                "SELECT payload_hash FROM raw_ingest_log "
                f"WHERE source = 'data' AND entity = '{entity}' FORMAT TSV"
            )
        except Exception:
            return set()
        return {line.strip() for line in output.splitlines() if line.strip()}


def trade_id(transaction_hash: str, token_id: str, timestamp: datetime, index: int) -> str:
    key = f"{transaction_hash}|{token_id}|{timestamp.isoformat()}|{index}".encode("utf-8")
    return hashlib.sha256(key).hexdigest()


def activity_rows(payload: Any, ingested_at: datetime) -> list[dict[str, Any]]:
    rows = []
    for index, item in enumerate(payload if isinstance(payload, list) else []):
        if not isinstance(item, dict):
            continue
        timestamp = parse_dt(item.get("timestamp"))
        if timestamp is None:
            continue
        transaction_hash = as_str(item.get("transactionHash"))
        token_id = as_str(item.get("asset"))
        price = as_float(item.get("price"))
        size = as_float(item.get("size"))
        rows.append(
            {
                "activity_id": trade_id(transaction_hash, token_id, timestamp, index),
                "user_address": as_str(item.get("proxyWallet")).lower(),
                "timestamp": timestamp,
                "activity_type": as_str(item.get("type")),
                "condition_id": as_str(item.get("conditionId")),
                "token_id": token_id,
                "transaction_hash": transaction_hash,
                "side": as_str(item.get("side")),
                "price": price,
                "size": size,
                "notional": as_float(item.get("usdcSize")) or price * size,
                "raw_json": json.dumps(item, ensure_ascii=False, separators=(",", ":")),
                "ingested_at": ingested_at,
            }
        )
    return rows


def holder_rows(payload: Any, ingested_at: datetime) -> list[dict[str, Any]]:
    rows = []
    for token_group in payload if isinstance(payload, list) else []:
        if not isinstance(token_group, dict):
            continue
        token_id = as_str(token_group.get("token"))
        holders = token_group.get("holders") if isinstance(token_group.get("holders"), list) else []
        for holder in holders:
            if not isinstance(holder, dict):
                continue
            rows.append(
                {
                    "condition_id": "",
                    "token_id": token_id or as_str(holder.get("asset")),
                    "user_address": as_str(holder.get("proxyWallet")).lower(),
                    "captured_at": ingested_at,
                    "amount": as_float(holder.get("amount")),
                    "outcome_index": int(as_float(holder.get("outcomeIndex"))),
                    "pseudonym": as_str(holder.get("pseudonym")),
                    "name": as_str(holder.get("name")),
                    "verified": bool(holder.get("verified")),
                    "raw_json": json.dumps(holder, ensure_ascii=False, separators=(",", ":")),
                    "ingested_at": ingested_at,
                }
            )
    return rows


def market_position_rows(payload: Any, ingested_at: datetime) -> list[dict[str, Any]]:
    rows = []
    for token_group in payload if isinstance(payload, list) else []:
        if not isinstance(token_group, dict):
            continue
        token_id = as_str(token_group.get("token"))
        positions = token_group.get("positions") if isinstance(token_group.get("positions"), list) else []
        for position in positions:
            if not isinstance(position, dict):
                continue
            rows.append(
                {
                    "condition_id": as_str(position.get("conditionId")),
                    "token_id": token_id or as_str(position.get("asset")),
                    "user_address": as_str(position.get("proxyWallet")).lower(),
                    "captured_at": ingested_at,
                    "size": as_float(position.get("size")),
                    "avg_price": as_float(position.get("avgPrice")),
                    "curr_price": as_float(position.get("currPrice")),
                    "current_value": as_float(position.get("currentValue")),
                    "cash_pnl": as_float(position.get("cashPnl")),
                    "realized_pnl": as_float(position.get("realizedPnl")),
                    "total_pnl": as_float(position.get("totalPnl")),
                    "total_bought": as_float(position.get("totalBought")),
                    "outcome": as_str(position.get("outcome")),
                    "outcome_index": int(as_float(position.get("outcomeIndex"))),
                    "raw_json": json.dumps(position, ensure_ascii=False, separators=(",", ":")),
                    "ingested_at": ingested_at,
                }
            )
    return rows


def open_interest_rows(payload: Any, ingested_at: datetime) -> list[dict[str, Any]]:
    rows = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "condition_id": as_str(item.get("market")),
                "captured_at": ingested_at,
                "value": as_float(item.get("value")),
                "raw_json": json.dumps(item, ensure_ascii=False, separators=(",", ":")),
                "ingested_at": ingested_at,
            }
        )
    return rows


def payload_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()
