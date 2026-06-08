from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from zetta.config import Settings
from zetta.loaders.incremental import (
    clickhouse_state_dir,
    loaded_max_raw_path,
    loaded_payload_hashes,
    loaded_payload_hashes_for_paths,
    loader_checkpoint_raw_path,
    save_loader_checkpoint_raw_path,
)
from zetta.loaders.parallel import load_in_parallel, raw_paths
from zetta.models.normalize import as_float, parse_dt
from zetta.storage.clickhouse import ClickHouseWriter
from zetta.storage.raw_reader import iter_raw_records, iter_raw_records_from_paths


@dataclass(frozen=True)
class ClobLoadResult:
    raw_records: int
    skipped_raw_records: int
    price_history: int
    orderbook_snapshots: int
    ingest_logs: int


@dataclass(frozen=True)
class ClobLoadStateResult(ClobLoadResult):
    last_raw_path: str = ""


class ClobRawLoader:
    def __init__(self, *, clickhouse: ClickHouseWriter) -> None:
        self.clickhouse = clickhouse

    def load_price_history(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> ClobLoadResult:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        checkpoint_path = None if force else self.loader_checkpoint_raw_path()
        database_after_path = None if force else self.loaded_max_raw_path()
        after_path = checkpoint_path or database_after_path
        paths = (
            raw_paths(raw_root, source="clob", entity="prices_history", after_path=after_path)
            if not force and after_path is not None
            else None
        )
        if not force and checkpoint_path is None and database_after_path is not None:
            self.save_loader_checkpoint_raw_path(database_after_path)
        loaded_hashes = (
            loaded_payload_hashes_for_paths(
                self.clickhouse,
                source="clob",
                entity="prices_history",
                paths=paths,
            )
            if paths is not None
            else self.loaded_payload_hashes()
        )
        price_rows: list[dict[str, Any]] = []
        log_rows: list[dict[str, Any]] = []
        raw_records = 0
        skipped = 0
        prices = 0
        logs = 0
        last_raw_path = ""

        records = (
            iter_raw_records_from_paths(paths, after_path=after_path)
            if paths is not None
            else iter_raw_records(
                raw_root,
                source="clob",
                entity="prices_history",
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
            request_url = str(record.get("request_url") or "")
            token_id = query_param(request_url, "market")
            ingested_at = parse_dt(record.get("collected_at")) or datetime.now(UTC)
            history = payload.get("history") if isinstance(payload, dict) else []
            if not isinstance(history, list):
                history = []

            if not already_loaded:
                log_rows.append(
                    {
                        "collected_at": ingested_at,
                        "source": "clob",
                        "entity": "prices_history",
                        "request_url": request_url,
                        "raw_path": str(record.get("_raw_path") or ""),
                        "payload_hash": digest,
                        "item_count": len(history),
                    }
                )
                loaded_hashes.add(digest)

            for point in history:
                if not isinstance(point, dict):
                    continue
                timestamp = parse_dt(point.get("t"))
                if timestamp is None:
                    continue
                price_rows.append(
                    {
                        "token_id": token_id,
                        "timestamp": timestamp,
                        "price": as_float(point.get("p")),
                        "source": "clob",
                        "raw_json": json.dumps(point, ensure_ascii=False, separators=(",", ":")),
                        "ingested_at": ingested_at,
                    }
                )
            if len(price_rows) + len(log_rows) >= batch_size:
                inserted_prices, inserted_logs = self.flush(price_rows, log_rows)
                prices += inserted_prices
                logs += inserted_logs

        inserted_prices, inserted_logs = self.flush(price_rows, log_rows)
        prices += inserted_prices
        logs += inserted_logs
        if not force and last_raw_path:
            self.save_loader_checkpoint_raw_path(last_raw_path)
        return ClobLoadResult(
            raw_records=raw_records,
            skipped_raw_records=skipped,
            price_history=prices,
            orderbook_snapshots=0,
            ingest_logs=logs,
        )

    def load_books(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
    ) -> ClobLoadResult:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        checkpoint_path = None if force else self.loader_checkpoint_raw_path(entity="book")
        database_after_path = None if force else self.loaded_max_raw_path(entity="book")
        after_path = checkpoint_path or database_after_path
        paths = (
            raw_paths(raw_root, source="clob", entity="book", after_path=after_path)
            if not force and after_path is not None
            else None
        )
        if not force and checkpoint_path is None and database_after_path is not None:
            self.save_loader_checkpoint_raw_path(database_after_path, entity="book")
        loaded_hashes = (
            loaded_payload_hashes_for_paths(
                self.clickhouse,
                source="clob",
                entity="book",
                paths=paths,
            )
            if paths is not None
            else self.loaded_payload_hashes(entity="book")
        )
        book_rows: list[dict[str, Any]] = []
        log_rows: list[dict[str, Any]] = []
        raw_records = 0
        skipped = 0
        snapshots = 0
        logs = 0
        last_raw_path = ""

        records = (
            iter_raw_records_from_paths(paths, after_path=after_path)
            if paths is not None
            else iter_raw_records(
                raw_root,
                source="clob",
                entity="book",
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
            request_url = str(record.get("request_url") or "")
            token_id = query_param(request_url, "token_id")
            ingested_at = parse_dt(record.get("collected_at")) or datetime.now(UTC)
            book = payload if isinstance(payload, dict) else {}

            if not already_loaded:
                log_rows.append(
                    {
                        "collected_at": ingested_at,
                        "source": "clob",
                        "entity": "book",
                        "request_url": request_url,
                        "raw_path": str(record.get("_raw_path") or ""),
                        "payload_hash": digest,
                        "item_count": 1,
                    }
                )
                loaded_hashes.add(digest)

            book_rows.append(orderbook_snapshot_row(book, token_id=token_id, ingested_at=ingested_at))

            if len(book_rows) + len(log_rows) >= batch_size:
                inserted_books, inserted_logs = self.flush_books(book_rows, log_rows)
                snapshots += inserted_books
                logs += inserted_logs

        inserted_books, inserted_logs = self.flush_books(book_rows, log_rows)
        snapshots += inserted_books
        logs += inserted_logs
        if not force and last_raw_path:
            self.save_loader_checkpoint_raw_path(last_raw_path, entity="book")
        return ClobLoadResult(
            raw_records=raw_records,
            skipped_raw_records=skipped,
            price_history=0,
            orderbook_snapshots=snapshots,
            ingest_logs=logs,
        )

    def load_ws_market_books(
        self,
        *,
        raw_root: Path,
        force: bool = False,
        batch_size: int = 10_000,
        workers: int = 1,
    ) -> ClobLoadResult:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        database_after_path = (
            None if force else self.loaded_max_raw_path(source="clob_ws", entity="market")
        )
        checkpoint_path = (
            None
            if force
            else self.loader_checkpoint_raw_path(source="clob_ws", entity="market")
        )
        after_path = checkpoint_path or database_after_path
        paths = (
            raw_paths(raw_root, source="clob_ws", entity="market", after_path=after_path)
            if not force and after_path is not None
            else None
        )
        if not force and checkpoint_path is None and database_after_path is not None:
            self.save_loader_checkpoint_raw_path(
                database_after_path,
                source="clob_ws",
                entity="market",
            )
        if workers > 1 and paths is not None:
            result = load_in_parallel(
                worker=ClobWsMarketBookLoadWorker(self.clickhouse.settings, batch_size),
                paths=paths,
                workers=workers,
            )
            if paths:
                last_raw_path = result.last_raw_path if result is not None else None
                self.save_loader_checkpoint_raw_path(
                    last_raw_path or paths[-1],
                    source="clob_ws",
                    entity="market",
                )
            return without_clob_last_raw_path(result) if result is not None else empty_clob_result()
        loaded_hashes = (
            loaded_payload_hashes_for_paths(
                self.clickhouse,
                source="clob_ws",
                entity="market",
                paths=paths,
            )
            if paths is not None
            else self.loaded_payload_hashes(source="clob_ws", entity="market")
        )
        result = self.load_ws_market_books_from_records(
            iter_raw_records_from_paths(paths, after_path=after_path)
            if paths is not None
            else iter_raw_records(
                raw_root,
                source="clob_ws",
                entity="market",
                after_path=after_path,
            ),
            loaded_hashes=loaded_hashes,
            batch_size=batch_size,
            skip_loaded=not force,
        )
        if not force and result.last_raw_path:
            self.save_loader_checkpoint_raw_path(
                result.last_raw_path,
                source="clob_ws",
                entity="market",
            )
        return without_clob_last_raw_path(result)

    def load_ws_market_books_from_paths(
        self,
        paths: list[str],
        *,
        batch_size: int = 10_000,
    ) -> ClobLoadStateResult:
        return self.load_ws_market_books_from_records(
            iter_raw_records_from_paths(paths),
            loaded_hashes=loaded_payload_hashes_for_paths(
                self.clickhouse,
                source="clob_ws",
                entity="market",
                paths=paths,
            ),
            batch_size=batch_size,
            skip_loaded=True,
        )

    def load_ws_market_books_from_records(
        self,
        records,
        *,
        loaded_hashes: set[str],
        batch_size: int,
        skip_loaded: bool,
    ) -> ClobLoadStateResult:
        book_rows: list[dict[str, Any]] = []
        log_rows: list[dict[str, Any]] = []
        raw_records = 0
        skipped = 0
        snapshots = 0
        logs = 0
        last_raw_path = ""

        for record in records:
            raw_records += 1
            last_raw_path = str(record.get("_raw_path") or last_raw_path)
            payload = record.get("payload")
            digest = payload_hash(payload)
            already_loaded = digest in loaded_hashes
            if already_loaded and skip_loaded:
                skipped += 1
                continue
            events = ws_market_events(payload)
            request_url = str(record.get("request_url") or "")
            ingested_at = parse_dt(record.get("collected_at")) or datetime.now(UTC)

            if not already_loaded:
                log_rows.append(
                    {
                        "collected_at": ingested_at,
                        "source": "clob_ws",
                        "entity": "market",
                        "request_url": request_url,
                        "raw_path": str(record.get("_raw_path") or ""),
                        "payload_hash": digest,
                        "item_count": len(events),
                    }
                )
                loaded_hashes.add(digest)

            for event in events:
                if event.get("event_type") != "book":
                    continue
                book_rows.append(orderbook_snapshot_row(event, token_id="", ingested_at=ingested_at))

            if len(book_rows) + len(log_rows) >= batch_size:
                inserted_books, inserted_logs = self.flush_books(book_rows, log_rows)
                snapshots += inserted_books
                logs += inserted_logs

        inserted_books, inserted_logs = self.flush_books(book_rows, log_rows)
        snapshots += inserted_books
        logs += inserted_logs
        return ClobLoadStateResult(
            raw_records=raw_records,
            skipped_raw_records=skipped,
            price_history=0,
            orderbook_snapshots=snapshots,
            ingest_logs=logs,
            last_raw_path=last_raw_path,
        )

    def flush(
        self,
        price_rows: list[dict[str, Any]],
        log_rows: list[dict[str, Any]],
    ) -> tuple[int, int]:
        price_count = self.clickhouse.insert("fact_price_history", price_rows)
        log_count = self.clickhouse.insert("raw_ingest_log", log_rows)
        price_rows.clear()
        log_rows.clear()
        return price_count, log_count

    def flush_books(
        self,
        book_rows: list[dict[str, Any]],
        log_rows: list[dict[str, Any]],
    ) -> tuple[int, int]:
        book_count = self.clickhouse.insert("fact_orderbook_snapshot", book_rows)
        log_count = self.clickhouse.insert("raw_ingest_log", log_rows)
        book_rows.clear()
        log_rows.clear()
        return book_count, log_count

    def loaded_payload_hashes(self, source: str = "clob", entity: str = "prices_history") -> set[str]:
        return loaded_payload_hashes(self.clickhouse, source=source, entity=entity)

    def loaded_max_raw_path(
        self,
        source: str = "clob",
        entity: str = "prices_history",
    ) -> str | None:
        return loaded_max_raw_path(self.clickhouse, source=source, entity=entity)

    def loader_checkpoint_raw_path(
        self,
        source: str = "clob",
        entity: str = "prices_history",
    ) -> str | None:
        return loader_checkpoint_raw_path(
            clickhouse_state_dir(self.clickhouse),
            source=source,
            entity=entity,
        )

    def save_loader_checkpoint_raw_path(
        self,
        raw_path: str,
        source: str = "clob",
        entity: str = "prices_history",
    ) -> None:
        save_loader_checkpoint_raw_path(
            clickhouse_state_dir(self.clickhouse),
            source=source,
            entity=entity,
            raw_path=raw_path,
        )


@dataclass(frozen=True)
class ClobWsMarketBookLoadWorker:
    settings: Settings
    batch_size: int

    def __call__(self, paths: list[str]) -> ClobLoadResult:
        return ClobRawLoader(
            clickhouse=ClickHouseWriter(self.settings)
        ).load_ws_market_books_from_paths(
            paths,
            batch_size=self.batch_size,
        )


def empty_clob_result() -> ClobLoadResult:
    return ClobLoadResult(
        raw_records=0,
        skipped_raw_records=0,
        price_history=0,
        orderbook_snapshots=0,
        ingest_logs=0,
    )


def without_clob_last_raw_path(result: ClobLoadStateResult) -> ClobLoadResult:
    return ClobLoadResult(
        raw_records=result.raw_records,
        skipped_raw_records=result.skipped_raw_records,
        price_history=result.price_history,
        orderbook_snapshots=result.orderbook_snapshots,
        ingest_logs=result.ingest_logs,
    )


def query_param(url: str, name: str) -> str:
    return parse_qs(urlparse(url).query).get(name, [""])[0]


def side_depth(levels: list[Any]) -> float:
    return sum(as_float(level.get("size")) for level in levels if isinstance(level, dict))


def ws_market_events(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [event for event in payload if isinstance(event, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def orderbook_snapshot_row(
    book: dict[str, Any],
    *,
    token_id: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    bids = book.get("bids") if isinstance(book.get("bids"), list) else []
    asks = book.get("asks") if isinstance(book.get("asks"), list) else []
    captured_at = parse_dt(book.get("timestamp")) or ingested_at
    bid_prices = [as_float(level.get("price")) for level in bids if isinstance(level, dict)]
    ask_prices = [as_float(level.get("price")) for level in asks if isinstance(level, dict)]
    return {
        "token_id": token_id or str(book.get("asset_id") or ""),
        "captured_at": captured_at,
        "market": str(book.get("market") or ""),
        "asset_id": str(book.get("asset_id") or ""),
        "best_bid": max(bid_prices) if bid_prices else None,
        "best_ask": min(ask_prices) if ask_prices else None,
        "bid_depth": side_depth(bids),
        "ask_depth": side_depth(asks),
        "bids_json": json.dumps(bids, ensure_ascii=False, separators=(",", ":")),
        "asks_json": json.dumps(asks, ensure_ascii=False, separators=(",", ":")),
        "raw_json": json.dumps(book, ensure_ascii=False, separators=(",", ":")),
        "ingested_at": ingested_at,
    }


def payload_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()
