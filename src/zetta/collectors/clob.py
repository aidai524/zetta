from __future__ import annotations

from dataclasses import dataclass

from zetta.http import HttpClientError
from zetta.polymarket import PolymarketClient
from zetta.storage.raw import RawJsonlWriter


@dataclass(frozen=True)
class ClobCollectionResult:
    entity: str
    items: int
    output_path: str


@dataclass(frozen=True)
class ClobBatchCollectionResult:
    entity: str
    tokens: int
    items: int
    failures: int


class ClobCollector:
    def __init__(self, *, client: PolymarketClient, raw_writer: RawJsonlWriter) -> None:
        self.client = client
        self.raw_writer = raw_writer

    def collect_prices_history(
        self,
        *,
        token_id: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str | None = None,
        fidelity: int | None = None,
    ) -> ClobCollectionResult:
        page = self.client.clob_prices_history(
            market=token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval,
            fidelity=fidelity,
        )
        output_path = self.raw_writer.write(
            source="clob",
            entity="prices_history",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return ClobCollectionResult(
            entity="prices_history",
            items=len(page.items),
            output_path=str(output_path),
        )

    def collect_book(self, *, token_id: str) -> ClobCollectionResult:
        try:
            page = self.client.clob_book(token_id=token_id)
        except HttpClientError as exc:
            if is_missing_orderbook(exc):
                return ClobCollectionResult(entity="book", items=0, output_path="")
            raise
        output_path = self.raw_writer.write(
            source="clob",
            entity="book",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return ClobCollectionResult(entity="book", items=len(page.items), output_path=str(output_path))

    def collect_books_batch(
        self,
        *,
        token_ids: list[str],
        sleep_seconds: float = 0.0,
    ) -> ClobBatchCollectionResult:
        import time

        total_items = 0
        failures = 0
        for token_id in token_ids:
            try:
                result = self.collect_book(token_id=token_id)
                total_items += result.items
            except Exception:
                failures += 1
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        return ClobBatchCollectionResult(
            entity="book",
            tokens=len(token_ids),
            items=total_items,
            failures=failures,
        )

    def collect_prices_history_batch(
        self,
        *,
        token_ids: list[str],
        interval: str | None = "all",
        fidelity: int | None = None,
        sleep_seconds: float = 0.0,
    ) -> ClobBatchCollectionResult:
        import time

        total_items = 0
        failures = 0
        for token_id in token_ids:
            try:
                result = self.collect_prices_history(
                    token_id=token_id,
                    interval=interval,
                    fidelity=fidelity,
                )
                total_items += result.items
            except Exception:
                failures += 1
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        return ClobBatchCollectionResult(
            entity="prices_history",
            tokens=len(token_ids),
            items=total_items,
            failures=failures,
        )


def is_missing_orderbook(exc: HttpClientError) -> bool:
    message = str(exc)
    return "failed with 404" in message and "No orderbook exists" in message
