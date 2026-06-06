from __future__ import annotations

from dataclasses import dataclass

from zetta.http import HttpClientError
from zetta.polymarket import PolymarketClient
from zetta.storage.raw import RawJsonlWriter
from zetta.storage.state import LocalStateStore


@dataclass(frozen=True)
class TradesCollectionResult:
    pages: int
    trades: int
    next_offset: int | None


@dataclass(frozen=True)
class DataCollectionResult:
    entity: str
    pages: int
    items: int
    next_offset: int | None = None


class DataCollector:
    def __init__(
        self,
        *,
        client: PolymarketClient,
        raw_writer: RawJsonlWriter,
        state_store: LocalStateStore,
    ) -> None:
        self.client = client
        self.raw_writer = raw_writer
        self.state_store = state_store

    def collect_trades(
        self,
        *,
        page_limit: int,
        max_pages: int,
        resume: bool,
        user: str | None = None,
        market: str | None = None,
        event_id: str | None = None,
    ) -> TradesCollectionResult:
        state_key = "data_trades_global"
        if user:
            state_key = f"data_trades_user_{user.lower()}"
        elif market:
            state_key = f"data_trades_market_{market}"
        elif event_id:
            state_key = f"data_trades_event_{event_id}"

        offset = int(self.state_store.get(state_key, {}).get("offset", 0)) if resume else 0
        pages = 0
        total_trades = 0

        while max_pages == 0 or pages < max_pages:
            try:
                page = self.client.data_trades(
                    limit=page_limit,
                    offset=offset,
                    user=user,
                    market=market,
                    event_id=event_id,
                )
            except HttpClientError as exc:
                if is_data_api_offset_limit(exc):
                    break
                raise
            self.raw_writer.write(
                source="data",
                entity="trades",
                request_url=page.response.url,
                payload=page.response.body,
            )
            pages += 1
            total_trades += len(page.items)
            offset += len(page.items)
            self.state_store.set(
                state_key,
                {
                    "offset": offset,
                    "last_request_url": page.response.url,
                    "last_page_items": len(page.items),
                },
            )
            if len(page.items) < page_limit:
                break

        next_offset = offset if total_trades else None
        return TradesCollectionResult(pages=pages, trades=total_trades, next_offset=next_offset)

    def collect_activity(
        self,
        *,
        user: str,
        page_limit: int,
        max_pages: int,
        resume: bool,
    ) -> DataCollectionResult:
        state_key = f"data_activity_user_{user.lower()}"
        offset = int(self.state_store.get(state_key, {}).get("offset", 0)) if resume else 0
        pages = 0
        total = 0
        while pages < max_pages:
            page = self.client.data_activity(user=user, limit=page_limit, offset=offset)
            self.raw_writer.write(
                source="data",
                entity="activity",
                request_url=page.response.url,
                payload=page.response.body,
            )
            pages += 1
            total += len(page.items)
            offset += len(page.items)
            self.state_store.set(
                state_key,
                {
                    "offset": offset,
                    "last_request_url": page.response.url,
                    "last_page_items": len(page.items),
                },
            )
            if len(page.items) < page_limit:
                break
        return DataCollectionResult("activity", pages, total, offset if total else None)

    def collect_holders(self, *, market: str, limit: int) -> DataCollectionResult:
        page = self.client.data_holders(market=market, limit=limit)
        self.raw_writer.write(
            source="data",
            entity="holders",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return DataCollectionResult("holders", 1, len(page.items))

    def collect_market_positions(self, *, market: str, limit: int) -> DataCollectionResult:
        page = self.client.data_market_positions(market=market, limit=limit)
        self.raw_writer.write(
            source="data",
            entity="market_positions",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return DataCollectionResult("market_positions", 1, len(page.items))

    def collect_open_interest(self, *, market: str | None = None) -> DataCollectionResult:
        page = self.client.data_open_interest(market=market)
        self.raw_writer.write(
            source="data",
            entity="open_interest",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return DataCollectionResult("open_interest", 1, len(page.items))


def is_data_api_offset_limit(exc: HttpClientError) -> bool:
    message = str(exc)
    return "failed with 400" in message and "max historical activity offset" in message
