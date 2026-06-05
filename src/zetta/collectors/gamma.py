from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal

from zetta.polymarket import PolymarketClient
from zetta.storage.raw import RawJsonlWriter
from zetta.storage.state import LocalStateStore


GammaEntity = Literal["events", "markets"]


@dataclass(frozen=True)
class CollectionResult:
    entity: str
    pages: int
    items: int
    last_cursor: str | None


class GammaCollector:
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

    def collect_keyset(
        self,
        entity: GammaEntity,
        *,
        page_limit: int,
        max_pages: int,
        resume: bool,
        sleep_seconds: float = 0.0,
        closed: bool | None = None,
        archived: bool | None = None,
        active: bool | None = None,
    ) -> CollectionResult:
        state_key = f"gamma_{entity}_keyset"
        cursor = self.state_store.get(state_key, {}).get("next_cursor") if resume else None
        pages = 0
        total_items = 0

        while max_pages == 0 or pages < max_pages:
            previous_cursor = cursor
            if entity == "events":
                page = self.client.gamma_events_keyset(
                    limit=page_limit,
                    next_cursor=cursor,
                    closed=closed,
                    archived=archived,
                    active=active,
                )
            else:
                page = self.client.gamma_markets_keyset(
                    limit=page_limit,
                    next_cursor=cursor,
                    closed=closed,
                    archived=archived,
                    active=active,
                )

            self.raw_writer.write(
                source="gamma",
                entity=entity,
                request_url=page.response.url,
                payload=page.response.body,
            )
            pages += 1
            total_items += len(page.items)
            cursor = page.next_cursor
            self.state_store.set(
                state_key,
                {
                    "next_cursor": cursor,
                    "last_request_url": page.response.url,
                    "last_page_items": len(page.items),
                },
            )
            if not cursor or not page.items:
                break
            if cursor == previous_cursor:
                break
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        return CollectionResult(
            entity=entity,
            pages=pages,
            items=total_items,
            last_cursor=cursor,
        )
