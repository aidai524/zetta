from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from zetta.loaders.clob import ws_market_events
from zetta.storage.raw_reader import iter_raw_records
from zetta.storage.redpanda import RpkPublisher


@dataclass(frozen=True)
class RawWsPublishResult:
    raw_records: int
    events: int
    published: int
    topic: str


def ws_market_stream_messages(record: dict[str, Any]) -> list[dict[str, Any]]:
    messages = []
    for event in ws_market_events(record.get("payload")):
        messages.append(
            {
                "source": "clob_ws",
                "channel": "market",
                "collected_at": str(record.get("collected_at") or ""),
                "request_url": str(record.get("request_url") or ""),
                "event": event,
            }
        )
    return messages


def publish_ws_market_raw(
    *,
    raw_root: Path,
    publisher: RpkPublisher,
    topic: str = "zetta.polymarket.clob_ws.market.raw",
    max_records: int | None = None,
    batch_size: int = 500,
    create_topic: bool = True,
) -> RawWsPublishResult:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if create_topic:
        publisher.create_topic(topic)
    raw_records = 0
    events = 0
    published = 0
    batch: list[dict[str, Any]] = []

    for record in iter_raw_records(raw_root, source="clob_ws", entity="market"):
        if max_records is not None and raw_records >= max_records:
            break
        raw_records += 1
        messages = ws_market_stream_messages(record)
        events += len(messages)
        batch.extend(messages)
        if len(batch) >= batch_size:
            published += publisher.publish_json(topic=topic, messages=batch).messages
            batch.clear()

    published += publisher.publish_json(topic=topic, messages=batch).messages
    return RawWsPublishResult(
        raw_records=raw_records,
        events=events,
        published=published,
        topic=topic,
    )
