from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from zetta.loaders.clob import side_depth, ws_market_events
from zetta.models.normalize import as_float, parse_dt
from zetta.storage.raw_reader import iter_raw_records


@dataclass
class OrderBookState:
    token_id: str
    market: str = ""
    timestamp: datetime | None = None
    hash: str = ""
    bids: dict[str, float] = field(default_factory=dict)
    asks: dict[str, float] = field(default_factory=dict)

    @property
    def best_bid(self) -> float | None:
        prices = [float(price) for price, size in self.bids.items() if size > 0]
        return max(prices) if prices else None

    @property
    def best_ask(self) -> float | None:
        prices = [float(price) for price, size in self.asks.items() if size > 0]
        return min(prices) if prices else None

    @property
    def bid_depth(self) -> float:
        return sum(size for size in self.bids.values() if size > 0)

    @property
    def ask_depth(self) -> float:
        return sum(size for size in self.asks.values() if size > 0)

    def summary(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "market": self.market,
            "timestamp": self.timestamp.isoformat() if self.timestamp else "",
            "hash": self.hash,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "bid_depth": self.bid_depth,
            "ask_depth": self.ask_depth,
            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),
        }


class OrderBookReconstructor:
    def __init__(self) -> None:
        self.books: dict[str, OrderBookState] = {}

    def apply_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("event_type") or "")
        if event_type == "book":
            self.apply_book(event)
        elif event_type == "price_change":
            self.apply_price_change(event)

    def apply_book(self, event: dict[str, Any]) -> None:
        token_id = str(event.get("asset_id") or event.get("token_id") or "")
        if not token_id:
            return
        state = self.books.setdefault(token_id, OrderBookState(token_id=token_id))
        state.market = str(event.get("market") or state.market)
        state.timestamp = parse_market_timestamp(event.get("timestamp")) or datetime.now(UTC)
        state.hash = str(event.get("hash") or state.hash)
        state.bids = levels_to_map(event.get("bids"))
        state.asks = levels_to_map(event.get("asks"))

    def apply_price_change(self, event: dict[str, Any]) -> None:
        changes = event.get("price_changes") or event.get("changes") or []
        if not isinstance(changes, list):
            return
        for change in changes:
            if not isinstance(change, dict):
                continue
            token_id = str(change.get("asset_id") or event.get("asset_id") or "")
            if not token_id:
                continue
            state = self.books.setdefault(token_id, OrderBookState(token_id=token_id))
            state.market = str(change.get("market") or event.get("market") or state.market)
            state.timestamp = (
                parse_market_timestamp(event.get("timestamp") or change.get("timestamp"))
                or datetime.now(UTC)
            )
            state.hash = str(event.get("hash") or state.hash)
            side = str(change.get("side") or "").upper()
            price = str(change.get("price") or "")
            size = as_float(change.get("size"))
            if not price:
                continue
            if side in {"BUY", "BID"}:
                apply_level_change(state.bids, price, size)
            elif side in {"SELL", "ASK"}:
                apply_level_change(state.asks, price, size)

    def summaries(self) -> list[dict[str, Any]]:
        return [self.books[token_id].summary() for token_id in sorted(self.books)]


def reconstruct_ws_market_raw(
    *,
    raw_root: Path,
    max_records: int | None = None,
) -> OrderBookReconstructor:
    reconstructor = OrderBookReconstructor()
    records = 0
    for record in iter_raw_records(raw_root, source="clob_ws", entity="market"):
        if max_records is not None and records >= max_records:
            break
        records += 1
        for event in ws_market_events(record.get("payload")):
            reconstructor.apply_event(event)
    return reconstructor


def rest_book_summary(book: dict[str, Any], *, token_id: str = "") -> dict[str, Any]:
    bids = book.get("bids") if isinstance(book.get("bids"), list) else []
    asks = book.get("asks") if isinstance(book.get("asks"), list) else []
    bid_prices = [as_float(level.get("price")) for level in bids if isinstance(level, dict)]
    ask_prices = [as_float(level.get("price")) for level in asks if isinstance(level, dict)]
    return {
        "token_id": token_id or str(book.get("asset_id") or ""),
        "market": str(book.get("market") or ""),
        "best_bid": max(bid_prices) if bid_prices else None,
        "best_ask": min(ask_prices) if ask_prices else None,
        "bid_depth": side_depth(bids),
        "ask_depth": side_depth(asks),
        "bid_levels": len(bids),
        "ask_levels": len(asks),
    }


def reconciliation_diff(
    *,
    reconstructed: dict[str, Any],
    rest: dict[str, Any],
) -> dict[str, Any]:
    return {
        "token_id": reconstructed.get("token_id") or rest.get("token_id") or "",
        "reconstructed": reconstructed,
        "rest": rest,
        "best_bid_delta": numeric_delta(reconstructed.get("best_bid"), rest.get("best_bid")),
        "best_ask_delta": numeric_delta(reconstructed.get("best_ask"), rest.get("best_ask")),
        "bid_depth_delta": numeric_delta(reconstructed.get("bid_depth"), rest.get("bid_depth")),
        "ask_depth_delta": numeric_delta(reconstructed.get("ask_depth"), rest.get("ask_depth")),
    }


def levels_to_map(levels: Any) -> dict[str, float]:
    if not isinstance(levels, list):
        return {}
    result: dict[str, float] = {}
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = str(level.get("price") or "")
        if not price:
            continue
        size = as_float(level.get("size"))
        if size > 0:
            result[price] = size
    return result


def parse_market_timestamp(value: Any) -> datetime | None:
    parsed = parse_dt(value)
    if parsed is not None:
        return parsed
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.isdigit():
            return parse_dt(int(cleaned))
    return None


def apply_level_change(levels: dict[str, float], price: str, size: float) -> None:
    if size <= 0:
        levels.pop(price, None)
    else:
        levels[price] = size


def numeric_delta(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    return as_float(left) - as_float(right)
