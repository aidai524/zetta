from __future__ import annotations

import asyncio
import contextlib
import json
import signal
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from zetta.api import ProductApi, api_datetime, parse_clickhouse_datetime
from zetta.config import Settings
from zetta.storage.clickhouse import ClickHouseWriter


def trade_stream_row_key(row: dict[str, Any]) -> str:
    trade_id = str(row.get("trade_id") or "")
    if trade_id:
        return f"id:{trade_id}"
    transaction_hash = str(row.get("transaction_hash") or "")
    if transaction_hash:
        return "|".join(
            [
                "tx",
                transaction_hash,
                str(row.get("token_id") or ""),
                str(row.get("user_address") or ""),
                str(row.get("side") or ""),
                str(row.get("price") or ""),
                str(row.get("size") or ""),
            ]
        )
    return "|".join(
        [
            "row",
            str(row.get("timestamp") or ""),
            str(row.get("user_address") or ""),
            str(row.get("condition_id") or ""),
            str(row.get("token_id") or ""),
            str(row.get("side") or ""),
            str(row.get("price") or ""),
            str(row.get("size") or ""),
        ]
    )


def trade_stream_message(row: dict[str, Any], *, source: str, captured_at: str | None) -> dict[str, Any]:
    price = _float_or_none(row.get("price"))
    size = _float_or_none(row.get("size"))
    notional = _float_or_none(row.get("notional"))
    if notional is None and price is not None and size is not None:
        notional = price * size
    return {
        "type": "trade",
        "key": trade_stream_row_key(row),
        "source": source,
        "captured_at": captured_at,
        "timestamp": row.get("timestamp"),
        "side": str(row.get("side") or "").upper(),
        "price": price,
        "size": size,
        "notional": notional,
        "asset_id": str(row.get("token_id") or ""),
        "market": str(row.get("market_slug") or row.get("condition_id") or ""),
        "market_id": str(row.get("market_id") or ""),
        "condition_id": str(row.get("condition_id") or ""),
        "hash": str(row.get("transaction_hash") or ""),
        "maker_address": str(row.get("user_address") or ""),
        "user_address": str(row.get("user_address") or ""),
        "question": str(row.get("question") or ""),
        "outcome": str(row.get("outcome") or ""),
        "category": str(row.get("category") or ""),
        "slug": str(row.get("market_slug") or ""),
        "event_slug": str(row.get("event_slug") or ""),
        "event_title": str(row.get("event_title") or ""),
        "trade": row,
    }


@dataclass
class TradeStreamState:
    api: ProductApi
    poll_seconds: float = 2.0
    limit: int = 150
    heartbeat_seconds: float = 20.0
    seen_limit: int = 10_000
    clients: set[Any] = field(default_factory=set)
    seen: set[str] = field(default_factory=set)
    seen_order: deque[str] = field(default_factory=deque)
    primed: bool = False
    last_body: dict[str, Any] = field(default_factory=dict)
    last_error: str = ""

    async def handle_client(self, websocket: Any, path: str = "") -> None:
        if path not in ("", "/", "/trades", "/stream/trades", "/trades/stream"):
            await websocket.close(code=1008, reason="unsupported path")
            return
        self.clients.add(websocket)
        try:
            await websocket.send(
                json.dumps(
                    {
                        "type": "connected",
                        "mode": "snapshot_delta",
                        "source": "zetta.trade_stream",
                        "server_time": api_datetime(datetime.now(UTC)),
                        "poll_seconds": self.poll_seconds,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            async for _message in websocket:
                # The frontend only consumes server pushes. Incoming messages are ignored for now.
                continue
        finally:
            self.clients.discard(websocket)

    async def poll_loop(self) -> None:
        while True:
            if not self.clients:
                await asyncio.sleep(min(self.poll_seconds, 1.0))
                continue
            try:
                if not self.primed:
                    await self.prime_seen()
                body = await asyncio.to_thread(self.fetch_snapshot)
                self.last_body = body
                self.last_error = ""
                messages = self.new_trade_messages(body)
                if messages:
                    for message in messages:
                        await self.broadcast(message)
                await self.broadcast_status(body)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                await self.broadcast(
                    {
                        "type": "status",
                        "status": "error",
                        "source": "zetta.trade_stream",
                        "server_time": api_datetime(datetime.now(UTC)),
                        "error": str(exc),
                    }
                )
            await asyncio.sleep(self.poll_seconds)

    async def heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            if self.clients:
                await self.broadcast(
                    {
                        "type": "heartbeat",
                        "source": "zetta.trade_stream",
                        "server_time": api_datetime(datetime.now(UTC)),
                        "clients": len(self.clients),
                        "status": "error" if self.last_error else "ok",
                    }
                )

    async def prime_seen(self) -> None:
        body = await asyncio.to_thread(self.fetch_snapshot)
        self.last_body = body
        for row in body.get("trades") or []:
            if isinstance(row, dict):
                self.remember(trade_stream_row_key(row))
        self.primed = True

    def fetch_snapshot(self) -> dict[str, Any]:
        query = {
            "limit": [str(max(1, min(self.limit, 500)))],
            "ttl": ["0.5"],
            "pages": ["8"],
            "page_size": ["100"],
            "chain_lookback_minutes": ["60"],
        }
        return self.api.live_trades(query)

    def new_trade_messages(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        source = str(body.get("source") or "live")
        captured_at = str(body.get("captured_at") or "") or None
        rows = [row for row in body.get("trades") or [] if isinstance(row, dict)]
        rows.sort(key=_trade_time, reverse=False)
        messages: list[dict[str, Any]] = []
        for row in rows:
            key = trade_stream_row_key(row)
            if key in self.seen:
                continue
            self.remember(key)
            messages.append(trade_stream_message(row, source=source, captured_at=captured_at))
        return messages

    def remember(self, key: str) -> None:
        if key in self.seen:
            return
        self.seen.add(key)
        self.seen_order.append(key)
        while len(self.seen_order) > self.seen_limit:
            old_key = self.seen_order.popleft()
            self.seen.discard(old_key)

    async def broadcast_status(self, body: dict[str, Any]) -> None:
        await self.broadcast(
            {
                "type": "status",
                "status": body.get("status") or "ok",
                "source": body.get("source") or "live",
                "captured_at": body.get("captured_at"),
                "latency_seconds": body.get("latency_seconds"),
                "candidate_count": body.get("candidate_count"),
                "server_time": api_datetime(datetime.now(UTC)),
            }
        )

    async def broadcast(self, message: dict[str, Any]) -> None:
        if not self.clients:
            return
        payload = json.dumps(message, ensure_ascii=False, default=str, separators=(",", ":"))
        stale_clients: list[Any] = []
        for websocket in list(self.clients):
            try:
                await websocket.send(payload)
            except Exception:
                stale_clients.append(websocket)
        for websocket in stale_clients:
            self.clients.discard(websocket)


async def serve_trade_stream_async(
    *,
    settings: Settings,
    clickhouse: ClickHouseWriter,
    host: str,
    port: int,
    poll_seconds: float = 2.0,
    limit: int = 150,
    heartbeat_seconds: float = 20.0,
) -> None:
    try:
        from websockets.asyncio.server import serve
    except ImportError:  # pragma: no cover - depends on installed websockets version.
        from websockets import serve  # type: ignore

    state = TradeStreamState(
        api=ProductApi(clickhouse=clickhouse, settings=settings),
        poll_seconds=max(0.5, poll_seconds),
        limit=max(1, min(limit, 500)),
        heartbeat_seconds=max(5.0, heartbeat_seconds),
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    async def handler(*args: Any) -> None:
        websocket = args[0]
        path = args[1] if len(args) > 1 else getattr(websocket, "path", "")
        await state.handle_client(websocket, str(path or ""))

    async with serve(handler, host, port, ping_interval=20, ping_timeout=20):
        print(
            json.dumps(
                {
                    "service": "zetta_trade_stream",
                    "status": "listening",
                    "host": host,
                    "port": port,
                    "poll_seconds": state.poll_seconds,
                    "started_at": api_datetime(datetime.now(UTC)),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        poll_task = asyncio.create_task(state.poll_loop())
        heartbeat_task = asyncio.create_task(state.heartbeat_loop())
        try:
            await stop_event.wait()
        finally:
            poll_task.cancel()
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poll_task
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task


def serve_trade_stream(
    *,
    settings: Settings,
    clickhouse: ClickHouseWriter,
    host: str,
    port: int,
    poll_seconds: float = 2.0,
    limit: int = 150,
    heartbeat_seconds: float = 20.0,
) -> None:
    asyncio.run(
        serve_trade_stream_async(
            settings=settings,
            clickhouse=clickhouse,
            host=host,
            port=port,
            poll_seconds=poll_seconds,
            limit=limit,
            heartbeat_seconds=heartbeat_seconds,
        )
    )


def _trade_time(row: dict[str, Any]) -> float:
    parsed = parse_clickhouse_datetime(row.get("timestamp"))
    if parsed is None:
        return 0.0
    return parsed.timestamp()


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number
