from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import signal
import time
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit


POLYMARKET_RTDS_URL = "wss://ws-live-data.polymarket.com"
POLYMARKET_DATA_API_URL = "https://data-api.polymarket.com"
POLYMARKET_TRADE_SUBSCRIPTION = {
    "action": "subscribe",
    "subscriptions": [{"topic": "activity", "type": "trades"}],
}
RECENT_CACHE_KEY = "official_trade_feed/recent"
WALLET_CACHE_DIR = "official_trade_feed/wallets"
MAX_CLIENT_WALLET_FILTERS = 500
DEFAULT_CLIENT_SEND_TIMEOUT_SECONDS = 1.0
DEFAULT_CLIENT_QUEUE_SIZE = 2_000


def normalize_rtds_trade_message(message: dict[str, Any]) -> dict[str, Any] | None:
    if message.get("topic") != "activity" or message.get("type") != "trades":
        return None
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None
    return normalize_activity_trade_payload(
        payload,
        upstream_timestamp=message.get("timestamp"),
        raw=message,
        source="polymarket-rtds",
    )


def normalize_activity_trade_payload(
    payload: dict[str, Any],
    *,
    upstream_timestamp: Any,
    raw: dict[str, Any],
    source: str,
) -> dict[str, Any] | None:
    timestamp = _payload_timestamp(payload.get("timestamp"), upstream_timestamp)
    price = _float_or_none(payload.get("price"))
    size = _float_or_none(payload.get("size"))
    notional = _float_or_none(payload.get("usdcSize"))
    if notional is None and price is not None and size is not None:
        notional = price * size
    transaction_hash = str(payload.get("transactionHash") or payload.get("transaction_hash") or "")
    token_id = str(payload.get("asset") or payload.get("token_id") or "")
    user_address = str(payload.get("proxyWallet") or payload.get("user") or "").lower()
    row = {
        "trade_id": _official_trade_id(payload, transaction_hash, token_id, timestamp),
        "transaction_hash": transaction_hash,
        "timestamp": timestamp,
        "market_id": "",
        "condition_id": str(payload.get("conditionId") or payload.get("condition_id") or ""),
        "token_id": token_id,
        "user_address": user_address,
        "side": str(payload.get("side") or "").upper(),
        "price": price,
        "size": size,
        "notional": notional,
        "question": str(payload.get("title") or ""),
        "market_slug": str(payload.get("slug") or ""),
        "event_id": "",
        "event_title": "",
        "event_slug": str(payload.get("eventSlug") or payload.get("event_slug") or ""),
        "category": "",
        "outcome": str(payload.get("outcome") or ""),
        "trader_name": str(payload.get("name") or ""),
        "trader_pseudonym": str(payload.get("pseudonym") or ""),
        "is_smart": False,
        "is_whale": False,
        "wallet_total_pnl": 0.0,
        "wallet_pnl_roi": 0.0,
        "wallet_traded_notional": 0.0,
        "icon": str(payload.get("icon") or ""),
        "profile_image": str(payload.get("profileImage") or ""),
        "fee": _float_or_none(payload.get("fee")),
        "source": source,
    }
    return {
        "type": "trade",
        "source": source,
        "topic": "activity",
        "feed_type": "trades",
        "received_at": api_datetime(datetime.now(UTC)),
        "upstream_timestamp": upstream_timestamp,
        "latency_seconds": _latency_seconds(timestamp),
        "trade": row,
        "raw": raw,
    }


@dataclass
class OfficialTradeFeedState:
    upstream_url: str = POLYMARKET_RTDS_URL
    state_dir: Path | None = None
    clients: set[Any] = field(default_factory=set)
    client_queues: dict[Any, asyncio.Queue[str]] = field(default_factory=dict)
    client_writer_tasks: dict[Any, asyncio.Task[None]] = field(default_factory=dict)
    seen: set[str] = field(default_factory=set)
    seen_order: deque[str] = field(default_factory=deque)
    seen_limit: int = 20_000
    recent_limit: int = 1_000
    wallet_recent_limit: int = 50
    wallet_cache_limit: int = 500
    replay_backfill_wallet_limit: int = 25
    client_send_timeout_seconds: float = DEFAULT_CLIENT_SEND_TIMEOUT_SECONDS
    client_queue_size: int = DEFAULT_CLIENT_QUEUE_SIZE
    cache_write_queue_size: int = 20_000
    cache_write_batch_size: int = 500
    cache_write_flush_seconds: float = 0.25
    recent_save_interval_seconds: float = 1.0
    upstream_idle_timeout_seconds: float = 90.0
    recent_messages: deque[dict[str, Any]] = field(default_factory=deque)
    wallet_recent_messages: dict[str, deque[dict[str, Any]]] = field(default_factory=dict)
    wallet_recent_order: deque[str] = field(default_factory=deque)
    client_wallet_filters: dict[Any, set[str]] = field(default_factory=dict)
    last_recent_save_at: float = 0.0
    upstream_connected: bool = False
    last_error: str = ""
    last_trade_at: str | None = None
    client_drop_count: int = 0
    client_queue_full_count: int = 0
    client_send_error_count: int = 0
    cache_write_queue: asyncio.Queue[dict[str, Any]] | None = field(default=None, init=False)
    cache_write_drop_count: int = 0
    cache_write_error_count: int = 0
    cache_write_batch_count: int = 0
    cache_write_last_at: str | None = None
    cache_write_last_error: str = ""

    def __post_init__(self) -> None:
        if self.state_dir is not None:
            self.cache_write_queue = asyncio.Queue(maxsize=self.cache_write_queue_size)
            loaded_wallets: set[str] = set()
            for message in load_recent_trade_messages(self.state_dir, limit=self.recent_limit):
                self.recent_messages.append(message)
                self.remember(_message_key(message))
                user_address = _message_user_address(message)
                if user_address and user_address not in loaded_wallets:
                    self.wallet_recent_order.append(user_address)
                    loaded_wallets.add(user_address)
                trade = message.get("trade")
                if isinstance(trade, dict) and not self.last_trade_at:
                    self.last_trade_at = str(trade.get("timestamp") or "") or None

    async def handle_client(self, websocket: Any, path: str = "") -> None:
        route, wallet_filter, replay_enabled = parse_client_path(path)
        if route not in ("", "/", "/official-trades", "/stream/official-trades"):
            await websocket.close(code=1008, reason="unsupported path")
            return
        self.register_client(websocket, wallet_filter)
        try:
            self.enqueue_client_payload(
                websocket,
                _json_dumps(
                    {
                        "type": "connected",
                        "source": "polymarket-rtds",
                        "server_time": api_datetime(datetime.now(UTC)),
                        "upstream_connected": self.upstream_connected,
                        "topic": "activity",
                        "feed_type": "trades",
                        "wallet_filter": sorted(wallet_filter),
                        "wallet_filter_count": len(wallet_filter),
                        "replay_enabled": replay_enabled,
                    }
                ),
            )
            if replay_enabled:
                replay_messages = await asyncio.to_thread(self.replay_messages, wallet_filter)
                for message in replay_messages:
                    if not self.enqueue_client_payload(
                        websocket,
                        _json_dumps({**message, "replay": True}),
                        drop_on_full=False,
                    ):
                        break
            with contextlib.suppress(Exception):
                async for _message in websocket:
                    continue
        finally:
            self.drop_client(websocket)

    def register_client(self, websocket: Any, wallet_filter: set[str]) -> None:
        self.clients.add(websocket)
        self.client_wallet_filters[websocket] = wallet_filter
        self.client_queues[websocket] = asyncio.Queue(maxsize=self.client_queue_size)
        self.client_writer_tasks[websocket] = asyncio.create_task(
            self.client_writer_loop(websocket)
        )

    async def client_writer_loop(self, websocket: Any) -> None:
        queue = self.client_queues.get(websocket)
        if queue is None:
            return
        try:
            while True:
                payload = await queue.get()
                await asyncio.wait_for(
                    websocket.send(payload),
                    timeout=self.client_send_timeout_seconds,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            self.client_send_error_count += 1
        finally:
            self.drop_client(websocket)
            with contextlib.suppress(Exception):
                await websocket.close()

    def enqueue_client_payload(
        self,
        websocket: Any,
        payload: str,
        *,
        drop_on_full: bool = True,
    ) -> bool:
        queue = self.client_queues.get(websocket)
        if queue is None:
            return False
        try:
            queue.put_nowait(payload)
            return True
        except asyncio.QueueFull:
            self.client_queue_full_count += 1
            if drop_on_full:
                self.drop_client(websocket)
            return False

    def drop_client(self, websocket: Any) -> None:
        if websocket in self.clients:
            self.client_drop_count += 1
        self.clients.discard(websocket)
        self.client_wallet_filters.pop(websocket, None)
        self.client_queues.pop(websocket, None)
        task = self.client_writer_tasks.pop(websocket, None)
        current_task = asyncio.current_task()
        if task is not None and task is not current_task:
            task.cancel()

    async def upstream_loop(self) -> None:
        try:
            from websockets.asyncio.client import connect
        except ImportError:  # pragma: no cover - depends on installed websockets version.
            from websockets import connect  # type: ignore

        reconnect_delay = 1.0
        while True:
            try:
                async with connect(self.upstream_url, ping_interval=20, ping_timeout=20) as websocket:
                    self.upstream_connected = True
                    self.last_error = ""
                    reconnect_delay = 1.0
                    await websocket.send(_json_dumps(POLYMARKET_TRADE_SUBSCRIPTION))
                    await self.broadcast_status("upstream_connected")
                    last_trade_message_at = time.monotonic()
                    while True:
                        raw_message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=self.upstream_idle_timeout_seconds,
                        )
                        if (
                            time.monotonic() - last_trade_message_at
                            > self.upstream_idle_timeout_seconds
                        ):
                            raise TimeoutError(
                                f"upstream trade idle for {self.upstream_idle_timeout_seconds:.0f}s"
                            )
                        if not raw_message:
                            continue
                        message = _parse_json(raw_message)
                        if message is None:
                            continue
                        normalized = normalize_rtds_trade_message(message)
                        if normalized is None:
                            continue
                        last_trade_message_at = time.monotonic()
                        key = _message_key(normalized)
                        if key in self.seen:
                            continue
                        self.remember(key)
                        self.remember_recent(normalized)
                        self.last_trade_at = normalized["trade"].get("timestamp")
                        await self.broadcast(normalized)
            except TimeoutError as exc:
                self.upstream_connected = False
                self.last_error = (
                    f"upstream idle for {self.upstream_idle_timeout_seconds:.0f}s"
                )
                await self.broadcast_status("upstream_idle", error=self.last_error)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.6, 15.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.upstream_connected = False
                self.last_error = str(exc)
                await self.broadcast_status("upstream_error", error=str(exc))
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.6, 15.0)

    async def heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(10)
            await self.broadcast_status("heartbeat")

    async def cache_writer_loop(self) -> None:
        if self.state_dir is None or self.cache_write_queue is None:
            while True:
                await asyncio.sleep(3600)
        queue = self.cache_write_queue
        while True:
            try:
                first_message = await queue.get()
                batch = [first_message]
                started_at = time.monotonic()
                while len(batch) < self.cache_write_batch_size:
                    timeout = self.cache_write_flush_seconds - (time.monotonic() - started_at)
                    if timeout <= 0:
                        break
                    try:
                        batch.append(await asyncio.wait_for(queue.get(), timeout=timeout))
                    except TimeoutError:
                        break
                recent_snapshot = list(self.recent_messages)
                wallet_new_messages: dict[str, list[dict[str, Any]]] = {}
                for message in batch:
                    user_address = _message_user_address(message)
                    if user_address:
                        wallet_new_messages.setdefault(user_address, []).append(message)
                await asyncio.to_thread(
                    save_trade_cache_batch,
                    self.state_dir,
                    recent_snapshot,
                    wallet_new_messages,
                    self.wallet_recent_limit,
                )
                self.last_recent_save_at = time.monotonic()
                self.cache_write_batch_count += 1
                self.cache_write_last_at = api_datetime(datetime.now(UTC))
                self.cache_write_last_error = ""
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.cache_write_error_count += 1
                self.cache_write_last_error = str(exc)
                await asyncio.sleep(0.5)

    def remember(self, key: str) -> None:
        if key in self.seen:
            return
        self.seen.add(key)
        self.seen_order.append(key)
        while len(self.seen_order) > self.seen_limit:
            old_key = self.seen_order.popleft()
            self.seen.discard(old_key)

    def remember_recent(self, message: dict[str, Any]) -> None:
        self.recent_messages.appendleft(message)
        while len(self.recent_messages) > self.recent_limit:
            self.recent_messages.pop()
        if self.state_dir is not None:
            self.remember_wallet_recent_memory(message)
            self.enqueue_cache_write(message)

    def remember_wallet_recent_memory(self, message: dict[str, Any]) -> None:
        if self.state_dir is None:
            return
        user_address = _message_user_address(message)
        if not user_address:
            return
        wallet_messages = self.wallet_recent_messages.get(user_address)
        if wallet_messages is None:
            wallet_messages = deque()
            self.wallet_recent_messages[user_address] = wallet_messages
            self.wallet_recent_order.append(user_address)
        key = _message_key(message)
        wallet_messages = deque(
            existing
            for existing in wallet_messages
            if _message_key(existing) != key
        )
        wallet_messages.appendleft(message)
        while len(wallet_messages) > self.wallet_recent_limit:
            wallet_messages.pop()
        self.wallet_recent_messages[user_address] = wallet_messages
        while len(self.wallet_recent_order) > self.wallet_cache_limit:
            old_user_address = self.wallet_recent_order.popleft()
            if old_user_address != user_address:
                self.wallet_recent_messages.pop(old_user_address, None)

    def enqueue_cache_write(self, message: dict[str, Any]) -> None:
        queue = self.cache_write_queue
        if queue is None:
            return
        try:
            queue.put_nowait(message)
        except asyncio.QueueFull:
            self.cache_write_drop_count += 1

    async def broadcast_status(self, status: str, *, error: str = "") -> None:
        await self.broadcast(
            {
                "type": "status",
                "status": status,
                "source": "polymarket-rtds",
                "server_time": api_datetime(datetime.now(UTC)),
                "upstream_connected": self.upstream_connected,
                "client_count": len(self.clients),
                "client_queue_count": len(self.client_queues),
                "client_drop_count": self.client_drop_count,
                "client_queue_full_count": self.client_queue_full_count,
                "client_send_error_count": self.client_send_error_count,
                "cache_write_queue_size": (
                    self.cache_write_queue.qsize() if self.cache_write_queue is not None else 0
                ),
                "cache_write_drop_count": self.cache_write_drop_count,
                "cache_write_error_count": self.cache_write_error_count,
                "cache_write_batch_count": self.cache_write_batch_count,
                "cache_write_last_at": self.cache_write_last_at,
                "cache_write_last_error": self.cache_write_last_error,
                "last_trade_at": self.last_trade_at,
                "error": error or self.last_error,
            }
        )

    async def broadcast(self, message: dict[str, Any]) -> None:
        if not self.clients:
            return
        payload = _json_dumps(message)
        for websocket in list(self.clients):
            if not self.client_accepts_message(websocket, message):
                continue
            self.enqueue_client_payload(websocket, payload)

    def client_accepts_message(self, websocket: Any, message: dict[str, Any]) -> bool:
        wallet_filter = self.client_wallet_filters.get(websocket) or set()
        if not wallet_filter or message.get("type") != "trade":
            return True
        return _message_user_address(message) in wallet_filter

    def replay_messages(self, wallet_filter: set[str]) -> list[dict[str, Any]]:
        if not wallet_filter:
            messages = list(self.recent_messages)
        else:
            messages = []
            if self.state_dir is not None:
                for user_address in sorted(wallet_filter):
                    messages.extend(
                        load_recent_wallet_trade_messages(
                            self.state_dir,
                            user_address,
                            limit=self.wallet_recent_limit,
                        )
                    )
            if len(wallet_filter) <= self.replay_backfill_wallet_limit:
                for user_address in sorted(wallet_filter):
                    messages.extend(self.backfill_wallet_recent_messages(user_address))
            messages.extend(
                message
                for message in self.recent_messages
                if _message_user_address(message) in wallet_filter
            )
        deduped: dict[str, dict[str, Any]] = {}
        for message in messages:
            deduped[_message_key(message)] = message
        sorted_messages = sorted(
            deduped.values(),
            key=_message_timestamp_text,
            reverse=True,
        )
        if self.state_dir is not None:
            for user_address in sorted(wallet_filter):
                wallet_messages = [
                    message
                    for message in sorted_messages
                    if _message_user_address(message) == user_address
                ][: self.wallet_recent_limit]
                if wallet_messages:
                    save_recent_wallet_trade_messages(
                        self.state_dir,
                        user_address,
                        wallet_messages,
                    )
        return sorted_messages[: self.recent_limit]

    def backfill_wallet_recent_messages(self, user_address: str) -> list[dict[str, Any]]:
        messages = fetch_wallet_activity_messages(
            user_address,
            limit=self.wallet_recent_limit,
        )
        if self.state_dir is not None:
            save_wallet_cache_messages(
                self.state_dir,
                user_address,
                messages,
                limit=self.wallet_recent_limit,
            )
        return messages


async def serve_official_trade_feed_async(
    *,
    host: str,
    port: int,
    upstream_url: str = POLYMARKET_RTDS_URL,
    state_dir: Path | None = None,
) -> None:
    try:
        from websockets.asyncio.server import serve
    except ImportError:  # pragma: no cover - depends on installed websockets version.
        from websockets import serve  # type: ignore

    state = OfficialTradeFeedState(upstream_url=upstream_url, state_dir=state_dir)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    async def handler(*args: Any) -> None:
        websocket = args[0]
        path = args[1] if len(args) > 1 else getattr(websocket, "path", "")
        if not path:
            request = getattr(websocket, "request", None)
            path = getattr(request, "path", "")
        await state.handle_client(websocket, str(path or ""))

    async with serve(handler, host, port, ping_interval=20, ping_timeout=20):
        print(
            _json_dumps(
                {
                    "service": "zetta_official_trade_feed",
                    "status": "listening",
                    "host": host,
                    "port": port,
                    "upstream_url": upstream_url,
                    "started_at": api_datetime(datetime.now(UTC)),
                }
            ),
            flush=True,
        )
        upstream_task = asyncio.create_task(state.upstream_loop())
        heartbeat_task = asyncio.create_task(state.heartbeat_loop())
        cache_writer_task = asyncio.create_task(state.cache_writer_loop())
        try:
            await stop_event.wait()
        finally:
            upstream_task.cancel()
            heartbeat_task.cancel()
            cache_writer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await upstream_task
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            with contextlib.suppress(asyncio.CancelledError):
                await cache_writer_task


def serve_official_trade_feed(
    *,
    host: str,
    port: int,
    upstream_url: str = POLYMARKET_RTDS_URL,
    state_dir: Path | None = None,
) -> None:
    asyncio.run(
        serve_official_trade_feed_async(
            host=host,
            port=port,
            upstream_url=upstream_url,
            state_dir=state_dir,
        )
    )


def load_recent_trade_messages(state_dir: Path, *, limit: int = 500) -> list[dict[str, Any]]:
    path = state_dir / f"{RECENT_CACHE_KEY}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    messages = payload.get("messages") if isinstance(payload, dict) else payload
    if not isinstance(messages, list):
        return []
    return [message for message in messages[:limit] if isinstance(message, dict)]


def load_recent_wallet_trade_messages(
    state_dir: Path,
    user_address: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    path = wallet_trade_messages_path(state_dir, user_address)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    messages = payload.get("messages") if isinstance(payload, dict) else payload
    if not isinstance(messages, list):
        return []
    return [message for message in messages[:limit] if isinstance(message, dict)]


def fetch_wallet_activity_messages(
    user_address: str,
    *,
    limit: int = 50,
    timeout_seconds: float = 5.0,
) -> list[dict[str, Any]]:
    normalized_user_address = normalize_wallet_filter_address(user_address)
    if not normalized_user_address:
        return []
    query = urlencode(
        {
            "user": normalized_user_address,
            "limit": max(1, min(limit, 100)),
            "offset": 0,
        }
    )
    request = urllib.request.Request(
        f"{POLYMARKET_DATA_API_URL}/activity?{query}",
        headers={
            "Accept": "application/json",
            "User-Agent": "ZettaPolymarketRealtime/0.1",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    messages: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").upper() != "TRADE":
            continue
        message = normalize_activity_trade_payload(
            item,
            upstream_timestamp=item.get("timestamp"),
            raw={
                "payload": item,
                "topic": "activity",
                "type": "trades",
                "source": "polymarket-data-api",
            },
            source="polymarket-data-api",
        )
        if message is not None:
            messages.append(message)
    return messages


def save_recent_trade_messages(state_dir: Path, messages: list[dict[str, Any]]) -> None:
    path = state_dir / f"{RECENT_CACHE_KEY}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(
            {
                "updated_at": api_datetime(datetime.now(UTC)),
                "messages": messages,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def save_recent_wallet_trade_messages(
    state_dir: Path,
    user_address: str,
    messages: list[dict[str, Any]],
) -> None:
    path = wallet_trade_messages_path(state_dir, user_address)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(
            {
                "updated_at": api_datetime(datetime.now(UTC)),
                "user_address": normalize_cached_wallet_address(user_address),
                "messages": messages,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def save_trade_cache_batch(
    state_dir: Path,
    recent_messages: list[dict[str, Any]],
    wallet_new_messages: dict[str, list[dict[str, Any]]],
    wallet_limit: int,
) -> None:
    save_recent_trade_messages(state_dir, recent_messages)
    for user_address, messages in wallet_new_messages.items():
        save_wallet_cache_messages(
            state_dir,
            user_address,
            messages,
            limit=wallet_limit,
        )


def save_wallet_cache_messages(
    state_dir: Path,
    user_address: str,
    messages: list[dict[str, Any]],
    *,
    limit: int,
) -> None:
    merged = merge_trade_messages(
        [
            *load_recent_wallet_trade_messages(state_dir, user_address, limit=limit),
            *messages,
        ],
        limit=limit,
    )
    if merged:
        save_recent_wallet_trade_messages(state_dir, user_address, merged)


def merge_trade_messages(messages: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for message in messages:
        if isinstance(message, dict):
            deduped[_message_key(message)] = message
    return sorted(
        deduped.values(),
        key=_message_timestamp_text,
        reverse=True,
    )[:limit]


def wallet_trade_messages_path(state_dir: Path, user_address: str) -> Path:
    return state_dir / WALLET_CACHE_DIR / f"{normalize_cached_wallet_address(user_address)}.json"


def parse_client_path(path: str) -> tuple[str, set[str], bool]:
    parsed = urlsplit(path or "")
    route = parsed.path or path or ""
    query = parse_qs(parsed.query, keep_blank_values=False)
    wallets = parse_wallet_filter_query(query)
    replay_enabled = not truthy_query_param(query, "live_only") and not falsey_query_param(
        query,
        "replay",
    )
    return route, wallets, replay_enabled


def parse_wallet_filter_query(query: dict[str, list[str]]) -> set[str]:
    raw_values: list[str] = []
    for key in ("wallets", "wallet", "users", "user", "addresses", "address"):
        raw_values.extend(query.get(key, []))
    wallets: list[str] = []
    for value in raw_values:
        text = str(value or "").strip()
        if not text:
            continue
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = None
            if isinstance(parsed, list):
                wallets.extend(str(item) for item in parsed)
                continue
        wallets.extend(part.strip() for part in text.replace("\n", ",").split(","))
    normalized = [
        normalize_wallet_filter_address(wallet)
        for wallet in wallets
    ]
    return {wallet for wallet in normalized if wallet} if len(normalized) <= MAX_CLIENT_WALLET_FILTERS else {
        wallet for wallet in normalized[:MAX_CLIENT_WALLET_FILTERS] if wallet
    }


def truthy_query_param(query: dict[str, list[str]], key: str) -> bool:
    values = query.get(key) or []
    if not values:
        return False
    return str(values[-1]).strip().lower() in {"1", "true", "yes", "y", "on"}


def falsey_query_param(query: dict[str, list[str]], key: str) -> bool:
    values = query.get(key) or []
    if not values:
        return False
    return str(values[-1]).strip().lower() in {"0", "false", "no", "n", "off"}


def normalize_wallet_filter_address(user_address: str) -> str:
    normalized = normalize_cached_wallet_address(user_address)
    if len(normalized) == 42 and normalized.startswith("0x"):
        return normalized
    return ""


def normalize_cached_wallet_address(user_address: str) -> str:
    normalized = str(user_address or "").strip().lower()
    return "".join(char for char in normalized if char.isalnum() or char in ("x", "_", "-"))[:96]


def api_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _payload_timestamp(payload_timestamp: Any, upstream_timestamp: Any) -> str:
    value = _float_or_none(payload_timestamp)
    if value is None:
        value = _float_or_none(upstream_timestamp)
        if value is not None and value > 10_000_000_000:
            value /= 1000
    if value is None:
        return api_datetime(datetime.now(UTC)) or ""
    return api_datetime(datetime.fromtimestamp(value, UTC)) or ""


def _latency_seconds(timestamp: str) -> float | None:
    try:
        parsed = datetime.fromisoformat(timestamp.replace(" ", "T")).replace(tzinfo=UTC)
    except ValueError:
        return None
    return max(0.0, time.time() - parsed.timestamp())


def _official_trade_id(payload: dict[str, Any], transaction_hash: str, token_id: str, timestamp: str) -> str:
    if transaction_hash:
        raw = "|".join(
            [
                transaction_hash,
                token_id,
                str(payload.get("proxyWallet") or ""),
                str(payload.get("side") or ""),
                str(payload.get("price") or ""),
                str(payload.get("size") or ""),
                timestamp,
            ]
        )
    else:
        raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _message_key(message: dict[str, Any]) -> str:
    trade = message.get("trade")
    if isinstance(trade, dict):
        trade_id = str(trade.get("trade_id") or "")
        if trade_id:
            return trade_id
    return hashlib.sha1(_json_dumps(message).encode("utf-8")).hexdigest()


def _message_user_address(message: dict[str, Any]) -> str:
    trade = message.get("trade")
    if not isinstance(trade, dict):
        return ""
    return normalize_cached_wallet_address(str(trade.get("user_address") or ""))


def _message_timestamp_text(message: dict[str, Any]) -> str:
    trade = message.get("trade")
    if isinstance(trade, dict):
        return str(trade.get("timestamp") or "")
    return ""


def _message_age_seconds(message: dict[str, Any]) -> float | None:
    timestamp = _message_timestamp_text(message)
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp.replace(" ", "T")).replace(tzinfo=UTC)
    except ValueError:
        return None
    return max(0.0, time.time() - parsed.timestamp())


def _message_is_older_than(message: dict[str, Any], max_age_seconds: float) -> bool:
    age = _message_age_seconds(message)
    return age is not None and age > max_age_seconds


def _parse_json(value: str | bytes) -> dict[str, Any] | None:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


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
