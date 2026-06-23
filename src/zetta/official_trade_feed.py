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
    seen: set[str] = field(default_factory=set)
    seen_order: deque[str] = field(default_factory=deque)
    seen_limit: int = 20_000
    recent_limit: int = 1_000
    wallet_recent_limit: int = 50
    wallet_cache_limit: int = 500
    replay_backfill_wallet_limit: int = 25
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

    def __post_init__(self) -> None:
        if self.state_dir is not None:
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
        route, wallet_filter = parse_client_path(path)
        if route not in ("", "/", "/official-trades", "/stream/official-trades"):
            await websocket.close(code=1008, reason="unsupported path")
            return
        self.clients.add(websocket)
        self.client_wallet_filters[websocket] = wallet_filter
        try:
            await websocket.send(
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
                    }
                )
            )
            replay_messages = await asyncio.to_thread(self.replay_messages, wallet_filter)
            for message in replay_messages:
                await websocket.send(_json_dumps({**message, "replay": True}))
            with contextlib.suppress(Exception):
                async for _message in websocket:
                    continue
        finally:
            self.clients.discard(websocket)
            self.client_wallet_filters.pop(websocket, None)

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
            now = time.monotonic()
            if now - self.last_recent_save_at >= self.recent_save_interval_seconds:
                save_recent_trade_messages(self.state_dir, list(self.recent_messages))
                self.last_recent_save_at = now
            self.remember_wallet_recent(message)

    def remember_wallet_recent(self, message: dict[str, Any]) -> None:
        if self.state_dir is None:
            return
        user_address = _message_user_address(message)
        if not user_address:
            return
        wallet_messages = self.wallet_recent_messages.get(user_address)
        if wallet_messages is None:
            wallet_messages = deque(
                load_recent_wallet_trade_messages(
                    self.state_dir,
                    user_address,
                    limit=self.wallet_recent_limit,
                )
            )
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
        save_recent_wallet_trade_messages(
            self.state_dir,
            user_address,
            list(wallet_messages),
        )

    async def broadcast_status(self, status: str, *, error: str = "") -> None:
        await self.broadcast(
            {
                "type": "status",
                "status": status,
                "source": "polymarket-rtds",
                "server_time": api_datetime(datetime.now(UTC)),
                "upstream_connected": self.upstream_connected,
                "client_count": len(self.clients),
                "last_trade_at": self.last_trade_at,
                "error": error or self.last_error,
            }
        )

    async def broadcast(self, message: dict[str, Any]) -> None:
        if not self.clients:
            return
        payload = _json_dumps(message)
        stale_clients: list[Any] = []
        for websocket in list(self.clients):
            if not self.client_accepts_message(websocket, message):
                continue
            try:
                await websocket.send(payload)
            except Exception:
                stale_clients.append(websocket)
        for websocket in stale_clients:
            self.clients.discard(websocket)
            self.client_wallet_filters.pop(websocket, None)

    def client_accepts_message(self, websocket: Any, message: dict[str, Any]) -> bool:
        wallet_filter = self.client_wallet_filters.get(websocket) or set()
        if not wallet_filter or message.get("type") != "trade":
            return True
        return _message_user_address(message) in wallet_filter

    def replay_messages(self, wallet_filter: set[str]) -> list[dict[str, Any]]:
        if not wallet_filter:
            return list(self.recent_messages)
        messages: list[dict[str, Any]] = []
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
            for message in messages:
                self.remember_wallet_recent(message)
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
        try:
            await stop_event.wait()
        finally:
            upstream_task.cancel()
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await upstream_task
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task


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


def wallet_trade_messages_path(state_dir: Path, user_address: str) -> Path:
    return state_dir / WALLET_CACHE_DIR / f"{normalize_cached_wallet_address(user_address)}.json"


def parse_client_path(path: str) -> tuple[str, set[str]]:
    parsed = urlsplit(path or "")
    route = parsed.path or path or ""
    query = parse_qs(parsed.query, keep_blank_values=False)
    wallets = parse_wallet_filter_query(query)
    return route, wallets


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
