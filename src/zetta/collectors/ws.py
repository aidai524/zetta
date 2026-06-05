from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from zetta.config import Settings
from zetta.storage.raw import RawJsonlWriter


@dataclass(frozen=True)
class WsCollectionResult:
    channel: str
    messages: int
    heartbeats: int
    timed_out: bool


class MarketWebSocketCollector:
    def __init__(self, *, settings: Settings, raw_writer: RawJsonlWriter) -> None:
        self.settings = settings
        self.raw_writer = raw_writer

    def collect(
        self,
        *,
        token_ids: list[str],
        max_messages: int = 10,
        max_seconds: float = 30.0,
        custom_feature_enabled: bool = True,
        include_heartbeats: bool = False,
    ) -> WsCollectionResult:
        return asyncio.run(
            self.collect_async(
                token_ids=token_ids,
                max_messages=max_messages,
                max_seconds=max_seconds,
                custom_feature_enabled=custom_feature_enabled,
                include_heartbeats=include_heartbeats,
            )
        )

    async def collect_async(
        self,
        *,
        token_ids: list[str],
        max_messages: int,
        max_seconds: float,
        custom_feature_enabled: bool,
        include_heartbeats: bool,
    ) -> WsCollectionResult:
        if not token_ids:
            raise ValueError("At least one token ID is required for WebSocket subscription.")
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError(
                "The websockets package is required for market WebSocket collection."
            ) from exc

        message_count = 0
        heartbeat_count = 0
        timed_out = False
        loop = asyncio.get_running_loop()
        deadline = None if max_seconds <= 0 else loop.time() + max_seconds
        async with websockets.connect(self.settings.clob_ws_market_url) as websocket:
            await websocket.send(json.dumps(subscription_message(token_ids, custom_feature_enabled)))
            ping_task = asyncio.create_task(send_pings(websocket))
            try:
                while max_messages <= 0 or message_count < max_messages:
                    timeout = None if deadline is None else max(0.0, deadline - loop.time())
                    if timeout is not None and timeout <= 0:
                        timed_out = True
                        break
                    try:
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        timed_out = True
                        break
                    payload = parse_ws_payload(raw_message)
                    if is_heartbeat(payload):
                        heartbeat_count += 1
                        if not include_heartbeats:
                            continue
                    self.raw_writer.write(
                        source="clob_ws",
                        entity="market",
                        request_url=self.settings.clob_ws_market_url,
                        payload=payload,
                    )
                    message_count += 1
            finally:
                ping_task.cancel()
        return WsCollectionResult(
            channel="market",
            messages=message_count,
            heartbeats=heartbeat_count,
            timed_out=timed_out,
        )


def subscription_message(
    token_ids: list[str], custom_feature_enabled: bool = True
) -> dict[str, Any]:
    return {
        "assets_ids": token_ids,
        "type": "market",
        "custom_feature_enabled": custom_feature_enabled,
    }


def parse_ws_payload(raw_message: str | bytes) -> Any:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    try:
        return json.loads(raw_message)
    except json.JSONDecodeError:
        return {"message": raw_message}


def is_heartbeat(payload: Any) -> bool:
    return isinstance(payload, dict) and str(payload.get("message", "")).upper() == "PONG"


async def send_pings(websocket) -> None:
    while True:
        await asyncio.sleep(10)
        await websocket.send("PING")
