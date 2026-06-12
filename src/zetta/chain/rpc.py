from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from zetta.config import Settings
from zetta.rate_limit import global_rate_limiter


class JsonRpcError(RuntimeError):
    pass


@dataclass(frozen=True)
class JsonRpcResponse:
    method: str
    result: Any


class PolygonRpcClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.request_id = 0
        self.rate_limiter = global_rate_limiter()

    def call(self, method: str, params: list[Any]) -> JsonRpcResponse:
        self.request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params,
        }
        self.rate_limiter.wait_all(["polygon", "polygon:rpc"])
        request = Request(
            self.settings.polygon_rpc_url,
            data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "User-Agent": self.settings.user_agent,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
                body = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise JsonRpcError(f"{method} HTTP {exc.code}: {details}") from exc
        except URLError as exc:
            raise JsonRpcError(f"{method} request failed: {exc}") from exc
        if "error" in body:
            raise JsonRpcError(f"{method} failed: {body['error']}")
        return JsonRpcResponse(method=method, result=body.get("result"))

    def block_number(self) -> int:
        return int(str(self.call("eth_blockNumber", []).result), 16)

    def eth_call(self, *, to: str, data: str, block: str = "latest") -> str:
        result = self.call("eth_call", [{"to": to, "data": data}, block]).result
        return str(result or "0x")

    def get_logs(
        self,
        *,
        from_block: int,
        to_block: int,
        addresses: list[str] | None = None,
        topics: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
        }
        if addresses:
            params["address"] = addresses[0] if len(addresses) == 1 else addresses
        if topics:
            params["topics"] = topics
        result = self.call("eth_getLogs", [params]).result
        return [item for item in result if isinstance(item, dict)] if isinstance(result, list) else []
