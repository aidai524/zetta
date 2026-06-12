from __future__ import annotations

import json
from http.client import IncompleteRead, RemoteDisconnected
import subprocess
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen

from zetta.rate_limit import RateLimiter


class HttpClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    headers: dict[str, str]
    body: Any


class JsonHttpClient:
    def __init__(
        self,
        *,
        timeout_seconds: float,
        user_agent: str,
        max_retries: int = 4,
        backoff_seconds: float = 0.75,
        rate_limiter: RateLimiter | None = None,
        resolve_overrides: dict[str, str] | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.rate_limiter = rate_limiter
        self.resolve_overrides = resolve_overrides or {}

    def get(self, url: str, params: dict[str, Any] | None = None) -> HttpResponse:
        full_url = self._with_query(url, params)
        if self.rate_limiter is not None:
            self.rate_limiter.wait_all(api_buckets(full_url))
        attempt = 0
        while True:
            request = Request(full_url, headers={"User-Agent": self.user_agent})
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    raw = response.read()
                    body = json.loads(raw.decode("utf-8")) if raw else None
                    return HttpResponse(
                        url=full_url,
                        status=response.status,
                        headers=dict(response.headers.items()),
                        body=body,
                    )
            except HTTPError as exc:
                if exc.code in {408, 425, 429, 500, 502, 503, 504} and attempt < self.max_retries:
                    self._sleep(attempt)
                    attempt += 1
                    continue
                details = exc.read().decode("utf-8", errors="replace")
                raise HttpClientError(f"GET {full_url} failed with {exc.code}: {details}") from exc
            except (URLError, TimeoutError, RemoteDisconnected, IncompleteRead) as exc:
                if attempt < self.max_retries:
                    self._sleep(attempt)
                    attempt += 1
                    continue
                return self._get_with_curl(full_url, exc)

    def _sleep(self, attempt: int) -> None:
        time.sleep(self.backoff_seconds * (2**attempt))

    def _get_with_curl(self, full_url: str, original_error: Exception) -> HttpResponse:
        args = [
            "curl",
            "-fsSL",
            "--max-time",
            str(max(1, int(self.timeout_seconds))),
            "-A",
            self.user_agent,
        ]
        args.extend(curl_resolve_args(full_url, self.resolve_overrides))
        args.append(full_url)
        try:
            result = subprocess.run(
                args,
                text=True,
                capture_output=True,
                check=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise HttpClientError(
                f"GET {full_url} failed: {original_error}; curl fallback failed: {exc}"
            ) from exc
        try:
            body = json.loads(result.stdout) if result.stdout else None
        except json.JSONDecodeError as exc:
            raise HttpClientError(f"GET {full_url} curl fallback returned invalid JSON") from exc
        return HttpResponse(url=full_url, status=200, headers={}, body=body)

    @staticmethod
    def _with_query(url: str, params: dict[str, Any] | None) -> str:
        if not params:
            return url
        clean_params = {
            key: value
            for key, value in params.items()
            if value is not None and value != "" and value != []
        }
        if not clean_params:
            return url
        return f"{url}?{urlencode(clean_params, doseq=True)}"


def api_buckets(url: str) -> list[str]:
    family = api_family(url)
    endpoint = api_endpoint(url, family)
    buckets = [family]
    if endpoint:
        buckets.append(f"{family}:{endpoint}")
    return buckets


def api_family(url: str) -> str:
    host = urlparse(url).netloc
    if "gamma-api.polymarket.com" in host:
        return "gamma"
    if "data-api.polymarket.com" in host:
        return "data"
    if "user-pnl-api.polymarket.com" in host:
        return "user_pnl"
    if "clob.polymarket.com" in host:
        return "clob"
    if "polygon" in host:
        return "polygon"
    return "default"


def api_endpoint(url: str, family: str | None = None) -> str | None:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    family = family or api_family(url)
    if family == "gamma":
        if path == "events/keyset":
            return "events_keyset"
        if path == "markets/keyset":
            return "markets_keyset"
    if family == "data":
        if path == "trades":
            return "trades"
        if path == "activity":
            return "activity"
        if path == "holders":
            return "holders"
        if path == "v1/market-positions":
            return "market_positions"
        if path == "oi":
            return "open_interest"
    if family == "user_pnl":
        if path == "user-pnl":
            return "user_pnl"
    if family == "clob":
        if path == "book":
            return "book"
        if path == "prices-history":
            return "prices_history"
    return None


def parse_resolve_overrides(value: str | dict[str, str] | None) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(host).strip(): str(ip).strip() for host, ip in value.items() if host and ip}
    overrides: dict[str, str] = {}
    for item in str(value or "").split(","):
        if not item.strip() or ":" not in item:
            continue
        host, ip = item.split(":", 1)
        host = host.strip()
        ip = ip.strip()
        if host and ip:
            overrides[host] = ip
    return overrides


def curl_resolve_args(url: str, overrides: dict[str, str]) -> list[str]:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    ip = overrides.get(host)
    if not ip:
        return []
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return ["--resolve", f"{host}:{port}:{ip}"]
