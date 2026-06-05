from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from zetta.config import Settings
from zetta.http import HttpResponse, JsonHttpClient, parse_resolve_overrides
from zetta.rate_limit import global_rate_limiter


@dataclass(frozen=True)
class Page:
    response: HttpResponse
    items: list[dict[str, Any]]
    next_cursor: str | None = None


class PolymarketClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.http = JsonHttpClient(
            timeout_seconds=settings.request_timeout_seconds,
            user_agent=settings.user_agent,
            rate_limiter=global_rate_limiter(),
            resolve_overrides=parse_resolve_overrides(settings.http_resolve_overrides),
        )

    def gamma_events_keyset(
        self,
        *,
        limit: int = 100,
        next_cursor: str | None = None,
        closed: bool | None = None,
        archived: bool | None = None,
        active: bool | None = None,
    ) -> Page:
        response = self.http.get(
            f"{self.settings.gamma_base_url}/events/keyset",
            {
                "limit": limit,
                "after_cursor": next_cursor,
                "closed": _bool_param(closed),
                "archived": _bool_param(archived),
                "active": _bool_param(active),
            },
        )
        return _keyset_page(response)

    def gamma_markets_keyset(
        self,
        *,
        limit: int = 100,
        next_cursor: str | None = None,
        closed: bool | None = None,
        archived: bool | None = None,
        active: bool | None = None,
    ) -> Page:
        response = self.http.get(
            f"{self.settings.gamma_base_url}/markets/keyset",
            {
                "limit": limit,
                "after_cursor": next_cursor,
                "closed": _bool_param(closed),
                "archived": _bool_param(archived),
                "active": _bool_param(active),
            },
        )
        return _keyset_page(response)

    def data_trades(
        self,
        *,
        limit: int = 500,
        offset: int = 0,
        user: str | None = None,
        market: str | None = None,
        event_id: str | None = None,
    ) -> Page:
        response = self.http.get(
            f"{self.settings.data_base_url}/trades",
            {
                "limit": limit,
                "offset": offset,
                "user": user,
                "market": market,
                "event": event_id,
            },
        )
        items = _list_body(response.body)
        next_cursor = str(offset + len(items)) if len(items) == limit else None
        return Page(response=response, items=items, next_cursor=next_cursor)

    def data_activity(
        self,
        *,
        user: str,
        limit: int = 500,
        offset: int = 0,
    ) -> Page:
        response = self.http.get(
            f"{self.settings.data_base_url}/activity",
            {"user": user, "limit": limit, "offset": offset},
        )
        items = _list_body(response.body)
        next_cursor = str(offset + len(items)) if len(items) == limit else None
        return Page(response=response, items=items, next_cursor=next_cursor)

    def data_holders(self, *, market: str, limit: int = 500) -> Page:
        response = self.http.get(
            f"{self.settings.data_base_url}/holders",
            {"market": market, "limit": limit},
        )
        return Page(response=response, items=_list_body(response.body))

    def data_market_positions(self, *, market: str, limit: int = 500) -> Page:
        response = self.http.get(
            f"{self.settings.data_base_url}/v1/market-positions",
            {"market": market, "limit": limit},
        )
        return Page(response=response, items=_list_body(response.body))

    def data_open_interest(self, *, market: str | None = None) -> Page:
        response = self.http.get(
            f"{self.settings.data_base_url}/oi",
            {"market": market},
        )
        return Page(response=response, items=_list_body(response.body))

    def clob_prices_history(
        self,
        *,
        market: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str | None = None,
        fidelity: int | None = None,
    ) -> Page:
        response = self.http.get(
            f"{self.settings.clob_base_url}/prices-history",
            {
                "market": market,
                "startTs": start_ts,
                "endTs": end_ts,
                "interval": interval,
                "fidelity": fidelity,
            },
        )
        body = response.body
        if isinstance(body, dict) and isinstance(body.get("history"), list):
            items = body["history"]
        else:
            items = _list_body(body)
        return Page(response=response, items=items)

    def clob_book(self, *, token_id: str) -> Page:
        response = self.http.get(
            f"{self.settings.clob_base_url}/book",
            {"token_id": token_id},
        )
        body = response.body if isinstance(response.body, dict) else {"body": response.body}
        return Page(response=response, items=[body])


def _keyset_page(response: HttpResponse) -> Page:
    body = response.body
    if isinstance(body, dict):
        items = _list_body(body.get("data") or body.get("events") or body.get("markets") or [])
        cursor = body.get("next_cursor") or body.get("nextCursor")
        return Page(response=response, items=items, next_cursor=str(cursor) if cursor else None)
    return Page(response=response, items=_list_body(body))


def _list_body(body: Any) -> list[dict[str, Any]]:
    if body is None:
        return []
    if isinstance(body, list):
        return [item for item in body if isinstance(item, dict)]
    if isinstance(body, dict):
        return [body]
    raise TypeError(f"Expected list or dict response body, got {type(body).__name__}")


def _bool_param(value: bool | None) -> str | None:
    if value is None:
        return None
    return "true" if value else "false"
