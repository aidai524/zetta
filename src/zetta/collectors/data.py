from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from zetta.http import HttpClientError
from zetta.polymarket import PolymarketClient
from zetta.chain.rpc import PolygonRpcClient
from zetta.storage.raw import RawJsonlWriter
from zetta.storage.state import LocalStateStore


PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
PUSD_DECIMALS = 1_000_000


@dataclass(frozen=True)
class TradesCollectionResult:
    pages: int
    trades: int
    next_offset: int | None
    raw_paths: list[str]


@dataclass(frozen=True)
class DataCollectionResult:
    entity: str
    pages: int
    items: int
    next_offset: int | None = None
    raw_paths: list[str] | None = None


class DataCollector:
    def __init__(
        self,
        *,
        client: PolymarketClient,
        raw_writer: RawJsonlWriter,
        state_store: LocalStateStore,
        rpc_client: PolygonRpcClient | None = None,
    ) -> None:
        self.client = client
        self.raw_writer = raw_writer
        self.state_store = state_store
        self.rpc_client = rpc_client

    def collect_trades(
        self,
        *,
        page_limit: int,
        max_pages: int,
        resume: bool,
        user: str | None = None,
        market: str | None = None,
        event_id: str | None = None,
    ) -> TradesCollectionResult:
        state_key = "data_trades_global"
        if user:
            state_key = f"data_trades_user_{user.lower()}"
        elif market:
            state_key = f"data_trades_market_{market}"
        elif event_id:
            state_key = f"data_trades_event_{event_id}"

        offset = int(self.state_store.get(state_key, {}).get("offset", 0)) if resume else 0
        pages = 0
        total_trades = 0
        raw_paths: list[str] = []

        while max_pages == 0 or pages < max_pages:
            try:
                page = self.client.data_trades(
                    limit=page_limit,
                    offset=offset,
                    user=user,
                    market=market,
                    event_id=event_id,
                )
            except HttpClientError as exc:
                if is_data_api_offset_limit(exc):
                    break
                raise
            raw_paths.append(
                str(
                    self.raw_writer.write(
                        source="data",
                        entity="trades",
                        request_url=page.response.url,
                        payload=page.response.body,
                    )
                )
            )
            pages += 1
            total_trades += len(page.items)
            offset += len(page.items)
            self.state_store.set(
                state_key,
                {
                    "offset": offset,
                    "last_request_url": page.response.url,
                    "last_page_items": len(page.items),
                },
            )
            if len(page.items) < page_limit:
                break

        next_offset = offset if total_trades else None
        return TradesCollectionResult(
            pages=pages,
            trades=total_trades,
            next_offset=next_offset,
            raw_paths=raw_paths,
        )

    def collect_activity(
        self,
        *,
        user: str,
        page_limit: int,
        max_pages: int,
        resume: bool,
    ) -> DataCollectionResult:
        state_key = f"data_activity_user_{user.lower()}"
        offset = int(self.state_store.get(state_key, {}).get("offset", 0)) if resume else 0
        pages = 0
        total = 0
        raw_paths: list[str] = []
        while pages < max_pages:
            page = self.client.data_activity(user=user, limit=page_limit, offset=offset)
            raw_paths.append(
                str(
                    self.raw_writer.write(
                        source="data",
                        entity="activity",
                        request_url=page.response.url,
                        payload=page.response.body,
                    )
                )
            )
            pages += 1
            total += len(page.items)
            offset += len(page.items)
            self.state_store.set(
                state_key,
                {
                    "offset": offset,
                    "last_request_url": page.response.url,
                    "last_page_items": len(page.items),
                },
            )
            if len(page.items) < page_limit:
                break
        return DataCollectionResult("activity", pages, total, offset if total else None, raw_paths)

    def collect_holders(self, *, market: str, limit: int) -> DataCollectionResult:
        page = self.client.data_holders(market=market, limit=limit)
        raw_path = self.raw_writer.write(
            source="data",
            entity="holders",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return DataCollectionResult("holders", 1, len(page.items), raw_paths=[str(raw_path)])

    def collect_market_positions(self, *, market: str, limit: int) -> DataCollectionResult:
        page = self.client.data_market_positions(market=market, limit=limit)
        raw_path = self.raw_writer.write(
            source="data",
            entity="market_positions",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return DataCollectionResult("market_positions", 1, len(page.items), raw_paths=[str(raw_path)])

    def collect_positions(self, *, user: str) -> DataCollectionResult:
        page = self.client.data_positions(user=user)
        raw_path = self.raw_writer.write(
            source="data",
            entity="positions",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return DataCollectionResult("positions", 1, len(page.items), raw_paths=[str(raw_path)])

    def collect_value(self, *, user: str) -> DataCollectionResult:
        page = self.client.data_value(user=user)
        raw_path = self.raw_writer.write(
            source="data",
            entity="value",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return DataCollectionResult("value", 1, len(page.items), raw_paths=[str(raw_path)])

    def collect_user_pnl(self, *, user: str, interval: str = "all", fidelity: str = "1d") -> DataCollectionResult:
        page = self.client.user_pnl(user=user, interval=interval, fidelity=fidelity)
        raw_path = self.raw_writer.write(
            source="data",
            entity="user_pnl",
            request_url=page.response.url,
            payload={"user": user.lower(), "points": page.response.body},
        )
        return DataCollectionResult("user_pnl", 1, len(page.items), raw_paths=[str(raw_path)])

    def collect_wallet_portfolio(self, *, user: str) -> DataCollectionResult:
        wallet = user.lower()
        raw_paths: list[str] = []
        positions = self.client.data_positions(user=wallet)
        value = self.client.data_value(user=wallet)
        pnl = self.client.user_pnl(user=wallet)
        raw_paths.append(
            str(
                self.raw_writer.write(
                    source="data",
                    entity="positions",
                    request_url=positions.response.url,
                    payload=positions.response.body,
                )
            )
        )
        items = len(positions.items) + len(value.items) + len(pnl.items)
        aggregate: dict[str, object] = {
            "user": wallet,
            "positions": positions.response.body,
            "value": value.response.body,
            "pnl": pnl.response.body,
        }
        if self.rpc_client is not None:
            balance = self.pusd_balance(user)
            aggregate["availableBalance"] = balance.get("balance")
            aggregate["availableBalanceRaw"] = balance
            items += 1
        raw_paths.append(
            str(
                self.raw_writer.write(
                    source="data",
                    entity="wallet_portfolio",
                    request_url=positions.response.url,
                    payload=aggregate,
                )
            )
        )
        return DataCollectionResult(
            "wallet_portfolio",
            2,
            items,
            raw_paths=raw_paths,
        )

    def pusd_balance(self, user: str) -> dict[str, object]:
        if self.rpc_client is None:
            raise ValueError("rpc_client is required to collect pUSD balance")
        wallet = user.lower()
        raw = self.rpc_client.eth_call(to=PUSD_ADDRESS, data=erc20_balance_of_data(wallet))
        amount = int(raw, 16) / PUSD_DECIMALS if raw and raw != "0x" else 0.0
        return {
            "user": wallet,
            "token": PUSD_ADDRESS,
            "balance": amount,
            "rawBalance": raw,
            "capturedAt": datetime.now(UTC).isoformat(),
        }

    def collect_open_interest(self, *, market: str | None = None) -> DataCollectionResult:
        page = self.client.data_open_interest(market=market)
        raw_path = self.raw_writer.write(
            source="data",
            entity="open_interest",
            request_url=page.response.url,
            payload=page.response.body,
        )
        return DataCollectionResult("open_interest", 1, len(page.items), raw_paths=[str(raw_path)])


def is_data_api_offset_limit(exc: HttpClientError) -> bool:
    message = str(exc)
    return "failed with 400" in message and "max historical activity offset" in message


def erc20_balance_of_data(address: str) -> str:
    cleaned = address.lower().removeprefix("0x")
    if len(cleaned) != 40:
        raise ValueError(f"invalid wallet address: {address}")
    return f"0x70a08231000000000000000000000000{cleaned}"
