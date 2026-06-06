from __future__ import annotations

import json
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from zetta.config import Settings
from zetta.scheduler.tasks import PostgresTaskStore
from zetta.storage.clickhouse import ClickHouseWriter


@dataclass(frozen=True)
class ApiResponse:
    status: int
    body: dict[str, Any] | list[dict[str, Any]]


class ProductApi:
    def __init__(self, *, clickhouse: ClickHouseWriter, settings: Settings | None = None) -> None:
        self.clickhouse = clickhouse
        self.settings = settings

    def handle(self, path: str, query: dict[str, list[str]]) -> ApiResponse:
        if path == "/health":
            return ApiResponse(HTTPStatus.OK, {"ok": True})
        if path == "/stats/overview":
            return ApiResponse(HTTPStatus.OK, {"overview": self.stats_overview()})
        if path == "/stats/ingestion":
            return ApiResponse(HTTPStatus.OK, {"ingestion": self.stats_ingestion()})
        if path == "/tasks/progress":
            return ApiResponse(HTTPStatus.OK, self.tasks_progress(query))
        if path == "/markets/search":
            return ApiResponse(HTTPStatus.OK, {"markets": self.market_search(query)})
        if path == "/markets/detail":
            market = self.market_detail(query)
            if market is None:
                return ApiResponse(HTTPStatus.NOT_FOUND, {"error": "market_not_found"})
            return ApiResponse(HTTPStatus.OK, {"market": market})
        if path == "/markets/trades":
            return ApiResponse(HTTPStatus.OK, {"trades": self.market_trades(query)})
        if path == "/events/timeline":
            return ApiResponse(HTTPStatus.OK, {"events": self.event_timeline(query)})
        if path == "/traders/profile":
            profile = self.trader_profile(query)
            if profile is None:
                return ApiResponse(HTTPStatus.NOT_FOUND, {"error": "trader_not_found"})
            return ApiResponse(HTTPStatus.OK, {"profile": profile})
        if path == "/markets/liquidity":
            return ApiResponse(HTTPStatus.OK, {"liquidity": self.market_liquidity(query)})
        if path == "/alerts":
            return ApiResponse(HTTPStatus.OK, {"alerts": self.alerts(query)})
        return ApiResponse(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def stats_overview(self) -> dict[str, Any]:
        sql = """
            select
              (select count() from dim_event final) as events,
              (select count() from dim_market final) as markets,
              (select count() from dim_outcome_token final) as outcome_tokens,
              (select count() from fact_trade final) as trades,
              (select count() from fact_price_history final) as price_points,
              (select count() from fact_orderbook_snapshot) as orderbook_snapshots,
              (select count() from fact_chain_log final) as chain_logs,
              (select max(collected_at) from raw_ingest_log) as last_ingested_at
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else {}

    def stats_ingestion(self) -> list[dict[str, Any]]:
        sql = """
            select
              source,
              entity,
              count() as raw_batches,
              sum(item_count) as items,
              max(collected_at) as last_collected_at,
              max(raw_path) as sample_raw_path
            from raw_ingest_log
            group by source, entity
            order by last_collected_at desc
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def tasks_progress(self, query: dict[str, list[str]]) -> dict[str, Any]:
        if self.settings is None:
            return {"error": "task_store_unavailable"}
        recent_limit = int_param(query, "recent_limit", 10, maximum=100)
        return PostgresTaskStore(
            dsn=self.settings.postgres_dsn,
            node_id="api",
        ).progress(recent_limit=recent_limit)

    def market_search(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        text = param(query, "q").lower()
        limit = int_param(query, "limit", 25, maximum=100)
        where = "where 1 = 1"
        if text:
            escaped = ch_string(text)
            where += (
                " and (positionCaseInsensitive(question, "
                f"{escaped}) > 0 or positionCaseInsensitive(slug, {escaped}) > 0)"
            )
        sql = f"""
            select
              market_id,
              event_id,
              condition_id,
              question,
              slug,
              active,
              closed,
              volume,
              liquidity
            from dim_market final
            {where}
            order by volume desc, liquidity desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def market_detail(self, query: dict[str, list[str]]) -> dict[str, Any] | None:
        market_id = param(query, "market_id")
        condition_id = param(query, "condition_id")
        where = "where 1 = 1"
        if market_id:
            where += f" and market_id = {ch_string(market_id)}"
        elif condition_id:
            where += f" and condition_id = {ch_string(condition_id)}"
        else:
            return None
        sql = f"""
            select
              market_id,
              condition_id,
              question,
              slug,
              event_id,
              active,
              closed,
              archived,
              accepting_orders,
              volume,
              liquidity,
              start_time,
              end_time,
              created_at,
              updated_at
            from dim_market final
            {where}
            order by updated_at desc
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        if not rows:
            return None
        market = rows[0]
        market["tokens"] = self.market_tokens(str(market.get("market_id", "")))
        return market

    def market_tokens(self, market_id: str) -> list[dict[str, Any]]:
        if not market_id:
            return []
        sql = f"""
            select
              token_id,
              market_id,
              condition_id,
              outcome,
              outcome_index
            from dim_outcome_token final
            where market_id = {ch_string(market_id)}
            order by outcome_index asc
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def market_trades(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        market_id = param(query, "market_id")
        condition_id = param(query, "condition_id")
        limit = int_param(query, "limit", 50, maximum=500)
        where = "where 1 = 1"
        if market_id:
            where += f" and market_id = {ch_string(market_id)}"
        elif condition_id:
            where += f" and condition_id = {ch_string(condition_id)}"
        else:
            where += " and 1 = 0"
        sql = f"""
            select
              trade_id,
              transaction_hash,
              timestamp,
              market_id,
              condition_id,
              token_id,
              user_address,
              side,
              price,
              size,
              notional,
              source
            from fact_trade final
            {where}
            order by timestamp desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def event_timeline(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        event_id = param(query, "event_id")
        limit = int_param(query, "limit", 100, maximum=500)
        where = "where 1 = 1"
        if event_id:
            where += f" and event_id = {ch_string(event_id)}"
        sql = f"""
            select
              event_id,
              market_id,
              condition_id,
              question,
              start_time,
              end_time,
              active,
              closed,
              volume,
              liquidity,
              updated_at
            from dim_market final
            {where}
            order by ifNull(start_time, toDateTime64(0, 3, 'UTC')) asc, updated_at desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def trader_profile(self, query: dict[str, list[str]]) -> dict[str, Any] | None:
        user = param(query, "user").lower()
        if not user:
            return None
        sql = f"""
            select
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              position_count,
              current_value,
              cash_pnl,
              realized_pnl,
              total_pnl,
              chain_fill_count,
              chain_traded_size,
              chain_traded_notional,
              chain_position_size,
              chain_current_value,
              chain_net_cashflow,
              chain_mark_to_market_pnl,
              first_trade_at,
              last_trade_at,
              last_position_at,
              last_chain_fill_block
            from mart_trader_profile final
            where user_address = {ch_string(user)}
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def market_liquidity(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        token_id = param(query, "token_id")
        limit = int_param(query, "limit", 25, maximum=100)
        where = "where best_bid is not null and best_ask is not null and best_ask > 0"
        if token_id:
            where += f" and token_id = {ch_string(token_id)}"
        sql = f"""
            select
              token_id,
              argMax(market, captured_at) as market_id,
              max(captured_at) as captured_at,
              argMax(best_bid, captured_at) as best_bid,
              argMax(best_ask, captured_at) as best_ask,
              argMax(bid_depth, captured_at) as bid_depth,
              argMax(ask_depth, captured_at) as ask_depth,
              if(best_ask = 0, 0.0, (best_ask - best_bid) / best_ask) as spread_ratio,
              least(bid_depth, ask_depth) as estimated_two_sided_depth
            from fact_orderbook_snapshot
            {where}
            group by token_id
            order by captured_at desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def alerts(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        alert_type = param(query, "type")
        token_id = param(query, "token_id")
        limit = int_param(query, "limit", 50, maximum=500)
        where = "where 1 = 1"
        if alert_type:
            where += f" and alert_type = {ch_string(alert_type)}"
        if token_id:
            where += f" and token_id = {ch_string(token_id)}"
        sql = f"""
            select
              alert_id,
              alert_type,
              severity,
              token_id,
              market_id,
              user_address,
              occurred_at,
              metric_name,
              metric_value,
              threshold,
              message
            from mart_alert final
            {where}
            order by occurred_at desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))


def serve_api(*, settings: Settings, host: str, port: int) -> None:
    api = ProductApi(clickhouse=ClickHouseWriter(settings), settings=settings)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            try:
                response = api.handle(parsed.path, parse_qs(parsed.query))
            except Exception as exc:
                print(f"api request failed path={parsed.path}: {exc}", flush=True)
                response = ApiResponse(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal_error"})
            encoded = json.dumps(response.body, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(int(response.status))
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    ThreadingHTTPServer((host, port), Handler).serve_forever()


def rows_json(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def param(query: dict[str, list[str]], key: str, default: str = "") -> str:
    value = query.get(key, [default])[0]
    return str(value or default)


def int_param(query: dict[str, list[str]], key: str, default: int, *, maximum: int) -> int:
    try:
        value = int(param(query, key, str(default)))
    except ValueError:
        value = default
    return max(1, min(value, maximum))


def ch_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
