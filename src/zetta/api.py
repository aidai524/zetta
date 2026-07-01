from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlparse

from zetta.chain.rpc import PolygonRpcClient
from zetta.config import Settings
from zetta.collectors.data import PUSD_ADDRESS, PUSD_DECIMALS, erc20_balance_of_data
from zetta.loaders.data import activity_rows, wallet_pnl_snapshot_rows, wallet_portfolio_rows
from zetta.official_trade_feed import load_recent_trade_messages, load_recent_wallet_trade_messages
from zetta.polymarket import PolymarketClient
from zetta.polycop_wallets import PolycopWalletSignalCacheStore
from zetta.scheduler.tasks import PostgresTaskStore, Task
from zetta.storage.clickhouse import ClickHouseWriter
from zetta.tracked_wallets import TrackedWalletStore, normalize_wallet_address
from zetta.unusual_betting_cache import UnusualBettingCacheStore
from zetta.worldcup_wallets import (
    RANKING_LIST_NAMES,
    base_match_slug,
    compact_worldcup_wallet_rankings,
    parse_slug_values,
    worldcup_event_slugs_for_scope,
    worldcup_wallet_rankings,
)


@dataclass(frozen=True)
class ApiResponse:
    status: int
    body: dict[str, Any] | list[dict[str, Any]]


class ProductApi:
    def __init__(self, *, clickhouse: ClickHouseWriter, settings: Settings | None = None) -> None:
        self.clickhouse = clickhouse
        self.settings = settings
        self._worldcup_wallet_rankings_cache: dict[
            tuple[tuple[str, ...], int], tuple[float, dict[str, Any]]
        ] = {}
        self._tasks_progress_cache: dict[tuple[int], tuple[float, dict[str, Any]]] = {}
        self._tasks_nodes_cache: dict[tuple[int], tuple[float, dict[str, Any]]] = {}
        self._market_search_cache: dict[tuple[str, str, int], tuple[float, list[dict[str, Any]]]] = {}
        self._live_trades_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
        self._unusual_betting_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
        self._live_trades_lock = Lock()
        self._unusual_betting_cache_store: UnusualBettingCacheStore | None = (
            UnusualBettingCacheStore(dsn=settings.postgres_dsn) if settings is not None else None
        )
        self._polycop_wallet_signal_cache_store: PolycopWalletSignalCacheStore | None = (
            PolycopWalletSignalCacheStore(dsn=settings.postgres_dsn)
            if settings is not None
            else None
        )

    def handle(self, path: str, query: dict[str, list[str]]) -> ApiResponse:
        return self.handle_request("GET", path, query, None)

    def handle_request(
        self,
        method: str,
        path: str,
        query: dict[str, list[str]],
        body: dict[str, Any] | None = None,
    ) -> ApiResponse:
        if path == "/health":
            return ApiResponse(HTTPStatus.OK, {"ok": True})
        if path == "/stats/overview":
            return ApiResponse(HTTPStatus.OK, {"overview": self.stats_overview()})
        if path == "/stats/ingestion":
            return ApiResponse(HTTPStatus.OK, {"ingestion": self.stats_ingestion()})
        if path == "/stats/system":
            return ApiResponse(HTTPStatus.OK, {"system": collect_system_stats()})
        if path == "/tasks/progress":
            return ApiResponse(HTTPStatus.OK, self.tasks_progress(query))
        if path == "/tasks/nodes":
            return ApiResponse(HTTPStatus.OK, self.tasks_nodes(query))
        if path == "/markets/overview":
            return ApiResponse(HTTPStatus.OK, {"overview": self.market_overview()})
        if path == "/markets/trending":
            return ApiResponse(HTTPStatus.OK, {"markets": self.trending_markets(query)})
        if path == "/categories/summary":
            return ApiResponse(HTTPStatus.OK, {"categories": self.category_summary(query)})
        if path == "/markets/search":
            return ApiResponse(HTTPStatus.OK, {"markets": self.market_search(query)})
        if path == "/markets/detail":
            market = self.market_detail(query)
            if market is None:
                return ApiResponse(HTTPStatus.NOT_FOUND, {"error": "market_not_found"})
            return ApiResponse(HTTPStatus.OK, {"market": market})
        if path == "/markets/trades":
            return ApiResponse(HTTPStatus.OK, {"trades": self.market_trades(query)})
        if path == "/trades/recent":
            return ApiResponse(HTTPStatus.OK, {"trades": self.recent_trades(query)})
        if path == "/trades/live":
            return ApiResponse(HTTPStatus.OK, self.live_trades(query))
        if path == "/events/timeline":
            return ApiResponse(HTTPStatus.OK, {"events": self.event_timeline(query)})
        if path == "/events/wallet-flow":
            return ApiResponse(HTTPStatus.OK, {"wallets": self.event_wallet_flow(query)})
        if path == "/events/pnl-leaderboard":
            return ApiResponse(HTTPStatus.OK, {"wallets": self.event_pnl_leaderboard(query)})
        if path == "/events/smart-wallet-options":
            return ApiResponse(HTTPStatus.OK, self.event_smart_wallet_options_response(query))
        if path == "/events/smart-wallets":
            return ApiResponse(HTTPStatus.OK, self.event_smart_wallets(query))
        if path == "/events/unusual-betting/summary":
            output = self.event_unusual_betting_summary(query)
            if output.get("status") == "event_not_found":
                return ApiResponse(HTTPStatus.NOT_FOUND, output)
            if output.get("status") == "missing_event":
                return ApiResponse(HTTPStatus.BAD_REQUEST, output)
            return ApiResponse(HTTPStatus.OK, output)
        if path == "/events/unusual-betting":
            output = self.event_unusual_betting(query)
            if output.get("status") == "event_not_found":
                return ApiResponse(HTTPStatus.NOT_FOUND, output)
            if output.get("status") == "missing_event":
                return ApiResponse(HTTPStatus.BAD_REQUEST, output)
            return ApiResponse(HTTPStatus.OK, output)
        if path == "/traders/profile":
            profile = self.trader_profile(query)
            if profile is None:
                return ApiResponse(HTTPStatus.NOT_FOUND, {"error": "trader_not_found"})
            return ApiResponse(HTTPStatus.OK, {"profile": profile})
        if path == "/wallets/reputation":
            profile = self.wallet_reputation(query)
            if profile is None:
                return ApiResponse(HTTPStatus.NOT_FOUND, {"error": "wallet_not_found"})
            return ApiResponse(HTTPStatus.OK, {"profile": profile})
        if path == "/wallets/summary":
            return ApiResponse(HTTPStatus.OK, {"summary": self.wallet_summary(query)})
        if path == "/wallets/screener":
            return ApiResponse(HTTPStatus.OK, {"wallets": self.wallet_screener(query)})
        if path == "/wallets/fifa-24h-pnl":
            return ApiResponse(HTTPStatus.OK, self.wallet_fifa_24h_pnl(query))
        if path == "/wallets/polycop-signals/summary":
            output = self.polycop_wallet_signals(query, include_wallets=False)
            status = (
                HTTPStatus.SERVICE_UNAVAILABLE
                if output.get("status") == "store_unavailable"
                else HTTPStatus.OK
            )
            return ApiResponse(status, output)
        if path == "/wallets/polycop-signals":
            output = self.polycop_wallet_signals(query)
            status = (
                HTTPStatus.SERVICE_UNAVAILABLE
                if output.get("status") == "store_unavailable"
                else HTTPStatus.OK
            )
            return ApiResponse(status, output)
        if path == "/wallets/polycop-fifa-signals":
            output = self.polycop_fifa_signals(query)
            status = (
                HTTPStatus.SERVICE_UNAVAILABLE
                if output.get("status") == "store_unavailable"
                else HTTPStatus.OK
            )
            return ApiResponse(status, output)
        if path == "/wallets/detail":
            detail = self.wallet_detail(query)
            if detail is None:
                return ApiResponse(HTTPStatus.NOT_FOUND, {"error": "wallet_not_found"})
            return ApiResponse(HTTPStatus.OK, detail)
        if path == "/wallets/tracked":
            return self.tracked_wallets_response(method, query, body)
        if path == "/wallets/live-positions":
            return ApiResponse(HTTPStatus.OK, {"positions": self.wallet_live_positions(query)})
        if path == "/wallets/smart-money/activity":
            return ApiResponse(HTTPStatus.OK, {"activity": self.smart_money_activity(query)})
        if path == "/worldcup/wallet-rankings":
            return ApiResponse(HTTPStatus.OK, self.worldcup_wallet_rankings(query))
        if path == "/markets/liquidity":
            return ApiResponse(HTTPStatus.OK, {"liquidity": self.market_liquidity(query)})
        if path == "/signals/anomalies":
            return ApiResponse(HTTPStatus.OK, {"signals": self.anomaly_signals(query)})
        if path == "/alerts":
            return ApiResponse(HTTPStatus.OK, {"alerts": self.alerts(query)})
        return ApiResponse(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def stats_overview(self) -> dict[str, Any]:
        sql = """
            select
              sumIf(rows, table = 'dim_event') as events,
              sumIf(rows, table = 'dim_market') as markets,
              sumIf(rows, table = 'dim_outcome_token') as outcome_tokens,
              sumIf(rows, table = 'fact_trade') as trades,
              sumIf(rows, table = 'fact_price_history') as price_points,
              sumIf(rows, table = 'fact_orderbook_snapshot') as orderbook_snapshots,
              sumIf(rows, table = 'fact_chain_log') as chain_logs,
              (select max(collected_at) from raw_ingest_log) as last_ingested_at,
              (select max(collected_at) from raw_ingest_log) as latest_data_at,
              (
                select maxIf(timestamp, timestamp <= now64(3) + interval 10 minute)
                from fact_trade_by_time
              ) as latest_trade_at
            from system.parts
            where database = currentDatabase()
              and active
              and table in (
                'dim_event',
                'dim_market',
                'dim_outcome_token',
                'fact_trade',
                'fact_price_history',
                'fact_orderbook_snapshot',
                'fact_chain_log'
              )
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
        cache_ttl_seconds = bounded_int_param(
            query,
            "cache_ttl_seconds",
            10,
            minimum=0,
            maximum=60,
        )
        cache_key = (recent_limit,)
        cached = self._tasks_progress_cache.get(cache_key)
        if (
            cache_ttl_seconds > 0
            and cached is not None
            and time.time() - cached[0] <= cache_ttl_seconds
        ):
            return cached[1]
        output = PostgresTaskStore(
            dsn=self.settings.postgres_dsn,
            node_id="api",
        ).progress(recent_limit=recent_limit)
        if cache_ttl_seconds > 0:
            self._tasks_progress_cache[cache_key] = (time.time(), output)
        return output

    def tasks_nodes(self, query: dict[str, list[str]]) -> dict[str, Any]:
        if self.settings is None:
            return {"error": "task_store_unavailable"}
        lookback_minutes = bounded_int_param(
            query,
            "lookback_minutes",
            30,
            minimum=1,
            maximum=1440,
        )
        cache_ttl_seconds = bounded_int_param(
            query,
            "cache_ttl_seconds",
            10,
            minimum=0,
            maximum=60,
        )
        cache_key = (lookback_minutes,)
        cached = self._tasks_nodes_cache.get(cache_key)
        if (
            cache_ttl_seconds > 0
            and cached is not None
            and time.time() - cached[0] <= cache_ttl_seconds
        ):
            return cached[1]
        output = PostgresTaskStore(
            dsn=self.settings.postgres_dsn,
            node_id="api",
        ).node_progress(lookback_minutes=lookback_minutes)
        if cache_ttl_seconds > 0:
            self._tasks_nodes_cache[cache_key] = (time.time(), output)
        return output

    def tracked_wallets_response(
        self,
        method: str,
        query: dict[str, list[str]],
        body: dict[str, Any] | None = None,
    ) -> ApiResponse:
        if self.settings is None:
            return ApiResponse(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "tracked_wallet_store_unavailable"})
        store = TrackedWalletStore(dsn=self.settings.postgres_dsn)
        if method == "GET":
            return ApiResponse(HTTPStatus.OK, {"wallets": store.list_wallets()})
        if method == "POST":
            payload = body or {}
            user_address = normalize_wallet_address(
                str(payload.get("address") or payload.get("user_address") or param(query, "user"))
            )
            if not user_address:
                return ApiResponse(HTTPStatus.BAD_REQUEST, {"error": "invalid_wallet_address"})
            wallet = store.upsert_wallet(
                user_address=user_address,
                name=str(payload.get("name") or ""),
            )
            self.enqueue_wallet_refresh(user_address)
            return ApiResponse(HTTPStatus.OK, {"wallet": wallet})
        if method == "DELETE":
            user_address = normalize_wallet_address(
                str((body or {}).get("address") or (body or {}).get("user_address") or param(query, "user"))
            )
            if not user_address:
                return ApiResponse(HTTPStatus.BAD_REQUEST, {"error": "invalid_wallet_address"})
            deleted = store.delete_wallet(user_address=user_address)
            return ApiResponse(HTTPStatus.OK, {"deleted": deleted})
        return ApiResponse(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "method_not_allowed"})

    def market_overview(self) -> dict[str, Any]:
        sql = """
            select
              (select sum(rows) from system.parts where database = currentDatabase() and active and table = 'dim_event') as events,
              (select sum(rows) from system.parts where database = currentDatabase() and active and table = 'dim_market') as markets,
              (select count() from dim_market final where active = true and closed = false) as active_markets,
              (select count() from dim_market final where closed = true) as completed_markets,
              (select sum(rows) from system.parts where database = currentDatabase() and active and table = 'dim_outcome_token') as outcome_tokens,
              (select sum(rows) from system.parts where database = currentDatabase() and active and table = 'fact_trade') as trades,
              (select count() from mart_trader_profile final) as tracked_wallets,
              (select uniqExact(user_address) from fact_trade_by_time where user_address != '' and timestamp >= now64(3) - interval 24 hour) as active_wallets_24h,
              (select sum(notional) from fact_trade_by_time where timestamp >= now64(3) - interval 24 hour) as volume_24h,
              (select sum(liquidity) from dim_market final where active = true and closed = false) as active_liquidity,
              (select count() from mart_event_anomaly_signal final) as anomaly_signals,
              (select countIf(severity = 'high') from mart_event_anomaly_signal final) as high_anomaly_signals,
              (select max(collected_at) from raw_ingest_log) as last_ingested_at
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else {}

    def trending_markets(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        limit = int_param(query, "limit", 20, maximum=100)
        status = param(query, "status")
        category = param(query, "category")
        where = "where base.market_id != ''"
        if status == "active":
            where += " and base.active = true and base.closed = false"
        elif status == "closed":
            where += " and base.closed = true"
        if category:
            where += f" and base.category = {ch_string(category)}"
        sql = f"""
            select
              base.market_id as market_id,
              base.event_id as event_id,
              base.condition_id as condition_id,
              base.question as question,
              base.slug as slug,
              base.category as category,
              base.active as active,
              base.closed as closed,
              base.volume as volume,
              base.liquidity as liquidity,
              base.start_time as start_time,
              base.end_time as end_time,
              base.updated_at as updated_at,
              ifNull(signal_count, 0) as signal_count,
              ifNull(high_signal_count, 0) as high_signal_count,
              ifNull(latest_trade_at, toDateTime64(0, 3, 'UTC')) as latest_trade_at,
              ifNull(volume_24h, 0.0) as volume_24h,
              ifNull(wallets_24h, 0) as wallets_24h
            from
            (
              select
                markets.market_id as market_id,
                markets.event_id as event_id,
                markets.condition_id as condition_id,
                markets.question as question,
                markets.slug as slug,
                events.category as category,
                markets.active as active,
                markets.closed as closed,
                markets.volume as volume,
                markets.liquidity as liquidity,
                markets.start_time as start_time,
                markets.end_time as end_time,
                markets.updated_at as updated_at
              from dim_market as markets final
              left join dim_event as events final on markets.event_id = events.event_id
            ) as base
            left join
            (
              select
                condition_id,
                max(timestamp) as latest_trade_at,
                sum(notional) as volume_24h,
                uniqExactIf(user_address, user_address != '') as wallets_24h
              from fact_trade_by_time
              where condition_id != ''
                and timestamp >= now64(3) - interval 24 hour
              group by condition_id
            ) as trade_stats on base.condition_id = trade_stats.condition_id
            left join
            (
              select
                market_id,
                count() as signal_count,
                countIf(severity = 'high') as high_signal_count
              from mart_event_anomaly_signal final
              where market_id != ''
              group by market_id
            ) as signal_stats on base.market_id = signal_stats.market_id
            {where}
            order by
              volume_24h desc,
              high_signal_count desc,
              signal_count desc,
              volume desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def category_summary(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        limit = int_param(query, "limit", 12, maximum=50)
        sql = f"""
            select
              base.category as category,
              base.market_count as market_count,
              base.active_market_count as active_market_count,
              base.closed_market_count as closed_market_count,
              base.volume as volume,
              base.liquidity as liquidity,
              ifNull(flows.volume_24h, 0.0) as volume_24h,
              ifNull(flows.active_wallets_24h, 0) as active_wallets_24h,
              ifNull(signals.signal_count, 0) as signal_count
            from
            (
              select
                if(events.category = '', 'Uncategorized', events.category) as category,
                count() as market_count,
                countIf(markets.active = true and markets.closed = false) as active_market_count,
                countIf(markets.closed = true) as closed_market_count,
                sum(markets.volume) as volume,
                sum(markets.liquidity) as liquidity
              from dim_market as markets final
              left join dim_event as events final on markets.event_id = events.event_id
              group by category
            ) as base
            left join
            (
              select
                if(events.category = '', 'Uncategorized', events.category) as category,
                sum(trades.notional) as volume_24h,
                uniqExact(trades.user_address) as active_wallets_24h
              from fact_trade_by_time as trades
              inner join dim_market as markets final on trades.condition_id = markets.condition_id
              left join dim_event as events final on markets.event_id = events.event_id
              where trades.timestamp >= now64(3) - interval 24 hour
                and trades.user_address != ''
              group by category
            ) as flows on base.category = flows.category
            left join
            (
              select
                if(events.category = '', 'Uncategorized', events.category) as category,
                count() as signal_count
              from mart_event_anomaly_signal as signals final
              left join dim_event as events final on signals.event_id = events.event_id
              group by category
            ) as signals on base.category = signals.category
            order by volume_24h desc, base.volume desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def market_search(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        text = param(query, "q").strip().lower()
        limit = int_param(query, "limit", 25, maximum=100)
        scope = param(query, "scope").lower()
        cache_ttl_seconds = bounded_int_param(
            query,
            "cache_ttl_seconds",
            15,
            minimum=0,
            maximum=300,
        )
        worldcup_scope = scope in ("world_cup", "worldcup", "fifwc")
        if worldcup_scope and text in ("world cup", "worldcup", "fifa world cup"):
            text = ""
        cache_key = (scope, text, limit)
        cached = self._market_search_cache.get(cache_key)
        if (
            cache_ttl_seconds > 0
            and cached is not None
            and time.time() - cached[0] <= cache_ttl_seconds
        ):
            return cached[1]
        where = "where 1 = 1"
        if worldcup_scope:
            where += (
                " and (startsWith(events.slug, 'fifwc-')"
                " or startsWith(markets.slug, 'fifwc-')"
                " or positionCaseInsensitive(events.title, 'World Cup') > 0"
                " or positionCaseInsensitive(markets.question, 'World Cup') > 0)"
            )
        if text:
            escaped = ch_string(text)
            where += (
                " and (positionCaseInsensitive(markets.question, "
                f"{escaped}) > 0 or positionCaseInsensitive(markets.slug, {escaped}) > 0"
                f" or positionCaseInsensitive(markets.condition_id, {escaped}) > 0"
                f" or positionCaseInsensitive(if(events.category = '', 'Uncategorized', events.category), {escaped}) > 0"
                f" or positionCaseInsensitive(events.title, {escaped}) > 0"
                f" or positionCaseInsensitive(events.slug, {escaped}) > 0)"
            )
        sql = f"""
            with
            selected_markets as
            (
              select
                markets.market_id as market_id,
                markets.event_id as event_id,
                markets.condition_id as condition_id,
                markets.question as question,
                markets.slug as slug,
                events.slug as event_slug,
                events.title as event_title,
                if(events.category = '', 'Uncategorized', events.category) as category,
                markets.active as active,
                markets.closed as closed,
                markets.volume as volume,
                markets.liquidity as liquidity,
                markets.start_time as start_time,
                markets.end_time as end_time,
                markets.updated_at as updated_at
              from dim_market as markets final
              left join dim_event as events final on markets.event_id = events.event_id
              {where}
            ),
            market_tokens as
            (
              select market_id, token_id, outcome, outcome_index
              from dim_outcome_token final
              where market_id in (select market_id from selected_markets)
                and token_id != ''
            ),
            primary_tokens as
            (
              select
                market_id,
                argMin(token_id, outcome_index) as primary_token_id,
                argMin(outcome, outcome_index) as primary_outcome
              from market_tokens
              group by market_id
            ),
            price_stats as
            (
              select
                token_id,
                argMax(price, timestamp) as last_price,
                argMinIf(
                  price,
                  abs(dateDiff('second', timestamp, now64(3) - interval 24 hour)),
                  timestamp <= now64(3) - interval 23 hour
                ) as price_24h_ago,
                countIf(timestamp <= now64(3) - interval 23 hour) as price_24h_count
              from fact_price_history
              where token_id in (select primary_token_id from primary_tokens)
              group by token_id
            ),
            book_stats as
            (
              select
                token_id,
                argMax(best_bid, captured_at) as book_best_bid,
                argMax(best_ask, captured_at) as book_best_ask
              from fact_orderbook_snapshot
              where token_id in (select primary_token_id from primary_tokens)
                and best_bid is not null
                and best_ask is not null
              group by token_id
            ),
            trade_stats as
            (
              select
                market_id,
                max(timestamp) as latest_trade_at,
                count() as trade_count_24h,
                sum(notional) as volume_24h
              from
              (
                select
                  raw_trade_key,
                  argMax(raw_market_id, raw_ingested_at) as market_id,
                  argMax(raw_timestamp, raw_ingested_at) as timestamp,
                  argMax(raw_notional, raw_ingested_at) as notional
                from
                (
                  select
                    if(
                      trades.trade_id != '',
                      concat(
                        trades.trade_id, '|', lower(trades.user_address), '|',
                        trades.token_id, '|', trades.side
                      ),
                      concat(
                        trades.transaction_hash, '|', toString(trades.log_index), '|',
                        lower(trades.user_address), '|', trades.token_id, '|', trades.side
                      )
                    ) as raw_trade_key,
                    market_tokens.market_id as raw_market_id,
                    trades.timestamp as raw_timestamp,
                    trades.notional as raw_notional,
                    trades.ingested_at as raw_ingested_at
                  from fact_trade_by_time as trades
                  inner join market_tokens on trades.token_id = market_tokens.token_id
                  where trades.timestamp >= now64(3) - interval 24 hour
                )
                group by raw_trade_key
              )
              group by market_id
            )
            select
              market_id,
              event_id,
              condition_id,
              question,
              slug,
              event_slug,
              event_title,
              category,
              active,
              closed,
              volume,
              liquidity,
              start_time,
              end_time,
              updated_at,
              primary_token_id,
              primary_outcome,
              last_price,
              if(
                last_price is null or price_24h_ago is null,
                cast(null, 'Nullable(Float64)'),
                last_price - price_24h_ago
              ) as price_change_24h,
              if(
                last_price is null or price_24h_ago is null or price_24h_ago = 0,
                cast(null, 'Nullable(Float64)'),
                (last_price - price_24h_ago) / price_24h_ago
              ) as price_change_pct_24h,
              volume_24h,
              trade_count_24h,
              latest_trade_at,
              best_bid,
              best_ask,
              spread
            from
            (
              select
                selected_markets.market_id as market_id,
                selected_markets.event_id as event_id,
                selected_markets.condition_id as condition_id,
                selected_markets.question as question,
                selected_markets.slug as slug,
                selected_markets.event_slug as event_slug,
                selected_markets.event_title as event_title,
                selected_markets.category as category,
                selected_markets.active as active,
                selected_markets.closed as closed,
                selected_markets.volume as volume,
                selected_markets.liquidity as liquidity,
                selected_markets.start_time as start_time,
                selected_markets.end_time as end_time,
                selected_markets.updated_at as updated_at,
                primary_tokens.primary_token_id as primary_token_id,
                primary_tokens.primary_outcome as primary_outcome,
                multiIf(
                  book_stats.book_best_bid is not null and book_stats.book_best_ask is not null,
                    cast((book_stats.book_best_bid + book_stats.book_best_ask) / 2, 'Nullable(Float64)'),
                  price_stats.last_price is not null,
                    cast(price_stats.last_price, 'Nullable(Float64)'),
                  cast(null, 'Nullable(Float64)')
                ) as last_price,
                if(
                  price_stats.price_24h_count > 0,
                  cast(price_stats.price_24h_ago, 'Nullable(Float64)'),
                  cast(null, 'Nullable(Float64)')
                ) as price_24h_ago,
                ifNull(trade_stats.volume_24h, 0.0) as volume_24h,
                ifNull(trade_stats.trade_count_24h, 0) as trade_count_24h,
                trade_stats.latest_trade_at as latest_trade_at,
                book_stats.book_best_bid as best_bid,
                book_stats.book_best_ask as best_ask,
                if(
                  book_stats.book_best_bid is null or book_stats.book_best_ask is null,
                  cast(null, 'Nullable(Float64)'),
                  book_stats.book_best_ask - book_stats.book_best_bid
                ) as spread
              from selected_markets
              left join primary_tokens on selected_markets.market_id = primary_tokens.market_id
              left join price_stats on primary_tokens.primary_token_id = price_stats.token_id
              left join book_stats on primary_tokens.primary_token_id = book_stats.token_id
              left join trade_stats on selected_markets.market_id = trade_stats.market_id
            ) as base
            order by volume_24h desc, volume desc, liquidity desc
            limit {limit}
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        if cache_ttl_seconds > 0:
            self._market_search_cache[cache_key] = (time.time(), rows)
        return rows

    def market_detail(self, query: dict[str, list[str]]) -> dict[str, Any] | None:
        market_id = param(query, "market_id")
        condition_id = param(query, "condition_id")
        where = "where 1 = 1"
        search_query: dict[str, list[str]]
        if market_id:
            where += f" and markets.market_id = {ch_string(market_id)}"
            search_query = {"market_id": [market_id], "limit": ["1"]}
        elif condition_id:
            where += f" and markets.condition_id = {ch_string(condition_id)}"
            search_query = {"condition_id": [condition_id], "limit": ["1"]}
        else:
            return None
        sql = f"""
            select
              markets.market_id as market_id,
              markets.condition_id as condition_id,
              markets.question as question,
              markets.slug as slug,
              markets.event_id as event_id,
              if(events.category = '', 'Uncategorized', events.category) as category,
              markets.active as active,
              markets.closed as closed,
              markets.archived as archived,
              markets.accepting_orders as accepting_orders,
              markets.volume as volume,
              markets.liquidity as liquidity,
              markets.start_time as start_time,
              markets.end_time as end_time,
              markets.created_at as created_at,
              markets.updated_at as updated_at
            from dim_market as markets final
            left join dim_event as events final on markets.event_id = events.event_id
            {where}
            order by markets.updated_at desc
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        if not rows:
            return None
        market = rows[0]
        market.update(self.market_enrichment(search_query))
        market["tokens"] = self.market_tokens(str(market.get("market_id", "")))
        return market

    def market_enrichment(self, query: dict[str, list[str]]) -> dict[str, Any]:
        market_id = param(query, "market_id")
        condition_id = param(query, "condition_id")
        where = "where 1 = 1"
        if market_id:
            where += f" and market_id = {ch_string(market_id)}"
        elif condition_id:
            where += f" and condition_id = {ch_string(condition_id)}"
        else:
            return {}
        sql = f"""
            with
            selected_markets as
            (
              select
                market_id,
                condition_id
              from dim_market final
              {where}
              limit 1
            ),
            market_tokens as
            (
              select market_id, token_id, outcome, outcome_index
              from dim_outcome_token final
              where market_id in (select market_id from selected_markets)
                and token_id != ''
            ),
            primary_tokens as
            (
              select
                market_id,
                argMin(token_id, outcome_index) as primary_token_id,
                argMin(outcome, outcome_index) as primary_outcome
              from market_tokens
              group by market_id
            ),
            price_stats as
            (
              select
                token_id,
                argMax(price, timestamp) as last_trade_price,
                argMinIf(
                  price,
                  abs(dateDiff('second', timestamp, now64(3) - interval 24 hour)),
                  timestamp <= now64(3) - interval 23 hour
                ) as price_24h_ago,
                countIf(timestamp <= now64(3) - interval 23 hour) as price_24h_count
              from fact_price_history
              where token_id in (select primary_token_id from primary_tokens)
              group by token_id
            ),
            book_stats as
            (
              select
                token_id,
                argMax(best_bid, captured_at) as book_best_bid,
                argMax(best_ask, captured_at) as book_best_ask
              from fact_orderbook_snapshot
              where token_id in (select primary_token_id from primary_tokens)
                and best_bid is not null
                and best_ask is not null
              group by token_id
            ),
            trade_stats as
            (
              select
                market_id,
                max(timestamp) as latest_trade_at,
                count() as trade_count_24h,
                sum(notional) as volume_24h
              from
              (
                select
                  raw_trade_key,
                  argMax(raw_market_id, raw_ingested_at) as market_id,
                  argMax(raw_timestamp, raw_ingested_at) as timestamp,
                  argMax(raw_notional, raw_ingested_at) as notional
                from
                (
                  select
                    if(
                      trades.trade_id != '',
                      concat(
                        trades.trade_id, '|', lower(trades.user_address), '|',
                        trades.token_id, '|', trades.side
                      ),
                      concat(
                        trades.transaction_hash, '|', toString(trades.log_index), '|',
                        lower(trades.user_address), '|', trades.token_id, '|', trades.side
                      )
                    ) as raw_trade_key,
                    market_tokens.market_id as raw_market_id,
                    trades.timestamp as raw_timestamp,
                    trades.notional as raw_notional,
                    trades.ingested_at as raw_ingested_at
                  from fact_trade_by_time as trades
                  inner join market_tokens on trades.token_id = market_tokens.token_id
                  where trades.timestamp >= now64(3) - interval 24 hour
                )
                group by raw_trade_key
              )
              group by market_id
            )
            select
              selected_markets.market_id as market_id,
              primary_tokens.primary_token_id as primary_token_id,
              primary_tokens.primary_outcome as primary_outcome,
              multiIf(
                book_stats.book_best_bid is not null and book_stats.book_best_ask is not null,
                  cast((book_stats.book_best_bid + book_stats.book_best_ask) / 2, 'Nullable(Float64)'),
                price_stats.last_trade_price is not null,
                  cast(price_stats.last_trade_price, 'Nullable(Float64)'),
                cast(null, 'Nullable(Float64)')
              ) as last_price,
              if(
                price_stats.price_24h_count > 0,
                cast(price_stats.price_24h_ago, 'Nullable(Float64)'),
                cast(null, 'Nullable(Float64)')
              ) as price_24h_ago,
              if(
                last_price is null or price_24h_ago is null,
                cast(null, 'Nullable(Float64)'),
                last_price - price_24h_ago
              ) as price_change_24h,
              if(
                last_price is null or price_24h_ago is null or price_24h_ago = 0,
                cast(null, 'Nullable(Float64)'),
                (last_price - price_24h_ago) / price_24h_ago
              ) as price_change_pct_24h,
              ifNull(trade_stats.volume_24h, 0.0) as volume_24h,
              ifNull(trade_stats.trade_count_24h, 0) as trade_count_24h,
              trade_stats.latest_trade_at as latest_trade_at,
              book_stats.book_best_bid as best_bid,
              book_stats.book_best_ask as best_ask,
              if(
                book_stats.book_best_bid is null or book_stats.book_best_ask is null,
                cast(null, 'Nullable(Float64)'),
                book_stats.book_best_ask - book_stats.book_best_bid
              ) as spread
            from selected_markets
            left join primary_tokens on selected_markets.market_id = primary_tokens.market_id
            left join price_stats on primary_tokens.primary_token_id = price_stats.token_id
            left join book_stats on primary_tokens.primary_token_id = book_stats.token_id
            left join trade_stats on selected_markets.market_id = trade_stats.market_id
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else {}

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
            where += (
                " and condition_id in "
                f"(select condition_id from dim_market final where market_id = {ch_string(market_id)})"
            )
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
            from fact_trade
            {where}
            order by timestamp desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def recent_trades(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        limit = int_param(query, "limit", 100, maximum=500)
        candidate_limit = min(max(limit * 50, 1000), 10000)
        side = param(query, "side").upper()
        category = param(query, "category")
        wallet_type = param(query, "wallet_type").lower()
        search = param(query, "q")
        min_notional = float_param(query, "min_notional", 0.0, minimum=0.0)
        max_notional = float_param(query, "max_notional", 0.0, minimum=0.0)

        trade_where = [
            "trades.timestamp >= now64(3) - interval 7 day",
            "trades.user_address != ''",
        ]
        if side in ("BUY", "SELL"):
            trade_where.append(f"trades.side = {ch_string(side)}")
        if min_notional > 0:
            trade_where.append(f"trades.notional >= {min_notional}")
        if max_notional > 0:
            trade_where.append(f"trades.notional <= {max_notional}")

        outer_where = ["1 = 1"]
        if wallet_type == "smart":
            outer_where.append("ifNull(screener.is_smart, false)")
        elif wallet_type == "whale":
            outer_where.append("ifNull(screener.is_whale, false)")
        elif wallet_type == "new":
            outer_where.append(
                "screener.first_trade_at is not null "
                "and screener.first_trade_at >= now64(3) - interval 7 day"
            )
        if search:
            quoted = ch_string(search)
            outer_where.append(
                "("
                f"positionCaseInsensitive(markets.question, {quoted}) > 0 "
                f"or positionCaseInsensitive(events.title, {quoted}) > 0 "
                f"or positionCaseInsensitive(user_address, {quoted}) > 0"
                ")"
            )

        sql = f"""
            with recent_candidates as
            (
              select
                raw_trade_key,
                argMax(raw_trade_id, raw_ingested_at) as trade_id,
                argMax(raw_transaction_hash, raw_ingested_at) as transaction_hash,
                argMax(raw_timestamp, raw_ingested_at) as timestamp,
                argMax(raw_market_id, raw_ingested_at) as market_id,
                argMax(raw_condition_id, raw_ingested_at) as condition_id,
                argMax(raw_token_id, raw_ingested_at) as token_id,
                argMax(raw_user_address, raw_ingested_at) as user_address,
                argMax(raw_side, raw_ingested_at) as side,
                argMax(raw_price, raw_ingested_at) as price,
                argMax(raw_size, raw_ingested_at) as size,
                argMax(raw_notional, raw_ingested_at) as notional,
                argMax(raw_source, raw_ingested_at) as source,
                argMax(raw_raw_json, raw_ingested_at) as raw_json,
                max(raw_ingested_at) as ingested_at
              from
              (
                select
                  if(
                    trades.trade_id != '',
                    concat(
                      trades.trade_id, '|', lower(trades.user_address), '|',
                      trades.token_id, '|', trades.side
                    ),
                    concat(
                      trades.transaction_hash, '|', toString(trades.log_index), '|',
                      lower(trades.user_address), '|', trades.token_id, '|', trades.side
                    )
                  ) as raw_trade_key,
                  trades.trade_id as raw_trade_id,
                  trades.transaction_hash as raw_transaction_hash,
                  trades.timestamp as raw_timestamp,
                  trades.market_id as raw_market_id,
                  trades.condition_id as raw_condition_id,
                  trades.token_id as raw_token_id,
                  lower(trades.user_address) as raw_user_address,
                  trades.side as raw_side,
                  trades.price as raw_price,
                  trades.size as raw_size,
                  trades.notional as raw_notional,
                  trades.source as raw_source,
                  trades.raw_json as raw_raw_json,
                  trades.ingested_at as raw_ingested_at
                from fact_trade_by_time as trades
                where {" and ".join(trade_where)}
                order by trades.timestamp desc
                limit {candidate_limit}
              )
              group by raw_trade_key
            )
            select
              recent_candidates.trade_id as trade_id,
              recent_candidates.transaction_hash as transaction_hash,
              recent_candidates.timestamp as timestamp,
              recent_candidates.market_id as market_id,
              recent_candidates.condition_id as condition_id,
              recent_candidates.token_id as token_id,
              recent_candidates.user_address as user_address,
              recent_candidates.side as side,
              recent_candidates.price as price,
              recent_candidates.size as size,
              recent_candidates.notional as notional,
              recent_candidates.source as source,
              recent_candidates.ingested_at as ingested_at,
              if(markets.question != '', markets.question, JSONExtractString(recent_candidates.raw_json, 'title'))
                as question,
              if(markets.slug != '', markets.slug, JSONExtractString(recent_candidates.raw_json, 'slug'))
                as market_slug,
              markets.event_id as event_id,
              events.title as event_title,
              if(events.slug != '', events.slug, JSONExtractString(recent_candidates.raw_json, 'eventSlug'))
                as event_slug,
              events.category as category,
              if(tokens.outcome != '', tokens.outcome, JSONExtractString(recent_candidates.raw_json, 'outcome'))
                as outcome,
              JSONExtractString(recent_candidates.raw_json, 'name') as trader_name,
              JSONExtractString(recent_candidates.raw_json, 'pseudonym') as trader_pseudonym,
              ifNull(screener.is_smart, false) as is_smart,
              ifNull(screener.is_whale, false) as is_whale,
              ifNull(screener.total_pnl, 0.0) as wallet_total_pnl,
              ifNull(screener.pnl_roi, 0.0) as wallet_pnl_roi,
              ifNull(screener.traded_notional, 0.0) as wallet_traded_notional
            from recent_candidates
            left join dim_market as markets final on recent_candidates.condition_id = markets.condition_id
            left join dim_event as events final on markets.event_id = events.event_id
            left join dim_outcome_token as tokens final on recent_candidates.token_id = tokens.token_id
            left join mart_wallet_screener as screener final on recent_candidates.user_address = screener.user_address
            where {" and ".join(outer_where)}
            order by recent_candidates.timestamp desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def live_trades(self, query: dict[str, list[str]]) -> dict[str, Any]:
        if self.settings is None:
            return {"source": "live", "status": "settings_unavailable", "trades": []}
        limit = int_param(query, "limit", 100, maximum=500)
        page_count = bounded_int_param(query, "pages", 6, minimum=1, maximum=10)
        page_size = bounded_int_param(query, "page_size", 100, minimum=100, maximum=500)
        ttl_seconds = float_param(query, "ttl", 2.0, minimum=0.5, maximum=15.0)
        side = param(query, "side").upper()
        search = param(query, "q").strip().lower()
        category = param(query, "category")
        min_notional = float_param(query, "min_notional", 0.0, minimum=0.0)
        max_notional = float_param(query, "max_notional", 0.0, minimum=0.0)
        cache_key = (
            page_count,
            page_size,
            side if side in ("BUY", "SELL") else "all",
            search,
            category.lower(),
            min_notional,
            max_notional,
        )
        now = time.monotonic()
        cached = self._live_trades_cache.get(cache_key)
        if cached is not None and now - cached[0] <= ttl_seconds:
            return limit_live_trades_response(cached[1], limit)

        with self._live_trades_lock:
            now = time.monotonic()
            cached = self._live_trades_cache.get(cache_key)
            if cached is not None and now - cached[0] <= ttl_seconds:
                return limit_live_trades_response(cached[1], limit)
            captured_at = datetime.now(UTC)
            chain_body = self.chain_live_trades_body(query, captured_at)
            if chain_body is not None:
                self._live_trades_cache[cache_key] = (time.monotonic(), chain_body)
                return limit_live_trades_response(chain_body, limit)
            pages: list[str] = []
            items: list[dict[str, Any]] = []
            try:
                client = PolymarketClient(self.settings)
                for page_index in range(page_count):
                    page = client.data_trades(limit=page_size, offset=page_index * page_size)
                    pages.append(page.response.url)
                    items.extend(page.items)
                    if len(page.items) < page_size:
                        break
            except Exception as exc:
                fallback = self._live_trades_cache.get(cache_key)
                fallback_body = fallback[1] if fallback is not None else None
                if fallback is not None:
                    body = limit_live_trades_response(fallback_body, limit)
                    body["status"] = "stale"
                    body["error"] = str(exc)
                    return body
                return {
                    "source": "live",
                    "status": "error",
                    "captured_at": api_datetime(captured_at),
                    "trades": [],
                    "error": str(exc),
                }
            trades = filter_live_trade_rows(compact_live_trade_rows(items, captured_at), query)
            body = {
                "source": "live",
                "status": "ok",
                "captured_at": api_datetime(captured_at),
                "request_url": pages[0] if pages else "",
                "request_urls": pages,
                "candidate_count": len(items),
                "latency_seconds": live_trades_latency_seconds(trades, captured_at),
                "trades": trades,
            }
            self._live_trades_cache[cache_key] = (time.monotonic(), body)
            return limit_live_trades_response(body, limit)

    def chain_live_trades_body(
        self,
        query: dict[str, list[str]],
        captured_at: datetime,
    ) -> dict[str, Any] | None:
        lookback_minutes = bounded_int_param(
            query, "chain_lookback_minutes", 30, minimum=1, maximum=180
        )
        candidate_limit = min(int_param(query, "limit", 100, maximum=500) * 20, 5000)
        side = param(query, "side").upper()
        search = param(query, "q").strip()
        category = param(query, "category")
        min_notional = float_param(query, "min_notional", 0.0, minimum=0.0)
        max_notional = float_param(query, "max_notional", 0.0, minimum=0.0)
        where = [
            f"fills.ingested_at >= now64(3) - interval {lookback_minutes} minute",
            "fills.token_id != ''",
            "trader_address not in ("
            "'0xe111180000d2663c0091e4f400237545b87b996b',"
            "'0xe2222d279d744050d28e00520010520000310f59',"
            "'0x0000000000000000000000000000000000000000'"
            ")",
        ]
        if side in ("BUY", "SELL"):
            where.append("trader_side = " + ch_string(side))
        if min_notional > 0:
            where.append(f"fills.notional >= {min_notional}")
        if max_notional > 0:
            where.append(f"fills.notional <= {max_notional}")
        if search:
            quoted = ch_string(search)
            where.append(
                "("
                f"positionCaseInsensitive(trader_address, {quoted}) > 0 "
                f"or positionCaseInsensitive(markets.question, {quoted}) > 0 "
                f"or positionCaseInsensitive(events.title, {quoted}) > 0 "
                f"or positionCaseInsensitive(tokens.outcome, {quoted}) > 0"
                ")"
            )
        sql = f"""
            select
              concat(
                fills.transaction_hash, '-', toString(fills.log_index), '-',
                role, '-', fills.token_id
              ) as trade_id,
              fills.transaction_hash as transaction_hash,
              fills.log_index as log_index,
              fills.ingested_at as timestamp,
              ifNull(tokens.market_id, '') as market_id,
              if(tokens.condition_id != '', tokens.condition_id, ifNull(markets.condition_id, ''))
                as condition_id,
              fills.token_id as token_id,
              trader_address as user_address,
              trader_side as side,
              fills.price as price,
              fills.size as size,
              fills.notional as notional,
              'chain-live' as source,
              fills.ingested_at as ingested_at,
              if(markets.question != '', markets.question, concat('Token ', left(fills.token_id, 8)))
                as question,
              ifNull(markets.slug, '') as market_slug,
              ifNull(markets.event_id, '') as event_id,
              ifNull(events.title, '') as event_title,
              ifNull(events.slug, '') as event_slug,
              ifNull(events.category, '') as category,
              ifNull(tokens.outcome, '') as outcome,
              '' as trader_name,
              '' as trader_pseudonym,
              ifNull(screener.is_smart, false) as is_smart,
              ifNull(screener.is_whale, false) as is_whale,
              ifNull(screener.total_pnl, 0.0) as wallet_total_pnl,
              ifNull(screener.pnl_roi, 0.0) as wallet_pnl_roi,
              ifNull(screener.traded_notional, 0.0) as wallet_traded_notional
            from fact_exchange_fill as fills final
            array join
              ['maker', 'taker'] as role,
              [fills.maker, fills.taker] as trader_address,
              [fills.side, if(fills.side = 'BUY', 'SELL', 'BUY')] as trader_side
            left join dim_outcome_token as tokens final on fills.token_id = tokens.token_id
            left join dim_market as markets final on tokens.market_id = markets.market_id
            left join dim_event as events final on markets.event_id = events.event_id
            left join mart_wallet_screener as screener final on trader_address = screener.user_address
            where {" and ".join(where)}
            order by fills.ingested_at desc, fills.block_number desc, fills.transaction_hash desc, fills.log_index desc
            limit {candidate_limit}
            format JSONEachRow
        """
        try:
            rows = rows_json(self.clickhouse.query_text(sql))
        except Exception:
            return None
        if not rows:
            return None
        rows = filter_live_trade_rows([json_ready_row(row) for row in rows], query)
        if not rows:
            return None
        metadata_missing_count = sum(
            1 for row in rows if str(row.get("question") or "").startswith("Token ")
        )
        return {
            "source": "chain-live",
            "status": "ok",
            "captured_at": api_datetime(captured_at),
            "request_url": "clickhouse:fact_exchange_fill",
            "request_urls": ["clickhouse:fact_exchange_fill"],
            "candidate_count": len(rows),
            "metadata_missing_count": metadata_missing_count,
            "latency_seconds": live_trades_latency_seconds(rows, captured_at),
            "trades": rows,
        }

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

    def event_wallet_flow(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        event_id = param(query, "event_id")
        market_id = param(query, "market_id")
        condition_id = param(query, "condition_id")
        limit = int_param(query, "limit", 50, maximum=500)
        if event_id:
            trade_filter = (
                "market_id in "
                f"(select market_id from dim_market final where event_id = {ch_string(event_id)})"
            )
        elif market_id:
            trade_filter = f"market_id = {ch_string(market_id)}"
        elif condition_id:
            trade_filter = f"condition_id = {ch_string(condition_id)}"
        else:
            return []
        sql = f"""
            select
              user_address,
              count() as trade_count,
              countIf(side = 'BUY') as buy_count,
              countIf(side = 'SELL') as sell_count,
              sumIf(notional, side = 'BUY') as buy_notional,
              sumIf(notional, side = 'SELL') as sell_notional,
              sum(notional) as traded_notional,
              sum(if(side = 'BUY', size, -size)) as net_size,
              sum(if(side = 'BUY', notional, -notional)) as net_buy_notional,
              min(timestamp) as first_trade_at,
              max(timestamp) as last_trade_at
            from fact_trade
            where user_address != ''
              and {trade_filter}
            group by user_address
            order by traded_notional desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def event_pnl_leaderboard(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        event_id = param(query, "event_id")
        limit = int_param(query, "limit", 50, maximum=500)
        if not event_id:
            return []
        sql = f"""
            select
              event_id,
              user_address,
              event_title,
              category,
              trade_count,
              traded_notional,
              buy_notional,
              sell_notional,
              net_cashflow,
              final_position_value,
              realized_pnl,
              roi,
              settlement_status,
              data_quality,
              first_trade_at,
              last_trade_at
            from mart_event_wallet_pnl final
            where event_id = {ch_string(event_id)}
            order by realized_pnl desc, traded_notional desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def event_smart_wallets(self, query: dict[str, list[str]]) -> dict[str, Any]:
        event_ref = (
            param(query, "event")
            or param(query, "slug")
            or param(query, "event_slug")
            or param(query, "event_id")
            or param(query, "name")
            or param(query, "q")
        ).strip()
        if not event_ref:
            return empty_event_smart_wallet_options_response("", status="missing_event")
        limit = int_param(query, "limit", 100, maximum=500)
        event = self.event_lookup(event_ref)
        if event is None:
            return empty_event_smart_wallet_options_response(event_ref, status="event_not_found")
        event_id = str(event.get("event_id", ""))
        if not event_id:
            return event_smart_wallet_options_response_body(event, [])
        include_details = param(query, "details").lower() in ("1", "true", "yes")
        if not include_details:
            return event_smart_wallet_options_response_body(
                event,
                self.event_smart_wallet_options(event_id),
            )

        base_cte = f"""
            with
            selected_event as
            (
              select
                event_id,
                slug,
                title,
                start_time,
                end_time,
                active,
                closed,
                updated_at
              from dim_event final
              where event_id = {ch_string(event_id)}
            ),
            selected_markets as
            (
              select
                events.event_id as event_id,
                events.slug as event_slug,
                events.title as event_title,
                markets.market_id as market_id,
                markets.condition_id as condition_id,
                markets.question as market_question
              from dim_market as markets final
              inner join selected_event as events on markets.event_id = events.event_id
              where markets.condition_id != ''
            ),
            smart_wallets as
            (
              select
                user_address,
                trade_count as wallet_trade_count,
                traded_notional as wallet_traded_notional,
                total_pnl as wallet_total_pnl,
                pnl_roi as wallet_pnl_roi,
                portfolio_value as wallet_portfolio_value,
                is_whale as wallet_is_whale,
                pnl_captured_at as wallet_pnl_captured_at
              from mart_wallet_screener final
              where is_smart
            ),
            dedup_trades as
            (
              select
                raw_trade_key,
                argMax(raw_timestamp, raw_ingested_at) as timestamp,
                argMax(raw_condition_id, raw_ingested_at) as condition_id,
                argMax(raw_token_id, raw_ingested_at) as token_id,
                argMax(raw_user_address, raw_ingested_at) as user_address,
                argMax(raw_side, raw_ingested_at) as side,
                argMax(raw_price, raw_ingested_at) as price,
                argMax(raw_size, raw_ingested_at) as size,
                argMax(raw_notional, raw_ingested_at) as notional
              from
              (
                select
                  if(
                    transaction_hash != '',
                    concat(
                      transaction_hash, '|', token_id, '|', toString(timestamp), '|',
                      side, '|', toString(price), '|', toString(size)
                    ),
                    trade_id
                  ) as raw_trade_key,
                  timestamp as raw_timestamp,
                  condition_id as raw_condition_id,
                  token_id as raw_token_id,
                  lower(user_address) as raw_user_address,
                  side as raw_side,
                  price as raw_price,
                  size as raw_size,
                  notional as raw_notional,
                  ingested_at as raw_ingested_at
                from fact_trade_by_user
                where user_address in (select user_address from smart_wallets)
                  and condition_id in (select condition_id from selected_markets)
                  and user_address != ''
                  and condition_id != ''
                  and token_id != ''
                  and side in ('BUY', 'SELL')
                  and timestamp <= now64(3) + interval 10 minute
              )
              group by raw_trade_key
            )
        """
        summary = rows_json(
            self.clickhouse.query_text(
                base_cte
                + """
                    select
                      selected_markets.event_slug,
                      selected_markets.event_title,
                      count() as smart_trade_count,
                      uniqExact(dedup_trades.user_address) as smart_wallet_count,
                      sum(dedup_trades.notional) as smart_traded_notional,
                      sumIf(dedup_trades.notional, dedup_trades.side = 'BUY') as smart_buy_notional,
                      sumIf(dedup_trades.notional, dedup_trades.side = 'SELL') as smart_sell_notional,
                      sum(if(dedup_trades.side = 'BUY', dedup_trades.size, -dedup_trades.size))
                        as smart_net_shares,
                      max(dedup_trades.timestamp) as latest_smart_trade_at,
                      uniqExactIf(
                        dedup_trades.user_address,
                        dedup_trades.timestamp >= now64(3) - interval 24 hour
                      ) as smart_wallets_24h,
                      countIf(dedup_trades.timestamp >= now64(3) - interval 24 hour)
                        as smart_trade_count_24h,
                      sumIf(dedup_trades.notional, dedup_trades.timestamp >= now64(3) - interval 24 hour)
                        as smart_traded_notional_24h
                    from dedup_trades
                    inner join selected_markets on dedup_trades.condition_id = selected_markets.condition_id
                    group by selected_markets.event_slug, selected_markets.event_title
                    format JSONEachRow
                """
            )
        )
        outcomes = rows_json(
            self.clickhouse.query_text(
                base_cte
                + """
                    select
                      selected_markets.event_slug,
                      selected_markets.event_title,
                      selected_markets.market_question,
                      ifNull(tokens.outcome, '') as token_outcome,
                      upper(ifNull(tokens.outcome, '')) as outcome_side,
                      concat(
                        selected_markets.market_question,
                        ' / ',
                        upper(ifNull(tokens.outcome, ''))
                      ) as selection,
                      count() as smart_trade_count,
                      uniqExact(dedup_trades.user_address) as smart_wallet_count,
                      sum(dedup_trades.notional) as smart_traded_notional,
                      sumIf(dedup_trades.notional, dedup_trades.side = 'BUY') as smart_buy_notional,
                      sumIf(dedup_trades.notional, dedup_trades.side = 'SELL') as smart_sell_notional,
                      sum(if(dedup_trades.side = 'BUY', dedup_trades.size, -dedup_trades.size))
                        as smart_net_shares,
                      max(dedup_trades.timestamp) as latest_smart_trade_at,
                      uniqExactIf(
                        dedup_trades.user_address,
                        dedup_trades.timestamp >= now64(3) - interval 24 hour
                      ) as smart_wallets_24h,
                      countIf(dedup_trades.timestamp >= now64(3) - interval 24 hour)
                        as smart_trade_count_24h,
                      sumIf(dedup_trades.notional, dedup_trades.timestamp >= now64(3) - interval 24 hour)
                        as smart_traded_notional_24h
                    from dedup_trades
                    inner join selected_markets on dedup_trades.condition_id = selected_markets.condition_id
                    left join dim_outcome_token as tokens final
                      on selected_markets.market_id = tokens.market_id
                     and dedup_trades.token_id = tokens.token_id
                    group by
                      selected_markets.event_slug,
                      selected_markets.event_title,
                      selected_markets.market_question,
                      token_outcome,
                      outcome_side,
                      selection
                    order by smart_traded_notional desc
                    format JSONEachRow
                """
            )
        )
        wallets = rows_json(
            self.clickhouse.query_text(
                base_cte
                + f"""
                    select
                      selected_markets.event_slug,
                      selected_markets.event_title,
                      selected_markets.market_question,
                      ifNull(tokens.outcome, '') as token_outcome,
                      upper(ifNull(tokens.outcome, '')) as outcome_side,
                      concat(
                        selected_markets.market_question,
                        ' / ',
                        upper(ifNull(tokens.outcome, ''))
                      ) as selection,
                      dedup_trades.user_address as user_address,
                      count() as smart_trade_count,
                      sum(dedup_trades.notional) as event_traded_notional,
                      sumIf(dedup_trades.notional, dedup_trades.side = 'BUY') as event_buy_notional,
                      sumIf(dedup_trades.notional, dedup_trades.side = 'SELL') as event_sell_notional,
                      sum(if(dedup_trades.side = 'BUY', dedup_trades.size, -dedup_trades.size))
                        as event_net_shares,
                      argMax(dedup_trades.side, dedup_trades.timestamp) as latest_side,
                      max(dedup_trades.timestamp) as latest_trade_at,
                      any(smart_wallets.wallet_trade_count) as wallet_trade_count,
                      any(smart_wallets.wallet_traded_notional) as wallet_traded_notional,
                      any(smart_wallets.wallet_total_pnl) as wallet_total_pnl,
                      any(smart_wallets.wallet_pnl_roi) as wallet_pnl_roi,
                      any(smart_wallets.wallet_portfolio_value) as wallet_portfolio_value,
                      any(smart_wallets.wallet_is_whale) as wallet_is_whale,
                      any(smart_wallets.wallet_pnl_captured_at) as wallet_pnl_captured_at
                    from dedup_trades
                    inner join selected_markets on dedup_trades.condition_id = selected_markets.condition_id
                    inner join smart_wallets on dedup_trades.user_address = smart_wallets.user_address
                    left join dim_outcome_token as tokens final
                      on selected_markets.market_id = tokens.market_id
                     and dedup_trades.token_id = tokens.token_id
                    group by
                      selected_markets.event_slug,
                      selected_markets.event_title,
                      selected_markets.market_question,
                      token_outcome,
                      outcome_side,
                      selection,
                      dedup_trades.user_address
                    order by event_traded_notional desc
                    limit {limit}
                    format JSONEachRow
                """
            )
        )
        positions = rows_json(
            self.clickhouse.query_text(
                f"""
                    select
                      events.slug as event_slug,
                      events.title as event_title,
                      markets.question as market_question,
                      pos.outcome as token_outcome,
                      upper(pos.outcome) as outcome_side,
                      concat(markets.question, ' / ', upper(pos.outcome)) as selection,
                      pos.user_address,
                      pos.trade_count,
                      pos.traded_notional,
                      pos.position_size,
                      pos.current_value,
                      pos.unrealized_pnl_estimate,
                      pos.latest_action,
                      pos.last_trade_at,
                      screener.traded_notional as wallet_traded_notional,
                      screener.total_pnl as wallet_total_pnl,
                      screener.pnl_roi as wallet_pnl_roi
                    from mart_live_wallet_position as pos final
                    inner join mart_wallet_screener as screener final
                      on pos.user_address = screener.user_address
                    inner join dim_event as events final on pos.event_id = events.event_id
                    inner join dim_market as markets final on pos.market_id = markets.market_id
                    where events.event_id = {ch_string(event_id)}
                      and screener.is_smart
                      and abs(pos.position_size) > 0.000001
                    order by pos.traded_notional desc
                    limit {limit}
                    format JSONEachRow
                """
            )
        )
        return {
            "event": event,
            "summary": summary[0] if summary else empty_event_smart_wallet_summary(event),
            "outcomes": outcomes,
            "wallets": wallets,
            "positions": positions,
        }

    def event_smart_wallet_options_response(self, query: dict[str, list[str]]) -> dict[str, Any]:
        event_ref = (
            param(query, "event")
            or param(query, "slug")
            or param(query, "event_slug")
            or param(query, "event_id")
            or param(query, "name")
            or param(query, "q")
        ).strip()
        if not event_ref:
            return empty_event_smart_wallet_options_response("", status="missing_event")
        event = self.event_lookup(event_ref)
        if event is None:
            return empty_event_smart_wallet_options_response(event_ref, status="event_not_found")
        event_id = str(event.get("event_id", ""))
        if not event_id:
            return event_smart_wallet_options_response_body(event, [])
        return event_smart_wallet_options_response_body(
            event,
            self.event_smart_wallet_options(event_id),
        )

    def event_unusual_betting(self, query: dict[str, list[str]]) -> dict[str, Any]:
        event_ref = (
            param(query, "event")
            or param(query, "slug")
            or param(query, "event_slug")
            or param(query, "event_id")
            or param(query, "q")
        ).strip()
        if not event_ref:
            return {"status": "missing_event", "error": "missing_event"}
        event = self.event_lookup(event_ref)
        if event is None:
            return {"status": "event_not_found", "event_ref": event_ref}
        event_id = str(event.get("event_id") or "")
        if not event_id:
            return {"status": "event_not_found", "event_ref": event_ref}
        include_related_markets = bool_param(query, "include_related_markets", True)
        event_scope = self.event_unusual_betting_scope(event, include_related_markets)
        event_ids = [
            str(row.get("event_id") or "")
            for row in event_scope
            if str(row.get("event_id") or "")
        ]
        if not event_ids:
            event_ids = [event_id]

        cold_price_threshold = float_param(
            query,
            "cold_price_threshold",
            0.25,
            minimum=0.01,
            maximum=0.5,
        )
        large_threshold = float_param(query, "large_threshold", 500_000.0, minimum=0.0)
        very_large_threshold = float_param(query, "very_large_threshold", 1_000_000.0, minimum=0.0)
        extreme_threshold = float_param(query, "extreme_threshold", 5_000_000.0, minimum=0.0)
        wallet_limit = int_param(query, "wallet_limit", 30, maximum=100)
        trade_limit = int_param(query, "trade_limit", 30, maximum=100)
        cache_ttl_seconds = bounded_int_param(
            query,
            "cache_ttl_seconds",
            60,
            minimum=0,
            maximum=900,
        )
        persisted_cache_ttl_seconds = bounded_int_param(
            query,
            "persisted_cache_ttl_seconds",
            3600,
            minimum=0,
            maximum=86_400,
        )
        use_persisted_cache = bool_param(query, "use_persisted_cache", True)
        refresh = bool_param(query, "refresh", False)
        trigger_reason = param(query, "trigger_reason", "api")
        cache_key = (
            tuple(event_ids),
            cold_price_threshold,
            large_threshold,
            very_large_threshold,
            extreme_threshold,
            wallet_limit,
            trade_limit,
            include_related_markets,
        )
        persisted_cache_key = unusual_betting_cache_key(
            event_ids=event_ids,
            cold_price_threshold=cold_price_threshold,
            large_threshold=large_threshold,
            very_large_threshold=very_large_threshold,
            extreme_threshold=extreme_threshold,
            include_related_markets=include_related_markets,
        )
        cached_row = None
        if (
            use_persisted_cache
            and not refresh
            and self._unusual_betting_cache_store is not None
        ):
            try:
                cached_row = self._unusual_betting_cache_store.get(
                    persisted_cache_key,
                    max_age_seconds=persisted_cache_ttl_seconds
                    if persisted_cache_ttl_seconds > 0
                    else None,
                )
            except Exception:
                cached_row = None
            if cached_row is not None:
                detail = dict(cached_row.get("detail") or {})
                if detail and unusual_betting_cached_detail_satisfies(
                    detail,
                    wallet_limit=wallet_limit,
                    trade_limit=trade_limit,
                ):
                    detail = unusual_betting_trim_cached_detail(
                        detail,
                        wallet_limit=wallet_limit,
                        trade_limit=trade_limit,
                    )
                    detail["cache"] = unusual_betting_cache_metadata(
                        cached_row,
                        source="postgres_cache",
                    )
                    return detail
            if cache_ttl_seconds > 0 and persisted_cache_ttl_seconds > 0:
                try:
                    cached_row = self._unusual_betting_cache_store.get(
                        persisted_cache_key,
                        max_age_seconds=None,
                    )
                except Exception:
                    cached_row = None
                if cached_row is not None:
                    detail = dict(cached_row.get("detail") or {})
                    if detail and unusual_betting_cached_detail_satisfies(
                        detail,
                        wallet_limit=wallet_limit,
                        trade_limit=trade_limit,
                    ):
                        detail = unusual_betting_trim_cached_detail(
                            detail,
                            wallet_limit=wallet_limit,
                            trade_limit=trade_limit,
                        )
                        detail["cache"] = unusual_betting_cache_metadata(
                            cached_row,
                            source="stale_postgres_cache",
                        )
                        return detail
        stale_fallback_row = None
        if (
            use_persisted_cache
            and self._unusual_betting_cache_store is not None
            and cached_row is not None
        ):
            stale_fallback_row = cached_row
        cached = self._unusual_betting_cache.get(cache_key)
        if (
            not refresh
            and
            cache_ttl_seconds > 0
            and cached is not None
            and time.time() - cached[0] <= cache_ttl_seconds
        ):
            return cached[1]
        excluded_addresses = unusual_betting_excluded_addresses()
        excluded_sql = "(" + ",".join(ch_string(address) for address in excluded_addresses) + ")"
        event_ids_sql = "(" + ",".join(ch_string(scope_event_id) for scope_event_id in event_ids) + ")"
        scope_event_slugs = [
            str(row.get("slug") or "")
            for row in event_scope
            if str(row.get("slug") or "")
        ]

        try:
            markets = rows_json(
                self.clickhouse.query_text(
                    f"""
                    select
                      market_id,
                      condition_id,
                      question,
                      slug,
                      active,
                      closed,
                      accepting_orders,
                      volume,
                      liquidity,
                      start_time,
                      end_time
                    from dim_market final
                    where event_id in {event_ids_sql}
                    order by volume desc
                    format JSONEachRow
                """
                )
            )
            tokens = rows_json(
                self.clickhouse.query_text(
                    f"""
                    select
                      tokens.market_id,
                      markets.question,
                      markets.slug as market_slug,
                      tokens.token_id,
                      tokens.outcome,
                      tokens.outcome_index
                    from dim_outcome_token as tokens final
                    inner join dim_market as markets final on tokens.market_id = markets.market_id
                    where markets.event_id in {event_ids_sql}
                    order by markets.volume desc, tokens.outcome_index
                    format JSONEachRow
                """
                )
            )
            token_ids = [
                str(row.get("token_id") or "")
                for row in tokens
                if str(row.get("token_id") or "")
            ]
            token_ids_sql = "(" + ",".join(ch_string(token_id) for token_id in token_ids) + ")"
            if not token_ids:
                token_ids_sql = "('')"
            fill_summary = rows_json(
                self.clickhouse.query_text(
                    f"""
                    select
                      count() as fill_rows,
                      round(sum(toFloat64(notional)), 2) as total_fill_notional,
                      round(max(toFloat64(notional)), 2) as max_fill_notional,
                      min(ingested_at) as first_ts,
                      max(ingested_at) as last_ts
                    from fact_exchange_fill final
                    where token_id in {token_ids_sql}
                    format JSONEachRow
                """
                )
            )
            outcome_summary = rows_json(
                self.clickhouse.query_text(
                    f"""
                    select
                      markets.slug as market_slug,
                      markets.question as question,
                      tokens.outcome as outcome,
                      user_side,
                      count() as user_fill_rows,
                      uniqExact(user_address) as wallet_count,
                      round(sum(toFloat64(fills.notional)), 2) as total_notional,
                      round(max(toFloat64(fills.notional)), 2) as max_notional,
                      round(avg(toFloat64(fills.price)), 4) as avg_price,
                      round(min(toFloat64(fills.price)), 4) as min_price,
                      round(max(toFloat64(fills.price)), 4) as max_price,
                      countIf(toFloat64(fills.notional) >= {large_threshold} and user_address not in {excluded_sql}) as large_trade_count,
                      countIf(toFloat64(fills.notional) >= {very_large_threshold} and user_address not in {excluded_sql}) as very_large_trade_count,
                      countIf(toFloat64(fills.notional) >= {extreme_threshold} and user_address not in {excluded_sql}) as extreme_trade_count,
                      round(maxIf(toFloat64(fills.notional), user_address not in {excluded_sql}), 2) as max_user_notional
                    from fact_exchange_fill as fills final
                    array join
                      ['maker', 'taker'] as role,
                      [fills.maker, fills.taker] as user_address,
                      [fills.side, if(fills.side = 'BUY', 'SELL', 'BUY')] as user_side
                    inner join dim_outcome_token as tokens final on fills.token_id = tokens.token_id
                    inner join dim_market as markets final on tokens.market_id = markets.market_id
                    where fills.token_id in {token_ids_sql}
                      and markets.event_id in {event_ids_sql}
                    group by market_slug, question, outcome, user_side
                    order by total_notional desc
                    format JSONEachRow
                """
                )
            )
        except Exception as exc:
            if stale_fallback_row is not None:
                detail = dict(stale_fallback_row.get("detail") or {})
                if detail and unusual_betting_cached_detail_satisfies(
                    detail,
                    wallet_limit=wallet_limit,
                    trade_limit=trade_limit,
                ):
                    detail = unusual_betting_trim_cached_detail(
                        detail,
                        wallet_limit=wallet_limit,
                        trade_limit=trade_limit,
                    )
                    detail["cache"] = unusual_betting_cache_metadata(
                        stale_fallback_row,
                        source="stale_postgres_cache_after_error",
                    )
                    detail["cache"]["refresh_error"] = str(exc)
                    return detail
            raise
        signal_filters = unusual_betting_signal_filters(
            outcome_summary,
            cold_price_threshold,
            large_threshold,
        )
        signal_condition_sql = unusual_betting_signal_condition_sql(signal_filters)
        if signal_condition_sql:
            signal_wallet_summary = rows_json(
                self.clickhouse.query_text(
                    f"""
                        select
                          count() as signal_wallet_count,
                          countIf(total_notional >= {large_threshold}) as abnormal_wallet_count,
                          countIf(total_notional >= {very_large_threshold}) as very_large_wallet_count,
                          countIf(total_notional >= {extreme_threshold}) as extreme_wallet_count,
                          round(maxIf(total_notional, total_notional >= {large_threshold}), 2) as max_abnormal_wallet_notional,
                          round(max(total_notional), 2) as max_watch_wallet_notional,
                          round(max(max_notional), 2) as max_watch_trade_notional
                        from
                        (
                          select
                            user_address,
                            sum(toFloat64(fills.notional)) as total_notional,
                            max(toFloat64(fills.notional)) as max_notional
                          from fact_exchange_fill as fills final
                          array join
                            ['maker', 'taker'] as role,
                            [fills.maker, fills.taker] as user_address,
                            [fills.side, if(fills.side = 'BUY', 'SELL', 'BUY')] as user_side
                          inner join dim_outcome_token as tokens final on fills.token_id = tokens.token_id
                          inner join dim_market as markets final on tokens.market_id = markets.market_id
                          where fills.token_id in {token_ids_sql}
                            and markets.event_id in {event_ids_sql}
                            and user_address not in {excluded_sql}
                            and ({signal_condition_sql})
                          group by user_address
                        )
                        format JSONEachRow
                    """
                )
            )
            signal_wallets = rows_json(
                self.clickhouse.query_text(
                    f"""
                        select
                          markets.slug as market_slug,
                          markets.question as question,
                          tokens.outcome as outcome,
                          user_side,
                          user_address,
                          count() as fills,
                          round(sum(toFloat64(fills.notional)), 2) as total_notional,
                          round(max(toFloat64(fills.notional)), 2) as max_notional,
                          round(avg(toFloat64(fills.price)), 4) as avg_price,
                          min(fills.ingested_at) as first_ts,
                          max(fills.ingested_at) as last_ts
                        from fact_exchange_fill as fills final
                        array join
                          ['maker', 'taker'] as role,
                          [fills.maker, fills.taker] as user_address,
                          [fills.side, if(fills.side = 'BUY', 'SELL', 'BUY')] as user_side
                        inner join dim_outcome_token as tokens final on fills.token_id = tokens.token_id
                        inner join dim_market as markets final on tokens.market_id = markets.market_id
                        where fills.token_id in {token_ids_sql}
                          and markets.event_id in {event_ids_sql}
                          and user_address not in {excluded_sql}
                          and ({signal_condition_sql})
                        group by market_slug, question, outcome, user_side, user_address
                        order by total_notional desc
                        limit {wallet_limit}
                        format JSONEachRow
                    """
                )
            )
            signal_trades = rows_json(
                self.clickhouse.query_text(
                    f"""
                        select
                          fills.ingested_at as timestamp,
                          markets.slug as market_slug,
                          markets.question as question,
                          tokens.outcome as outcome,
                          user_side,
                          user_address,
                          round(toFloat64(fills.notional), 2) as notional,
                          round(toFloat64(fills.price), 4) as price,
                          round(toFloat64(fills.size), 2) as size,
                          fills.transaction_hash as transaction_hash
                        from fact_exchange_fill as fills final
                        array join
                          ['maker', 'taker'] as role,
                          [fills.maker, fills.taker] as user_address,
                          [fills.side, if(fills.side = 'BUY', 'SELL', 'BUY')] as user_side
                        inner join dim_outcome_token as tokens final on fills.token_id = tokens.token_id
                        inner join dim_market as markets final on tokens.market_id = markets.market_id
                        where fills.token_id in {token_ids_sql}
                          and markets.event_id in {event_ids_sql}
                          and user_address not in {excluded_sql}
                          and ({signal_condition_sql})
                        order by toFloat64(fills.notional) desc
                        limit {trade_limit}
                        format JSONEachRow
                    """
                )
            )
        else:
            signal_wallet_summary = []
            signal_wallets = []
            signal_trades = []

        analysis = summarize_unusual_betting(
            event,
            outcome_summary,
            signal_wallets,
            signal_trades,
            signal_filters=signal_filters,
            cold_price_threshold=cold_price_threshold,
            large_threshold=large_threshold,
            very_large_threshold=very_large_threshold,
            extreme_threshold=extreme_threshold,
        )
        output = {
            "status": "ok",
            "event": event,
            "parameters": {
                "cold_price_threshold": cold_price_threshold,
                "large_threshold": large_threshold,
                "very_large_threshold": very_large_threshold,
                "extreme_threshold": extreme_threshold,
                "excluded_addresses": excluded_addresses,
                "wallet_limit": wallet_limit,
                "trade_limit": trade_limit,
                "cache_ttl_seconds": cache_ttl_seconds,
                "persisted_cache_ttl_seconds": persisted_cache_ttl_seconds,
                "use_persisted_cache": use_persisted_cache,
                "refresh": refresh,
                "include_related_markets": include_related_markets,
                "event_ids": event_ids,
                "event_slugs": scope_event_slugs,
            },
            "event_scope": event_scope,
            "markets": markets,
            "tokens": tokens,
            "fill_summary": fill_summary[0] if fill_summary else {},
            "outcome_summary": outcome_summary,
            "signal_outcomes": signal_filters,
            "signal_wallet_summary": signal_wallet_summary[0] if signal_wallet_summary else {},
            "signal_wallets": signal_wallets,
            "signal_trades": signal_trades,
            "cold_buy_outcomes": signal_filters,
            "cold_wallets": signal_wallets,
            "cold_trades": signal_trades,
            "analysis": analysis,
            "generated_at": api_datetime(datetime.now(UTC)),
            "cache": {
                "source": "computed",
                "cache_key": persisted_cache_key,
                "refreshed_at": None,
                "age_seconds": None,
                "trigger_reason": trigger_reason,
            },
        }
        if cache_ttl_seconds > 0:
            self._unusual_betting_cache[cache_key] = (time.time(), output)
        if self._unusual_betting_cache_store is not None:
            summary = unusual_betting_summary_response(output)
            try:
                cached_row = self.write_unusual_betting_cache(
                    cache_key=persisted_cache_key,
                    detail=output,
                    summary=summary,
                    trigger_reason=trigger_reason,
                )
                output["cache"] = unusual_betting_cache_metadata(
                    cached_row,
                    source="computed_and_stored",
                )
            except Exception as exc:
                output["cache"]["store_error"] = str(exc)
        return output

    def event_unusual_betting_scope(
        self,
        event: dict[str, Any],
        include_related_markets: bool,
    ) -> list[dict[str, Any]]:
        event_id = str(event.get("event_id") or "")
        slug = str(event.get("slug") or "")
        if not include_related_markets or not slug.startswith("fifwc-"):
            return [event] if event_id else []
        base_slug = base_match_slug(slug)
        slugs = worldcup_event_slugs_for_scope([base_slug], expand_variants=True)
        slugs_sql = "(" + ",".join(ch_string(item) for item in slugs) + ")"
        rows = rows_json(
            self.clickhouse.query_text(
                f"""
                    select
                      event_id,
                      slug,
                      title,
                      category,
                      active,
                      closed,
                      start_time,
                      end_time,
                      updated_at
                    from dim_event final
                    where slug in {slugs_sql}
                    order by slug
                    format JSONEachRow
                """
            )
        )
        return rows or ([event] if event_id else [])

    def event_unusual_betting_summary(self, query: dict[str, list[str]]) -> dict[str, Any]:
        summary_query = {key: list(values) for key, values in query.items()}
        summary_query.setdefault("wallet_limit", ["100"])
        summary_query.setdefault("trade_limit", ["50"])
        detail = self.event_unusual_betting(summary_query)
        if detail.get("status") != "ok":
            return detail
        summary = unusual_betting_summary_response(detail)
        cache = detail.get("cache")
        if isinstance(cache, dict):
            summary["cache"] = cache
        return summary

    def write_unusual_betting_cache(
        self,
        *,
        cache_key: str,
        detail: dict[str, Any],
        summary: dict[str, Any],
        trigger_reason: str,
    ) -> dict[str, Any] | None:
        if self._unusual_betting_cache_store is None:
            return None
        event = detail.get("event") if isinstance(detail.get("event"), dict) else {}
        return self._unusual_betting_cache_store.upsert(
            cache_key=cache_key,
            event_id=str(event.get("event_id") or ""),
            event_slug=str(event.get("slug") or ""),
            event_title=str(event.get("title") or ""),
            status=str(detail.get("status") or summary.get("status") or ""),
            severity=str(summary.get("severity") or "none"),
            abnormal_wallet_count=int_value(summary.get("abnormal_wallet_count")),
            max_abnormal_wallet_notional=float_value(summary.get("max_abnormal_wallet_notional")),
            signal_total_notional=float_value(summary.get("signal_total_notional")),
            parameters=detail.get("parameters") if isinstance(detail.get("parameters"), dict) else {},
            summary=summary,
            detail=detail,
            trigger_reason=trigger_reason,
            generated_at=detail.get("generated_at"),
        )

    def event_lookup(self, event_ref: str) -> dict[str, Any] | None:
        value = event_ref.strip()
        if not value:
            return None
        if value.isdigit():
            where = f"event_id = {ch_string(value)}"
        elif value.startswith("fifwc-") or "-" in value:
            where = f"slug = {ch_string(value)}"
        else:
            escaped = ch_string(value)
            where = f"positionCaseInsensitive(title, {escaped}) > 0"
        sql = f"""
            select
              event_id,
              slug,
              title,
              category,
              active,
              closed,
              start_time,
              end_time,
              updated_at
            from dim_event final
            where {where}
            order by
              lower(title) = lower({ch_string(value)}) desc,
              positionCaseInsensitive(title, 'Exact Score') = 0 desc,
              length(title) asc,
              updated_at desc
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def event_smart_wallet_options(self, event_id: str) -> list[dict[str, Any]]:
        sql = f"""
            with
            selected_event as
            (
              select event_id, slug, title
              from dim_event final
              where event_id = {ch_string(event_id)}
            ),
            selected_markets as
            (
              select
                events.slug as event_slug,
                events.title as event_title,
                markets.market_id as market_id,
                markets.condition_id as condition_id,
                markets.question as market_question
              from dim_market as markets final
              inner join selected_event as events on markets.event_id = events.event_id
              where markets.condition_id != ''
            ),
            wallet_segments as
            (
              select
                lower(user_address) as user_address,
                is_smart,
                is_whale
              from mart_wallet_screener final
              where is_smart or is_whale
            ),
            dedup_trades as
            (
              select
                raw_trade_key,
                argMax(raw_timestamp, raw_ingested_at) as timestamp,
                argMax(raw_condition_id, raw_ingested_at) as condition_id,
                argMax(raw_token_id, raw_ingested_at) as token_id,
                argMax(raw_user_address, raw_ingested_at) as user_address,
                argMax(raw_notional, raw_ingested_at) as notional
              from
              (
                select
                  if(
                    transaction_hash != '',
                    concat(
                      transaction_hash, '|', token_id, '|', toString(timestamp), '|',
                      side, '|', toString(price), '|', toString(size)
                    ),
                    trade_id
                  ) as raw_trade_key,
                  timestamp as raw_timestamp,
                  condition_id as raw_condition_id,
                  token_id as raw_token_id,
                  lower(user_address) as raw_user_address,
                  notional as raw_notional,
                  ingested_at as raw_ingested_at
                from fact_trade_by_user
                where user_address in (select user_address from wallet_segments)
                  and condition_id in (select condition_id from selected_markets)
                  and user_address != ''
                  and condition_id != ''
                  and token_id != ''
                  and side in ('BUY', 'SELL')
                  and timestamp <= now64(3) + interval 10 minute
              )
              group by raw_trade_key
            ),
            option_stats as
            (
              select
                dedup_trades.condition_id as condition_id,
                dedup_trades.token_id as token_id,
                uniqExactIf(dedup_trades.user_address, wallet_segments.is_smart) as smart_wallet_count,
                sumIf(dedup_trades.notional, wallet_segments.is_smart) as smart_amount,
                countIf(wallet_segments.is_smart) as smart_trade_count,
                uniqExactIf(dedup_trades.user_address, wallet_segments.is_whale) as whale_wallet_count,
                sumIf(dedup_trades.notional, wallet_segments.is_whale) as whale_amount
              from dedup_trades
              inner join wallet_segments on dedup_trades.user_address = wallet_segments.user_address
              group by dedup_trades.condition_id, dedup_trades.token_id
            )
            select
              selected_markets.event_slug as event_slug,
              selected_markets.event_title as event_title,
              selected_markets.market_question as market_question,
              ifNull(tokens.outcome, '') as token_outcome,
              upper(ifNull(tokens.outcome, '')) as outcome_side,
              concat(
                selected_markets.market_question,
                ' / ',
                upper(ifNull(tokens.outcome, ''))
              ) as selection,
              ifNull(option_stats.smart_wallet_count, 0) as smart_wallet_count,
              ifNull(option_stats.smart_amount, 0.0) as smart_amount,
              ifNull(option_stats.smart_trade_count, 0) as smart_trade_count,
              ifNull(option_stats.whale_wallet_count, 0) as whale_wallet_count,
              ifNull(option_stats.whale_amount, 0.0) as whale_amount
            from selected_markets
            left join dim_outcome_token as tokens final
              on selected_markets.market_id = tokens.market_id
            left join option_stats
              on selected_markets.condition_id = option_stats.condition_id
             and tokens.token_id = option_stats.token_id
            order by
              selected_markets.market_question asc,
              outcome_side desc
            format JSONEachRow
        """
        return compact_event_smart_wallet_options(rows_json(self.clickhouse.query_text(sql)))

    def trader_profile(self, query: dict[str, list[str]]) -> dict[str, Any] | None:
        user = param(query, "user").lower()
        if not user:
            return None
        sql = f"""
            select
              coalesce(profile.user_address, rollup.user_address) as user_address,
              if(
                isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                ifNull(profile.trade_count, 0) + ifNull(rollup.trade_count, 0),
                greatest(ifNull(profile.trade_count, 0), ifNull(rollup.trade_count, 0))
              ) as trade_count,
              if(
                isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                ifNull(profile.buy_count, 0) + ifNull(rollup.buy_count, 0),
                greatest(ifNull(profile.buy_count, 0), ifNull(rollup.buy_count, 0))
              ) as buy_count,
              if(
                isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                ifNull(profile.sell_count, 0) + ifNull(rollup.sell_count, 0),
                greatest(ifNull(profile.sell_count, 0), ifNull(rollup.sell_count, 0))
              ) as sell_count,
              if(
                isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                ifNull(profile.traded_size, 0.0) + ifNull(rollup.traded_size, 0.0),
                greatest(ifNull(profile.traded_size, 0.0), ifNull(rollup.traded_size, 0.0))
              ) as traded_size,
              if(
                isNull(profile.last_trade_at) or profile.last_trade_at < rollup.first_trade_at,
                ifNull(profile.traded_notional, 0.0) + ifNull(rollup.traded_notional, 0.0),
                greatest(ifNull(profile.traded_notional, 0.0), ifNull(rollup.traded_notional, 0.0))
              ) as traded_notional,
              ifNull(profile.position_count, 0) as position_count,
              ifNull(profile.current_value, 0.0) as current_value,
              ifNull(profile.cash_pnl, 0.0) as cash_pnl,
              ifNull(profile.realized_pnl, 0.0) as realized_pnl,
              ifNull(profile.total_pnl, 0.0) as total_pnl,
              ifNull(profile.chain_fill_count, 0) as chain_fill_count,
              ifNull(profile.chain_traded_size, 0.0) as chain_traded_size,
              ifNull(profile.chain_traded_notional, 0.0) as chain_traded_notional,
              ifNull(profile.chain_position_size, 0.0) as chain_position_size,
              ifNull(profile.chain_current_value, 0.0) as chain_current_value,
              ifNull(profile.chain_net_cashflow, 0.0) as chain_net_cashflow,
              ifNull(profile.chain_mark_to_market_pnl, 0.0) as chain_mark_to_market_pnl,
              multiIf(
                isNull(profile.first_trade_at), rollup.first_trade_at,
                isNull(rollup.first_trade_at), profile.first_trade_at,
                least(profile.first_trade_at, rollup.first_trade_at)
              ) as first_trade_at,
              multiIf(
                isNull(profile.last_trade_at), rollup.last_trade_at,
                isNull(rollup.last_trade_at), profile.last_trade_at,
                greatest(profile.last_trade_at, rollup.last_trade_at)
              ) as last_trade_at,
              profile.last_position_at as last_position_at,
              ifNull(profile.last_chain_fill_block, 0) as last_chain_fill_block,
              ifNull(rollup.trade_count_24h, 0) as trade_count_24h,
              ifNull(rollup.traded_notional_24h, 0.0) as traded_notional_24h,
              ifNull(rollup.buy_notional_24h, 0.0) as buy_notional_24h,
              ifNull(rollup.sell_notional_24h, 0.0) as sell_notional_24h,
              ifNull(rollup.latest_action, '') as latest_action,
              ifNull(rollup.data_lag_seconds, 0) as data_lag_seconds
            from
            (
              select *
              from mart_trader_profile final
              where user_address = {ch_string(user)}
            ) as profile
            full outer join
            (
              select *
              from mart_wallet_trade_rollup final
              where user_address = {ch_string(user)}
            ) as rollup
              on profile.user_address = rollup.user_address
            limit 1
            settings join_use_nulls = 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        profile = rows[0] if rows else None
        activity_profile = self.trader_trade_by_user_profile(user)
        if activity_profile is None:
            activity_profile = self.trader_activity_profile(user)
        chain_profile = self.trader_chain_profile(user)
        portfolio_profile = self.trader_portfolio_snapshot_profile(user)
        merged = merge_trader_profile(profile, activity_profile, chain_profile)
        return merge_portfolio_profile(merged, portfolio_profile)

    def trader_portfolio_snapshot_profile(self, user: str) -> dict[str, Any] | None:
        sql = f"""
            select
              user_address,
              argMax(position_count, captured_at) as position_count,
              argMax(positions_value, captured_at) as positions_value,
              argMax(portfolio_value, captured_at) as portfolio_value,
              argMax(available_balance, captured_at) as available_balance,
              argMax(total_pnl, captured_at) as total_pnl,
              max(captured_at) as last_position_at
            from fact_wallet_portfolio_snapshot
            where user_address = {ch_string(user)}
            group by user_address
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def trader_activity_profile(self, user: str) -> dict[str, Any] | None:
        sql = f"""
            select
              activity.user_address,
              activity.trade_count,
              activity.buy_count,
              activity.sell_count,
              activity.traded_size,
              activity.traded_notional,
              ifNull(positions.position_count, 0) as position_count,
              ifNull(positions.current_value, 0.0) as current_value,
              0.0 as cash_pnl,
              0.0 as realized_pnl,
              ifNull(positions.total_pnl, 0.0) as total_pnl,
              0 as chain_fill_count,
              0.0 as chain_traded_size,
              0.0 as chain_traded_notional,
              0.0 as chain_position_size,
              0.0 as chain_current_value,
              0.0 as chain_net_cashflow,
              0.0 as chain_mark_to_market_pnl,
              activity.first_trade_at,
              activity.last_trade_at,
              positions.last_position_at as last_position_at,
              0 as last_chain_fill_block,
              activity.trade_count_24h,
              activity.traded_notional_24h,
              activity.buy_notional_24h,
              activity.sell_notional_24h,
              activity.latest_action,
              greatest(0, toUInt32(dateDiff('second', activity.last_trade_at, now64(3))))
                as data_lag_seconds
            from
            (
              select
                user_address,
                count() as trade_count,
                countIf(side = 'BUY') as buy_count,
                countIf(side = 'SELL') as sell_count,
                sum(size) as traded_size,
                sum(notional) as traded_notional,
                min(timestamp) as first_trade_at,
                max(timestamp) as last_trade_at,
                countIf(timestamp >= now64(3) - interval 24 hour) as trade_count_24h,
                sumIf(notional, timestamp >= now64(3) - interval 24 hour)
                  as traded_notional_24h,
                sumIf(notional, side = 'BUY' and timestamp >= now64(3) - interval 24 hour)
                  as buy_notional_24h,
                sumIf(notional, side = 'SELL' and timestamp >= now64(3) - interval 24 hour)
                  as sell_notional_24h,
                argMax(side, timestamp) as latest_action
              from
              (
                select
                  raw_user_address as user_address,
                  activity_id,
                  anyLast(raw_timestamp) as timestamp,
                  anyLast(raw_side) as side,
                  anyLast(raw_size) as size,
                  anyLast(raw_notional) as notional
                from
                (
                  select
                    lower(user_address) as raw_user_address,
                    activity_id,
                    timestamp as raw_timestamp,
                    side as raw_side,
                    size as raw_size,
                    notional as raw_notional
                  from fact_user_activity
                  where user_address = {ch_string(user)}
                    and activity_type = 'TRADE'
                    and condition_id != ''
                    and token_id != ''
                    and side in ('BUY', 'SELL')
                    and timestamp <= now64(3) + interval 10 minute
                )
                group by raw_user_address, activity_id
              )
              group by user_address
            ) as activity
            left join
            (
              select
                positions.user_address,
                count() as position_count,
                sum(positions.position_size * ifNull(marks.mark_price, positions.last_price))
                  as current_value,
                sum(
                  positions.net_cashflow
                  + positions.position_size * ifNull(marks.mark_price, positions.last_price)
                ) as total_pnl,
                max(positions.last_trade_at) as last_position_at
              from
              (
                select
                  user_address,
                  token_id,
                  sum(if(side = 'BUY', size, -size)) as position_size,
                  sum(if(side = 'SELL', notional, -notional)) as net_cashflow,
                  argMax(price, timestamp) as last_price,
                  max(timestamp) as last_trade_at
                from
                (
                  select
                    raw_user_address as user_address,
                    activity_id,
                    anyLast(raw_timestamp) as timestamp,
                    anyLast(raw_token_id) as token_id,
                    anyLast(raw_side) as side,
                    anyLast(raw_price) as price,
                    anyLast(raw_size) as size,
                    anyLast(raw_notional) as notional
                  from
                  (
                    select
                      lower(user_address) as raw_user_address,
                      activity_id,
                      timestamp as raw_timestamp,
                      token_id as raw_token_id,
                      side as raw_side,
                      price as raw_price,
                      size as raw_size,
                      notional as raw_notional
                    from fact_user_activity
                    where user_address = {ch_string(user)}
                      and activity_type = 'TRADE'
                      and condition_id != ''
                      and token_id != ''
                      and side in ('BUY', 'SELL')
                      and timestamp <= now64(3) + interval 10 minute
                  )
                  group by raw_user_address, activity_id
                )
                group by user_address, token_id
                having abs(position_size) > 0.000001
              ) as positions
              left join
              (
                select
                  wallet_tokens.token_id as token_id,
                  multiIf(
                    latest_book.best_bid is not null and latest_book.best_ask is not null,
                      cast((latest_book.best_bid + latest_book.best_ask) / 2, 'Nullable(Float64)'),
                    latest_price.mark_at > toDateTime64(0, 3, 'UTC'),
                      cast(latest_price.price, 'Nullable(Float64)'),
                    cast(null, 'Nullable(Float64)')
                  ) as mark_price
                from
                (
                  select distinct token_id
                  from fact_user_activity
                  where user_address = {ch_string(user)}
                    and activity_type = 'TRADE'
                    and token_id != ''
                ) as wallet_tokens
                left join
                (
                  select
                    token_id,
                    argMax(price, timestamp) as price,
                    max(timestamp) as mark_at
                  from fact_price_history
                  where token_id in
                  (
                    select distinct token_id
                    from fact_user_activity
                    where user_address = {ch_string(user)}
                      and activity_type = 'TRADE'
                      and token_id != ''
                  )
                  group by token_id
                ) as latest_price on wallet_tokens.token_id = latest_price.token_id
                left join
                (
                  select
                    token_id,
                    argMax(best_bid, captured_at) as best_bid,
                    argMax(best_ask, captured_at) as best_ask
                  from fact_orderbook_snapshot
                  where token_id in
                  (
                    select distinct token_id
                    from fact_user_activity
                    where user_address = {ch_string(user)}
                      and activity_type = 'TRADE'
                      and token_id != ''
                  )
                  group by token_id
                ) as latest_book on wallet_tokens.token_id = latest_book.token_id
              ) as marks on positions.token_id = marks.token_id
              group by positions.user_address
            ) as positions on activity.user_address = positions.user_address
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def trader_trade_by_user_profile(self, user: str) -> dict[str, Any] | None:
        source_sql = f"""
                  select
                    lower(user_address) as raw_user_address,
                    if(
                      transaction_hash != '',
                      concat(
                        transaction_hash, '|', token_id, '|', toString(timestamp), '|',
                        side, '|', toString(price), '|', toString(size)
                      ),
                      activity_id
                    ) as raw_trade_key,
                    timestamp as raw_timestamp,
                    token_id as raw_token_id,
                    side as raw_side,
                    price as raw_price,
                    size as raw_size,
                    notional as raw_notional,
                    ingested_at as raw_ingested_at,
                    2 as raw_source_priority
                  from fact_user_activity
                  where user_address = {ch_string(user)}
                    and activity_type = 'TRADE'
                    and condition_id != ''
                    and token_id != ''
                    and side in ('BUY', 'SELL')
                    and timestamp <= now64(3) + interval 10 minute
                  union all
                  select
                    lower(user_address) as raw_user_address,
                    if(
                      transaction_hash != '',
                      concat(
                        transaction_hash, '|', token_id, '|', toString(timestamp), '|',
                        side, '|', toString(price), '|', toString(size)
                      ),
                      trade_id
                    ) as raw_trade_key,
                    timestamp as raw_timestamp,
                    token_id as raw_token_id,
                    side as raw_side,
                    price as raw_price,
                    size as raw_size,
                    notional as raw_notional,
                    ingested_at as raw_ingested_at,
                    1 as raw_source_priority
                  from fact_trade_by_user
                  where user_address = {ch_string(user)}
                    and condition_id != ''
                    and token_id != ''
                    and side in ('BUY', 'SELL')
                    and timestamp <= now64(3) + interval 10 minute
        """
        sql = f"""
            select
              activity.user_address,
              activity.trade_count,
              activity.buy_count,
              activity.sell_count,
              activity.traded_size,
              activity.traded_notional,
              ifNull(positions.position_count, 0) as position_count,
              ifNull(positions.current_value, 0.0) as current_value,
              0.0 as cash_pnl,
              0.0 as realized_pnl,
              ifNull(positions.total_pnl, 0.0) as total_pnl,
              0 as chain_fill_count,
              0.0 as chain_traded_size,
              0.0 as chain_traded_notional,
              0.0 as chain_position_size,
              0.0 as chain_current_value,
              0.0 as chain_net_cashflow,
              0.0 as chain_mark_to_market_pnl,
              activity.first_trade_at,
              activity.last_trade_at,
              positions.last_position_at as last_position_at,
              0 as last_chain_fill_block,
              activity.trade_count_24h,
              activity.traded_notional_24h,
              activity.buy_notional_24h,
              activity.sell_notional_24h,
              activity.latest_action,
              greatest(0, toUInt32(dateDiff('second', activity.last_trade_at, now64(3))))
                as data_lag_seconds
            from
            (
              select
                user_address,
                count() as trade_count,
                countIf(side = 'BUY') as buy_count,
                countIf(side = 'SELL') as sell_count,
                sum(size) as traded_size,
                sum(notional) as traded_notional,
                min(timestamp) as first_trade_at,
                max(timestamp) as last_trade_at,
                countIf(timestamp >= now64(3) - interval 24 hour) as trade_count_24h,
                sumIf(notional, timestamp >= now64(3) - interval 24 hour)
                  as traded_notional_24h,
                sumIf(notional, side = 'BUY' and timestamp >= now64(3) - interval 24 hour)
                  as buy_notional_24h,
                sumIf(notional, side = 'SELL' and timestamp >= now64(3) - interval 24 hour)
                  as sell_notional_24h,
                argMax(side, timestamp) as latest_action
              from
              (
                select
                  raw_user_address as user_address,
                  raw_trade_key,
                  argMax(raw_timestamp, (raw_source_priority, raw_ingested_at)) as timestamp,
                  argMax(raw_side, (raw_source_priority, raw_ingested_at)) as side,
                  argMax(raw_size, (raw_source_priority, raw_ingested_at)) as size,
                  argMax(raw_notional, (raw_source_priority, raw_ingested_at)) as notional
                from
                (
{source_sql}
                )
                group by raw_user_address, raw_trade_key
              )
              group by user_address
            ) as activity
            left join
            (
              select
                positions.user_address,
                count() as position_count,
                sum(positions.position_size * ifNull(marks.mark_price, positions.last_price))
                  as current_value,
                sum(
                  positions.net_cashflow
                  + positions.position_size * ifNull(marks.mark_price, positions.last_price)
                ) as total_pnl,
                max(positions.last_trade_at) as last_position_at
              from
              (
                select
                  user_address,
                  token_id,
                  sum(if(side = 'BUY', size, -size)) as position_size,
                  sum(if(side = 'SELL', notional, -notional)) as net_cashflow,
                  argMax(price, timestamp) as last_price,
                  max(timestamp) as last_trade_at
                from
                (
                  select
                    raw_user_address as user_address,
                    raw_trade_key,
                    argMax(raw_timestamp, (raw_source_priority, raw_ingested_at)) as timestamp,
                    argMax(raw_token_id, (raw_source_priority, raw_ingested_at)) as token_id,
                    argMax(raw_side, (raw_source_priority, raw_ingested_at)) as side,
                    argMax(raw_price, (raw_source_priority, raw_ingested_at)) as price,
                    argMax(raw_size, (raw_source_priority, raw_ingested_at)) as size,
                    argMax(raw_notional, (raw_source_priority, raw_ingested_at)) as notional
                  from
                  (
{source_sql}
                  )
                  group by raw_user_address, raw_trade_key
                )
                group by user_address, token_id
                having abs(position_size) > 0.000001
              ) as positions
              left join
              (
                select
                  wallet_tokens.token_id as token_id,
                  multiIf(
                    latest_book.best_bid is not null and latest_book.best_ask is not null,
                      cast((latest_book.best_bid + latest_book.best_ask) / 2, 'Nullable(Float64)'),
                    latest_price.mark_at > toDateTime64(0, 3, 'UTC'),
                      cast(latest_price.price, 'Nullable(Float64)'),
                    cast(null, 'Nullable(Float64)')
                  ) as mark_price
                from
                (
                  select distinct raw_token_id as token_id
                  from
                  (
{source_sql}
                  )
                  where raw_token_id != ''
                ) as wallet_tokens
                left join
                (
                  select
                    token_id,
                    argMax(price, timestamp) as price,
                    max(timestamp) as mark_at
                  from fact_price_history
                  where token_id in
                  (
                    select distinct raw_token_id as token_id
                    from
                    (
{source_sql}
                    )
                    where raw_token_id != ''
                  )
                  group by token_id
                ) as latest_price on wallet_tokens.token_id = latest_price.token_id
                left join
                (
                  select
                    token_id,
                    argMax(best_bid, captured_at) as best_bid,
                    argMax(best_ask, captured_at) as best_ask
                  from fact_orderbook_snapshot
                  where token_id in
                  (
                    select distinct raw_token_id as token_id
                    from
                    (
{source_sql}
                    )
                    where raw_token_id != ''
                  )
                  group by token_id
                ) as latest_book on wallet_tokens.token_id = latest_book.token_id
              ) as marks on positions.token_id = marks.token_id
              group by positions.user_address
            ) as positions on activity.user_address = positions.user_address
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def trader_chain_profile(self, user: str) -> dict[str, Any] | None:
        sql = f"""
            select
              user_address,
              chain_fill_count,
              chain_traded_size,
              chain_traded_notional,
              chain_position_size,
              chain_current_value,
              chain_net_cashflow,
              chain_mark_to_market_pnl,
              last_chain_fill_block
            from mart_trader_chain_pnl final
            where user_address = {ch_string(user)}
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def wallet_reputation(self, query: dict[str, list[str]]) -> dict[str, Any] | None:
        user = param(query, "user").lower()
        if not user:
            return None
        sql = f"""
            select
              user_address,
              completed_event_count,
              profitable_event_count,
              losing_event_count,
              win_rate,
              realized_pnl,
              positive_pnl,
              negative_pnl,
              buy_notional,
              sell_notional,
              traded_notional,
              trade_count,
              avg_event_roi,
              best_event_pnl,
              worst_event_pnl,
              active_position_count,
              active_event_count,
              active_unrealized_pnl_estimate,
              favorite_category,
              favorite_category_notional,
              first_trade_at,
              last_trade_at
            from mart_wallet_reputation final
            where user_address = {ch_string(user)}
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        if rows:
            return rows[0]
        fallback_sql = f"""
            select
              user_address,
              0 as completed_event_count,
              0 as profitable_event_count,
              0 as losing_event_count,
              0.0 as win_rate,
              realized_pnl,
              greatest(realized_pnl, 0.0) as positive_pnl,
              least(realized_pnl, 0.0) as negative_pnl,
              0.0 as buy_notional,
              0.0 as sell_notional,
              traded_notional,
              trade_count,
              0.0 as avg_event_roi,
              0.0 as best_event_pnl,
              0.0 as worst_event_pnl,
              position_count as active_position_count,
              0 as active_event_count,
              total_pnl as active_unrealized_pnl_estimate,
              '' as favorite_category,
              0.0 as favorite_category_notional,
              first_trade_at,
              last_trade_at
            from mart_trader_profile final
            where user_address = {ch_string(user)}
            limit 1
            format JSONEachRow
        """
        fallback_rows = rows_json(self.clickhouse.query_text(fallback_sql))
        return fallback_rows[0] if fallback_rows else None

    def wallet_summary(self, query: dict[str, list[str]]) -> dict[str, Any]:
        if wallet_detail_scope(param(query, "scope", "all")) == "fifa":
            return self.wallet_summary_fifa(query)
        min_smart_notional = float_param(
            query, "min_smart_notional", 10_000.0, minimum=0.0
        )
        min_roi = float_param(query, "min_roi", 0.55, minimum=0.0)
        candidate_min_notional = float_param(
            query, "candidate_min_notional", 5_000.0, minimum=0.0
        )
        candidate_min_roi = float_param(query, "candidate_min_roi", 0.10, minimum=0.0)
        whale_min_notional = float_param(
            query, "whale_min_notional", 1_000_000.0, minimum=0.0
        )
        whale_min_single_trade = float_param(
            query, "whale_min_single_trade", 100_000.0, minimum=0.0
        )
        watch_min_notional = float_param(query, "watch_min_notional", 100_000.0, minimum=0.0)
        watch_min_notional_24h = float_param(
            query, "watch_min_notional_24h", 5_000.0, minimum=0.0
        )
        strict_expr = (
            f"screener.traded_notional >= {min_smart_notional} "
            f"and screener.pnl_roi >= {min_roi}"
        )
        candidate_expr = (
            f"screener.traded_notional >= {candidate_min_notional} "
            f"and screener.pnl_captured_at is not null "
            f"and screener.pnl_roi >= {candidate_min_roi}"
        )
        whale_expr = (
            f"screener.traded_notional >= {whale_min_notional} "
            f"or screener.max_single_trade_notional >= {whale_min_single_trade}"
        )
        sql = f"""
            with recent_flow as
            (
              select
                user_address,
                traded_notional_24h
              from mart_wallet_trade_rollup final
            )
            select
              count() as total_wallets,
              countIf(screener.traded_notional >= {min_smart_notional}) as wallets_over_10k,
              countIf({strict_expr}) as smart_wallets,
              countIf({candidate_expr}) as candidate_smart_wallets,
              countIf({whale_expr}) as whale_wallets,
              countIf(
                {candidate_expr}
                or {whale_expr}
                or screener.traded_notional >= {watch_min_notional}
                or ifNull(recent_flow.traded_notional_24h, 0.0) >= {watch_min_notional_24h}
              ) as watch_wallets,
              countIf(screener.pnl_captured_at is not null) as pnl_covered_wallets,
              countIf(
                screener.traded_notional >= {min_smart_notional}
                and screener.pnl_captured_at is not null
              ) as over_10k_with_pnl,
              countIf(
                screener.traded_notional >= {min_smart_notional}
                and screener.pnl_captured_at is null
              ) as over_10k_without_pnl,
              max(screener.updated_at) as updated_at
            from mart_wallet_screener as screener final
            left join recent_flow on screener.user_address = recent_flow.user_address
            where screener.user_address != ''
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else {}

    def wallet_screener(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        if wallet_detail_scope(param(query, "scope", "all")) == "fifa":
            return self.wallet_screener_fifa(query)
        limit = int_param(query, "limit", 50, maximum=500)
        min_notional = float_param(query, "min_notional", 0.0, minimum=0.0)
        min_notional_24h = float_param(query, "min_notional_24h", 0.0, minimum=0.0)
        tier = param(query, "tier")
        mode = param(query, "mode", "active")
        range_value = param(query, "range", "all").lower()
        category = param(query, "category")
        whale_min_notional = float_param(
            query, "whale_min_notional", 1_000_000.0, minimum=0.0
        )
        whale_min_single_trade = float_param(
            query, "whale_min_single_trade", 100_000.0, minimum=0.0
        )
        min_smart_notional = float_param(
            query, "min_smart_notional", 10_000.0, minimum=0.0
        )
        min_roi = float_param(query, "min_roi", 0.55, minimum=0.0)
        candidate_min_notional = float_param(
            query, "candidate_min_notional", 5_000.0, minimum=0.0
        )
        candidate_min_roi = float_param(query, "candidate_min_roi", 0.10, minimum=0.0)
        watch_min_notional = float_param(query, "watch_min_notional", 100_000.0, minimum=0.0)
        watch_min_notional_24h = float_param(
            query, "watch_min_notional_24h", 5_000.0, minimum=0.0
        )
        strict_smart_expr = (
            f"(screener.traded_notional >= {min_smart_notional} "
            f"and screener.pnl_roi >= {min_roi})"
        )
        candidate_smart_expr = (
            f"(screener.traded_notional >= {candidate_min_notional} "
            f"and screener.pnl_captured_at is not null "
            f"and screener.pnl_roi >= {candidate_min_roi})"
        )
        whale_expr = (
            f"(screener.traded_notional >= {whale_min_notional} "
            f"or screener.max_single_trade_notional >= {whale_min_single_trade})"
        )
        recent_flow_expr = f"ifNull(rollup.traded_notional_24h, 0.0) >= {watch_min_notional_24h}"
        watch_expr = (
            f"({candidate_smart_expr} or {whale_expr} "
            f"or screener.traded_notional >= {watch_min_notional} "
            f"or {recent_flow_expr})"
        )
        where = [
            "screener.user_address != ''",
            f"screener.traded_notional >= {min_notional}",
            f"ifNull(rollup.traded_notional_24h, 0.0) >= {min_notional_24h}",
        ]
        joins = [
            "left join mart_wallet_trade_rollup as rollup final\n"
            "              on screener.user_address = rollup.user_address",
            "left join mart_wallet_reputation as rep final\n"
            "              on screener.user_address = rep.user_address",
        ]
        select_fields = [
            "screener.trade_count as trade_count",
            "screener.buy_count as buy_count",
            "screener.sell_count as sell_count",
            "screener.traded_size as traded_size",
            "screener.traded_notional as traded_notional",
            "screener.first_trade_at as first_trade_at",
            "screener.last_trade_at as last_trade_at",
            "ifNull(rollup.traded_notional_24h, 0.0) as traded_notional_24h",
            "ifNull(rollup.trade_count_24h, 0) as trade_count_24h",
            "ifNull(rollup.buy_notional_24h, 0.0) as buy_notional_24h",
            "ifNull(rollup.sell_notional_24h, 0.0) as sell_notional_24h",
            "ifNull(rollup.net_notional_24h, 0.0) as net_notional_24h",
            "ifNull(rollup.latest_action, '') as latest_action",
            "0.0 as category_traded_notional",
            "0 as category_trade_count",
            "0.0 as category_buy_notional",
            "0.0 as category_sell_notional",
            "if(screener.trade_count = 0, cast(null, 'Nullable(Float64)'), screener.traded_notional / screener.trade_count) as avg_bet",
        ]

        category_filter = wallet_screener_category_filter(category)
        range_filter = wallet_screener_range_filter(range_value)
        scoped = bool(category_filter or range_filter)
        if scoped:
            scoped_where = ["trades.user_address in (select user_address from candidate_wallets)"]
            if range_filter:
                scoped_where.append(range_filter)
            if category_filter:
                scoped_where.append(category_filter)
            joins.append(
                f"""
            inner join
            (
              select
                user_address,
                count() as scoped_trade_count,
                countIf(side = 'BUY') as scoped_buy_count,
                countIf(side = 'SELL') as scoped_sell_count,
                sum(size) as scoped_traded_size,
                sum(notional) as scoped_traded_notional,
                sumIf(notional, side = 'BUY') as scoped_buy_notional,
                sumIf(notional, side = 'SELL') as scoped_sell_notional,
                min(timestamp) as scoped_first_trade_at,
                max(timestamp) as scoped_last_trade_at
              from
              (
                select
                  raw_trade_key,
                  argMax(raw_timestamp, raw_ingested_at) as timestamp,
                  argMax(raw_user_address, raw_ingested_at) as user_address,
                  argMax(raw_side, raw_ingested_at) as side,
                  argMax(raw_size, raw_ingested_at) as size,
                  argMax(raw_notional, raw_ingested_at) as notional
                from
                (
                  select
                    if(
                      trades.trade_id != '',
                      concat(
                        trades.trade_id, '|', lower(trades.user_address), '|',
                        trades.token_id, '|', trades.side
                      ),
                      concat(
                        trades.transaction_hash, '|', toString(trades.log_index), '|',
                        lower(trades.user_address), '|', trades.token_id, '|', trades.side
                      )
                    ) as raw_trade_key,
                    trades.timestamp as raw_timestamp,
                    lower(trades.user_address) as raw_user_address,
                    trades.side as raw_side,
                    trades.size as raw_size,
                    trades.notional as raw_notional,
                    trades.ingested_at as raw_ingested_at
                  from fact_trade_by_user as trades
                  left join dim_market as markets final on trades.condition_id = markets.condition_id
                  left join dim_event as events final on markets.event_id = events.event_id
                  where {" and ".join(scoped_where)}
                )
                group by raw_trade_key
              )
              group by user_address
            ) as scoped on screener.user_address = scoped.user_address
                """
            )
            select_fields = [
                "scoped.scoped_trade_count as trade_count",
                "scoped.scoped_buy_count as buy_count",
                "scoped.scoped_sell_count as sell_count",
                "scoped.scoped_traded_size as traded_size",
                "scoped.scoped_traded_notional as traded_notional",
                "scoped.scoped_first_trade_at as first_trade_at",
                "scoped.scoped_last_trade_at as last_trade_at",
                "ifNull(rollup.traded_notional_24h, 0.0) as traded_notional_24h",
                "ifNull(rollup.trade_count_24h, 0) as trade_count_24h",
                "scoped.scoped_buy_notional as buy_notional_24h",
                "scoped.scoped_sell_notional as sell_notional_24h",
                "scoped.scoped_buy_notional - scoped.scoped_sell_notional as net_notional_24h",
                "if(scoped.scoped_buy_notional >= scoped.scoped_sell_notional, 'BUY', 'SELL') as latest_action",
                "scoped.scoped_traded_notional as category_traded_notional",
                "scoped.scoped_trade_count as category_trade_count",
                "scoped.scoped_buy_notional as category_buy_notional",
                "scoped.scoped_sell_notional as category_sell_notional",
                "if(scoped.scoped_trade_count = 0, cast(null, 'Nullable(Float64)'), scoped.scoped_traded_notional / scoped.scoped_trade_count) as avg_bet",
            ]
        if tier:
            tier_floor = {
                "10m_plus": 10_000_000,
                "5m_plus": 5_000_000,
                "1m_plus": 1_000_000,
                "100k_plus": 100_000,
                "standard": 0,
            }.get(tier)
            if tier_floor is not None:
                where.append(f"screener.traded_notional >= {float(tier_floor)}")
        if mode in ("smart", "strict_smart"):
            where.append(strict_smart_expr)
        elif mode == "candidate_smart":
            where.append(candidate_smart_expr)
        elif mode == "whale":
            where.append(whale_expr)
        elif mode == "watch":
            where.append(watch_expr)

        if mode in ("smart", "strict_smart"):
            order_by = (
                "screener.pnl_roi desc, screener.total_pnl desc, traded_notional desc"
            )
        elif mode == "candidate_smart":
            order_by = (
                "screener.pnl_roi desc, screener.total_pnl desc, traded_notional desc"
            )
        elif mode == "whale":
            order_by = (
                f"greatest(screener.traded_notional / greatest({whale_min_notional}, 1), "
                f"screener.max_single_trade_notional / greatest({whale_min_single_trade}, 1)) desc, "
                "traded_notional desc"
            )
        elif mode == "watch":
            order_by = (
                f"multiIf({strict_smart_expr}, 4, {candidate_smart_expr}, 3, {whale_expr}, 2, "
                f"{recent_flow_expr}, 1, 0) desc, "
                "screener.pnl_roi desc, ifNull(rollup.traded_notional_24h, 0.0) desc, traded_notional desc"
            )
        else:
            order_by = "last_trade_at desc, traded_notional desc"

        sql = f"""
            with candidate_wallets as
            (
              select screener.user_address as user_address
              from mart_wallet_screener as screener final
              left join mart_wallet_trade_rollup as rollup final
                on screener.user_address = rollup.user_address
              where {" and ".join(where)}
            )
            select
              screener.user_address as user_address,
              {",\n              ".join(select_fields)},
              screener.max_single_trade_notional as max_single_trade_notional,
              screener.position_count as position_count,
              screener.positions_value as positions_value,
              screener.portfolio_value as portfolio_value,
              screener.available_balance as available_balance,
              screener.total_pnl as total_pnl,
              screener.portfolio_captured_at as portfolio_captured_at,
              screener.pnl_captured_at as pnl_captured_at,
              screener.pnl_roi as pnl_roi,
              {whale_expr} as is_whale,
              {strict_smart_expr} as is_smart,
              {candidate_smart_expr} as is_candidate_smart,
              multiIf(
                {strict_smart_expr}, 'strict_smart',
                {candidate_smart_expr}, 'candidate_smart',
                {whale_expr}, 'whale',
                {recent_flow_expr}, 'recent_flow',
                'active'
              ) as wallet_segment,
              multiIf(
                {strict_smart_expr}, 'strict_smart_roi',
                {candidate_smart_expr}, 'positive_roi_candidate',
                {whale_expr}, 'whale_volume',
                {recent_flow_expr}, 'recent_flow',
                ''
              ) as candidate_reason,
              screener.whale_reason as whale_reason,
              ifNull(rollup.buy_notional, 0.0) as recent_buy_notional,
              ifNull(rollup.sell_notional, 0.0) as recent_sell_notional,
              multiIf(
                screener.traded_notional >= 10000000, '10m_plus',
                screener.traded_notional >= 5000000, '5m_plus',
                screener.traded_notional >= 1000000, '1m_plus',
                screener.traded_notional >= 100000, '100k_plus',
                'standard'
              ) as whale_tier,
              ifNull(rollup.data_lag_seconds, 0) as data_lag_seconds,
              if(rep.completed_event_count > 0, cast(rep.win_rate, 'Nullable(Float64)'), cast(null, 'Nullable(Float64)')) as win_rate,
              if(rep.completed_event_count > 0, cast(rep.realized_pnl, 'Nullable(Float64)'), cast(null, 'Nullable(Float64)')) as realized_pnl,
              ifNull(rep.completed_event_count, 0) as completed_event_count,
              cast(rep.active_unrealized_pnl_estimate, 'Nullable(Float64)') as active_unrealized_pnl_estimate,
              ifNull(rep.favorite_category, '') as favorite_category,
              screener.updated_at as updated_at
            from mart_wallet_screener as screener final
            {" ".join(joins)}
            where {" and ".join(where)}
            order by {order_by}
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def wallet_summary_fifa(self, query: dict[str, list[str]]) -> dict[str, Any]:
        min_smart_notional = float_param(
            query, "min_smart_notional", 10_000.0, minimum=0.0
        )
        min_roi = float_param(query, "min_roi", 0.55, minimum=0.0)
        candidate_min_notional = float_param(
            query, "candidate_min_notional", 1_000.0, minimum=0.0
        )
        candidate_min_roi = float_param(query, "candidate_min_roi", 0.10, minimum=0.0)
        whale_min_notional = float_param(
            query, "whale_min_notional", 1_000_000.0, minimum=0.0
        )
        whale_min_single_trade = float_param(
            query, "whale_min_single_trade", 100_000.0, minimum=0.0
        )
        watch_min_notional = float_param(query, "watch_min_notional", 10_000.0, minimum=0.0)
        watch_min_notional_24h = float_param(
            query, "watch_min_notional_24h", 1_000.0, minimum=0.0
        )
        fifa_roi = "if(fifa.buy_notional = 0, 0.0, fifa.equity_now / fifa.buy_notional)"
        max_single = "ifNull(trade_max.max_single_trade_notional, 0.0)"
        strict_smart_expr = (
            f"fifa.buy_notional >= {min_smart_notional} "
            f"and fifa.data_quality = 'estimate' "
            f"and {fifa_roi} >= {min_roi}"
        )
        candidate_smart_expr = (
            f"fifa.buy_notional >= {candidate_min_notional} "
            f"and fifa.data_quality = 'estimate' "
            f"and {fifa_roi} >= {candidate_min_roi}"
        )
        whale_expr = (
            f"fifa.traded_notional >= {whale_min_notional} "
            f"or {max_single} >= {whale_min_single_trade}"
        )
        sql = f"""
            with trade_max as
            (
              select
                user_address,
                max(notional) as max_single_trade_notional
              from mart_fifa_trade final
              where user_address != ''
              group by user_address
            )
            select
              'fifa' as scope,
              count() as total_wallets,
              countIf(fifa.buy_notional >= {min_smart_notional}) as wallets_over_10k,
              countIf({strict_smart_expr}) as smart_wallets,
              countIf({candidate_smart_expr}) as candidate_smart_wallets,
              countIf({whale_expr}) as whale_wallets,
              countIf(
                {candidate_smart_expr}
                or {whale_expr}
                or fifa.traded_notional >= {watch_min_notional}
                or fifa.traded_notional_24h >= {watch_min_notional_24h}
              ) as watch_wallets,
              count() as pnl_covered_wallets,
              countIf(fifa.buy_notional >= {min_smart_notional}) as over_10k_with_pnl,
              0 as over_10k_without_pnl,
              sum(fifa.traded_notional) as traded_notional,
              sum(fifa.traded_notional_24h) as traded_notional_24h,
              sum(fifa.equity_now) as total_pnl,
              max(fifa.updated_at) as updated_at
            from mart_wallet_fifa_24h_pnl as fifa final
            left join trade_max on fifa.user_address = trade_max.user_address
            where fifa.user_address != ''
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else {}

    def wallet_screener_fifa(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        limit = int_param(query, "limit", 50, maximum=500)
        min_notional = float_param(query, "min_notional", 0.0, minimum=0.0)
        min_notional_24h = float_param(query, "min_notional_24h", 0.0, minimum=0.0)
        tier = param(query, "tier")
        mode = param(query, "mode", "active")
        range_value = param(query, "range", "all").lower()
        whale_min_notional = float_param(
            query, "whale_min_notional", 1_000_000.0, minimum=0.0
        )
        whale_min_single_trade = float_param(
            query, "whale_min_single_trade", 100_000.0, minimum=0.0
        )
        min_smart_notional = float_param(
            query, "min_smart_notional", 10_000.0, minimum=0.0
        )
        min_roi = float_param(query, "min_roi", 0.55, minimum=0.0)
        candidate_min_notional = float_param(
            query, "candidate_min_notional", 1_000.0, minimum=0.0
        )
        candidate_min_roi = float_param(query, "candidate_min_roi", 0.10, minimum=0.0)
        watch_min_notional = float_param(query, "watch_min_notional", 10_000.0, minimum=0.0)
        watch_min_notional_24h = float_param(
            query, "watch_min_notional_24h", 1_000.0, minimum=0.0
        )
        fifa_roi = "if(fifa.buy_notional = 0, 0.0, fifa.equity_now / fifa.buy_notional)"
        max_single = "ifNull(trade_max.max_single_trade_notional, 0.0)"
        strict_smart_expr = (
            f"(fifa.buy_notional >= {min_smart_notional} "
            f"and fifa.data_quality = 'estimate' "
            f"and {fifa_roi} >= {min_roi})"
        )
        candidate_smart_expr = (
            f"(fifa.buy_notional >= {candidate_min_notional} "
            f"and fifa.data_quality = 'estimate' "
            f"and {fifa_roi} >= {candidate_min_roi})"
        )
        whale_expr = (
            f"(fifa.traded_notional >= {whale_min_notional} "
            f"or {max_single} >= {whale_min_single_trade})"
        )
        recent_flow_expr = f"fifa.traded_notional_24h >= {watch_min_notional_24h}"
        watch_expr = (
            f"({candidate_smart_expr} or {whale_expr} "
            f"or fifa.traded_notional >= {watch_min_notional} "
            f"or {recent_flow_expr})"
        )
        where = [
            "fifa.user_address != ''",
            f"fifa.traded_notional >= {min_notional}",
            f"fifa.traded_notional_24h >= {min_notional_24h}",
        ]
        range_filter = wallet_screener_fifa_range_filter(range_value)
        if range_filter:
            where.append(range_filter)
        if tier:
            tier_floor = {
                "10m_plus": 10_000_000,
                "5m_plus": 5_000_000,
                "1m_plus": 1_000_000,
                "100k_plus": 100_000,
                "standard": 0,
            }.get(tier)
            if tier_floor is not None:
                where.append(f"fifa.traded_notional >= {float(tier_floor)}")
        if mode in ("smart", "strict_smart"):
            where.append(strict_smart_expr)
            order_by = f"{fifa_roi} desc, fifa.equity_now desc, fifa.traded_notional desc"
        elif mode == "candidate_smart":
            where.append(candidate_smart_expr)
            order_by = f"{fifa_roi} desc, fifa.equity_now desc, fifa.traded_notional desc"
        elif mode == "whale":
            where.append(whale_expr)
            order_by = (
                f"greatest(fifa.traded_notional / greatest({whale_min_notional}, 1), "
                f"{max_single} / greatest({whale_min_single_trade}, 1)) desc, "
                "fifa.traded_notional desc"
            )
        elif mode == "watch":
            where.append(watch_expr)
            order_by = (
                f"multiIf({strict_smart_expr}, 4, {candidate_smart_expr}, 3, {whale_expr}, 2, "
                f"{recent_flow_expr}, 1, 0) desc, "
                f"{fifa_roi} desc, fifa.traded_notional_24h desc, fifa.traded_notional desc"
            )
        else:
            order_by = "fifa.last_trade_at desc, fifa.traded_notional desc"
        if wallet_screener_fifa_range_is_24h(range_value):
            order_by = f"fifa.traded_notional_24h desc, {order_by}"

        sql = f"""
            with trade_max as
            (
              select
                user_address,
                max(notional) as max_single_trade_notional
              from mart_fifa_trade final
              where user_address != ''
              group by user_address
            )
            select
              'fifa' as scope,
              fifa.user_address as user_address,
              fifa.trade_count as trade_count,
              fifa.buy_count as buy_count,
              fifa.sell_count as sell_count,
              fifa.traded_size as traded_size,
              fifa.traded_notional as traded_notional,
              fifa.first_trade_at as first_trade_at,
              fifa.last_trade_at as last_trade_at,
              fifa.traded_notional_24h as traded_notional_24h,
              fifa.trade_count_24h as trade_count_24h,
              fifa.buy_notional_24h as buy_notional_24h,
              fifa.sell_notional_24h as sell_notional_24h,
              fifa.net_notional_24h as net_notional_24h,
              fifa.latest_action as latest_action,
              fifa.traded_notional as category_traded_notional,
              fifa.trade_count as category_trade_count,
              fifa.buy_notional as category_buy_notional,
              fifa.sell_notional as category_sell_notional,
              if(fifa.trade_count = 0, cast(null, 'Nullable(Float64)'), fifa.traded_notional / fifa.trade_count)
                as avg_bet,
              {max_single} as max_single_trade_notional,
              fifa.open_position_count as position_count,
              fifa.open_position_value_now as positions_value,
              fifa.open_position_value_now as portfolio_value,
              0.0 as available_balance,
              fifa.equity_now as total_pnl,
              fifa.updated_at as portfolio_captured_at,
              fifa.updated_at as pnl_captured_at,
              {fifa_roi} as pnl_roi,
              {whale_expr} as is_whale,
              {strict_smart_expr} as is_smart,
              {candidate_smart_expr} as is_candidate_smart,
              multiIf(
                {strict_smart_expr}, 'strict_smart',
                {candidate_smart_expr}, 'candidate_smart',
                {whale_expr}, 'whale',
                {recent_flow_expr}, 'recent_flow',
                'active'
              ) as wallet_segment,
              multiIf(
                {strict_smart_expr}, 'fifa_strict_smart_roi',
                {candidate_smart_expr}, 'fifa_positive_roi_candidate',
                {whale_expr}, 'fifa_whale_volume',
                {recent_flow_expr}, 'fifa_recent_flow',
                ''
              ) as candidate_reason,
              multiIf(
                fifa.traded_notional >= {whale_min_notional} and {max_single} >= {whale_min_single_trade},
                  'fifa_total_volume_and_single_trade',
                fifa.traded_notional >= {whale_min_notional}, 'fifa_total_volume',
                {max_single} >= {whale_min_single_trade}, 'fifa_single_trade',
                ''
              ) as whale_reason,
              fifa.buy_notional_24h as recent_buy_notional,
              fifa.sell_notional_24h as recent_sell_notional,
              multiIf(
                fifa.traded_notional >= 10000000, '10m_plus',
                fifa.traded_notional >= 5000000, '5m_plus',
                fifa.traded_notional >= 1000000, '1m_plus',
                fifa.traded_notional >= 100000, '100k_plus',
                'standard'
              ) as whale_tier,
              0 as data_lag_seconds,
              fifa.win_rate as win_rate,
              fifa.win_rate_24h as win_rate_24h,
              fifa.win_rate_7d as win_rate_7d,
              cast(fifa.equity_now, 'Nullable(Float64)') as realized_pnl,
              fifa.profitable_token_count + fifa.losing_token_count as completed_event_count,
              fifa.profitable_token_count as profitable_event_count,
              fifa.losing_token_count as losing_event_count,
              cast(fifa.open_position_value_now, 'Nullable(Float64)') as active_unrealized_pnl_estimate,
              'Sports' as favorite_category,
              fifa.event_count as fifa_event_count,
              fifa.market_count as fifa_market_count,
              fifa.event_count_24h as fifa_event_count_24h,
              fifa.market_count_24h as fifa_market_count_24h,
              fifa.total_pnl as fifa_total_pnl,
              fifa.total_pnl_roi as fifa_total_pnl_roi,
              fifa.pnl_24h as fifa_pnl_24h,
              fifa.pnl_roi_24h as fifa_pnl_roi_24h,
              fifa.pnl_7d as fifa_pnl_7d,
              fifa.pnl_roi_7d as fifa_pnl_roi_7d,
              fifa.win_rate as fifa_win_rate,
              fifa.win_rate_24h as fifa_win_rate_24h,
              fifa.win_rate_7d as fifa_win_rate_7d,
              fifa.profitable_token_count as fifa_profitable_token_count,
              fifa.losing_token_count as fifa_losing_token_count,
              fifa.profitable_token_count_24h as fifa_profitable_token_count_24h,
              fifa.losing_token_count_24h as fifa_losing_token_count_24h,
              fifa.profitable_token_count_7d as fifa_profitable_token_count_7d,
              fifa.losing_token_count_7d as fifa_losing_token_count_7d,
              fifa.traded_notional_24h as fifa_traded_notional_24h,
              fifa.data_quality as fifa_data_quality,
              fifa.updated_at as updated_at
            from mart_wallet_fifa_24h_pnl as fifa final
            left join trade_max on fifa.user_address = trade_max.user_address
            where {" and ".join(where)}
            order by {order_by}
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def wallet_fifa_24h_pnl(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = int_param(query, "limit", 50, maximum=500)
        offset = bounded_int_param(query, "offset", 0, minimum=0, maximum=100_000)
        min_notional_24h = float_param(query, "min_notional_24h", 0.0, minimum=0.0)
        min_trades_24h = bounded_int_param(query, "min_trades_24h", 0, minimum=0, maximum=1_000_000)
        data_quality = param(query, "data_quality").strip().lower()
        user = normalize_wallet_address(param(query, "user"))
        search = param(query, "q").strip().lower()
        sort = param(query, "sort", "pnl_24h").strip().lower()
        direction = param(query, "direction", "desc").strip().lower()
        active_24h_only = bool_param(query, "active_24h", False)

        where = [
            "fifa.user_address != ''",
            f"fifa.traded_notional_24h >= {min_notional_24h}",
            f"fifa.trade_count_24h >= {min_trades_24h}",
        ]
        if data_quality:
            where.append(f"lower(fifa.data_quality) = {ch_string(data_quality)}")
        if user:
            where.append(f"fifa.user_address = {ch_string(user)}")
        if search:
            where.append(f"positionCaseInsensitive(fifa.user_address, {ch_string(search)}) > 0")
        if active_24h_only:
            where.append("fifa.trade_count_24h > 0")

        sort_columns = {
            "pnl_24h": "fifa.pnl_24h",
            "roi_24h": "fifa.pnl_roi_24h",
            "notional_24h": "fifa.traded_notional_24h",
            "volume_24h": "fifa.traded_notional_24h",
            "trades_24h": "fifa.trade_count_24h",
            "pnl_7d": "fifa.pnl_7d",
            "roi_7d": "fifa.pnl_roi_7d",
            "win_rate": "fifa.win_rate",
            "win_rate_24h": "fifa.win_rate_24h",
            "win_rate_7d": "fifa.win_rate_7d",
            "total_pnl": "fifa.total_pnl",
            "equity": "fifa.equity_now",
            "last_trade": "fifa.last_trade_at",
            "updated_at": "fifa.updated_at",
        }
        sort_column = sort_columns.get(sort, "fifa.pnl_24h")
        sort_direction = "asc" if direction == "asc" else "desc"
        sort_is_24h = sort in (
            "pnl_24h",
            "roi_24h",
            "notional_24h",
            "volume_24h",
            "trades_24h",
            "win_rate_24h",
        )
        active_rank_prefix = ""
        if sort_is_24h and not user and not search and not active_24h_only:
            active_rank_prefix = "fifa.trade_count_24h > 0 desc, "
        sql = f"""
            select
              fifa.user_address as user_address,
              fifa.trade_count as trade_count,
              fifa.buy_count as buy_count,
              fifa.sell_count as sell_count,
              fifa.traded_size as traded_size,
              fifa.traded_notional as traded_notional,
              fifa.buy_notional as buy_notional,
              fifa.sell_notional as sell_notional,
              fifa.trade_count_24h as trade_count_24h,
              fifa.buy_notional_24h as buy_notional_24h,
              fifa.sell_notional_24h as sell_notional_24h,
              fifa.traded_notional_24h as traded_notional_24h,
              fifa.net_notional_24h as net_notional_24h,
              fifa.event_count as event_count,
              fifa.market_count as market_count,
              fifa.event_count_24h as event_count_24h,
              fifa.market_count_24h as market_count_24h,
              fifa.token_count as token_count,
              fifa.open_position_count as open_position_count,
              fifa.open_position_value_now as open_position_value_now,
              fifa.open_position_value_24h_ago as open_position_value_24h_ago,
              fifa.open_position_value_7d_ago as open_position_value_7d_ago,
              fifa.equity_now as equity_now,
              fifa.equity_24h_ago as equity_24h_ago,
              fifa.equity_7d_ago as equity_7d_ago,
              fifa.total_pnl as total_pnl,
              fifa.total_pnl_roi as total_pnl_roi,
              fifa.pnl_24h as pnl_24h,
              fifa.pnl_base_24h as pnl_base_24h,
              fifa.pnl_roi_24h as pnl_roi_24h,
              fifa.pnl_7d as pnl_7d,
              fifa.pnl_base_7d as pnl_base_7d,
              fifa.pnl_roi_7d as pnl_roi_7d,
              fifa.profitable_token_count as profitable_token_count,
              fifa.losing_token_count as losing_token_count,
              fifa.win_rate as win_rate,
              fifa.profitable_token_count_24h as profitable_token_count_24h,
              fifa.losing_token_count_24h as losing_token_count_24h,
              fifa.win_rate_24h as win_rate_24h,
              fifa.profitable_token_count_7d as profitable_token_count_7d,
              fifa.losing_token_count_7d as losing_token_count_7d,
              fifa.win_rate_7d as win_rate_7d,
              fifa.first_trade_at as first_trade_at,
              fifa.last_trade_at as last_trade_at,
              fifa.latest_action as latest_action,
              fifa.missing_mark_count as missing_mark_count,
              fifa.negative_position_count as negative_position_count,
              fifa.data_quality as data_quality,
              fifa.updated_at as updated_at,
              ifNull(screener.is_whale, false) as is_whale,
              ifNull(screener.is_smart, false) as is_smart,
              ifNull(screener.total_pnl, 0.0) as all_site_total_pnl,
              ifNull(screener.pnl_roi, 0.0) as all_site_pnl_roi,
              ifNull(screener.portfolio_value, 0.0) as portfolio_value,
              ifNull(screener.max_single_trade_notional, 0.0) as max_single_trade_notional
            from mart_wallet_fifa_24h_pnl as fifa final
            left join mart_wallet_screener as screener final
              on fifa.user_address = screener.user_address
            where {" and ".join(where)}
            order by {active_rank_prefix}{sort_column} {sort_direction}, fifa.traded_notional_24h desc
            limit {limit} offset {offset}
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        summary_sql = f"""
            select
              count() as total,
              countIf(fifa.trade_count_24h > 0) as active_wallets_24h,
              countIf(fifa.pnl_24h != 0) as nonzero_pnl_wallets_24h,
              countIf(fifa.pnl_24h > 0) as profitable_wallets,
              countIf(fifa.pnl_24h < 0) as losing_wallets,
              sum(traded_notional_24h) as traded_notional_24h,
              sum(fifa.pnl_24h) as pnl_24h,
              sum(fifa.pnl_7d) as pnl_7d,
              sum(fifa.total_pnl) as total_pnl,
              avgIf(fifa.win_rate, fifa.profitable_token_count + fifa.losing_token_count > 0)
                as avg_win_rate,
              avgIf(
                fifa.win_rate_24h,
                fifa.profitable_token_count_24h + fifa.losing_token_count_24h > 0
              ) as avg_win_rate_24h,
              avgIf(
                fifa.win_rate_7d,
                fifa.profitable_token_count_7d + fifa.losing_token_count_7d > 0
              ) as avg_win_rate_7d,
              max(updated_at) as updated_at
            from mart_wallet_fifa_24h_pnl as fifa final
            where {" and ".join(where)}
            format JSONEachRow
        """
        summary_rows = rows_json(self.clickhouse.query_text(summary_sql))
        return {
            "scope": "fifa",
            "window_hours": 24,
            "limit": limit,
            "offset": offset,
            "sort": sort,
            "direction": sort_direction,
            "active_24h": active_24h_only,
            "summary": summary_rows[0] if summary_rows else {},
            "wallets": rows,
        }

    def polycop_wallet_signals(
        self,
        query: dict[str, list[str]],
        *,
        include_wallets: bool = True,
    ) -> dict[str, Any]:
        if self._polycop_wallet_signal_cache_store is None:
            return {"status": "store_unavailable", "wallets": [] if include_wallets else None}
        max_age_seconds = bounded_int_param(
            query,
            "max_age_seconds",
            0,
            minimum=0,
            maximum=7 * 24 * 3600,
        )
        row = self._polycop_wallet_signal_cache_store.get(
            max_age_seconds=max_age_seconds if max_age_seconds > 0 else None
        )
        if row is None:
            output: dict[str, Any] = {
                "status": "missing_cache",
                "cache": {"source": "postgres", "hit": False},
                "summary": {},
                "parameters": {},
            }
            if include_wallets:
                output.update({"wallets": [], "total": 0, "limit": 0, "offset": 0})
            return output

        detail = row.get("detail") if isinstance(row.get("detail"), dict) else {}
        summary = dict(row.get("summary") or {})
        output = {
            "status": row.get("status") or "unknown",
            "source": row.get("source") or "polycop",
            "cache": {
                "source": "postgres",
                "hit": True,
                "cache_key": row.get("cache_key"),
                "refreshed_at": row.get("refreshed_at"),
                "generated_at": row.get("generated_at"),
                "age_seconds": row.get("age_seconds"),
                "trigger_reason": row.get("trigger_reason"),
                "error": row.get("error"),
            },
            "summary": summary,
            "parameters": row.get("parameters") or {},
        }
        if not include_wallets:
            return output

        segment = normalize_polycop_segment(param(query, "segment", "ai_top"))
        limit = bounded_int_param(query, "limit", 50, minimum=1, maximum=500)
        offset = bounded_int_param(query, "offset", 0, minimum=0, maximum=100_000)
        min_ai_score = float_param(query, "min_ai_score", 0.0, minimum=0.0, maximum=100.0)
        search = param(query, "q").strip().lower()
        wallets = polycop_wallet_segment(detail, segment)
        if min_ai_score > 0:
            wallets = [
                wallet
                for wallet in wallets
                if float(wallet.get("ai_score") or 0.0) >= min_ai_score
            ]
        if search:
            wallets = [
                wallet for wallet in wallets if polycop_wallet_matches_search(wallet, search)
            ]
        total = len(wallets)
        output.update(
            {
                "segment": segment,
                "total": total,
                "limit": limit,
                "offset": offset,
                "wallets": wallets[offset : offset + limit],
            }
        )
        return output

    def polycop_fifa_signals(self, query: dict[str, list[str]]) -> dict[str, Any]:
        if self._polycop_wallet_signal_cache_store is None:
            return {"status": "store_unavailable", "wallets": []}
        max_age_seconds = bounded_int_param(
            query,
            "max_age_seconds",
            0,
            minimum=0,
            maximum=7 * 24 * 3600,
        )
        row = self._polycop_wallet_signal_cache_store.get(
            max_age_seconds=max_age_seconds if max_age_seconds > 0 else None
        )
        if row is None:
            return {
                "status": "missing_cache",
                "source": "polycop_fifa",
                "cache": {"source": "postgres", "hit": False},
                "summary": {},
                "parameters": {},
                "wallets": [],
                "total": 0,
                "limit": 0,
                "offset": 0,
            }

        detail = row.get("detail") if isinstance(row.get("detail"), dict) else {}
        segment = normalize_polycop_segment(param(query, "segment", "ai_top"))
        limit = bounded_int_param(query, "limit", 100, minimum=1, maximum=500)
        offset = bounded_int_param(query, "offset", 0, minimum=0, maximum=100_000)
        candidate_limit = bounded_int_param(
            query,
            "candidate_limit",
            max(limit + offset, 2000),
            minimum=1,
            maximum=2000,
        )
        min_ai_score = float_param(query, "min_ai_score", 0.0, minimum=0.0, maximum=100.0)
        min_fifa_notional = float_param(query, "min_fifa_notional", 0.0, minimum=0.0)
        min_fifa_events = bounded_int_param(
            query, "min_fifa_events", 0, minimum=0, maximum=1_000_000
        )
        require_positive_fifa = bool_param(query, "positive_fifa", False)
        active_24h_only = bool_param(query, "active_24h", False)
        data_quality = param(query, "data_quality").strip().lower()
        search = param(query, "q").strip().lower()

        polycop_wallets = polycop_wallet_segment(detail, segment)
        if min_ai_score > 0:
            polycop_wallets = [
                wallet
                for wallet in polycop_wallets
                if float(wallet.get("ai_score") or 0.0) >= min_ai_score
            ]
        if search:
            polycop_wallets = [
                wallet
                for wallet in polycop_wallets
                if polycop_wallet_matches_search(wallet, search)
            ]

        polycop_by_address: dict[str, dict[str, Any]] = {}
        for wallet in polycop_wallets[:candidate_limit]:
            address = normalize_wallet_address(str(wallet.get("address") or ""))
            if address and address not in polycop_by_address:
                polycop_by_address[address] = wallet
        addresses = list(polycop_by_address)
        if addresses:
            address_values = ", ".join(ch_string(address) for address in addresses)
            where = [
                f"fifa.user_address in ({address_values})",
                f"fifa.traded_notional >= {min_fifa_notional}",
                f"fifa.event_count >= {min_fifa_events}",
            ]
            if require_positive_fifa:
                where.append("fifa.equity_now > 0")
            if active_24h_only:
                where.append("fifa.trade_count_24h > 0")
            if data_quality:
                where.append(f"lower(fifa.data_quality) = {ch_string(data_quality)}")
            fifa_sql = f"""
                with trade_max as
                (
                  select
                    user_address,
                    max(notional) as max_single_trade_notional
                  from mart_fifa_trade final
                  where user_address in ({address_values})
                  group by user_address
                )
                select
                  fifa.user_address as user_address,
                  fifa.trade_count as fifa_trade_count,
                  fifa.buy_count as fifa_buy_count,
                  fifa.sell_count as fifa_sell_count,
                  fifa.traded_notional as fifa_traded_notional,
                  fifa.buy_notional as fifa_buy_notional,
                  fifa.sell_notional as fifa_sell_notional,
                  fifa.trade_count_24h as fifa_trade_count_24h,
                  fifa.traded_notional_24h as fifa_traded_notional_24h,
                  fifa.total_pnl as fifa_total_pnl,
                  fifa.total_pnl_roi as fifa_total_pnl_roi,
                  fifa.pnl_24h as fifa_pnl_24h,
                  fifa.pnl_roi_24h as fifa_pnl_roi_24h,
                  fifa.pnl_7d as fifa_pnl_7d,
                  fifa.pnl_roi_7d as fifa_pnl_roi_7d,
                  fifa.win_rate as fifa_win_rate,
                  fifa.win_rate_24h as fifa_win_rate_24h,
                  fifa.win_rate_7d as fifa_win_rate_7d,
                  fifa.profitable_token_count as fifa_profitable_token_count,
                  fifa.losing_token_count as fifa_losing_token_count,
                  fifa.profitable_token_count_24h as fifa_profitable_token_count_24h,
                  fifa.losing_token_count_24h as fifa_losing_token_count_24h,
                  fifa.profitable_token_count_7d as fifa_profitable_token_count_7d,
                  fifa.losing_token_count_7d as fifa_losing_token_count_7d,
                  fifa.equity_now as fifa_equity_now,
                  fifa.total_pnl_roi as fifa_pnl_roi,
                  fifa.event_count as fifa_event_count,
                  fifa.market_count as fifa_market_count,
                  fifa.token_count as fifa_token_count,
                  fifa.open_position_count as fifa_open_position_count,
                  fifa.open_position_value_now as fifa_open_position_value_now,
                  fifa.first_trade_at as fifa_first_trade_at,
                  fifa.last_trade_at as fifa_last_trade_at,
                  fifa.latest_action as fifa_latest_action,
                  fifa.data_quality as fifa_data_quality,
                  ifNull(trade_max.max_single_trade_notional, 0.0)
                    as fifa_max_single_trade_notional,
                  fifa.updated_at as fifa_updated_at
                from mart_wallet_fifa_24h_pnl as fifa final
                left join trade_max on fifa.user_address = trade_max.user_address
                where {" and ".join(where)}
                format JSONEachRow
            """
            fifa_rows = rows_json(self.clickhouse.query_text(fifa_sql))
        else:
            fifa_rows = []

        fifa_by_address = {row.get("user_address"): row for row in fifa_rows}
        wallets: list[dict[str, Any]] = []
        for address, polycop_wallet in polycop_by_address.items():
            fifa = fifa_by_address.get(address)
            if not fifa:
                continue
            metrics = polycop_wallet.get("metrics") if isinstance(polycop_wallet.get("metrics"), dict) else {}
            wallets.append(
                {
                    "address": address,
                    "user_address": address,
                    "polycop_rank": polycop_wallet.get("rank"),
                    "user_name": polycop_wallet.get("user_name"),
                    "x_name": polycop_wallet.get("x_name"),
                    "profile_image": polycop_wallet.get("profile_image"),
                    "ai_score": polycop_wallet.get("ai_score"),
                    "source_score": polycop_wallet.get("source_score"),
                    "segments": polycop_wallet.get("segments") or [],
                    "primary_segment": polycop_wallet.get("primary_segment"),
                    "reasons": polycop_wallet.get("reasons") or [],
                    "polycop_metrics": metrics,
                    **fifa,
                }
            )
        wallets.sort(
            key=lambda wallet: (
                float(wallet.get("ai_score") or 0.0),
                float(wallet.get("fifa_traded_notional") or 0.0),
                float(wallet.get("fifa_equity_now") or 0.0),
            ),
            reverse=True,
        )
        for index, wallet in enumerate(wallets, start=1):
            wallet["rank"] = index

        total = len(wallets)
        active_wallet_count_24h = sum(
            1 for wallet in wallets if int_value(wallet.get("fifa_trade_count_24h")) > 0
        )
        nonzero_pnl_wallet_count_24h = sum(
            1 for wallet in wallets if float_value(wallet.get("fifa_pnl_24h")) != 0.0
        )
        return {
            "status": row.get("status") or "unknown",
            "source": "polycop_fifa",
            "cache": {
                "source": "postgres",
                "hit": True,
                "cache_key": row.get("cache_key"),
                "refreshed_at": row.get("refreshed_at"),
                "generated_at": row.get("generated_at"),
                "age_seconds": row.get("age_seconds"),
                "trigger_reason": row.get("trigger_reason"),
                "error": row.get("error"),
            },
            "summary": {
                "polycop_wallet_count": len(polycop_wallets),
                "candidate_wallet_count": len(addresses),
                "fifa_wallet_count": total,
                "active_wallets_24h": active_wallet_count_24h,
                "nonzero_pnl_wallets_24h": nonzero_pnl_wallet_count_24h,
                "returned_wallet_count": len(wallets[offset : offset + limit]),
            },
            "parameters": {
                "segment": segment,
                "limit": limit,
                "offset": offset,
                "candidate_limit": candidate_limit,
                "min_ai_score": min_ai_score,
                "min_fifa_notional": min_fifa_notional,
                "min_fifa_events": min_fifa_events,
                "positive_fifa": require_positive_fifa,
                "active_24h": active_24h_only,
                "data_quality": data_quality,
                "source_parameters": row.get("parameters") or {},
            },
            "segment": segment,
            "total": total,
            "limit": limit,
            "offset": offset,
            "wallets": wallets[offset : offset + limit],
        }

    def wallet_detail(self, query: dict[str, list[str]]) -> dict[str, Any] | None:
        user = param(query, "user").lower()
        if not user:
            return None
        if truthy_param(query, "refresh"):
            self.enqueue_wallet_refresh(user)
        position_limit = int_param(query, "position_limit", 50, maximum=500)
        activity_limit = int_param(query, "activity_limit", 50, maximum=200)
        pnl_points_limit = int_param(query, "pnl_points_limit", 180, maximum=2000)
        position_scope = param(query, "position_scope", "all").lower()
        position_sort = param(query, "position_sort", "current_value").lower()
        detail_scope = wallet_detail_scope(param(query, "scope", "all"))

        if detail_scope == "fifa":
            return self.wallet_detail_fifa(
                user,
                position_limit=position_limit,
                activity_limit=activity_limit,
                pnl_points_limit=pnl_points_limit,
                position_scope=position_scope,
                position_sort=position_sort,
            )

        if truthy_param(query, "live"):
            live_detail = self.wallet_detail_live(
                user,
                position_limit=position_limit,
                activity_limit=activity_limit,
                pnl_points_limit=pnl_points_limit,
                position_scope=position_scope,
                position_sort=position_sort,
            )
            if live_detail is not None:
                return live_detail

        if truthy_param(query, "realtime"):
            return self.wallet_detail_realtime(
                user,
                position_limit=position_limit,
                activity_limit=activity_limit,
                pnl_points_limit=pnl_points_limit,
                position_scope=position_scope,
                position_sort=position_sort,
            )

        portfolio = self.wallet_latest_portfolio_snapshot(user)
        pnl = self.wallet_latest_pnl_snapshot(user)
        activity_summary = self.wallet_activity_summary(user)
        activity_by_type = self.wallet_activity_by_type(user)
        full_recent_activity = self.wallet_recent_activity(user, max(activity_limit, 500))
        activity_positions = wallet_closed_positions_from_activity(
            full_recent_activity,
            include_open=True,
        )
        closed_positions = [
            position for position in activity_positions if position.get("is_settled_or_redeemable")
        ]
        recent_activity = full_recent_activity[:activity_limit]
        reputation = self.wallet_detail_reputation(user)
        detail = self.wallet_detail_response(
            user,
            portfolio=portfolio,
            pnl=pnl,
            activity_summary=activity_summary,
            activity_by_type=activity_by_type,
            recent_activity=recent_activity,
            reputation=reputation,
            closed_positions=closed_positions,
            activity_positions=activity_positions,
            position_limit=position_limit,
            activity_limit=activity_limit,
            pnl_points_limit=pnl_points_limit,
            position_scope=position_scope,
            position_sort=position_sort,
            data_source="snapshot",
            data_scope="all",
        )
        return detail

    def wallet_detail_fifa(
        self,
        user: str,
        *,
        position_limit: int,
        activity_limit: int,
        pnl_points_limit: int,
        position_scope: str,
        position_sort: str,
    ) -> dict[str, Any] | None:
        summary = self.wallet_fifa_summary(user)
        activity_summary = self.wallet_fifa_activity_summary(user)
        positions = self.wallet_fifa_positions(user)
        recent_activity = self.wallet_fifa_recent_activity(user, max(activity_limit, 500))
        if not any((summary, positions, recent_activity)):
            return None

        captured_at = (
            (summary or {}).get("updated_at")
            or (activity_summary or {}).get("last_activity_at")
            or datetime.now(UTC)
        )
        total_pnl = value_or_fallback(
            (summary or {}).get("equity_now"),
            sum(float_value(position.get("cash_pnl")) for position in positions),
        )
        positions_value = value_or_fallback(
            (summary or {}).get("open_position_value_now"),
            sum(
                float_value(position.get("current_value"))
                for position in positions
                if position.get("is_open")
            ),
        )
        portfolio = {
            "user_address": user,
            "captured_at": captured_at,
            "position_count": sum(1 for position in positions if position.get("is_open")),
            "positions_value": float_value(positions_value),
            "portfolio_value": float_value(positions_value),
            "available_balance": 0.0,
            "total_pnl": float_value(total_pnl),
            "raw_json": "",
        }
        pnl = wallet_fifa_pnl_snapshot(user, summary)
        if pnl is None:
            pnl = {
                "user_address": user,
                "captured_at": captured_at,
                "total_pnl": float_value(total_pnl),
                "raw_json": "",
            }
        activity_by_type = wallet_fifa_activity_by_type(activity_summary)
        closed_positions = [
            position for position in positions if position.get("is_settled_or_redeemable")
        ]
        detail = self.wallet_detail_response(
            user,
            portfolio=portfolio,
            pnl=pnl,
            activity_summary=activity_summary,
            activity_by_type=activity_by_type,
            recent_activity=recent_activity[:activity_limit],
            reputation=None,
            closed_positions=closed_positions,
            activity_positions=positions,
            positions_override=positions,
            position_limit=position_limit,
            activity_limit=activity_limit,
            pnl_points_limit=pnl_points_limit,
            position_scope=position_scope,
            position_sort=position_sort,
            data_source="fifa",
            data_scope="fifa",
        )
        if detail is not None and summary:
            wallet = detail.get("wallet")
            if isinstance(wallet, dict):
                wallet.update(
                    {
                        "total_pnl": float_value(summary.get("total_pnl")),
                        "total_pnl_roi": float_value(summary.get("total_pnl_roi")),
                        "pnl_24h": float_value(summary.get("pnl_24h")),
                        "pnl_roi_24h": float_value(summary.get("pnl_roi_24h")),
                        "pnl_7d": float_value(summary.get("pnl_7d")),
                        "pnl_roi_7d": float_value(summary.get("pnl_roi_7d")),
                        "win_rate": float_value(summary.get("win_rate")),
                        "win_rate_24h": float_value(summary.get("win_rate_24h")),
                        "win_rate_7d": float_value(summary.get("win_rate_7d")),
                        "profitable_token_count": int_value(
                            summary.get("profitable_token_count")
                        ),
                        "losing_token_count": int_value(summary.get("losing_token_count")),
                        "profitable_token_count_24h": int_value(
                            summary.get("profitable_token_count_24h")
                        ),
                        "losing_token_count_24h": int_value(
                            summary.get("losing_token_count_24h")
                        ),
                        "profitable_token_count_7d": int_value(
                            summary.get("profitable_token_count_7d")
                        ),
                        "losing_token_count_7d": int_value(
                            summary.get("losing_token_count_7d")
                        ),
                    }
                )
        return detail

    def wallet_detail_live(
        self,
        user: str,
        *,
        position_limit: int,
        activity_limit: int,
        pnl_points_limit: int,
        position_scope: str,
        position_sort: str,
    ) -> dict[str, Any] | None:
        if self.settings is None:
            return None
        client = PolymarketClient(self.settings)
        live_activity_limit = max(activity_limit, 500)
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                positions_future = executor.submit(client.data_positions, user=user)
                value_future = executor.submit(client.data_value, user=user)
                pnl_future = executor.submit(client.user_pnl, user=user, interval="all", fidelity="1h")
                activity_future = executor.submit(
                    client.data_activity,
                    user=user,
                    limit=live_activity_limit,
                    offset=0,
                )
                balance_future = executor.submit(self.live_pusd_balance, user)
                positions_page = positions_future.result()
                value_page = value_future.result()
                pnl_page = pnl_future.result()
                activity_page = activity_future.result()
                available_balance = balance_future.result()
        except Exception:
            return None

        captured_at = datetime.now(UTC)
        pnl_points_payload = (
            pnl_page.response.body if isinstance(pnl_page.response.body, list) else pnl_page.items
        )
        portfolio_rows = wallet_portfolio_rows(
            {
                "user": user,
                "positions": positions_page.items,
                "value": value_page.items,
                "pnl": pnl_points_payload,
                "availableBalance": available_balance,
            },
            captured_at,
        )
        pnl_rows = wallet_pnl_snapshot_rows(
            {"user": user, "points": pnl_points_payload},
            captured_at,
        )
        live_activity_rows = activity_rows(activity_page.items, captured_at)
        live_activity_rows = [
            {**row, "user_address": str(row.get("user_address") or user).lower()}
            for row in live_activity_rows
        ]
        live_activity_rows = merge_wallet_rtds_activity_rows(
            live_activity_rows,
            self.wallet_rtds_activity_rows(user, captured_at),
        )
        live_activity_rows = dedupe_wallet_activity_rows(live_activity_rows)

        portfolio = json_ready_row(portfolio_rows[0]) if portfolio_rows else None
        pnl = json_ready_row(pnl_rows[0]) if pnl_rows else None
        activity_summary = summarize_activity_rows(user, live_activity_rows)
        activity_by_type = summarize_activity_rows_by_type(live_activity_rows)
        activity_positions = wallet_closed_positions_from_activity(
            live_activity_rows,
            include_open=True,
        )
        closed_positions = [
            position for position in activity_positions if position.get("is_settled_or_redeemable")
        ]
        recent_activity = recent_activity_from_rows(live_activity_rows, activity_limit)
        try:
            reputation = self.wallet_detail_reputation(user)
        except Exception:
            reputation = None

        if not any((portfolio, pnl, live_activity_rows)):
            return None
        return self.wallet_detail_response(
            user,
            portfolio=portfolio,
            pnl=pnl,
            activity_summary=activity_summary,
            activity_by_type=activity_by_type,
            recent_activity=recent_activity,
            reputation=reputation,
            closed_positions=closed_positions,
            activity_positions=activity_positions,
            position_limit=position_limit,
            activity_limit=activity_limit,
            pnl_points_limit=pnl_points_limit,
            position_scope=position_scope,
            position_sort=position_sort,
            data_source="live",
            data_scope="all",
        )

    def wallet_detail_realtime(
        self,
        user: str,
        *,
        position_limit: int,
        activity_limit: int,
        pnl_points_limit: int,
        position_scope: str,
        position_sort: str,
    ) -> dict[str, Any]:
        captured_at = datetime.now(UTC)
        realtime_rows = dedupe_wallet_activity_rows(
            self.wallet_rtds_activity_rows(user, captured_at, limit=max(activity_limit, 100))
        )
        activity_summary = summarize_activity_rows(user, realtime_rows)
        activity_by_type = summarize_activity_rows_by_type(realtime_rows)
        activity_positions = wallet_closed_positions_from_activity(
            realtime_rows,
            include_open=True,
        )
        closed_positions = [
            position for position in activity_positions if position.get("is_settled_or_redeemable")
        ]
        recent_activity = recent_activity_from_rows(realtime_rows, activity_limit)
        detail = self.wallet_detail_response(
            user,
            portfolio=None,
            pnl=None,
            activity_summary=activity_summary,
            activity_by_type=activity_by_type,
            recent_activity=recent_activity,
            reputation=None,
            closed_positions=closed_positions,
            activity_positions=activity_positions,
            position_limit=position_limit,
            activity_limit=activity_limit,
            pnl_points_limit=pnl_points_limit,
            position_scope=position_scope,
            position_sort=position_sort,
            data_source="realtime",
            data_scope="all",
        )
        detail["wallet"]["data_status"] = "ok" if realtime_rows else "no_realtime_activity"
        detail["wallet"]["data_freshness_status"] = "ok" if realtime_rows else "missing"
        detail["wallet"]["realtime_activity_count"] = len(realtime_rows)
        detail["wallet"]["realtime_last_activity_at"] = activity_summary.get("last_activity_at")
        detail["realtime"] = {
            "enabled": True,
            "source": "polymarket-rtds",
            "cache_status": "hit" if realtime_rows else "miss",
            "activity_count": len(realtime_rows),
            "activity_limit": activity_limit,
            "captured_at": api_datetime(captured_at),
            "last_activity_at": activity_summary.get("last_activity_at"),
        }
        return detail

    def live_pusd_balance(self, user: str) -> float:
        if self.settings is None or not self.settings.polygon_rpc_url:
            return 0.0
        try:
            raw = PolygonRpcClient(self.settings).eth_call(
                to=PUSD_ADDRESS,
                data=erc20_balance_of_data(user),
            )
        except Exception:
            return 0.0
        return int(raw, 16) / PUSD_DECIMALS if raw and raw != "0x" else 0.0

    def wallet_rtds_activity_rows(
        self,
        user: str,
        captured_at: datetime,
        *,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        if self.settings is None:
            return []
        messages = load_recent_wallet_trade_messages(
            self.settings.state_dir,
            user,
            limit=limit,
        )
        if len(messages) < limit:
            seen_keys = {rtds_message_key(message) for message in messages}
            for message in load_recent_trade_messages(
                self.settings.state_dir,
                limit=max(limit, 1_000),
            ):
                key = rtds_message_key(message)
                if key in seen_keys:
                    continue
                trade = message.get("trade")
                if not isinstance(trade, dict):
                    continue
                if str(trade.get("user_address") or "").lower() != user.lower():
                    continue
                messages.append(message)
                seen_keys.add(key)
                if len(messages) >= limit:
                    break
        return wallet_rtds_activity_rows(
            user,
            messages,
            captured_at,
        )

    def wallet_detail_response(
        self,
        user: str,
        *,
        portfolio: dict[str, Any] | None,
        pnl: dict[str, Any] | None,
        activity_summary: dict[str, Any] | None,
        activity_by_type: list[dict[str, Any]],
        recent_activity: list[dict[str, Any]],
        reputation: dict[str, Any] | None,
        position_limit: int,
        activity_limit: int,
        pnl_points_limit: int,
        position_scope: str,
        position_sort: str,
        data_source: str,
        data_scope: str = "all",
        closed_positions: list[dict[str, Any]] | None = None,
        activity_positions: list[dict[str, Any]] | None = None,
        positions_override: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        if not any((portfolio, pnl, activity_summary, activity_by_type, recent_activity)):
            return None

        positions = (
            list(positions_override)
            if positions_override is not None
            else wallet_positions_from_snapshot(portfolio)
        )
        activity_positions = activity_positions or wallet_closed_positions_from_activity(
            recent_activity,
            include_open=True,
        )
        closed_positions = closed_positions or [
            position for position in activity_positions if position.get("is_settled_or_redeemable")
        ]
        closed_positions = merge_closed_positions(
            closed_positions,
            positions,
            all_activity_positions=activity_positions,
        )
        position_summary = summarize_wallet_positions(positions)
        filtered_positions = filter_wallet_positions(positions, position_scope)
        sorted_positions = sort_wallet_positions(filtered_positions, position_sort)
        all_pnl_points = wallet_pnl_points_from_snapshot(pnl, 10_000)
        risk_metrics = wallet_risk_metrics(closed_positions, all_pnl_points)
        performance_metrics = wallet_performance_metrics(closed_positions)
        pnl_points = all_pnl_points[-pnl_points_limit:]
        pnl_7d = wallet_pnl_delta(all_pnl_points, days=7)
        latest_total_pnl = (
            float_value(pnl.get("total_pnl"))
            if pnl
            else float_value((portfolio or {}).get("total_pnl"))
        )
        pnl_captured_at = (pnl or {}).get("captured_at")
        portfolio_captured_at = (portfolio or {}).get("captured_at")
        pnl_lag_minutes = datetime_lag_minutes(pnl_captured_at, portfolio_captured_at)

        return {
            "scope": data_scope,
            "wallet": {
                "user_address": user,
                "scope": data_scope,
                "data_status": "ok",
                "data_source": data_source,
                "data_freshness_status": wallet_data_freshness_status(
                    pnl_captured_at,
                    portfolio_captured_at,
                    pnl_lag_minutes,
                ),
                "latest_total_pnl": latest_total_pnl,
                "pnl_captured_at": pnl_captured_at,
                "portfolio_captured_at": portfolio_captured_at,
                "pnl_lag_minutes": pnl_lag_minutes,
                "position_count": int_value((portfolio or {}).get("position_count")),
                "positions_value": float_value((portfolio or {}).get("positions_value")),
                "portfolio_value": float_value((portfolio or {}).get("portfolio_value")),
                "available_balance": float_value((portfolio or {}).get("available_balance")),
                "cash": float_value((portfolio or {}).get("available_balance")),
                "portfolio_total_pnl": float_value((portfolio or {}).get("total_pnl")),
                "current_pnl": float_value(position_summary.get("open_cash_pnl")),
                "position_cash_pnl": float_value(position_summary.get("cash_pnl")),
                "pnl_7d": pnl_7d,
                "win_rate": value_or_fallback(
                    risk_metrics.get("win_rate"),
                    (reputation or {}).get("win_rate"),
                ),
                "completed_event_count": int_value(
                    value_or_fallback(
                        risk_metrics.get("completed_event_count"),
                        (reputation or {}).get("completed_event_count"),
                    )
                ),
                "profitable_event_count": int_value(
                    value_or_fallback(
                        risk_metrics.get("profitable_event_count"),
                        (reputation or {}).get("profitable_event_count"),
                    )
                ),
                "losing_event_count": int_value(
                    value_or_fallback(
                        risk_metrics.get("losing_event_count"),
                        (reputation or {}).get("losing_event_count"),
                    )
                ),
                "realized_pnl": value_or_fallback(
                    risk_metrics.get("realized_pnl"),
                    (reputation or {}).get("realized_pnl"),
                ),
                "avg_event_roi": value_or_fallback(
                    risk_metrics.get("avg_event_roi"),
                    (reputation or {}).get("avg_event_roi"),
                ),
                "avg_bet": float_value((activity_summary or {}).get("avg_bet")),
                "trade_volume_7d": float_value(
                    (activity_summary or {}).get("traded_notional_7d")
                ),
                "trade_count_7d": int_value(
                    (activity_summary or {}).get("trade_activity_count_7d")
                ),
                "activity_count": int_value((activity_summary or {}).get("activity_count")),
                "trade_activity_count": int_value(
                    (activity_summary or {}).get("trade_activity_count")
                ),
                "first_activity_at": (activity_summary or {}).get("first_activity_at"),
                "last_activity_at": (activity_summary or {}).get("last_activity_at"),
            },
            "position_summary": position_summary,
            "risk_metrics": risk_metrics,
            "performance_metrics": performance_metrics,
            "positions": sorted_positions[:position_limit],
            "closed_positions": closed_positions[:position_limit],
            "positions_returned": min(len(sorted_positions), position_limit),
            "positions_available": len(sorted_positions),
            "closed_positions_returned": min(len(closed_positions), position_limit),
            "closed_positions_available": len(closed_positions),
            "position_scope": position_scope,
            "position_sort": position_sort,
            "pnl_points": pnl_points,
            "pnl_point_count": len(pnl_points),
            "activity_summary": activity_summary or empty_wallet_activity_summary(user),
            "activity_by_type": activity_by_type,
            "reputation": reputation or empty_wallet_reputation(user),
            "recent_activity": recent_activity,
        }

    def wallet_fifa_summary(self, user: str) -> dict[str, Any] | None:
        sql = f"""
            select
              user_address,
              trade_count,
              buy_count,
              sell_count,
              traded_size,
              traded_notional,
              buy_notional,
              sell_notional,
              trade_count_24h,
              buy_notional_24h,
              sell_notional_24h,
              traded_notional_24h,
              net_notional_24h,
              event_count,
              market_count,
              event_count_24h,
              market_count_24h,
              token_count,
              open_position_count,
              open_position_value_now,
              open_position_value_24h_ago,
              open_position_value_7d_ago,
              equity_now,
              equity_24h_ago,
              equity_7d_ago,
              total_pnl,
              total_pnl_roi,
              pnl_24h,
              pnl_base_24h,
              pnl_roi_24h,
              pnl_7d,
              pnl_base_7d,
              pnl_roi_7d,
              profitable_token_count,
              losing_token_count,
              win_rate,
              profitable_token_count_24h,
              losing_token_count_24h,
              win_rate_24h,
              profitable_token_count_7d,
              losing_token_count_7d,
              win_rate_7d,
              first_trade_at,
              last_trade_at,
              latest_action,
              missing_mark_count,
              negative_position_count,
              data_quality,
              updated_at
            from mart_wallet_fifa_24h_pnl final
            where user_address = {ch_string(user)}
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def wallet_fifa_activity_summary(self, user: str) -> dict[str, Any]:
        sql = f"""
            select
              user_address,
              count() as activity_count,
              count() as trade_activity_count,
              countIf(side = 'BUY') as buy_count,
              countIf(side = 'SELL') as sell_count,
              sum(size) as traded_size,
              sum(notional) as traded_notional,
              sumIf(notional, side = 'BUY') as buy_notional,
              sumIf(notional, side = 'SELL') as sell_notional,
              countIf(timestamp >= now64(3) - interval 24 hour) as activity_count_24h,
              countIf(timestamp >= now64(3) - interval 24 hour) as trade_activity_count_24h,
              sumIf(notional, timestamp >= now64(3) - interval 24 hour) as traded_notional_24h,
              countIf(timestamp >= now64(3) - interval 7 day) as trade_activity_count_7d,
              sumIf(notional, timestamp >= now64(3) - interval 7 day) as traded_notional_7d,
              avgIf(notional, notional > 0) as avg_bet,
              'TRADE' as latest_activity_type,
              argMax(side, timestamp) as latest_side,
              min(timestamp) as first_activity_at,
              max(timestamp) as last_activity_at
            from mart_fifa_trade final
            where user_address = {ch_string(user)}
              and timestamp <= now64(3) + interval 10 minute
            group by user_address
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return normalize_wallet_activity_summary(rows[0]) if rows else empty_wallet_activity_summary(user)

    def wallet_fifa_recent_activity(self, user: str, limit: int) -> list[dict[str, Any]]:
        sql = f"""
            select
              trades.timestamp as timestamp,
              'TRADE' as activity_type,
              trades.side as side,
              trades.price as price,
              trades.size as size,
              trades.notional as notional,
              trades.condition_id as condition_id,
              trades.token_id as token_id,
              '' as transaction_hash,
              ifNull(markets.question, '') as title,
              ifNull(markets.slug, '') as slug,
              ifNull(events.slug, '') as event_slug,
              ifNull(tokens.outcome, '') as outcome,
              'fifa-mart' as source
            from mart_fifa_trade as trades final
            left join dim_market as markets final
              on trades.condition_id = markets.condition_id
            left join dim_event as events final
              on markets.event_id = events.event_id
            left join dim_outcome_token as tokens final
              on trades.token_id = tokens.token_id
            where trades.user_address = {ch_string(user)}
              and trades.timestamp <= now64(3) + interval 10 minute
            order by trades.timestamp desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def wallet_fifa_positions(self, user: str) -> list[dict[str, Any]]:
        sql = f"""
            with
              now64(3) as as_of,
              user_tokens as
              (
                select
                  token_id,
                  anyLast(market_id) as market_id
                from mart_fifa_trade final
                where user_address = {ch_string(user)}
                  and token_id != ''
                group by token_id
              ),
              final_prices as
              (
                select
                  tokens.token_id as token_id,
                  resolved.final_price as final_price
                from
                (
                  select
                    market_id,
                    price_index - 1 as outcome_index,
                    toFloat64OrZero(JSONExtractString(price_raw)) as final_price
                  from
                  (
                    select
                      market_id,
                      JSONExtractString(raw_json, 'outcomePrices') as prices
                    from dim_market as markets final
                    where market_id in (select market_id from user_tokens)
                      and closed = true
                      and JSONExtractString(raw_json, 'outcomePrices') != ''
                  )
                  array join
                    JSONExtractArrayRaw(prices) as price_raw,
                    arrayEnumerate(JSONExtractArrayRaw(prices)) as price_index
                ) as resolved
                inner join dim_outcome_token as tokens final
                  on tokens.market_id = resolved.market_id
                 and tokens.outcome_index = resolved.outcome_index
              ),
              marks as
              (
                select
                  token_ids.token_id as token_id,
                  multiIf(
                    final_prices.final_price is not null,
                      cast(final_prices.final_price, 'Nullable(Float64)'),
                    book_marks.book_count > 0,
                      cast(
                        (book_marks.book_best_bid + book_marks.book_best_ask) / 2,
                        'Nullable(Float64)'
                      ),
                    price_marks.price_count > 0,
                      cast(price_marks.price, 'Nullable(Float64)'),
                    cast(null, 'Nullable(Float64)')
                  ) as mark_price
                from user_tokens as token_ids
                left join final_prices on token_ids.token_id = final_prices.token_id
                left join
                (
                  select
                    token_id,
                    argMax(price, timestamp) as price,
                    count() as price_count
                  from fact_price_history
                  where token_id in (select token_id from user_tokens)
                    and timestamp <= as_of + toIntervalMinute(10)
                  group by token_id
                ) as price_marks on token_ids.token_id = price_marks.token_id
                left join
                (
                  select
                    token_id,
                    argMax(best_bid, captured_at) as book_best_bid,
                    argMax(best_ask, captured_at) as book_best_ask,
                    count() as book_count
                  from fact_orderbook_snapshot
                  where token_id in (select token_id from user_tokens)
                    and best_bid is not null
                    and best_ask is not null
                    and captured_at <= as_of + toIntervalMinute(10)
                  group by token_id
                ) as book_marks on token_ids.token_id = book_marks.token_id
              )
            select
              position_rows.token_id as asset,
              position_rows.condition_id as condition_id,
              ifNull(markets.question, '') as title,
              ifNull(markets.slug, '') as slug,
              ifNull(markets.event_id, '') as event_id,
              ifNull(events.slug, '') as event_slug,
              ifNull(tokens.outcome, '') as outcome,
              position_rows.position_size as size,
              position_rows.avg_price as avg_price,
              ifNull(marks.mark_price, 0.0) as cur_price,
              position_rows.buy_notional as initial_value,
              if(position_rows.position_size > 0.000001, position_rows.position_size * ifNull(marks.mark_price, 0.0), 0.0)
                as current_value,
              position_rows.sell_notional,
              position_rows.buy_notional,
              position_rows.buy_size,
              position_rows.sell_size,
              position_rows.buy_count,
              position_rows.sell_count,
              position_rows.trade_count,
              position_rows.first_activity_at,
              position_rows.last_activity_at,
              ifNull(markets.closed, false) as market_closed,
              if(marks.mark_price is null, true, false) as missing_mark
            from
            (
              select
                condition_id,
                token_id,
                anyLast(market_id) as market_id,
                count() as trade_count,
                countIf(side = 'BUY') as buy_count,
                countIf(side = 'SELL') as sell_count,
                sumIf(size, side = 'BUY') as buy_size,
                sumIf(size, side = 'SELL') as sell_size,
                sumIf(notional, side = 'BUY') as buy_notional,
                sumIf(notional, side = 'SELL') as sell_notional,
                sum(if(side = 'BUY', size, -size)) as position_size,
                if(sumIf(size, side = 'BUY') = 0, 0.0, sumIf(notional, side = 'BUY') / sumIf(size, side = 'BUY'))
                  as avg_price,
                min(timestamp) as first_activity_at,
                max(timestamp) as last_activity_at
              from mart_fifa_trade final
              where user_address = {ch_string(user)}
                and timestamp <= as_of + toIntervalMinute(10)
              group by condition_id, token_id
            ) as position_rows
            left join marks on position_rows.token_id = marks.token_id
            left join dim_market as markets final
              on position_rows.condition_id = markets.condition_id
            left join dim_event as events final
              on markets.event_id = events.event_id
            left join dim_outcome_token as tokens final
              on position_rows.token_id = tokens.token_id
            order by abs(position_rows.sell_notional + if(position_rows.position_size > 0.000001, position_rows.position_size * ifNull(marks.mark_price, 0.0), 0.0) - position_rows.buy_notional) desc
            limit 1000
            format JSONEachRow
        """
        return [normalize_fifa_position(row) for row in rows_json(self.clickhouse.query_text(sql))]

    def enqueue_wallet_refresh(self, user: str) -> dict[str, Any]:
        if self.settings is None:
            return {"added": 0, "status": "task_store_unavailable"}
        refresh_run = datetime.now(UTC).replace(microsecond=0).isoformat()
        params = {
            "user": user,
            "page_limit": 500,
            "max_pages": 2,
            "resume": False,
            "_refresh_run": refresh_run,
            "_requeue_done": True,
        }
        tasks = [
            Task(
                kind="wallet-trades",
                params={**params, "market": None, "event_id": None},
                priority=-2,
            ),
            Task(kind="wallet-activity", params=dict(params), priority=-2),
            Task(
                kind="wallet-portfolio",
                params={"user": user, "_refresh_run": refresh_run, "_requeue_done": True},
                priority=-2,
            ),
            Task(
                kind="wallet-pnl",
                params={
                    "user": user,
                    "interval": "all",
                    "fidelity": "1d",
                    "_refresh_run": refresh_run,
                    "_requeue_done": True,
                },
                priority=-2,
            ),
        ]
        added = PostgresTaskStore(
            dsn=self.settings.postgres_dsn,
            node_id="api",
        ).add_many(tasks)
        return {"added": added, "refresh_run": refresh_run}

    def wallet_latest_portfolio_snapshot(self, user: str) -> dict[str, Any] | None:
        sql = f"""
            select
              user_address,
              captured_at,
              position_count,
              positions_value,
              portfolio_value,
              available_balance,
              total_pnl,
              raw_json
            from fact_wallet_portfolio_snapshot
            where user_address = {ch_string(user)}
            order by captured_at desc
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def wallet_latest_pnl_snapshot(self, user: str) -> dict[str, Any] | None:
        sql = f"""
            select
              user_address,
              captured_at,
              total_pnl,
              raw_json
            from fact_wallet_pnl_snapshot
            where user_address = {ch_string(user)}
            order by captured_at desc
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def wallet_activity_summary(self, user: str) -> dict[str, Any] | None:
        sql = f"""
            select
              user_address,
              count() as activity_count,
              countIf(activity_type = 'TRADE') as trade_activity_count,
              countIf(activity_type = 'TRADE' and side = 'BUY') as buy_count,
              countIf(activity_type = 'TRADE' and side = 'SELL') as sell_count,
              sumIf(size, activity_type = 'TRADE') as traded_size,
              sumIf(notional, activity_type = 'TRADE') as traded_notional,
              sumIf(notional, activity_type = 'TRADE' and side = 'BUY') as buy_notional,
              sumIf(notional, activity_type = 'TRADE' and side = 'SELL') as sell_notional,
              countIf(timestamp >= now64(3) - interval 24 hour) as activity_count_24h,
              countIf(activity_type = 'TRADE' and timestamp >= now64(3) - interval 24 hour)
                as trade_activity_count_24h,
              sumIf(
                notional,
                activity_type = 'TRADE' and timestamp >= now64(3) - interval 24 hour
              ) as traded_notional_24h,
              countIf(activity_type = 'TRADE' and timestamp >= now64(3) - interval 7 day)
                as trade_activity_count_7d,
              sumIf(
                notional,
                activity_type = 'TRADE' and timestamp >= now64(3) - interval 7 day
              ) as traded_notional_7d,
              avgIf(notional, activity_type = 'TRADE' and notional > 0) as avg_bet,
              argMax(activity_type, timestamp) as latest_activity_type,
              argMax(side, timestamp) as latest_side,
              min(timestamp) as first_activity_at,
              max(timestamp) as last_activity_at
            from
            (
              select
                raw_user_address as user_address,
                trade_key,
                argMax(raw_timestamp, raw_ingested_at) as timestamp,
                argMax(raw_activity_type, raw_ingested_at) as activity_type,
                argMax(raw_side, raw_ingested_at) as side,
                argMax(raw_size, raw_ingested_at) as size,
                argMax(raw_notional, raw_ingested_at) as notional
              from
              (
                select
                  lower(user_address) as raw_user_address,
                  if(
                    transaction_hash != '',
                    concat(
                      transaction_hash, '|', condition_id, '|', token_id, '|',
                      side, '|', toString(price), '|', toString(size), '|',
                      toString(timestamp)
                    ),
                    activity_id
                  ) as trade_key,
                  timestamp as raw_timestamp,
                  activity_type as raw_activity_type,
                  side as raw_side,
                  size as raw_size,
                  notional as raw_notional,
                  ingested_at as raw_ingested_at
                from fact_user_activity final
                where user_address = {ch_string(user)}
                  and timestamp <= now64(3) + interval 10 minute
              )
              group by raw_user_address, trade_key
            )
            group by user_address
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return normalize_wallet_activity_summary(rows[0]) if rows else None

    def wallet_detail_reputation(self, user: str) -> dict[str, Any] | None:
        sql = f"""
            select
              user_address,
              completed_event_count,
              profitable_event_count,
              losing_event_count,
              win_rate,
              realized_pnl,
              avg_event_roi,
              best_event_pnl,
              worst_event_pnl,
              active_position_count,
              active_event_count,
              active_unrealized_pnl_estimate,
              favorite_category,
              favorite_category_notional,
              first_trade_at,
              last_trade_at
            from mart_wallet_reputation final
            where user_address = {ch_string(user)}
            limit 1
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        return rows[0] if rows else None

    def wallet_activity_by_type(self, user: str) -> list[dict[str, Any]]:
        sql = f"""
            select
              activity_type,
              count() as count,
              sum(size) as size,
              sum(notional) as notional,
              min(timestamp) as first_activity_at,
              max(timestamp) as last_activity_at
            from
            (
              select
                raw_user_address as user_address,
                trade_key,
                argMax(raw_timestamp, raw_ingested_at) as timestamp,
                argMax(raw_activity_type, raw_ingested_at) as activity_type,
                argMax(raw_size, raw_ingested_at) as size,
                argMax(raw_notional, raw_ingested_at) as notional
              from
              (
                select
                  lower(user_address) as raw_user_address,
                  if(
                    transaction_hash != '',
                    concat(
                      transaction_hash, '|', condition_id, '|', token_id, '|',
                      side, '|', toString(price), '|', toString(size), '|',
                      toString(timestamp)
                    ),
                    activity_id
                  ) as trade_key,
                  timestamp as raw_timestamp,
                  activity_type as raw_activity_type,
                  size as raw_size,
                  notional as raw_notional,
                  ingested_at as raw_ingested_at
                from fact_user_activity final
                where user_address = {ch_string(user)}
                  and timestamp <= now64(3) + interval 10 minute
              )
              group by raw_user_address, trade_key
            )
            group by activity_type
            order by count desc, notional desc
            format JSONEachRow
        """
        return [normalize_wallet_activity_type(row) for row in rows_json(self.clickhouse.query_text(sql))]

    def wallet_recent_activity(self, user: str, limit: int) -> list[dict[str, Any]]:
        sql = f"""
            select
              timestamp,
              activity_type,
              side,
              price,
              size,
              notional,
              condition_id,
              token_id,
              transaction_hash,
              JSONExtractString(raw_json, 'title') as title,
              JSONExtractString(raw_json, 'slug') as slug,
              JSONExtractString(raw_json, 'eventSlug') as event_slug,
              JSONExtractString(raw_json, 'outcome') as outcome
            from
            (
              select
                argMax(raw_timestamp, raw_ingested_at) as timestamp,
                argMax(raw_activity_type, raw_ingested_at) as activity_type,
                argMax(raw_side, raw_ingested_at) as side,
                argMax(raw_price, raw_ingested_at) as price,
                argMax(raw_size, raw_ingested_at) as size,
                argMax(raw_notional, raw_ingested_at) as notional,
                argMax(raw_condition_id, raw_ingested_at) as condition_id,
                argMax(raw_token_id, raw_ingested_at) as token_id,
                argMax(raw_transaction_hash, raw_ingested_at) as transaction_hash,
                argMax(raw_json, raw_ingested_at) as raw_json
              from
              (
                select
                  if(
                    transaction_hash != '',
                    concat(
                      transaction_hash, '|', condition_id, '|', token_id, '|',
                      side, '|', toString(price), '|', toString(size), '|',
                      toString(timestamp)
                    ),
                    activity_id
                  ) as trade_key,
                  timestamp as raw_timestamp,
                  activity_type as raw_activity_type,
                  side as raw_side,
                  price as raw_price,
                  size as raw_size,
                  notional as raw_notional,
                  condition_id as raw_condition_id,
                  token_id as raw_token_id,
                  transaction_hash as raw_transaction_hash,
                  raw_json,
                  ingested_at as raw_ingested_at
                from fact_user_activity final
                where user_address = {ch_string(user)}
                  and timestamp <= now64(3) + interval 10 minute
              )
              group by trade_key
            )
            order by timestamp desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def wallet_live_positions(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        user = param(query, "user").lower()
        event_id = param(query, "event_id")
        limit = int_param(query, "limit", 50, maximum=500)
        where = "where 1 = 1"
        if user:
            where += f" and user_address = {ch_string(user)}"
        if event_id:
            where += f" and event_id = {ch_string(event_id)}"
        if not user and not event_id:
            where += " and 1 = 0"
        sql = f"""
            select
              event_id,
              market_id,
              condition_id,
              token_id,
              outcome,
              user_address,
              position_size,
              avg_entry_price,
              mark_price,
              mark_price_source,
              mark_price_at,
              current_value,
              unrealized_pnl_estimate,
              traded_notional,
              net_size_24h,
              net_notional_24h,
              latest_action,
              is_accumulating,
              data_quality,
              last_trade_at
            from mart_live_wallet_position final
            {where}
            order by abs(unrealized_pnl_estimate) desc, traded_notional desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(sql))

    def smart_money_activity(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        limit = int_param(query, "limit", 30, maximum=100)
        sql = f"""
            select
              positions.event_id,
              positions.market_id,
              positions.condition_id,
              positions.token_id,
              positions.outcome,
              positions.user_address,
              positions.position_size,
              positions.traded_notional,
              positions.unrealized_pnl_estimate,
              positions.net_size_24h,
              positions.net_notional_24h,
              positions.latest_action,
              positions.last_trade_at,
              ifNull(rep.win_rate, 0.0) as win_rate,
              ifNull(rep.realized_pnl, 0.0) as realized_pnl,
              ifNull(rep.completed_event_count, 0) as completed_event_count,
              ifNull(rep.favorite_category, '') as favorite_category
            from mart_live_wallet_position as positions final
            left join mart_wallet_reputation as rep final
              on positions.user_address = rep.user_address
            where positions.user_address != ''
            order by
              rep.realized_pnl desc,
              abs(positions.net_notional_24h) desc,
              positions.traded_notional desc
            limit {limit}
            format JSONEachRow
        """
        rows = rows_json(self.clickhouse.query_text(sql))
        if rows:
            return rows
        fallback_sql = f"""
            select
              '' as event_id,
              '' as market_id,
              '' as condition_id,
              '' as token_id,
              '' as outcome,
              user_address,
              0.0 as position_size,
              traded_notional,
              total_pnl as unrealized_pnl_estimate,
              0.0 as net_size_24h,
              0.0 as net_notional_24h,
              'PROFILE' as latest_action,
              last_trade_at,
              0.0 as win_rate,
              realized_pnl,
              0 as completed_event_count,
              '' as favorite_category
            from mart_trader_profile final
            where user_address != ''
            order by traded_notional desc, trade_count desc
            limit {limit}
            format JSONEachRow
        """
        return rows_json(self.clickhouse.query_text(fallback_sql))

    def worldcup_wallet_rankings(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = int_param(query, "limit", 100, maximum=500)
        include_details = param(query, "details").lower() in ("1", "true", "yes")
        refresh = param(query, "refresh").lower() in ("1", "true", "yes")
        cache_ttl_seconds = bounded_int_param(
            query,
            "cache_ttl_seconds",
            60,
            minimum=0,
            maximum=3600,
        )
        expand_variants = param(query, "expand_variants", "true").lower() not in (
            "0",
            "false",
            "no",
        )
        supplied_slugs = parse_slug_values(query, "slug", "slugs", "event_slug", "event_slugs")
        input_slugs = worldcup_event_slugs_for_scope(
            supplied_slugs or None,
            date_from=param(query, "date_from"),
            date_to=param(query, "date_to"),
            expand_variants=expand_variants,
        )
        cache_key = (tuple(input_slugs), limit)
        cached = self._worldcup_wallet_rankings_cache.get(cache_key)
        if (
            not refresh
            and cache_ttl_seconds > 0
            and cached is not None
            and time.time() - cached[0] <= cache_ttl_seconds
        ):
            output = cached[1]
        else:
            output = self.cached_worldcup_wallet_rankings(query, input_slugs, limit)
            if output is None:
                output = worldcup_wallet_rankings(
                    self.worldcup_clickhouse(),
                    input_slugs,
                    rank_limit=limit,
                )
            if cache_ttl_seconds > 0:
                self._worldcup_wallet_rankings_cache[cache_key] = (time.time(), output)
        return output if include_details else compact_worldcup_wallet_rankings(output)

    def cached_worldcup_wallet_rankings(
        self,
        query: dict[str, list[str]],
        input_slugs: list[str],
        limit: int,
    ) -> dict[str, Any] | None:
        if param(query, "refresh").lower() in ("1", "true", "yes"):
            return None
        if parse_slug_values(query, "slug", "slugs", "event_slug", "event_slugs"):
            return None
        if param(query, "date_from") or param(query, "date_to"):
            return None
        cache_path = Path("data/worldcup_wallet_per_wallet_rankings_latest.json")
        if not cache_path.exists():
            return None
        try:
            output = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if output.get("data_status") != "ok":
            return None
        cached_slugs = output.get("scope", {}).get("input_slugs")
        if cached_slugs != input_slugs:
            return None
        return limit_worldcup_rankings(output, limit)

    def worldcup_clickhouse(self) -> ClickHouseWriter:
        if not hasattr(self.clickhouse, "settings"):
            return self.clickhouse
        if self.clickhouse.settings.request_timeout_seconds >= 300:
            return self.clickhouse
        return ClickHouseWriter(
            replace(
                self.clickhouse.settings,
                request_timeout_seconds=300,
            )
        )

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

    def anomaly_signals(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        signal_type = param(query, "type")
        severity = param(query, "severity")
        event_id = param(query, "event_id")
        market_id = param(query, "market_id")
        wallet = param(query, "user").lower()
        limit = int_param(query, "limit", 50, maximum=500)
        where = "where 1 = 1"
        if signal_type:
            where += f" and signal_type = {ch_string(signal_type)}"
        if severity:
            where += f" and severity = {ch_string(severity)}"
        if event_id:
            where += f" and event_id = {ch_string(event_id)}"
        if market_id:
            where += f" and market_id = {ch_string(market_id)}"
        if wallet:
            where += f" and user_address = {ch_string(wallet)}"
        sql = f"""
            select
              signal_id,
              signal_type,
              severity,
              event_id,
              market_id,
              condition_id,
              token_id,
              outcome,
              user_address,
              occurred_at,
              metric_name,
              metric_value,
              baseline_value,
              threshold,
              evidence_json,
              message,
              uncertainty,
              updated_at
            from mart_event_anomaly_signal final
            {where}
            order by occurred_at desc
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
            self.handle_api_request("GET")

        def do_POST(self) -> None:
            self.handle_api_request("POST")

        def do_DELETE(self) -> None:
            self.handle_api_request("DELETE")

        def do_OPTIONS(self) -> None:
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def handle_api_request(self, method: str) -> None:
            parsed = urlparse(self.path)
            try:
                response = api.handle_request(
                    method,
                    parsed.path,
                    parse_qs(parsed.query),
                    self.read_json_body() if method in ("POST", "DELETE") else None,
                )
            except Exception as exc:
                print(f"api request failed path={parsed.path}: {exc}", flush=True)
                response = ApiResponse(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal_error"})
            encoded = json.dumps(response.body, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(int(response.status))
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or "0")
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            payload = json.loads(raw.decode("utf-8"))
            return payload if isinstance(payload, dict) else {}

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    ThreadingHTTPServer((host, port), Handler).serve_forever()


def rows_json(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def compact_live_trade_rows(items: list[dict[str, Any]], captured_at: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, float, float, int]] = set()
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        timestamp = parse_clickhouse_datetime(item.get("timestamp"))
        if timestamp is None:
            timestamp = captured_at
        transaction_hash = str(item.get("transactionHash") or item.get("transaction_hash") or "")
        token_id = str(item.get("asset") or item.get("token_id") or "")
        user_address = str(item.get("proxyWallet") or item.get("user") or item.get("user_address") or "").lower()
        side = str(item.get("side") or "").upper()
        price = float_value(item.get("price"))
        size = float_value(item.get("size"))
        key = (transaction_hash, token_id, user_address, side, price, size, int(timestamp.timestamp()))
        if key in seen:
            continue
        seen.add(key)
        condition_id = str(item.get("conditionId") or item.get("condition_id") or "")
        rows.append(
            {
                "trade_id": trade_row_id(transaction_hash, token_id, timestamp, index),
                "transaction_hash": transaction_hash,
                "timestamp": api_datetime(timestamp),
                "market_id": "",
                "condition_id": condition_id,
                "token_id": token_id,
                "user_address": user_address,
                "side": side,
                "price": price,
                "size": size,
                "notional": price * size,
                "source": "polymarket-live",
                "ingested_at": api_datetime(captured_at),
                "question": str(item.get("title") or ""),
                "market_slug": str(item.get("slug") or ""),
                "event_id": str(item.get("eventId") or item.get("event_id") or ""),
                "event_title": str(item.get("eventTitle") or item.get("event_title") or ""),
                "event_slug": str(item.get("eventSlug") or item.get("event_slug") or ""),
                "category": str(item.get("category") or ""),
                "outcome": str(item.get("outcome") or ""),
                "trader_name": str(item.get("name") or ""),
                "trader_pseudonym": str(item.get("pseudonym") or ""),
                "is_smart": False,
                "is_whale": False,
                "wallet_total_pnl": 0.0,
                "wallet_pnl_roi": 0.0,
                "wallet_traded_notional": 0.0,
            }
        )
    rows.sort(key=lambda row: parse_clickhouse_datetime(row.get("timestamp")) or captured_at, reverse=True)
    return rows


def live_trades_latency_seconds(rows: list[dict[str, Any]], captured_at: datetime) -> float | None:
    if not rows:
        return None
    latest = parse_clickhouse_datetime(rows[0].get("timestamp"))
    if latest is None:
        return None
    return max(0.0, (captured_at - latest).total_seconds())


def filter_live_trade_rows(rows: list[dict[str, Any]], query: dict[str, list[str]]) -> list[dict[str, Any]]:
    side = param(query, "side").upper()
    search = param(query, "q").strip().lower()
    category = param(query, "category")
    min_notional = float_param(query, "min_notional", 0.0, minimum=0.0)
    max_notional = float_param(query, "max_notional", 0.0, minimum=0.0)
    output: list[dict[str, Any]] = []
    for row in rows:
        row_side = str(row.get("side") or "").upper()
        if side in ("BUY", "SELL") and row_side != side:
            continue
        notional = float_value(row.get("notional"))
        if min_notional > 0 and notional < min_notional:
            continue
        if max_notional > 0 and notional > max_notional:
            continue
        if not live_trade_matches_category(row, category):
            continue
        if search:
            haystack = " ".join(
                str(row.get(key) or "")
                for key in (
                    "user_address",
                    "trader_name",
                    "trader_pseudonym",
                    "question",
                    "event_title",
                    "market_slug",
                    "event_slug",
                    "outcome",
                )
            ).lower()
            if search not in haystack:
                continue
        output.append(row)
    return output


def live_trade_matches_category(row: dict[str, Any], category: str) -> bool:
    normalized = str(category or "").strip().lower()
    if not normalized or normalized in ("all", "全部"):
        return True
    terms = live_trade_category_terms(normalized)
    haystack = " ".join(
        str(row.get(key) or "")
        for key in ("category", "question", "event_title", "market_slug", "event_slug")
    ).lower()
    if normalized in ("sports", "体育") and live_trade_is_esports_haystack(haystack):
        return False
    return any(term.lower() in haystack for term in terms)


def live_trade_is_esports_haystack(haystack: str) -> bool:
    return any(
        term in haystack
        for term in ("esports", "e-sports", "valorant", "counter-strike", "counter strike", "cs2", "dota")
    )


def live_trade_category_terms(category: str) -> list[str]:
    terms_by_category = {
        "sports": [
            "sports",
            "sport",
            "nba",
            "fifa",
            "world cup",
            "soccer",
            "football",
            "tennis",
            "dublin",
            "halle",
            "argentina",
            "france",
            "portugal",
        ],
        "体育": [
            "sports",
            "sport",
            "nba",
            "fifa",
            "world cup",
            "soccer",
            "football",
            "tennis",
            "dublin",
            "halle",
            "argentina",
            "france",
            "portugal",
        ],
        "politics": ["politics", "election", "congress", "senate", "trump", "biden", "referendum", "government", "starmer"],
        "政治": ["politics", "election", "congress", "senate", "trump", "biden", "referendum", "government", "starmer"],
        "crypto": ["crypto", "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "dogecoin"],
        "加密货币": ["crypto", "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "dogecoin"],
        "esports": ["esports", "e-sports", "gaming", "lol", "dota", "valorant", "counter-strike"],
        "电竞": ["esports", "e-sports", "gaming", "lol", "dota", "valorant", "counter-strike"],
        "finance": ["finance", "business", "stock", "nasdaq", "s&p", "fed", "rates", "treasury"],
        "金融": ["finance", "business", "stock", "nasdaq", "s&p", "fed", "rates", "treasury"],
        "culture": ["culture", "pop-culture", "music", "movie", "celebrity", "oscars"],
        "文化": ["culture", "pop-culture", "music", "movie", "celebrity", "oscars"],
        "weather": ["weather", "temperature", "hurricane", "rain", "snow", "storm"],
        "天气": ["weather", "temperature", "hurricane", "rain", "snow", "storm"],
    }
    return terms_by_category.get(category, [category])


def limit_live_trades_response(body: dict[str, Any], limit: int) -> dict[str, Any]:
    limited = dict(body)
    trades = body.get("trades")
    limited["trades"] = trades[:limit] if isinstance(trades, list) else []
    return limited


def trade_row_id(transaction_hash: str, token_id: str, timestamp: datetime, index: int) -> str:
    raw = f"{transaction_hash}|{token_id}|{int(timestamp.timestamp())}|{index}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def json_ready_row(row: dict[str, Any]) -> dict[str, Any]:
    output = dict(row)
    for key, value in output.items():
        if isinstance(value, datetime):
            output[key] = api_datetime(value)
    return output


def unusual_betting_excluded_addresses() -> list[str]:
    return [
        "0xe111180000d2663c0091e4f400237545b87b996b",
        "0xe2222d279d744050d28e00520010520000310f59",
        "0x0000000000000000000000000000000000000000",
    ]


def unusual_betting_signal_filters(
    outcome_summary: list[dict[str, Any]],
    cold_price_threshold: float,
    large_threshold: float,
) -> list[dict[str, Any]]:
    filters = []
    for row in outcome_summary:
        signal_type = unusual_betting_signal_type(row, cold_price_threshold, large_threshold)
        if not signal_type:
            continue
        avg_price = unusual_betting_user_price(row)
        filters.append(
            {
                "market_slug": str(row.get("market_slug") or ""),
                "question": str(row.get("question") or ""),
                "outcome": str(row.get("outcome") or ""),
                "user_side": str(row.get("user_side") or ""),
                "signal_type": signal_type,
                "avg_price": avg_price,
                "total_notional": float_value(row.get("total_notional")),
                "max_notional": float_value(row.get("max_notional")),
                "max_user_notional": float_value(row.get("max_user_notional")),
                "large_trade_count": int_value(row.get("large_trade_count")),
                "very_large_trade_count": int_value(row.get("very_large_trade_count")),
                "extreme_trade_count": int_value(row.get("extreme_trade_count")),
            }
        )
    return sorted(filters, key=lambda row: float_value(row.get("total_notional")), reverse=True)


def unusual_betting_signal_type(
    row: dict[str, Any],
    cold_price_threshold: float,
    large_threshold: float,
) -> str:
    side = str(row.get("user_side") or "").upper()
    outcome = str(row.get("outcome") or "")
    outcome_lower = outcome.lower()
    question = str(row.get("question") or "")
    avg_price = unusual_betting_user_price(row)
    total_notional = float_value(row.get("total_notional"))
    if side not in ("BUY", "SELL"):
        return ""
    has_size = total_notional >= large_threshold
    if not has_size and avg_price > cold_price_threshold:
        return ""
    if side == "BUY" and avg_price <= cold_price_threshold:
        return "low_price_buy_no" if outcome_lower == "no" else "low_price_buy"
    if side == "SELL" and avg_price <= cold_price_threshold:
        return "sell_high_probability"
    if side == "BUY" and unusual_betting_is_spread_market(question) and outcome_lower not in ("yes", "no"):
        return "spread_side_buy"
    if side == "SELL" and has_size:
        return "large_sell"
    if has_size and avg_price <= 0.5:
        return "large_mid_price_buy" if side == "BUY" else "large_mid_price_sell"
    return ""


def unusual_betting_user_price(row: dict[str, Any]) -> float:
    avg_price = float_value(row.get("avg_price"))
    if str(row.get("user_side") or "").upper() == "SELL":
        return max(0.0, min(1.0, 1.0 - avg_price))
    return avg_price


def unusual_betting_is_spread_market(question: str) -> bool:
    normalized = question.lower()
    return "spread:" in normalized or "handicap" in normalized


def unusual_betting_signal_condition_sql(filters: list[dict[str, Any]]) -> str:
    conditions = []
    for row in filters:
        market_slug = str(row.get("market_slug") or "")
        outcome = str(row.get("outcome") or "")
        user_side = str(row.get("user_side") or "")
        if not market_slug or not outcome:
            continue
        conditions.append(
            f"(markets.slug = {ch_string(market_slug)} and tokens.outcome = {ch_string(outcome)}"
            f" and user_side = {ch_string(user_side)})"
        )
    return " or ".join(conditions)


def unusual_betting_cold_buy_filters(
    outcome_summary: list[dict[str, Any]],
    cold_price_threshold: float,
) -> list[dict[str, Any]]:
    return [
        row
        for row in unusual_betting_signal_filters(outcome_summary, cold_price_threshold, 500_000.0)
        if str(row.get("signal_type") or "") in ("low_price_buy", "low_price_buy_no")
    ]


def unusual_betting_cold_condition_sql(filters: list[dict[str, Any]]) -> str:
    return unusual_betting_signal_condition_sql(filters)


def summarize_unusual_betting(
    event: dict[str, Any],
    outcome_summary: list[dict[str, Any]],
    signal_wallets: list[dict[str, Any]],
    signal_trades: list[dict[str, Any]],
    *,
    signal_filters: list[dict[str, Any]] | None = None,
    cold_price_threshold: float,
    large_threshold: float,
    very_large_threshold: float,
    extreme_threshold: float,
) -> dict[str, Any]:
    signal_rows = signal_filters or unusual_betting_signal_filters(
        outcome_summary,
        cold_price_threshold,
        large_threshold,
    )
    signal_wallet_groups = unusual_betting_aggregate_wallets(signal_wallets)
    large_signal_count = sum(
        1 for row in signal_wallet_groups if float_value(row.get("total_notional")) >= large_threshold
    )
    very_large_signal_count = sum(
        1
        for row in signal_wallet_groups
        if float_value(row.get("total_notional")) >= very_large_threshold
    )
    extreme_signal_count = sum(
        1
        for row in signal_wallet_groups
        if float_value(row.get("total_notional")) >= extreme_threshold
    )
    max_signal_trade = max(
        [float_value(row.get("max_user_notional")) for row in signal_rows]
        + [float_value(row.get("notional")) for row in signal_trades]
        or [0.0]
    )
    max_signal_wallet = max(
        [float_value(row.get("total_notional")) for row in signal_wallet_groups] or [0.0]
    )
    top_mainstream = sorted(
        [
            row
            for row in outcome_summary
            if str(row.get("user_side") or "").upper() == "BUY"
            and str(row.get("outcome") or "").lower() == "yes"
            and float_value(row.get("avg_price")) > cold_price_threshold
        ],
        key=lambda row: float_value(row.get("total_notional")),
        reverse=True,
    )[:3]

    if extreme_signal_count > 0 or max_signal_wallet >= extreme_threshold:
        severity = "critical"
    elif very_large_signal_count > 0 or max_signal_wallet >= very_large_threshold:
        severity = "high"
    elif large_signal_count > 0 or max_signal_wallet >= large_threshold:
        severity = "medium"
    elif max_signal_trade > 0 or max_signal_wallet > 0:
        severity = "low"
    else:
        severity = "none"

    event_title = str(event.get("title") or event.get("slug") or "")
    if severity in ("critical", "high", "medium"):
        conclusion = (
            f"{event_title} 存在异常方向的大额成交信号："
            f"最大钱包累计约 ${max_signal_wallet:,.0f}。"
        )
    elif signal_rows:
        conclusion = (
            f"{event_title} 未发现 ${large_threshold:,.0f}+ 级别异常方向钱包累计；"
            f"观察到的最大钱包累计约 ${max_signal_wallet:,.0f}。"
        )
    else:
        conclusion = (
            f"{event_title} 未识别到符合当前规则的异常方向。"
        )

    return {
        "severity": severity,
        "has_large_signal": large_signal_count > 0,
        "large_signal_trade_count": 0,
        "very_large_signal_trade_count": 0,
        "extreme_signal_trade_count": 0,
        "large_signal_wallet_count": large_signal_count,
        "very_large_signal_wallet_count": very_large_signal_count,
        "extreme_signal_wallet_count": extreme_signal_count,
        "max_signal_trade_notional": max_signal_trade,
        "max_signal_wallet_notional": max_signal_wallet,
        "signal_total_notional": sum(float_value(row.get("total_notional")) for row in signal_rows),
        "signal_outcome_count": len(signal_rows),
        "has_large_cold_buy": large_signal_count > 0,
        "large_cold_trade_count": 0,
        "very_large_cold_trade_count": 0,
        "extreme_cold_trade_count": 0,
        "max_cold_trade_notional": max_signal_trade,
        "max_cold_wallet_notional": max_signal_wallet,
        "cold_buy_total_notional": sum(float_value(row.get("total_notional")) for row in signal_rows),
        "cold_buy_outcome_count": len(signal_rows),
        "top_mainstream_yes": top_mainstream,
        "top_signal_wallets": signal_wallets[:5],
        "top_signal_wallet_groups": signal_wallet_groups[:5],
        "top_signal_trades": signal_trades[:5],
        "top_cold_wallets": signal_wallets[:5],
        "top_cold_wallet_groups": signal_wallet_groups[:5],
        "top_cold_trades": signal_trades[:5],
        "conclusion": conclusion,
        "notes": [
            "分析基于已成交链上 fill，并按 maker/taker 展开为用户侧 BUY/SELL。",
            "不包含未成交挂单。",
            "系统/流动性内部地址已从阈值统计和异常钱包排行中排除。",
            "异常方向覆盖低价买入、热门高价卖出、No 低价买入和 spread 反向大额成交。",
        ],
        "thresholds": {
            "cold_price_threshold": cold_price_threshold,
            "large_threshold": large_threshold,
            "very_large_threshold": very_large_threshold,
            "extreme_threshold": extreme_threshold,
        },
    }


def unusual_betting_summary_response(detail: dict[str, Any]) -> dict[str, Any]:
    event = detail.get("event") if isinstance(detail.get("event"), dict) else {}
    parameters = detail.get("parameters") if isinstance(detail.get("parameters"), dict) else {}
    analysis = detail.get("analysis") if isinstance(detail.get("analysis"), dict) else {}
    signal_wallets = (
        detail.get("signal_wallets")
        if isinstance(detail.get("signal_wallets"), list)
        else detail.get("cold_wallets")
        if isinstance(detail.get("cold_wallets"), list)
        else []
    )
    signal_trades = (
        detail.get("signal_trades")
        if isinstance(detail.get("signal_trades"), list)
        else detail.get("cold_trades")
        if isinstance(detail.get("cold_trades"), list)
        else []
    )
    signal_outcomes = (
        detail.get("signal_outcomes")
        if isinstance(detail.get("signal_outcomes"), list)
        else detail.get("cold_buy_outcomes")
        if isinstance(detail.get("cold_buy_outcomes"), list)
        else []
    )
    signal_wallet_summary = (
        detail.get("signal_wallet_summary")
        if isinstance(detail.get("signal_wallet_summary"), dict)
        else {}
    )
    large_threshold = float_value(parameters.get("large_threshold"))
    signal_wallet_groups = unusual_betting_aggregate_wallets(signal_wallets)
    abnormal_wallets = [
        row
        for row in signal_wallet_groups
        if float_value(row.get("total_notional")) >= large_threshold
    ]
    watch_wallets = signal_wallet_groups[:5]
    abnormal_wallet_count = int_value(signal_wallet_summary.get("abnormal_wallet_count"))
    if abnormal_wallet_count <= 0:
        abnormal_wallet_count = len(abnormal_wallets)
    max_abnormal_wallet_notional = max(
        [float_value(signal_wallet_summary.get("max_abnormal_wallet_notional"))]
        + [float_value(row.get("total_notional")) for row in abnormal_wallets]
        or [0.0]
    )
    max_watch_wallet_notional = max(
        [float_value(signal_wallet_summary.get("max_watch_wallet_notional"))]
        + [float_value(row.get("total_notional")) for row in watch_wallets]
        or [0.0]
    )
    max_trade_notional = max(
        [float_value(row.get("notional")) for row in signal_trades]
        + [
            float_value(analysis.get("max_signal_trade_notional")),
            float_value(analysis.get("max_cold_trade_notional")),
            float_value(signal_wallet_summary.get("max_watch_trade_notional")),
        ]
        or [0.0]
    )
    large_wallet_count = int_value(
        signal_wallet_summary.get("abnormal_wallet_count", analysis.get("large_signal_wallet_count"))
    )
    very_large_wallet_count = int_value(
        signal_wallet_summary.get(
            "very_large_wallet_count",
            analysis.get("very_large_signal_wallet_count"),
        )
    )
    extreme_wallet_count = int_value(
        signal_wallet_summary.get(
            "extreme_wallet_count",
            analysis.get("extreme_signal_wallet_count"),
        )
    )
    severity = str(analysis.get("severity") or "none")
    slug = str(event.get("slug") or "")
    conclusion = unusual_betting_summary_conclusion(
        event,
        severity=severity,
        abnormal_wallet_count=abnormal_wallet_count,
        large_trade_count=large_wallet_count,
        very_large_trade_count=very_large_wallet_count,
        extreme_trade_count=extreme_wallet_count,
        max_trade_notional=max_trade_notional,
        max_wallet_notional=max_abnormal_wallet_notional
        if abnormal_wallet_count > 0
        else max_watch_wallet_notional,
        large_threshold=large_threshold,
        cold_price_threshold=float_value(parameters.get("cold_price_threshold")),
        fallback=str(analysis.get("conclusion") or ""),
    )
    return {
        "status": "ok",
        "event": event,
        "slug": slug,
        "severity": severity,
        "conclusion": conclusion,
        "abnormal_wallet_count": abnormal_wallet_count,
        "abnormal_trade_count": 0,
        "very_large_trade_count": 0,
        "extreme_trade_count": 0,
        "large_signal_wallet_count": large_wallet_count,
        "very_large_signal_wallet_count": very_large_wallet_count,
        "extreme_signal_wallet_count": extreme_wallet_count,
        "max_abnormal_trade_notional": 0.0,
        "max_abnormal_wallet_notional": max_abnormal_wallet_notional,
        "max_watch_trade_notional": max_trade_notional,
        "max_watch_wallet_notional": max_watch_wallet_notional,
        "signal_outcome_count": int_value(
            analysis.get("signal_outcome_count", analysis.get("cold_buy_outcome_count"))
        ),
        "signal_total_notional": float_value(
            analysis.get("signal_total_notional", analysis.get("cold_buy_total_notional"))
        ),
        "signal_wallet_count": int_value(signal_wallet_summary.get("signal_wallet_count")),
        "cold_buy_outcome_count": int_value(
            analysis.get("signal_outcome_count", analysis.get("cold_buy_outcome_count"))
        ),
        "cold_buy_total_notional": float_value(
            analysis.get("signal_total_notional", analysis.get("cold_buy_total_notional"))
        ),
        "thresholds": analysis.get("thresholds") or {},
        "abnormal_wallets": abnormal_wallets,
        "watch_wallets": watch_wallets,
        "signal_outcomes": signal_outcomes[:5],
        "cold_outcomes": signal_outcomes[:5],
        "detail_url": f"/api/events/unusual-betting?slug={slug}" if slug else "",
        "chart_url": f"/#/unusual-betting?slug={slug}" if slug else "",
        "generated_at": detail.get("generated_at"),
    }


def unusual_betting_cache_key(
    *,
    event_ids: list[str],
    cold_price_threshold: float,
    large_threshold: float,
    very_large_threshold: float,
    extreme_threshold: float,
    include_related_markets: bool,
) -> str:
    payload = {
        "version": "large_direction_signal_v1",
        "event_ids": sorted(str(event_id) for event_id in event_ids if event_id),
        "cold_price_threshold": cold_price_threshold,
        "large_threshold": large_threshold,
        "very_large_threshold": very_large_threshold,
        "extreme_threshold": extreme_threshold,
        "include_related_markets": include_related_markets,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def unusual_betting_cache_metadata(row: dict[str, Any] | None, *, source: str) -> dict[str, Any]:
    row = row or {}
    return {
        "source": source,
        "cache_key": str(row.get("cache_key") or ""),
        "refreshed_at": row.get("refreshed_at"),
        "generated_at": row.get("generated_at"),
        "age_seconds": row.get("age_seconds"),
        "trigger_reason": row.get("trigger_reason"),
        "error": row.get("error"),
    }


def unusual_betting_cached_detail_satisfies(
    detail: dict[str, Any],
    *,
    wallet_limit: int,
    trade_limit: int,
) -> bool:
    parameters = detail.get("parameters") if isinstance(detail.get("parameters"), dict) else {}
    cached_wallet_limit = int_value(parameters.get("wallet_limit"))
    cached_trade_limit = int_value(parameters.get("trade_limit"))
    return cached_wallet_limit >= wallet_limit and cached_trade_limit >= trade_limit


def unusual_betting_trim_cached_detail(
    detail: dict[str, Any],
    *,
    wallet_limit: int,
    trade_limit: int,
) -> dict[str, Any]:
    trimmed = dict(detail)
    if isinstance(trimmed.get("signal_wallets"), list):
        trimmed["signal_wallets"] = trimmed["signal_wallets"][:wallet_limit]
    if isinstance(trimmed.get("cold_wallets"), list):
        trimmed["cold_wallets"] = trimmed["cold_wallets"][:wallet_limit]
    if isinstance(trimmed.get("signal_trades"), list):
        trimmed["signal_trades"] = trimmed["signal_trades"][:trade_limit]
    if isinstance(trimmed.get("cold_trades"), list):
        trimmed["cold_trades"] = trimmed["cold_trades"][:trade_limit]
    parameters = dict(trimmed.get("parameters") or {})
    parameters["wallet_limit"] = wallet_limit
    parameters["trade_limit"] = trade_limit
    trimmed["parameters"] = parameters
    return trimmed


def unusual_betting_aggregate_wallets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        address = str(row.get("user_address") or "").lower()
        if not address:
            continue
        group = grouped.setdefault(
            address,
            {
                "user_address": address,
                "total_notional": 0.0,
                "max_notional": 0.0,
                "fills": 0,
                "first_ts": None,
                "last_ts": None,
                "selections": [],
            },
        )
        total_notional = float_value(row.get("total_notional"))
        max_notional = float_value(row.get("max_notional"))
        group["total_notional"] = float_value(group.get("total_notional")) + total_notional
        group["max_notional"] = max(float_value(group.get("max_notional")), max_notional)
        group["fills"] = int_value(group.get("fills")) + int_value(row.get("fills"))
        group["first_ts"] = earliest_datetime_value(group.get("first_ts"), row.get("first_ts"))
        group["last_ts"] = latest_datetime_value(group.get("last_ts"), row.get("last_ts"))
        group["selections"].append(
            {
                "market_slug": str(row.get("market_slug") or ""),
                "question": str(row.get("question") or ""),
                "outcome": str(row.get("outcome") or ""),
                "total_notional": total_notional,
                "max_notional": max_notional,
                "avg_price": float_value(row.get("avg_price")),
                "fills": int_value(row.get("fills")),
                "first_ts": api_datetime(row.get("first_ts")),
                "last_ts": api_datetime(row.get("last_ts")),
            }
        )
    output = []
    for group in grouped.values():
        selections = sorted(
            group.get("selections") or [],
            key=lambda row: float_value(row.get("total_notional")),
            reverse=True,
        )
        output.append(
            {
                "user_address": group.get("user_address"),
                "total_notional": round(float_value(group.get("total_notional")), 2),
                "max_notional": round(float_value(group.get("max_notional")), 2),
                "fills": int_value(group.get("fills")),
                "first_ts": api_datetime(group.get("first_ts")),
                "last_ts": api_datetime(group.get("last_ts")),
                "selections": selections[:5],
            }
        )
    return sorted(
        output,
        key=lambda row: (
            float_value(row.get("total_notional")),
            float_value(row.get("max_notional")),
        ),
        reverse=True,
    )


def unusual_betting_summary_conclusion(
    event: dict[str, Any],
    *,
    severity: str,
    abnormal_wallet_count: int,
    large_trade_count: int,
    very_large_trade_count: int,
    extreme_trade_count: int,
    max_trade_notional: float,
    max_wallet_notional: float,
    large_threshold: float,
    cold_price_threshold: float,
    fallback: str,
) -> str:
    event_title = str(event.get("title") or event.get("slug") or "该比赛")
    if abnormal_wallet_count > 0:
        size_clause = f"最大钱包累计约 ${max_wallet_notional:,.0f}"
        if extreme_trade_count > 0:
            level = "五百万级/超大额"
        elif very_large_trade_count > 0:
            level = "百万美金级"
        elif large_trade_count > 0:
            level = "五十万美金级"
        else:
            level = "累计大额"
        return (
            f"{event_title} 发现 {abnormal_wallet_count} 个异常钱包在异常方向下注，"
            f"信号级别为{level}；{size_clause}。"
        )
    if large_trade_count > 0:
        return (
            f"{event_title} 发现 {large_trade_count} 个 ${large_threshold:,.0f}+ 信号方向钱包。"
        )
    if max_trade_notional > 0 or max_wallet_notional > 0:
        return (
            f"{event_title} 暂未发现 ${large_threshold:,.0f}+ 信号方向钱包累计；"
            f"低价方向阈值为均价 <= {cold_price_threshold:.0%}，"
            f"当前最大钱包累计约 ${max_wallet_notional:,.0f}。"
        )
    if severity != "none" and fallback:
        return fallback
    return f"{event_title} 暂未识别到冷门 Yes 异常下注信号。"


def earliest_datetime_value(left: Any, right: Any) -> Any:
    left_dt = parse_clickhouse_datetime(left)
    right_dt = parse_clickhouse_datetime(right)
    if left_dt is None:
        return right
    if right_dt is None:
        return left
    return left if left_dt <= right_dt else right


def latest_datetime_value(left: Any, right: Any) -> Any:
    left_dt = parse_clickhouse_datetime(left)
    right_dt = parse_clickhouse_datetime(right)
    if left_dt is None:
        return right
    if right_dt is None:
        return left
    return left if left_dt >= right_dt else right


def api_datetime(value: Any) -> str | None:
    dt = parse_clickhouse_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def wallet_detail_scope(value: str) -> str:
    normalized = str(value or "all").strip().lower().replace("-", "_")
    if normalized in ("fifa", "fifwc", "worldcup", "world_cup", "world cup"):
        return "fifa"
    return "all"


def wallet_fifa_pnl_snapshot(
    user: str,
    summary: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not summary:
        return None
    captured_at = summary.get("updated_at") or datetime.now(UTC)
    captured_dt = parse_clickhouse_datetime(captured_at) or datetime.now(UTC)
    equity_now = float_value(summary.get("equity_now"))
    equity_24h_ago = float_value(summary.get("equity_24h_ago"))
    equity_7d_ago = float_value(summary.get("equity_7d_ago"))
    points = [
        {"t": int(captured_dt.timestamp()) - 7 * 86_400, "p": equity_7d_ago},
        {"t": int(captured_dt.timestamp()) - 86_400, "p": equity_24h_ago},
        {"t": int(captured_dt.timestamp()), "p": equity_now},
    ]
    return {
        "user_address": user,
        "captured_at": api_datetime(captured_dt),
        "total_pnl": equity_now,
        "raw_json": json.dumps({"points": points}, separators=(",", ":")),
    }


def wallet_fifa_activity_by_type(summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not summary or int_value(summary.get("trade_activity_count")) <= 0:
        return []
    return [
        {
            "activity_type": "TRADE",
            "count": int_value(summary.get("trade_activity_count")),
            "size": float_value(summary.get("traded_size")),
            "notional": float_value(summary.get("traded_notional")),
            "first_activity_at": summary.get("first_activity_at"),
            "last_activity_at": summary.get("last_activity_at"),
        }
    ]


def normalize_fifa_position(row: dict[str, Any]) -> dict[str, Any]:
    position_size = float_value(row.get("size"))
    buy_size = float_value(row.get("buy_size"))
    sell_size = float_value(row.get("sell_size"))
    buy_notional = float_value(row.get("buy_notional"))
    sell_notional = float_value(row.get("sell_notional"))
    current_value = float_value(row.get("current_value"))
    cash_pnl = sell_notional + current_value - buy_notional
    market_closed = bool_value(row.get("market_closed"))
    dust_threshold = max(0.01, buy_size * 0.0001)
    is_open = position_size > dust_threshold and not market_closed
    display_size = position_size if is_open else max(buy_size, sell_size, position_size)
    percent_pnl = cash_pnl / buy_notional * 100.0 if buy_notional else 0.0
    return {
        "asset": str(row.get("asset") or ""),
        "condition_id": str(row.get("condition_id") or ""),
        "title": str(row.get("title") or ""),
        "slug": str(row.get("slug") or ""),
        "event_id": str(row.get("event_id") or ""),
        "event_slug": str(row.get("event_slug") or ""),
        "outcome": str(row.get("outcome") or ""),
        "opposite_outcome": "",
        "size": display_size,
        "avg_price": float_value(row.get("avg_price")),
        "cur_price": float_value(row.get("cur_price")),
        "initial_value": buy_notional,
        "current_value": current_value if is_open else 0.0,
        "cash_pnl": cash_pnl,
        "percent_pnl": percent_pnl,
        "realized_pnl": cash_pnl if not is_open else 0.0,
        "percent_realized_pnl": percent_pnl if not is_open else 0.0,
        "total_bought": buy_size,
        "redeemable": market_closed and position_size > dust_threshold,
        "mergeable": False,
        "negative_risk": False,
        "end_date": row.get("last_activity_at"),
        "icon": "",
        "is_worldcup": True,
        "is_open": is_open,
        "is_settled_or_redeemable": not is_open,
        "cost_basis_estimate": buy_notional,
        "sold_value": sell_notional,
        "redeemed_value": 0.0,
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "buy_size": buy_size,
        "sell_size": sell_size,
        "buy_count": int_value(row.get("buy_count")),
        "sell_count": int_value(row.get("sell_count")),
        "trade_count": int_value(row.get("trade_count")),
        "activity_count": int_value(row.get("trade_count")),
        "first_activity_at": api_datetime(row.get("first_activity_at")),
        "last_activity_at": api_datetime(row.get("last_activity_at")),
        "missing_mark": bool_value(row.get("missing_mark")),
    }


def dedupe_wallet_activity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = wallet_activity_row_key(row)
        current = deduped.get(key)
        if current is None or timestamp_key(row.get("ingested_at")) >= timestamp_key(
            current.get("ingested_at")
        ):
            deduped[key] = row
    return sorted(
        deduped.values(),
        key=lambda row: timestamp_key(api_datetime(row.get("timestamp"))),
        reverse=True,
    )


def wallet_activity_row_key(row: dict[str, Any]) -> tuple[Any, ...]:
    transaction_hash = str(row.get("transaction_hash") or "")
    if transaction_hash:
        return (
            transaction_hash,
            str(row.get("condition_id") or ""),
            str(row.get("token_id") or ""),
            str(row.get("side") or ""),
            float_value(row.get("price")),
            float_value(row.get("size")),
            api_datetime(row.get("timestamp")) or "",
        )
    return ("activity_id", str(row.get("activity_id") or ""))


def rtds_message_key(message: dict[str, Any]) -> str:
    trade = message.get("trade")
    if isinstance(trade, dict):
        trade_id = str(trade.get("trade_id") or "")
        if trade_id:
            return trade_id
        return "|".join(
            [
                str(trade.get("transaction_hash") or ""),
                str(trade.get("token_id") or ""),
                str(trade.get("user_address") or "").lower(),
                str(trade.get("side") or ""),
                str(trade.get("price") or ""),
                str(trade.get("size") or ""),
                str(trade.get("timestamp") or ""),
            ]
        )
    return json.dumps(message, sort_keys=True, default=str, separators=(",", ":"))


def merge_wallet_rtds_activity_rows(
    activity_rows_payload: list[dict[str, Any]],
    rtds_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not rtds_rows:
        return activity_rows_payload
    return [*rtds_rows, *activity_rows_payload]


def wallet_rtds_activity_rows(
    user: str,
    messages: list[dict[str, Any]],
    captured_at: datetime,
) -> list[dict[str, Any]]:
    normalized_user = str(user or "").lower()
    rows: list[dict[str, Any]] = []
    for message in messages:
        trade = message.get("trade")
        raw = message.get("raw")
        payload = raw.get("payload") if isinstance(raw, dict) else None
        if not isinstance(trade, dict):
            continue
        if str(trade.get("user_address") or "").lower() != normalized_user:
            continue
        timestamp = parse_clickhouse_datetime(trade.get("timestamp"))
        if timestamp is None:
            continue
        raw_payload = payload if isinstance(payload, dict) else rtds_trade_raw_payload(trade)
        rows.append(
            {
                "activity_id": str(trade.get("trade_id") or ""),
                "user_address": normalized_user,
                "timestamp": timestamp,
                "activity_type": "TRADE",
                "condition_id": str(trade.get("condition_id") or ""),
                "token_id": str(trade.get("token_id") or ""),
                "transaction_hash": str(trade.get("transaction_hash") or ""),
                "side": str(trade.get("side") or ""),
                "price": float_value(trade.get("price")),
                "size": float_value(trade.get("size")),
                "notional": float_value(trade.get("notional")),
                "raw_json": json.dumps(raw_payload, ensure_ascii=False, separators=(",", ":")),
                "ingested_at": captured_at,
                "source": str(trade.get("source") or message.get("source") or "polymarket-rtds"),
            }
        )
    return rows


def rtds_trade_raw_payload(trade: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": str(trade.get("question") or ""),
        "slug": str(trade.get("market_slug") or ""),
        "eventSlug": str(trade.get("event_slug") or ""),
        "outcome": str(trade.get("outcome") or ""),
        "transactionHash": str(trade.get("transaction_hash") or ""),
        "asset": str(trade.get("token_id") or ""),
        "conditionId": str(trade.get("condition_id") or ""),
        "proxyWallet": str(trade.get("user_address") or ""),
        "side": str(trade.get("side") or ""),
        "price": float_value(trade.get("price")),
        "size": float_value(trade.get("size")),
        "usdcSize": float_value(trade.get("notional")),
    }


def summarize_activity_rows(user: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return empty_wallet_activity_summary(user)
    now = datetime.now(UTC)
    cutoff_24h = now.timestamp() - 86_400
    cutoff_7d = now.timestamp() - 7 * 86_400
    trade_rows = [row for row in rows if str(row.get("activity_type") or "") == "TRADE"]
    buy_rows = [row for row in trade_rows if str(row.get("side") or "").upper() == "BUY"]
    sell_rows = [row for row in trade_rows if str(row.get("side") or "").upper() == "SELL"]
    timestamps = [parse_clickhouse_datetime(row.get("timestamp")) for row in rows]
    timestamps = [dt for dt in timestamps if dt is not None]
    latest = rows[0] if rows else {}
    notional_values = [
        float_value(row.get("notional")) for row in trade_rows if float_value(row.get("notional")) > 0
    ]
    rows_24h = [
        row
        for row in rows
        if (parse_clickhouse_datetime(row.get("timestamp")) or datetime.fromtimestamp(0, UTC)).timestamp()
        >= cutoff_24h
    ]
    trade_rows_24h = [row for row in rows_24h if str(row.get("activity_type") or "") == "TRADE"]
    trade_rows_7d = [
        row
        for row in trade_rows
        if (parse_clickhouse_datetime(row.get("timestamp")) or datetime.fromtimestamp(0, UTC)).timestamp()
        >= cutoff_7d
    ]
    return {
        "user_address": user,
        "activity_count": len(rows),
        "trade_activity_count": len(trade_rows),
        "buy_count": len(buy_rows),
        "sell_count": len(sell_rows),
        "traded_size": sum(float_value(row.get("size")) for row in trade_rows),
        "traded_notional": sum(float_value(row.get("notional")) for row in trade_rows),
        "buy_notional": sum(float_value(row.get("notional")) for row in buy_rows),
        "sell_notional": sum(float_value(row.get("notional")) for row in sell_rows),
        "activity_count_24h": len(rows_24h),
        "trade_activity_count_24h": len(trade_rows_24h),
        "traded_notional_24h": sum(float_value(row.get("notional")) for row in trade_rows_24h),
        "trade_activity_count_7d": len(trade_rows_7d),
        "traded_notional_7d": sum(float_value(row.get("notional")) for row in trade_rows_7d),
        "avg_bet": sum(notional_values) / len(notional_values) if notional_values else 0.0,
        "latest_activity_type": str(latest.get("activity_type") or ""),
        "latest_side": str(latest.get("side") or ""),
        "first_activity_at": api_datetime(min(timestamps)) if timestamps else None,
        "last_activity_at": api_datetime(max(timestamps)) if timestamps else None,
    }


def summarize_activity_rows_by_type(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        activity_type = str(row.get("activity_type") or "")
        grouped.setdefault(activity_type, []).append(row)
    output = []
    for activity_type, group in grouped.items():
        timestamps = [parse_clickhouse_datetime(row.get("timestamp")) for row in group]
        timestamps = [dt for dt in timestamps if dt is not None]
        output.append(
            {
                "activity_type": activity_type,
                "count": len(group),
                "size": sum(float_value(row.get("size")) for row in group),
                "notional": sum(float_value(row.get("notional")) for row in group),
                "first_activity_at": api_datetime(min(timestamps)) if timestamps else None,
                "last_activity_at": api_datetime(max(timestamps)) if timestamps else None,
            }
        )
    return sorted(output, key=lambda row: (int_value(row.get("count")), float_value(row.get("notional"))), reverse=True)


def recent_activity_from_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    output = []
    for row in rows[:limit]:
        raw = activity_raw_json(row)
        output.append(
            {
                "timestamp": api_datetime(row.get("timestamp")),
                "activity_type": str(row.get("activity_type") or ""),
                "side": str(row.get("side") or ""),
                "price": float_value(row.get("price")),
                "size": float_value(row.get("size")),
                "notional": float_value(row.get("notional")),
                "condition_id": str(row.get("condition_id") or ""),
                "token_id": str(row.get("token_id") or ""),
                "transaction_hash": str(row.get("transaction_hash") or ""),
                "title": str(raw.get("title") or ""),
                "slug": str(raw.get("slug") or ""),
                "event_slug": str(raw.get("eventSlug") or raw.get("event_slug") or ""),
                "outcome": str(raw.get("outcome") or ""),
                "source": str(row.get("source") or ""),
            }
        )
    return output


def wallet_closed_positions_from_activity(
    rows: list[dict[str, Any]],
    *,
    include_open: bool = False,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        activity_type = str(row.get("activity_type") or "").upper()
        side = str(row.get("side") or "").upper()
        condition_id = str(row.get("condition_id") or "")
        token_id = str(row.get("token_id") or "")
        raw = activity_raw_json(row)
        title = str(row.get("title") or raw.get("title") or "")
        slug = str(row.get("slug") or raw.get("slug") or "")
        event_slug = str(row.get("event_slug") or raw.get("eventSlug") or raw.get("event_slug") or "")
        outcome = str(row.get("outcome") or raw.get("outcome") or "")
        if activity_type == "TRADE" and not token_id:
            continue
        if not condition_id and not title:
            continue
        group_token = token_id if activity_type == "TRADE" else ""
        group_outcome = outcome if activity_type == "TRADE" else ""
        key = (condition_id, "", "")
        group = groups.setdefault(
            key,
            {
                "asset": group_token,
                "condition_id": condition_id,
                "title": title,
                "slug": slug,
                "event_slug": event_slug,
                "outcome": group_outcome,
                "size": 0.0,
                "avg_price": 0.0,
                "cur_price": 0.0,
                "initial_value": 0.0,
                "current_value": 0.0,
                "cash_pnl": 0.0,
                "percent_pnl": 0.0,
                "realized_pnl": 0.0,
                "total_bought": 0.0,
                "redeemed_value": 0.0,
                "redeem_count": 0,
                "redeemed_size": 0.0,
                "sold_value": 0.0,
                "buy_notional": 0.0,
                "sell_notional": 0.0,
                "buy_size": 0.0,
                "sell_size": 0.0,
                "buy_count": 0,
                "sell_count": 0,
                "trade_count": 0,
                "activity_count": 0,
                "first_activity_at": None,
                "last_activity_at": None,
                "end_date": None,
                "is_open": False,
                "is_settled_or_redeemable": True,
                "is_worldcup": False,
            },
        )
        if title and not group.get("title"):
            group["title"] = title
        if slug and not group.get("slug"):
            group["slug"] = slug
        if event_slug and not group.get("event_slug"):
            group["event_slug"] = event_slug
        if group_token and not group.get("asset"):
            group["asset"] = group_token
        if group_outcome and not group.get("outcome"):
            group["outcome"] = group_outcome
        timestamp = api_datetime(row.get("timestamp"))
        update_group_activity_bounds(group, timestamp)
        notional = float_value(row.get("notional"))
        size = float_value(row.get("size"))
        price = float_value(row.get("price"))
        group["activity_count"] = int_value(group.get("activity_count")) + 1
        if activity_type == "TRADE":
            group["trade_count"] = int_value(group.get("trade_count")) + 1
            if side == "BUY":
                group["buy_count"] = int_value(group.get("buy_count")) + 1
                group["buy_notional"] = float_value(group.get("buy_notional")) + notional
                group["buy_size"] = float_value(group.get("buy_size")) + size
                group["total_bought"] = float_value(group.get("total_bought")) + size
            elif side == "SELL":
                group["sell_count"] = int_value(group.get("sell_count")) + 1
                group["sell_notional"] = float_value(group.get("sell_notional")) + notional
                group["sell_size"] = float_value(group.get("sell_size")) + size
        elif activity_type in ("REDEEM", "REDEMPTION"):
            group["redeem_count"] = int_value(group.get("redeem_count")) + 1
            group["redeemed_size"] = float_value(group.get("redeemed_size")) + size
            group["redeemed_value"] = float_value(group.get("redeemed_value")) + notional
            if not group.get("outcome"):
                group["outcome"] = "结算"

    positions = []
    for group in groups.values():
        buy_size = float_value(group.get("buy_size"))
        sell_size = float_value(group.get("sell_size"))
        buy_notional = float_value(group.get("buy_notional"))
        sell_notional = float_value(group.get("sell_notional"))
        redeemed_value = float_value(group.get("redeemed_value"))
        redeem_count = int_value(group.get("redeem_count"))
        remaining_size = max(0.0, buy_size - sell_size)
        recovered = sell_notional + redeemed_value
        pnl = recovered - buy_notional
        dust_threshold = max(0.01, buy_size * 0.0001)
        is_open = remaining_size > dust_threshold and recovered <= 0 and redeem_count <= 0
        group["size"] = remaining_size if is_open else max(buy_size, sell_size)
        group["avg_price"] = buy_notional / buy_size if buy_size else 0.0
        group["initial_value"] = buy_notional
        group["current_value"] = 0.0
        group["cash_pnl"] = pnl
        group["realized_pnl"] = pnl
        group["sold_value"] = sell_notional
        group["percent_pnl"] = (pnl / buy_notional * 100.0) if buy_notional else 0.0
        group["cost_basis_estimate"] = buy_notional
        group["is_open"] = is_open
        group["is_settled_or_redeemable"] = not group["is_open"]
        group["is_worldcup"] = str(group.get("slug") or "").startswith("fifwc-") or str(
            group.get("event_slug") or ""
        ).startswith("fifwc-")
        group["end_date"] = group.get("last_activity_at")
        if (include_open or group["is_settled_or_redeemable"]) and (
            buy_notional > 0 or recovered > 0 or redeem_count > 0
        ):
            positions.append(group)
    return sort_wallet_positions(positions, "abs_pnl")


def wallet_risk_metrics(
    closed_positions: list[dict[str, Any]],
    pnl_points: list[dict[str, Any]],
) -> dict[str, Any]:
    realized_positions = [
        position
        for position in closed_positions
        if float_value(position.get("initial_value")) > 0
        or float_value(position.get("sold_value")) > 0
        or float_value(position.get("redeemed_value")) > 0
    ]
    completed_count = len(realized_positions)
    pnl_values = [float_value(position.get("cash_pnl")) for position in realized_positions]
    positive_pnl = sum(value for value in pnl_values if value > 0)
    negative_pnl = sum(value for value in pnl_values if value < 0)
    profitable_count = sum(1 for value in pnl_values if value > 0)
    losing_count = sum(1 for value in pnl_values if value < 0)
    buy_notional = sum(float_value(position.get("initial_value")) for position in realized_positions)
    total_pnl = positive_pnl + negative_pnl
    settled_positions = [
        position for position in realized_positions if int_value(position.get("redeem_count")) > 0
    ]
    short_positions = [
        position
        for position in realized_positions
        if wallet_position_holding_seconds(position) is not None
        and wallet_position_holding_seconds(position) <= 86_400
    ]
    return {
        "completed_event_count": completed_count if completed_count else None,
        "profitable_event_count": profitable_count if completed_count else None,
        "losing_event_count": losing_count if completed_count else None,
        "realized_pnl": total_pnl if completed_count else None,
        "win_rate": profitable_count / completed_count if completed_count else None,
        "profit_factor": positive_pnl / abs(negative_pnl) if negative_pnl < 0 else None,
        "max_drawdown": wallet_max_drawdown(pnl_points),
        "sharpe_ratio": wallet_sharpe_ratio(pnl_points),
        "short_term_ratio": len(short_positions) / completed_count if completed_count else None,
        "short_term_win_rate": (
            sum(1 for position in short_positions if float_value(position.get("cash_pnl")) > 0)
            / len(short_positions)
            if short_positions
            else None
        ),
        "short_term_value": (
            sum(float_value(position.get("cash_pnl")) for position in short_positions)
            if short_positions
            else None
        ),
        "settlement_ratio": len(settled_positions) / completed_count if completed_count else None,
        "settlement_win_rate": (
            sum(1 for position in settled_positions if float_value(position.get("cash_pnl")) > 0)
            / len(settled_positions)
            if settled_positions
            else None
        ),
        "avg_event_roi": total_pnl / buy_notional if buy_notional else None,
        "prediction_score": total_pnl / buy_notional if buy_notional else None,
    }


def wallet_performance_metrics(closed_positions: list[dict[str, Any]]) -> dict[str, Any]:
    realized_positions = [
        position
        for position in closed_positions
        if float_value(position.get("initial_value")) > 0
        or float_value(position.get("sold_value")) > 0
        or float_value(position.get("redeemed_value")) > 0
    ]
    holding_seconds = []
    add_counts = []
    for position in realized_positions:
        duration = wallet_position_holding_seconds(position)
        if duration is not None and duration > 0:
            holding_seconds.append(duration)
        buy_count = int_value(position.get("buy_count"))
        if buy_count <= 0 and float_value(position.get("buy_notional")) > 0:
            buy_count = 1
        if buy_count > 0:
            add_counts.append(max(0, buy_count - 1))
    return {
        "avg_holding_seconds": (
            sum(holding_seconds) / len(holding_seconds) if holding_seconds else None
        ),
        "holding_sample_count": len(holding_seconds),
        "holding_position_count": len(realized_positions),
        "avg_add_count": sum(add_counts) / len(add_counts) if add_counts else None,
        "add_sample_count": len(add_counts),
    }


def merge_closed_positions(
    closed_activity_positions: list[dict[str, Any]],
    snapshot_positions: list[dict[str, Any]],
    *,
    all_activity_positions: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    merged = list(closed_activity_positions)
    activity_by_key = {
        wallet_position_key(position): position for position in (all_activity_positions or [])
    }
    seen = {wallet_position_key(position) for position in merged}
    seen_condition_ids = {
        str(position.get("condition_id") or "")
        for position in merged
        if str(position.get("condition_id") or "")
    }
    for position in snapshot_positions:
        if not position.get("is_settled_or_redeemable"):
            continue
        key = wallet_position_key(position)
        condition_id = str(position.get("condition_id") or "")
        if key in seen or (condition_id and condition_id in seen_condition_ids):
            continue
        activity_position = activity_by_key.get(key, {})
        closed = dict(position)
        closed["is_open"] = False
        closed["is_settled_or_redeemable"] = True
        closed.setdefault("sold_value", 0.0)
        closed.setdefault("redeemed_value", 0.0)
        closed.setdefault("buy_notional", float_value(closed.get("initial_value")))
        closed.setdefault("sell_notional", 0.0)
        closed.setdefault("redeem_count", 1 if closed.get("redeemable") else 0)
        closed["first_activity_at"] = activity_position.get("first_activity_at")
        closed["last_activity_at"] = (
            activity_position.get("last_activity_at")
            or closed.get("last_activity_at")
            or closed.get("end_date")
        )
        if activity_position:
            activity_buy_notional = value_or_fallback(
                activity_position.get("buy_notional"),
                closed.get("buy_notional"),
            )
            closed["buy_notional"] = activity_buy_notional
            closed["sell_notional"] = value_or_fallback(
                activity_position.get("sell_notional"),
                closed.get("sell_notional"),
            )
            closed["sold_value"] = value_or_fallback(
                activity_position.get("sold_value"),
                closed.get("sold_value"),
            )
            closed["trade_count"] = activity_position.get("trade_count")
            closed["activity_count"] = activity_position.get("activity_count")
            closed["buy_count"] = activity_position.get("buy_count")
            closed["sell_count"] = activity_position.get("sell_count")
            recovered = float_value(closed.get("sold_value")) + float_value(
                closed.get("redeemed_value")
            )
            if recovered <= 0 and float_value(activity_buy_notional) > 0:
                closed["initial_value"] = float_value(activity_buy_notional)
                closed["cost_basis_estimate"] = float_value(activity_buy_notional)
                closed["cash_pnl"] = -float_value(activity_buy_notional)
                closed["realized_pnl"] = -float_value(activity_buy_notional)
                closed["percent_pnl"] = -100.0
        merged.append(closed)
        seen.add(key)
        if condition_id:
            seen_condition_ids.add(condition_id)
    return sort_wallet_positions(merged, "abs_pnl")


def wallet_position_key(position: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(position.get("condition_id") or ""),
        str(position.get("asset") or ""),
        str(position.get("outcome") or ""),
    )


def wallet_position_holding_seconds(position: dict[str, Any]) -> float | None:
    first = parse_clickhouse_datetime(position.get("first_activity_at"))
    last = parse_clickhouse_datetime(position.get("last_activity_at") or position.get("end_date"))
    if first is None or last is None:
        return None
    return max(0.0, (last - first).total_seconds())


def wallet_max_drawdown(points: list[dict[str, Any]]) -> float | None:
    peak: float | None = None
    drawdown = 0.0
    for point in points:
        value = float_value(point.get("pnl"))
        if peak is None:
            peak = value
            continue
        peak = max(peak, value)
        drawdown = min(drawdown, value - peak)
    return abs(drawdown) if drawdown < 0 else None


def wallet_sharpe_ratio(points: list[dict[str, Any]]) -> float | None:
    ordered = [
        point
        for point in points
        if int_value(point.get("timestamp")) > 0 and point.get("pnl") is not None
    ]
    if len(ordered) < 3:
        return None
    deltas = [
        float_value(current.get("pnl")) - float_value(previous.get("pnl"))
        for previous, current in zip(ordered, ordered[1:])
    ]
    if len(deltas) < 2:
        return None
    mean_delta = sum(deltas) / len(deltas)
    variance = sum((value - mean_delta) ** 2 for value in deltas) / (len(deltas) - 1)
    if variance <= 0:
        return None
    return mean_delta / math.sqrt(variance) * math.sqrt(len(deltas))


def update_group_activity_bounds(group: dict[str, Any], timestamp: str | None) -> None:
    if not timestamp:
        return
    first = str(group.get("first_activity_at") or "")
    last = str(group.get("last_activity_at") or "")
    if not first or timestamp < first:
        group["first_activity_at"] = timestamp
    if not last or timestamp > last:
        group["last_activity_at"] = timestamp


def activity_raw_json(row: dict[str, Any]) -> dict[str, Any]:
    raw_json = row.get("raw_json")
    if isinstance(raw_json, dict):
        return raw_json
    try:
        parsed = json.loads(str(raw_json or "{}"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def wallet_positions_from_snapshot(snapshot: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not snapshot:
        return []
    raw_json = snapshot.get("raw_json")
    if not raw_json:
        return []
    try:
        payload = json.loads(str(raw_json))
    except json.JSONDecodeError:
        return []
    raw_positions = payload.get("positions") if isinstance(payload, dict) else None
    if not isinstance(raw_positions, list):
        return []

    positions = []
    for item in raw_positions:
        if not isinstance(item, dict):
            continue
        size = float_value(item.get("size"))
        current_value = float_value(item.get("currentValue"))
        cash_pnl = float_value(item.get("cashPnl"))
        avg_price = float_value(item.get("avgPrice"))
        cur_price = float_value(item.get("curPrice"))
        initial_value = float_value(item.get("initialValue"))
        position = {
            "asset": str(item.get("asset") or ""),
            "condition_id": str(item.get("conditionId") or item.get("condition_id") or ""),
            "title": str(item.get("title") or ""),
            "slug": str(item.get("slug") or ""),
            "event_id": str(item.get("eventId") or item.get("event_id") or ""),
            "event_slug": str(item.get("eventSlug") or item.get("event_slug") or ""),
            "outcome": str(item.get("outcome") or ""),
            "opposite_outcome": str(item.get("oppositeOutcome") or ""),
            "size": size,
            "avg_price": avg_price,
            "cur_price": cur_price,
            "initial_value": initial_value,
            "current_value": current_value,
            "cash_pnl": cash_pnl,
            "percent_pnl": float_value(item.get("percentPnl")),
            "realized_pnl": float_value(item.get("realizedPnl")),
            "percent_realized_pnl": float_value(item.get("percentRealizedPnl")),
            "total_bought": float_value(item.get("totalBought")),
            "redeemable": bool(item.get("redeemable")),
            "mergeable": bool(item.get("mergeable")),
            "negative_risk": bool(item.get("negativeRisk")),
            "end_date": item.get("endDate"),
            "icon": str(item.get("icon") or ""),
        }
        position["is_worldcup"] = (
            position["slug"].startswith("fifwc-")
            or position["event_slug"].startswith("fifwc-")
        )
        position["is_open"] = current_value > 0 or (size > 0 and cur_price > 0)
        position["is_settled_or_redeemable"] = bool(position["redeemable"]) or (
            current_value <= 0 and cur_price <= 0
        )
        position["cost_basis_estimate"] = size * avg_price if avg_price else initial_value
        positions.append(position)
    return positions


def filter_wallet_positions(positions: list[dict[str, Any]], scope: str) -> list[dict[str, Any]]:
    if scope == "open":
        return [position for position in positions if position.get("is_open")]
    if scope in ("history", "historical", "settled"):
        return [position for position in positions if position.get("is_settled_or_redeemable")]
    if scope == "worldcup":
        return [position for position in positions if position.get("is_worldcup")]
    return positions


def sort_wallet_positions(positions: list[dict[str, Any]], sort: str) -> list[dict[str, Any]]:
    key_name = {
        "pnl": "cash_pnl",
        "cash_pnl": "cash_pnl",
        "loss": "cash_pnl",
        "current_value": "current_value",
        "value": "current_value",
        "size": "size",
        "percent_pnl": "percent_pnl",
        "abs_pnl": "cash_pnl",
    }.get(sort, "current_value")
    reverse = sort != "loss"
    return sorted(
        positions,
        key=lambda position: abs(float_value(position.get(key_name)))
        if sort == "abs_pnl"
        else float_value(position.get(key_name)),
        reverse=reverse,
    )


def summarize_wallet_positions(positions: list[dict[str, Any]]) -> dict[str, Any]:
    open_positions = [position for position in positions if position.get("is_open")]
    worldcup_positions = [position for position in positions if position.get("is_worldcup")]
    return {
        "position_count": len(positions),
        "open_position_count": len(open_positions),
        "historical_or_redeemable_position_count": len(positions) - len(open_positions),
        "positive_pnl_position_count": sum(
            1 for position in positions if float_value(position.get("cash_pnl")) > 0
        ),
        "negative_pnl_position_count": sum(
            1 for position in positions if float_value(position.get("cash_pnl")) < 0
        ),
        "worldcup_position_count": len(worldcup_positions),
        "current_value": sum(float_value(position.get("current_value")) for position in positions),
        "open_current_value": sum(
            float_value(position.get("current_value")) for position in open_positions
        ),
        "cash_pnl": sum(float_value(position.get("cash_pnl")) for position in positions),
        "open_cash_pnl": sum(float_value(position.get("cash_pnl")) for position in open_positions),
        "worldcup_cash_pnl": sum(
            float_value(position.get("cash_pnl")) for position in worldcup_positions
        ),
        "worldcup_current_value": sum(
            float_value(position.get("current_value")) for position in worldcup_positions
        ),
    }


def wallet_pnl_points_from_snapshot(
    snapshot: dict[str, Any] | None,
    limit: int,
) -> list[dict[str, Any]]:
    if not snapshot:
        return []
    raw_json = snapshot.get("raw_json")
    if not raw_json:
        return []
    try:
        payload = json.loads(str(raw_json))
    except json.JSONDecodeError:
        return []
    points = payload.get("points") if isinstance(payload, dict) else None
    if not isinstance(points, list):
        return []
    normalized = []
    for point in points:
        if not isinstance(point, dict):
            continue
        timestamp = int_value(point.get("t"))
        normalized.append(
            {
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp, UTC).isoformat()
                if timestamp
                else None,
                "pnl": float_value(point.get("p")),
            }
        )
    return normalized[-limit:]


def wallet_pnl_delta(points: list[dict[str, Any]], *, days: int) -> float | None:
    dated_points = [
        point
        for point in points
        if int_value(point.get("timestamp")) > 0 and point.get("pnl") is not None
    ]
    if len(dated_points) < 2:
        return None
    latest = dated_points[-1]
    cutoff = int_value(latest.get("timestamp")) - days * 86400
    baseline = None
    for point in dated_points:
        if int_value(point.get("timestamp")) <= cutoff:
            baseline = point
        else:
            break
    if baseline is None:
        return 0.0
    return float_value(latest.get("pnl")) - float_value(baseline.get("pnl"))


def datetime_lag_minutes(older: Any, newer: Any) -> float | None:
    older_dt = parse_clickhouse_datetime(older)
    newer_dt = parse_clickhouse_datetime(newer)
    if older_dt is None or newer_dt is None:
        return None
    return max(0.0, (newer_dt - older_dt).total_seconds() / 60.0)


def parse_clickhouse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        try:
            return datetime.fromtimestamp(timestamp, UTC)
        except (OSError, OverflowError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return parse_clickhouse_datetime(int(text))
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def wallet_data_freshness_status(
    pnl_captured_at: Any,
    portfolio_captured_at: Any,
    pnl_lag_minutes: float | None,
) -> str:
    if not pnl_captured_at and not portfolio_captured_at:
        return "missing"
    if not pnl_captured_at:
        return "pnl_missing"
    if not portfolio_captured_at:
        return "portfolio_missing"
    if pnl_lag_minutes is not None and pnl_lag_minutes > 10:
        return "pnl_lagging"
    return "ok"


def empty_wallet_activity_summary(user: str) -> dict[str, Any]:
    return {
        "user_address": user,
        "activity_count": 0,
        "trade_activity_count": 0,
        "buy_count": 0,
        "sell_count": 0,
        "traded_size": 0.0,
        "traded_notional": 0.0,
        "buy_notional": 0.0,
        "sell_notional": 0.0,
        "activity_count_24h": 0,
        "trade_activity_count_24h": 0,
        "traded_notional_24h": 0.0,
        "trade_activity_count_7d": 0,
        "traded_notional_7d": 0.0,
        "avg_bet": 0.0,
        "latest_activity_type": "",
        "latest_side": "",
        "first_activity_at": None,
        "last_activity_at": None,
    }


def empty_wallet_reputation(user: str) -> dict[str, Any]:
    return {
        "user_address": user,
        "completed_event_count": 0,
        "profitable_event_count": 0,
        "losing_event_count": 0,
        "win_rate": None,
        "realized_pnl": 0.0,
        "avg_event_roi": None,
        "best_event_pnl": 0.0,
        "worst_event_pnl": 0.0,
        "active_position_count": 0,
        "active_event_count": 0,
        "active_unrealized_pnl_estimate": 0.0,
        "favorite_category": "",
        "favorite_category_notional": 0.0,
        "first_trade_at": None,
        "last_trade_at": None,
    }


def normalize_wallet_activity_summary(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for key in (
        "activity_count",
        "trade_activity_count",
        "buy_count",
        "sell_count",
        "activity_count_24h",
        "trade_activity_count_24h",
        "trade_activity_count_7d",
    ):
        normalized[key] = int_value(row.get(key))
    for key in (
        "traded_size",
        "traded_notional",
        "buy_notional",
        "sell_notional",
        "traded_notional_24h",
        "traded_notional_7d",
        "avg_bet",
    ):
        normalized[key] = float_value(row.get(key))
    return normalized


def normalize_wallet_activity_type(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["count"] = int_value(row.get("count"))
    normalized["size"] = float_value(row.get("size"))
    normalized["notional"] = float_value(row.get("notional"))
    return normalized


def limit_worldcup_rankings(output: dict[str, Any], limit: int) -> dict[str, Any]:
    limited = dict(output)
    for list_name in RANKING_LIST_NAMES:
        rows = output.get(list_name, [])
        limited[list_name] = rows[:limit] if isinstance(rows, list) else []
    return limited


def compact_event_smart_wallet_options(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    empty_side = {
        "smart_wallet_count": 0,
        "smart_amount": 0.0,
        "whale_wallet_count": 0,
        "whale_amount": 0.0,
    }
    options_by_question: dict[str, dict[str, Any]] = {}
    ordered_questions: list[str] = []

    for row in rows:
        question = str(row.get("market_question", ""))
        if not question:
            continue
        if question not in options_by_question:
            options_by_question[question] = {
                "market_question": question,
                "yes": dict(empty_side),
                "no": dict(empty_side),
            }
            ordered_questions.append(question)

        side = str(row.get("outcome_side") or row.get("token_outcome") or "").lower()
        if side not in ("yes", "no"):
            continue
        options_by_question[question][side] = {
            "smart_wallet_count": int_value(row.get("smart_wallet_count")),
            "smart_amount": float_value(row.get("smart_amount")),
            "whale_wallet_count": int_value(row.get("whale_wallet_count")),
            "whale_amount": float_value(row.get("whale_amount")),
        }

    return [options_by_question[question] for question in ordered_questions]


def event_smart_wallet_options_response_body(
    event: dict[str, Any],
    options: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "event": event,
        "options": options,
        "data_status": "ok" if options else "no_options",
        "message": "" if options else "No option data is available for this event yet.",
    }


def empty_event_smart_wallet_options_response(
    event_ref: str,
    *,
    status: str,
) -> dict[str, Any]:
    if status == "missing_event":
        message = "Missing event query parameter."
    else:
        message = "No data is available for this event yet."
    return {
        "event": {
            "event_id": "",
            "slug": event_ref,
            "title": event_ref,
            "category": "",
            "active": False,
            "closed": False,
            "start_time": None,
            "end_time": None,
            "updated_at": None,
        },
        "options": [],
        "data_status": status,
        "message": message,
    }


def empty_event_smart_wallet_summary(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_slug": event.get("slug", ""),
        "event_title": event.get("title", ""),
        "smart_trade_count": 0,
        "smart_wallet_count": 0,
        "smart_traded_notional": 0.0,
        "smart_buy_notional": 0.0,
        "smart_sell_notional": 0.0,
        "smart_net_shares": 0.0,
        "latest_smart_trade_at": None,
        "smart_wallets_24h": 0,
        "smart_trade_count_24h": 0,
        "smart_traded_notional_24h": 0.0,
    }


def merge_trader_profile(
    profile: dict[str, Any] | None,
    activity: dict[str, Any] | None,
    chain: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if profile is None and activity is None and chain is None:
        return None
    if profile is None:
        merged = dict(activity or chain or {})
    else:
        merged = dict(profile)
    if activity is None:
        activity = {}

    profile_first = timestamp_key(merged.get("first_trade_at"))
    activity_first = timestamp_key(activity.get("first_trade_at"))
    if activity_first and (not profile_first or activity_first < profile_first):
        merged["first_trade_at"] = activity.get("first_trade_at")
    if timestamp_key(activity.get("last_trade_at")) >= timestamp_key(merged.get("last_trade_at")):
        for key in (
            "user_address",
            "trade_count",
            "buy_count",
            "sell_count",
            "traded_size",
            "traded_notional",
            "last_trade_at",
            "trade_count_24h",
            "traded_notional_24h",
            "buy_notional_24h",
            "sell_notional_24h",
            "latest_action",
            "data_lag_seconds",
        ):
            if key in activity:
                merged[key] = activity[key]
    if float_value(activity.get("position_count")) > float_value(merged.get("position_count")):
        for key in ("position_count", "current_value", "total_pnl", "last_position_at"):
            if key in activity:
                merged[key] = activity[key]
    if chain is not None and float_value(chain.get("chain_fill_count")) > float_value(
        merged.get("chain_fill_count")
    ):
        for key in (
            "chain_fill_count",
            "chain_traded_size",
            "chain_traded_notional",
            "chain_position_size",
            "chain_current_value",
            "chain_net_cashflow",
            "chain_mark_to_market_pnl",
            "last_chain_fill_block",
        ):
            if key in chain:
                merged[key] = chain[key]
    return merged


def merge_portfolio_profile(
    profile: dict[str, Any] | None,
    portfolio: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if profile is None:
        return dict(portfolio) if portfolio is not None else None
    if portfolio is None:
        return profile
    merged = dict(profile)
    for key in (
        "position_count",
        "positions_value",
        "portfolio_value",
        "available_balance",
        "total_pnl",
        "last_position_at",
    ):
        if key in portfolio:
            merged[key] = portfolio[key]
    if "positions_value" in portfolio:
        merged["current_value"] = portfolio["positions_value"]
    return merged


def timestamp_key(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def float_value(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def value_or_fallback(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    try:
        if isinstance(value, float) and math.isnan(value):
            return fallback
    except TypeError:
        return fallback
    return value


def int_value(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def collect_system_stats() -> dict[str, Any]:
    return {
        "collected_at": datetime.now(UTC).isoformat(),
        "cpu": cpu_stats(),
        "memory": memory_stats(),
        "disk": disk_stats("/"),
        "uptime_seconds": read_uptime_seconds(),
    }


def cpu_stats(interval_seconds: float = 0.1) -> dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    load_avg = read_load_avg()
    percent = read_cpu_percent(interval_seconds)
    return {
        "percent": percent,
        "count": cpu_count,
        "load_avg_1m": load_avg[0] if load_avg else None,
        "load_avg_5m": load_avg[1] if load_avg else None,
        "load_avg_15m": load_avg[2] if load_avg else None,
        "load_per_cpu_percent": ratio_percent(load_avg[0], cpu_count) if load_avg else None,
    }


def read_cpu_percent(interval_seconds: float) -> float | None:
    first = read_proc_cpu_times()
    if first is None:
        return None
    time.sleep(interval_seconds)
    second = read_proc_cpu_times()
    if second is None:
        return None
    busy_delta = second[0] - first[0]
    total_delta = second[1] - first[1]
    if total_delta <= 0:
        return None
    return ratio_percent(busy_delta, total_delta)


def read_proc_cpu_times() -> tuple[int, int] | None:
    try:
        with open("/proc/stat", encoding="utf-8") as proc_stat:
            line = proc_stat.readline()
    except OSError:
        return None
    parts = line.split()
    if not parts or parts[0] != "cpu":
        return None
    try:
        values = [int(value) for value in parts[1:]]
    except ValueError:
        return None
    if len(values) < 4:
        return None
    idle_all = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    busy = total - idle_all
    return busy, total


def read_load_avg() -> tuple[float, float, float] | None:
    try:
        return os.getloadavg()
    except (AttributeError, OSError):
        return None


def memory_stats() -> dict[str, Any]:
    meminfo = read_meminfo()
    total = meminfo.get("MemTotal", 0) * 1024
    available = meminfo.get("MemAvailable", meminfo.get("MemFree", 0)) * 1024
    used = max(total - available, 0) if total else 0
    return {
        "total_bytes": total,
        "used_bytes": used,
        "available_bytes": available,
        "percent": ratio_percent(used, total),
    }


def read_meminfo() -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        with open("/proc/meminfo", encoding="utf-8") as meminfo:
            for line in meminfo:
                key, _, rest = line.partition(":")
                first = rest.strip().split(" ", 1)[0]
                if first:
                    values[key] = int(first)
    except (OSError, ValueError):
        return {}
    return values


def disk_stats(path: str) -> dict[str, Any]:
    usage = shutil.disk_usage(path)
    return {
        "path": path,
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "percent": ratio_percent(usage.used, usage.total),
    }


def read_uptime_seconds() -> float | None:
    try:
        with open("/proc/uptime", encoding="utf-8") as uptime:
            raw_seconds = uptime.read().split()[0]
    except (OSError, IndexError):
        return None
    try:
        return round(float(raw_seconds), 2)
    except ValueError:
        return None


def ratio_percent(value: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return round((value / total) * 100, 2)


def param(query: dict[str, list[str]], key: str, default: str = "") -> str:
    value = query.get(key, [default])[0]
    return str(value or default)


def truthy_param(query: dict[str, list[str]], key: str) -> bool:
    return param(query, key).lower() in ("1", "true", "yes", "on")


def bool_param(query: dict[str, list[str]], key: str, default: bool) -> bool:
    raw = param(query, key, "1" if default else "0").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def wallet_screener_range_filter(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in ("1d", "1day", "day", "24h"):
        return "trades.timestamp >= now64(3) - interval 1 day"
    if normalized in ("7d", "7day", "week"):
        return "trades.timestamp >= now64(3) - interval 7 day"
    if normalized in ("30d", "30day", "month"):
        return "trades.timestamp >= now64(3) - interval 30 day"
    return ""


def wallet_screener_fifa_range_filter(value: str) -> str:
    if wallet_screener_fifa_range_is_24h(value):
        return "fifa.trade_count_24h > 0"
    return ""


def wallet_screener_fifa_range_is_24h(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in ("1d", "1day", "day", "24h")


def wallet_screener_category_filter(value: str) -> str:
    return wallet_category_filter(value, raw_json_expr="trades.raw_json")


def wallet_category_filter(value: str, *, raw_json_expr: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized or normalized in ("all", "全部"):
        return ""
    terms_by_category = {
        "sports": [
            "Sports",
            "NBA",
            "Olympics",
            "World Cup",
            "FIFA",
            "soccer",
            "football",
            "basketball",
            "tennis",
            "golf",
            "ufc",
            "f1",
            "formula",
        ],
        "体育": [
            "Sports",
            "NBA",
            "Olympics",
            "World Cup",
            "FIFA",
            "soccer",
            "football",
            "basketball",
            "tennis",
            "golf",
            "ufc",
            "f1",
            "formula",
        ],
        "politics": [
            "Politics",
            "US-current-affairs",
            "Global Politics",
            "election",
            "congress",
            "senate",
            "trump",
            "biden",
            "referendum",
            "government",
        ],
        "政治": [
            "Politics",
            "US-current-affairs",
            "Global Politics",
            "election",
            "congress",
            "senate",
            "trump",
            "biden",
            "referendum",
            "government",
        ],
        "crypto": [
            "Crypto",
            "Bitcoin",
            "BTC",
            "Ethereum",
            "ETH",
            "Solana",
            "SOL",
            "XRP",
            "Dogecoin",
            "crypto",
        ],
        "加密货币": [
            "Crypto",
            "Bitcoin",
            "BTC",
            "Ethereum",
            "ETH",
            "Solana",
            "SOL",
            "XRP",
            "Dogecoin",
            "crypto",
        ],
        "esports": ["Esports", "E-Sports", "gaming", "LoL", "Dota", "Valorant", "Counter-Strike"],
        "电竞": ["Esports", "E-Sports", "gaming", "LoL", "Dota", "Valorant", "Counter-Strike"],
        "iran": ["Iran", "Iranian"],
        "伊朗": ["Iran", "Iranian"],
        "finance": [
            "Finance",
            "Business",
            "stock",
            "NASDAQ",
            "S&P",
            "Fed",
            "rates",
            "Treasury",
        ],
        "金融": [
            "Finance",
            "Business",
            "stock",
            "NASDAQ",
            "S&P",
            "Fed",
            "rates",
            "Treasury",
        ],
        "geopolitics": [
            "Global Politics",
            "geopolitics",
            "Ukraine",
            "Russia",
            "Israel",
            "Gaza",
            "China",
            "war",
            "tariff",
        ],
        "地缘政治": [
            "Global Politics",
            "geopolitics",
            "Ukraine",
            "Russia",
            "Israel",
            "Gaza",
            "China",
            "war",
            "tariff",
        ],
        "tech": ["Tech", "Science", "AI", "OpenAI", "Apple", "Tesla", "SpaceX"],
        "科技": ["Tech", "Science", "AI", "OpenAI", "Apple", "Tesla", "SpaceX"],
        "culture": [
            "Pop-Culture",
            "Culture",
            "Art",
            "music",
            "movie",
            "celebrity",
            "Oscars",
        ],
        "文化": [
            "Pop-Culture",
            "Culture",
            "Art",
            "music",
            "movie",
            "celebrity",
            "Oscars",
        ],
        "economy": ["Economy", "Business", "GDP", "inflation", "recession", "unemployment", "CPI"],
        "经济": ["Economy", "Business", "GDP", "inflation", "recession", "unemployment", "CPI"],
        "weather": ["Weather", "temperature", "hurricane", "rain", "snow", "storm"],
        "天气": ["Weather", "temperature", "hurricane", "rain", "snow", "storm"],
        "election": ["Election", "election", "primary", "vote", "winner", "nominee"],
        "选举": ["Election", "election", "primary", "vote", "winner", "nominee"],
        "mentions": ["mention", "mentions", "say", "tweet", "Truth Social", "post"],
        "提及": ["mention", "mentions", "say", "tweet", "Truth Social", "post"],
    }
    terms = terms_by_category.get(normalized, [value])
    haystack = (
        "concat("
        "events.category, ' ', events.title, ' ', markets.question, ' ', "
        f"JSONExtractString({raw_json_expr}, 'title'), ' ', "
        f"JSONExtractString({raw_json_expr}, 'slug'), ' ', "
        f"JSONExtractString({raw_json_expr}, 'eventSlug')"
        ")"
    )
    return "(" + " or ".join(
        f"positionCaseInsensitive({haystack}, {ch_string(term)}) > 0"
        for term in terms
    ) + ")"


def int_param(query: dict[str, list[str]], key: str, default: int, *, maximum: int) -> int:
    try:
        value = int(param(query, key, str(default)))
    except ValueError:
        value = default
    return max(1, min(value, maximum))


def bounded_int_param(
    query: dict[str, list[str]],
    key: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    try:
        value = int(param(query, key, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(value, maximum))


def float_param(
    query: dict[str, list[str]],
    key: str,
    default: float,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    try:
        value = float(param(query, key, str(default)))
    except ValueError:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def normalize_polycop_segment(value: str) -> str:
    segment = str(value or "").strip().lower().replace("-", "_")
    if segment in ("stable", "flow", "burst", "watch", "all", "ai_top"):
        return segment
    return "ai_top"


def polycop_wallet_segment(detail: dict[str, Any], segment: str) -> list[dict[str, Any]]:
    if segment == "all":
        wallets = detail.get("wallets", [])
    elif segment == "watch":
        wallets = [
            wallet
            for wallet in detail.get("wallets", [])
            if "watch" in list(wallet.get("segments") or [])
        ]
    else:
        wallets = (detail.get("segments") or {}).get(segment, [])
    return [wallet for wallet in wallets if isinstance(wallet, dict)]


def polycop_wallet_matches_search(wallet: dict[str, Any], search: str) -> bool:
    fields = (
        wallet.get("address"),
        wallet.get("user_name"),
        wallet.get("x_name"),
        wallet.get("primary_segment"),
    )
    return any(search in str(field or "").lower() for field in fields)


def ch_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
