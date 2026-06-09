from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
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
        if path == "/stats/system":
            return ApiResponse(HTTPStatus.OK, {"system": collect_system_stats()})
        if path == "/tasks/progress":
            return ApiResponse(HTTPStatus.OK, self.tasks_progress(query))
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
        if path == "/events/timeline":
            return ApiResponse(HTTPStatus.OK, {"events": self.event_timeline(query)})
        if path == "/events/wallet-flow":
            return ApiResponse(HTTPStatus.OK, {"wallets": self.event_wallet_flow(query)})
        if path == "/events/pnl-leaderboard":
            return ApiResponse(HTTPStatus.OK, {"wallets": self.event_pnl_leaderboard(query)})
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
        if path == "/wallets/live-positions":
            return ApiResponse(HTTPStatus.OK, {"positions": self.wallet_live_positions(query)})
        if path == "/wallets/smart-money/activity":
            return ApiResponse(HTTPStatus.OK, {"activity": self.smart_money_activity(query)})
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
              (select max(collected_at) from raw_ingest_log) as last_ingested_at
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
        return PostgresTaskStore(
            dsn=self.settings.postgres_dsn,
            node_id="api",
        ).progress(recent_limit=recent_limit)

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
              (select uniqExact(user_address) from fact_trade where user_address != '' and timestamp >= now64(3) - interval 24 hour) as active_wallets_24h,
              (select sum(notional) from fact_trade where timestamp >= now64(3) - interval 24 hour) as volume_24h,
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
              from fact_trade
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
              from fact_trade as trades
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
        text = param(query, "q").lower()
        limit = int_param(query, "limit", 25, maximum=100)
        where = "where 1 = 1"
        if text:
            escaped = ch_string(text)
            where += (
                " and (positionCaseInsensitive(question, "
                f"{escaped}) > 0 or positionCaseInsensitive(slug, {escaped}) > 0"
                f" or positionCaseInsensitive(condition_id, {escaped}) > 0"
                f" or positionCaseInsensitive(category, {escaped}) > 0)"
            )
        sql = f"""
            select
              market_id,
              event_id,
              condition_id,
              question,
              slug,
              category,
              active,
              closed,
              volume,
              liquidity
            from
            (
              select
                markets.market_id as market_id,
                markets.event_id as event_id,
                markets.condition_id as condition_id,
                markets.question as question,
                markets.slug as slug,
                if(events.category = '', 'Uncategorized', events.category) as category,
                markets.active as active,
                markets.closed as closed,
                markets.volume as volume,
                markets.liquidity as liquidity
              from dim_market as markets final
              left join dim_event as events final on markets.event_id = events.event_id
            ) as base
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
            where += f" and markets.market_id = {ch_string(market_id)}"
        elif condition_id:
            where += f" and markets.condition_id = {ch_string(condition_id)}"
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


def int_param(query: dict[str, list[str]], key: str, default: int, *, maximum: int) -> int:
    try:
        value = int(param(query, key, str(default)))
    except ValueError:
        value = default
    return max(1, min(value, maximum))


def ch_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
