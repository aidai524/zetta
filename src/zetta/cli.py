from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from zetta.api import serve_api
from zetta.chain.polymarket import (
    FEE_CHARGED_TOPIC,
    ORDER_FILLED_TOPIC,
    ORDERS_MATCHED_TOPIC,
    PAYOUT_REDEMPTION_TOPIC,
    POSITIONS_MERGE_TOPIC,
    POSITION_SPLIT_TOPIC,
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
)
from zetta.chain.rpc import PolygonRpcClient
from zetta.collectors.chain import ChainCollector
from zetta.collectors.clob import ClobCollector
from zetta.collectors.data import DataCollector
from zetta.collectors.gamma import GammaCollector
from zetta.collectors.ws import MarketWebSocketCollector
from zetta.config import Settings, settings
from zetta.loaders.chain import ChainRawLoader
from zetta.loaders.clob import ClobRawLoader
from zetta.loaders.data import DataRawLoader
from zetta.loaders.gamma import GammaRawLoader
from zetta.loaders.marts import MartBuilder
from zetta.polymarket import PolymarketClient
from zetta.official_trade_feed import POLYMARKET_RTDS_URL, serve_official_trade_feed
from zetta.realtime.orderbook import reconciliation_diff, reconstruct_ws_market_raw, rest_book_summary
from zetta.realtime_trades import serve_trade_stream
from zetta.scheduler.runner import TaskRunner
from zetta.scheduler.tasks import LocalRunStore, LocalTaskStore, PostgresTaskStore, Task
from zetta.streams import publish_ws_market_raw
from zetta.storage.clickhouse import ClickHouseWriter
from zetta.storage.raw import RawJsonlWriter
from zetta.storage.redpanda import RpkPublisher
from zetta.storage.state import LocalStateStore
from zetta.tracked_wallets import TrackedWalletStore
from zetta.worldcup_wallets import base_match_slug


FRONTIER_GAMMA_PRIORITY = 1
FRONTIER_EVENT_PRIORITY = 2
FRONTIER_TRADES_PRIORITY = 3
FRONTIER_PRICE_HISTORY_PRIORITY = 4
FRONTIER_BOOK_PRIORITY = 5
WALLET_REFRESH_PRIORITY = 2
WALLET_EXPLICIT_REFRESH_PRIORITY = -2
ACTIVE_EVENT_WALLET_PRIORITY = -1
WALLET_PORTFOLIO_PRIORITY = 0
WALLET_PNL_PRIORITY = -1
UNUSUAL_BETTING_REFRESH_PRIORITY = -1
DISCOVERY_PRIORITY = 50
HISTORY_BACKFILL_PRIORITY = 100
CHAIN_BACKFILL_PRIORITY = 150


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    app_settings = Settings(
        gamma_base_url=args.gamma_base_url,
        data_base_url=args.data_base_url,
        clob_base_url=args.clob_base_url,
        clob_ws_market_url=args.clob_ws_market_url,
        polygon_rpc_url=args.polygon_rpc_url,
        clickhouse_host=args.clickhouse_host,
        clickhouse_port=args.clickhouse_port,
        clickhouse_user=args.clickhouse_user,
        clickhouse_password=args.clickhouse_password,
        clickhouse_database=args.clickhouse_database,
        postgres_dsn=args.postgres_dsn,
        raw_data_dir=Path(args.raw_data_dir),
        state_dir=Path(args.state_dir),
        raw_chunk_records=args.raw_chunk_records,
        raw_chunk_seconds=args.raw_chunk_seconds,
        request_timeout_seconds=args.timeout,
        user_agent=args.user_agent,
        http_resolve_overrides=args.http_resolve_overrides,
    )
    try:
        result = args.func(args, app_settings)
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130
    if result is not None:
        print_json(result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="zetta")
    parser.add_argument("--gamma-base-url", default=settings.gamma_base_url)
    parser.add_argument("--data-base-url", default=settings.data_base_url)
    parser.add_argument("--clob-base-url", default=settings.clob_base_url)
    parser.add_argument("--clob-ws-market-url", default=settings.clob_ws_market_url)
    parser.add_argument("--polygon-rpc-url", default=settings.polygon_rpc_url)
    parser.add_argument("--clickhouse-host", default=settings.clickhouse_host)
    parser.add_argument("--clickhouse-port", type=int, default=settings.clickhouse_port)
    parser.add_argument("--clickhouse-user", default=settings.clickhouse_user)
    parser.add_argument("--clickhouse-password", default=settings.clickhouse_password)
    parser.add_argument("--clickhouse-database", default=settings.clickhouse_database)
    parser.add_argument("--postgres-dsn", default=settings.postgres_dsn)
    parser.add_argument("--raw-data-dir", default=str(settings.raw_data_dir))
    parser.add_argument("--state-dir", default=str(settings.state_dir))
    parser.add_argument("--raw-chunk-records", type=int, default=settings.raw_chunk_records)
    parser.add_argument("--raw-chunk-seconds", type=float, default=settings.raw_chunk_seconds)
    parser.add_argument("--task-file", default="data/state/tasks.json")
    parser.add_argument("--task-store", choices=["local", "postgres"], default="local")
    parser.add_argument("--node-id", default="local-node")
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--timeout", type=float, default=settings.request_timeout_seconds)
    parser.add_argument("--user-agent", default=settings.user_agent)
    parser.add_argument(
        "--http-resolve-overrides",
        default=settings.http_resolve_overrides,
        help="Comma-separated host:ip curl fallback overrides, e.g. gamma-api.polymarket.com:1.2.3.4.",
    )

    subparsers = parser.add_subparsers(required=True)

    endpoints = subparsers.add_parser("endpoints", help="Print configured API endpoints.")
    endpoints.set_defaults(func=cmd_endpoints)

    db = subparsers.add_parser("db", help="Database utility commands.")
    db_subparsers = db.add_subparsers(required=True)

    ping = db_subparsers.add_parser("ping", help="Ping ClickHouse.")
    ping.set_defaults(func=cmd_db_ping)

    migrate = db_subparsers.add_parser("migrate", help="Apply ClickHouse schema.")
    migrate.add_argument("--schema-path", default="infra/clickhouse/schema.sql")
    migrate.set_defaults(func=cmd_db_migrate)

    collect = subparsers.add_parser("collect", help="Collect Polymarket data.")
    collect_subparsers = collect.add_subparsers(required=True)

    gamma_events = collect_subparsers.add_parser("gamma-events", help="Collect Gamma events.")
    add_gamma_args(gamma_events)
    gamma_events.set_defaults(func=lambda args, s: cmd_gamma(args, s, "events"))

    gamma_markets = collect_subparsers.add_parser("gamma-markets", help="Collect Gamma markets.")
    add_gamma_args(gamma_markets)
    gamma_markets.set_defaults(func=lambda args, s: cmd_gamma(args, s, "markets"))

    trades = collect_subparsers.add_parser("trades", help="Collect public trade records.")
    trades.add_argument("--page-limit", type=int, default=500)
    trades.add_argument("--max-pages", type=int, default=1)
    trades.add_argument("--resume", action="store_true")
    trades.add_argument("--user")
    trades.add_argument("--market")
    trades.add_argument("--event-id")
    trades.set_defaults(func=cmd_trades)

    activity = collect_subparsers.add_parser("activity", help="Collect public user activity.")
    activity.add_argument("--user", required=True)
    activity.add_argument("--page-limit", type=int, default=500)
    activity.add_argument("--max-pages", type=int, default=1)
    activity.add_argument("--resume", action="store_true")
    activity.set_defaults(func=cmd_activity)

    holders = collect_subparsers.add_parser("holders", help="Collect market holders.")
    holders.add_argument("--market", required=True)
    holders.add_argument("--limit", type=int, default=500)
    holders.set_defaults(func=cmd_holders)

    market_positions = collect_subparsers.add_parser(
        "market-positions", help="Collect market positions."
    )
    market_positions.add_argument("--market", required=True)
    market_positions.add_argument("--limit", type=int, default=500)
    market_positions.set_defaults(func=cmd_market_positions)

    positions = collect_subparsers.add_parser("positions", help="Collect wallet positions.")
    positions.add_argument("--user", required=True)
    positions.set_defaults(func=cmd_positions)

    wallet_portfolio = collect_subparsers.add_parser(
        "wallet-portfolio", help="Collect wallet positions, value, and available balance."
    )
    wallet_portfolio.add_argument("--user", required=True)
    wallet_portfolio.set_defaults(func=cmd_wallet_portfolio)

    wallet_pnl = collect_subparsers.add_parser("wallet-pnl", help="Collect wallet PnL only.")
    wallet_pnl.add_argument("--user", required=True)
    wallet_pnl.add_argument("--interval", default="all")
    wallet_pnl.add_argument("--fidelity", default="1d")
    wallet_pnl.set_defaults(func=cmd_wallet_pnl)

    oi = collect_subparsers.add_parser("open-interest", help="Collect market open interest.")
    oi.add_argument("--market")
    oi.set_defaults(func=cmd_open_interest)

    prices = collect_subparsers.add_parser("prices-history", help="Collect CLOB price history.")
    prices.add_argument("--token-id", required=True)
    prices.add_argument("--start-ts", type=int)
    prices.add_argument("--end-ts", type=int)
    prices.add_argument("--interval")
    prices.add_argument("--fidelity", type=int)
    prices.set_defaults(func=cmd_prices_history)

    prices_batch = collect_subparsers.add_parser(
        "prices-history-batch", help="Collect CLOB price history for discovered tokens."
    )
    prices_batch.add_argument("--limit", type=int, default=10)
    prices_batch.add_argument("--active-only", action="store_true")
    prices_batch.add_argument("--interval", default="all")
    prices_batch.add_argument("--fidelity", type=int)
    prices_batch.add_argument("--sleep-seconds", type=float, default=0.1)
    prices_batch.set_defaults(func=cmd_prices_history_batch)

    book = collect_subparsers.add_parser("book", help="Collect one CLOB order book snapshot.")
    book.add_argument("--token-id", required=True)
    book.set_defaults(func=cmd_book)

    books_batch = collect_subparsers.add_parser(
        "books-batch", help="Collect CLOB order books for discovered tokens."
    )
    books_batch.add_argument("--limit", type=int, default=10)
    books_batch.add_argument("--active-only", action="store_true")
    books_batch.add_argument("--sleep-seconds", type=float, default=0.1)
    books_batch.set_defaults(func=cmd_books_batch)

    ws_market = collect_subparsers.add_parser(
        "ws-market", help="Collect CLOB market WebSocket messages."
    )
    ws_market.add_argument(
        "--token-id",
        action="append",
        dest="token_ids",
        default=[],
        help="Subscribe to a token ID. Repeat or pass comma-separated values.",
    )
    ws_market.add_argument("--limit", type=int, default=2, help="Discover token IDs when omitted.")
    ws_market.add_argument("--active-only", action="store_true")
    ws_market.add_argument(
        "--max-messages",
        type=int,
        default=10,
        help="Messages to persist. Use 0 to run until --max-seconds elapses.",
    )
    ws_market.add_argument(
        "--max-seconds",
        type=float,
        default=30.0,
        help="Wall clock collection limit. Use 0 for no timeout.",
    )
    ws_market.add_argument("--include-heartbeats", action="store_true")
    ws_market.add_argument("--disable-custom-feature", action="store_true")
    ws_market.set_defaults(func=cmd_ws_market)

    chain_logs = collect_subparsers.add_parser("chain-logs", help="Collect Polygon raw logs.")
    chain_logs.add_argument("--from-block", type=int, required=True)
    chain_logs.add_argument("--to-block", type=int, required=True)
    chain_logs.add_argument("--address", action="append", dest="addresses", default=[])
    chain_logs.add_argument("--topic", action="append", dest="topics", default=[])
    chain_logs.set_defaults(func=cmd_chain_logs)

    chain = subparsers.add_parser("chain", help="Polygon chain utility commands.")
    chain_subparsers = chain.add_subparsers(required=True)

    block_number = chain_subparsers.add_parser("block-number", help="Print latest Polygon block.")
    block_number.set_defaults(func=cmd_chain_block_number)

    frontier = chain_subparsers.add_parser(
        "frontier", help="Collect and decode recent Polymarket chain logs."
    )
    frontier.add_argument("--lookback-blocks", type=int, default=300)
    frontier.add_argument("--confirmations", type=int, default=64)
    frontier.add_argument("--overlap-blocks", type=int, default=20)
    frontier.add_argument("--step-blocks", type=int, default=300)
    frontier.add_argument("--exchange-address", action="append", dest="exchange_addresses", default=[])
    frontier.add_argument("--conditional-tokens-address", default=POLYMARKET_CTF_ADDRESS)
    frontier.add_argument("--batch-size", type=int, default=10_000)
    frontier.set_defaults(func=cmd_chain_frontier)

    discover = subparsers.add_parser("discover", help="Discover work from analytical tables.")
    discover_subparsers = discover.add_subparsers(required=True)

    tokens = discover_subparsers.add_parser("tokens", help="Discover CLOB token IDs.")
    tokens.add_argument("--limit", type=int, default=100)
    tokens.add_argument("--active-only", action="store_true")
    tokens.set_defaults(func=cmd_discover_tokens)

    markets = discover_subparsers.add_parser("markets", help="Discover condition IDs.")
    markets.add_argument("--limit", type=int, default=100)
    markets.add_argument("--active-only", action="store_true")
    markets.set_defaults(func=cmd_discover_markets)

    wallets = discover_subparsers.add_parser("wallets", help="Discover wallets from trades.")
    wallets.add_argument("--limit", type=int, default=100)
    wallets.set_defaults(func=cmd_discover_wallets)

    refresh = subparsers.add_parser("refresh", help="Refresh targeted derived data.")
    refresh_subparsers = refresh.add_subparsers(required=True)

    live_token_metadata = refresh_subparsers.add_parser(
        "live-token-metadata",
        help="Refresh Gamma metadata for live chain tokens missing from dim_outcome_token.",
    )
    live_token_metadata.add_argument("--lookback-minutes", type=int, default=15)
    live_token_metadata.add_argument("--limit", type=int, default=200)
    live_token_metadata.add_argument("--min-notional", type=float, default=100.0)
    live_token_metadata.add_argument("--sleep-seconds", type=float, default=0.05)
    live_token_metadata.add_argument("--load-batch-size", type=int, default=1000)
    live_token_metadata.set_defaults(func=cmd_refresh_live_token_metadata)

    polycop_wallet_signals = refresh_subparsers.add_parser(
        "polycop-wallet-signals",
        help="Refresh cached Polycop smart wallet signal rankings.",
    )
    polycop_wallet_signals.add_argument("--page-size", type=int, default=50)
    polycop_wallet_signals.add_argument("--max-pages", type=int, default=25)
    polycop_wallet_signals.add_argument("--sleep-seconds", type=float, default=0.1)
    polycop_wallet_signals.add_argument("--timeout-seconds", type=float, default=20.0)
    polycop_wallet_signals.add_argument("--limit", type=int, default=500)
    polycop_wallet_signals.add_argument("--trigger-reason", default="scheduled")
    polycop_wallet_signals.set_defaults(func=cmd_refresh_polycop_wallet_signals)

    tasks = subparsers.add_parser("tasks", help="Manage local collection tasks.")
    task_subparsers = tasks.add_subparsers(required=True)

    seed = task_subparsers.add_parser("seed-basic", help="Seed basic discovery tasks.")
    seed.add_argument("--page-limit", type=int, default=100)
    seed.add_argument("--max-pages", type=int, default=1)
    seed.set_defaults(func=cmd_tasks_seed_basic)

    seed_frontier = task_subparsers.add_parser(
        "seed-frontier", help="Seed high-priority tasks for recent event-level analysis."
    )
    seed_frontier.add_argument("--event-limit", type=int, default=50)
    seed_frontier.add_argument("--condition-limit", type=int, default=50)
    seed_frontier.add_argument("--token-limit", type=int, default=100)
    seed_frontier.add_argument("--sync-mode", choices=["event", "typed"], default="event")
    seed_frontier.add_argument(
        "--include-gamma", action=argparse.BooleanOptionalAction, default=True
    )
    seed_frontier.add_argument(
        "--active-only", action=argparse.BooleanOptionalAction, default=True
    )
    seed_frontier.add_argument(
        "--include-trades", action=argparse.BooleanOptionalAction, default=True
    )
    seed_frontier.add_argument(
        "--include-price-history", action=argparse.BooleanOptionalAction, default=True
    )
    seed_frontier.add_argument(
        "--include-books", action=argparse.BooleanOptionalAction, default=True
    )
    seed_frontier.add_argument("--gamma-page-limit", type=int, default=100)
    seed_frontier.add_argument("--gamma-max-pages", type=int, default=1)
    seed_frontier.add_argument("--gamma-resume", action="store_true")
    seed_frontier.add_argument("--gamma-sleep-seconds", type=float, default=0.0)
    seed_frontier.add_argument("--trade-page-limit", type=int, default=500)
    seed_frontier.add_argument("--trade-max-pages", type=int, default=1)
    seed_frontier.add_argument("--price-interval", default="1d")
    seed_frontier.add_argument("--price-fidelity", type=int)
    seed_frontier.add_argument("--holders-limit", type=int, default=500)
    seed_frontier.add_argument("--positions-limit", type=int, default=500)
    seed_frontier.add_argument(
        "--include-holders", action=argparse.BooleanOptionalAction, default=True
    )
    seed_frontier.add_argument(
        "--include-market-positions", action=argparse.BooleanOptionalAction, default=True
    )
    seed_frontier.add_argument(
        "--include-open-interest", action=argparse.BooleanOptionalAction, default=True
    )
    seed_frontier.add_argument("--refresh-run", help=argparse.SUPPRESS)
    seed_frontier.add_argument("--requeue-done", action="store_true", help=argparse.SUPPRESS)
    seed_frontier.add_argument("--include-global-trades", action="store_true", help=argparse.SUPPRESS)
    seed_frontier.set_defaults(func=cmd_tasks_seed_frontier)

    seed_wallets = task_subparsers.add_parser(
        "seed-wallets", help="Seed wallet-scoped trade and activity refresh tasks."
    )
    seed_wallets.add_argument("--wallet", action="append", dest="wallets", default=[])
    seed_wallets.add_argument("--wallet-limit", type=int, default=100)
    seed_wallets.add_argument("--since-hours", type=int, default=48)
    seed_wallets.add_argument("--page-limit", type=int, default=500)
    seed_wallets.add_argument("--max-pages", type=int, default=2)
    seed_wallets.add_argument(
        "--candidate-mode",
        choices=["recent", "all", "smart-candidates", "whale-candidates"],
        default="recent",
        help="Choose wallets from recent activity, all history, smart candidates, or whale candidates.",
    )
    seed_wallets.add_argument("--min-notional", type=float, default=10_000.0)
    seed_wallets.add_argument("--include-trades", action=argparse.BooleanOptionalAction, default=True)
    seed_wallets.add_argument("--include-activity", action=argparse.BooleanOptionalAction, default=True)
    seed_wallets.add_argument(
        "--include-wallet-portfolio", action=argparse.BooleanOptionalAction, default=True
    )
    seed_wallets.add_argument("--include-wallet-pnl", action=argparse.BooleanOptionalAction, default=False)
    seed_wallets.add_argument(
        "--include-tracked-wallets",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=argparse.SUPPRESS,
    )
    seed_wallets.add_argument("--refresh-run", help=argparse.SUPPRESS)
    seed_wallets.add_argument("--requeue-done", action="store_true", help=argparse.SUPPRESS)
    seed_wallets.set_defaults(func=cmd_tasks_seed_wallets)

    seed_active_event_wallets = task_subparsers.add_parser(
        "seed-active-event-wallets",
        help="Seed hot wallet refresh tasks from wallets trading unfinished active events.",
    )
    seed_active_event_wallets.add_argument("--wallet", action="append", dest="wallets", default=[])
    seed_active_event_wallets.add_argument("--event-limit", type=int, default=0)
    seed_active_event_wallets.add_argument("--condition-limit", type=int, default=1000)
    seed_active_event_wallets.add_argument("--wallet-limit", type=int, default=1000)
    seed_active_event_wallets.add_argument("--since-minutes", type=int, default=120)
    seed_active_event_wallets.add_argument("--min-notional", type=float, default=0.0)
    seed_active_event_wallets.add_argument("--trade-page-limit", type=int, default=500)
    seed_active_event_wallets.add_argument("--trade-max-pages", type=int, default=1)
    seed_active_event_wallets.add_argument("--wallet-page-limit", type=int, default=500)
    seed_active_event_wallets.add_argument("--wallet-max-pages", type=int, default=1)
    seed_active_event_wallets.add_argument(
        "--include-market-trades", action=argparse.BooleanOptionalAction, default=True
    )
    seed_active_event_wallets.add_argument(
        "--include-wallet-trades", action=argparse.BooleanOptionalAction, default=True
    )
    seed_active_event_wallets.add_argument(
        "--include-wallet-activity", action=argparse.BooleanOptionalAction, default=True
    )
    seed_active_event_wallets.add_argument(
        "--include-wallet-portfolio", action=argparse.BooleanOptionalAction, default=True
    )
    seed_active_event_wallets.add_argument(
        "--include-wallet-pnl", action=argparse.BooleanOptionalAction, default=True
    )
    seed_active_event_wallets.add_argument(
        "--include-unusual-betting-refresh",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Also seed large-direction signal refresh tasks for monitored active events.",
    )
    seed_active_event_wallets.add_argument("--unusual-betting-event-limit", type=int, default=20)
    seed_active_event_wallets.add_argument(
        "--unusual-betting-min-notional",
        type=float,
        default=500_000.0,
        help="Recent per-wallet event notional required before linking active-event wallet monitoring to large-direction analysis.",
    )
    seed_active_event_wallets.add_argument(
        "--include-tracked-wallets",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=argparse.SUPPRESS,
    )
    seed_active_event_wallets.add_argument("--refresh-run", help=argparse.SUPPRESS)
    seed_active_event_wallets.add_argument(
        "--requeue-done",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=argparse.SUPPRESS,
    )
    seed_active_event_wallets.set_defaults(func=cmd_tasks_seed_active_event_wallets)

    seed_unusual_betting = task_subparsers.add_parser(
        "seed-unusual-betting",
        help="Seed large-direction signal refresh tasks for active or recently updated events.",
    )
    seed_unusual_betting.add_argument("--event", action="append", dest="events", default=[])
    seed_unusual_betting.add_argument("--event-limit", type=int, default=20)
    seed_unusual_betting.add_argument("--recent-limit", type=int, default=20)
    seed_unusual_betting.add_argument("--since-minutes", type=int, default=30)
    seed_unusual_betting.add_argument("--wallet-limit", type=int, default=100)
    seed_unusual_betting.add_argument("--trade-limit", type=int, default=100)
    seed_unusual_betting.add_argument("--large-threshold", type=float, default=500_000.0)
    seed_unusual_betting.add_argument("--very-large-threshold", type=float, default=1_000_000.0)
    seed_unusual_betting.add_argument("--extreme-threshold", type=float, default=5_000_000.0)
    seed_unusual_betting.add_argument("--cold-price-threshold", type=float, default=0.25)
    seed_unusual_betting.add_argument(
        "--include-related-markets", action=argparse.BooleanOptionalAction, default=True
    )
    seed_unusual_betting.add_argument("--refresh-run", default="unusual-betting")
    seed_unusual_betting.add_argument("--trigger-reason", default="scheduled")
    seed_unusual_betting.add_argument(
        "--requeue-done",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=argparse.SUPPRESS,
    )
    seed_unusual_betting.set_defaults(func=cmd_tasks_seed_unusual_betting)

    seed_history = task_subparsers.add_parser(
        "seed-history", help="Seed deep historical backfill tasks from ClickHouse."
    )
    seed_history.add_argument("--event-limit", type=int, default=0, help="Use 0 for all events.")
    seed_history.add_argument("--active-only", action="store_true")
    seed_history.add_argument("--include-trades", action=argparse.BooleanOptionalAction, default=True)
    seed_history.add_argument(
        "--include-price-history", action=argparse.BooleanOptionalAction, default=True
    )
    seed_history.add_argument("--include-books", action=argparse.BooleanOptionalAction, default=True)
    seed_history.add_argument(
        "--include-chain-logs", action=argparse.BooleanOptionalAction, default=True
    )
    seed_history.add_argument("--trade-page-limit", type=int, default=500)
    seed_history.add_argument("--price-interval", default="all")
    seed_history.add_argument("--price-fidelity", type=int)
    seed_history.add_argument("--chain-from-block", type=int)
    seed_history.add_argument("--chain-to-block", type=int)
    seed_history.add_argument("--chain-block-step", type=int, default=50_000)
    seed_history.add_argument("--chain-address", action="append", dest="chain_addresses", default=[])
    seed_history.add_argument("--chain-topic", action="append", dest="chain_topics", default=[])
    seed_history.set_defaults(func=cmd_tasks_seed_history)

    status = task_subparsers.add_parser("status", help="Print task status counts.")
    status.set_defaults(func=cmd_tasks_status)

    progress = task_subparsers.add_parser("progress", help="Print task progress details.")
    progress.add_argument("--recent-limit", type=int, default=10)
    progress.set_defaults(func=cmd_tasks_progress)

    run_once = task_subparsers.add_parser("run-once", help="Claim and run one pending task.")
    run_once.add_argument(
        "--task-kind",
        action="append",
        dest="task_kinds",
        default=[],
        help="Only claim tasks of this kind. Repeat or pass comma-separated values.",
    )
    run_once.set_defaults(func=cmd_tasks_run_once)

    run_loop = task_subparsers.add_parser("run-loop", help="Continuously claim and run tasks.")
    run_loop.add_argument("--max-tasks", type=int, default=0, help="Use 0 to run forever.")
    run_loop.add_argument("--idle-sleep-seconds", type=float, default=5.0)
    run_loop.add_argument("--stop-on-idle", action="store_true")
    run_loop.add_argument(
        "--task-kind",
        action="append",
        dest="task_kinds",
        default=[],
        help="Only claim tasks of this kind. Repeat or pass comma-separated values.",
    )
    run_loop.set_defaults(func=cmd_tasks_run_loop)

    load = subparsers.add_parser("load", help="Load raw data into analytical stores.")
    load_subparsers = load.add_subparsers(required=True)

    gamma_raw = load_subparsers.add_parser("gamma-raw", help="Load raw Gamma data into ClickHouse.")
    gamma_raw.add_argument("--force", action="store_true", help="Replay already logged raw payloads.")
    gamma_raw.add_argument("--batch-size", type=int, default=10_000)
    gamma_raw.set_defaults(func=cmd_load_gamma_raw)

    clob_prices = load_subparsers.add_parser(
        "clob-price-history", help="Load raw CLOB price history into ClickHouse."
    )
    clob_prices.add_argument("--force", action="store_true")
    clob_prices.add_argument("--batch-size", type=int, default=10_000)
    clob_prices.set_defaults(func=cmd_load_clob_price_history)

    clob_books = load_subparsers.add_parser(
        "clob-books", help="Load raw CLOB order books into ClickHouse."
    )
    clob_books.add_argument("--force", action="store_true")
    clob_books.add_argument("--batch-size", type=int, default=10_000)
    clob_books.set_defaults(func=cmd_load_clob_books)

    clob_ws_books = load_subparsers.add_parser(
        "clob-ws-market-books", help="Load raw CLOB WebSocket book snapshots into ClickHouse."
    )
    clob_ws_books.add_argument("--force", action="store_true")
    clob_ws_books.add_argument("--batch-size", type=int, default=10_000)
    clob_ws_books.add_argument("--workers", type=int, default=1)
    clob_ws_books.set_defaults(func=cmd_load_clob_ws_market_books)

    data_trades = load_subparsers.add_parser(
        "data-trades", help="Load raw Data API trades into ClickHouse."
    )
    data_trades.add_argument("--force", action="store_true")
    data_trades.add_argument("--batch-size", type=int, default=10_000)
    data_trades.add_argument("--workers", type=int, default=1)
    data_trades.add_argument("--max-paths", type=int)
    data_trades.add_argument("--newest-first", action="store_true")
    data_trades.set_defaults(func=cmd_load_data_trades)

    data_activity = load_subparsers.add_parser("data-activity", help="Load raw user activity.")
    data_activity.add_argument("--force", action="store_true")
    data_activity.add_argument("--batch-size", type=int, default=10_000)
    data_activity.set_defaults(func=cmd_load_data_activity)

    data_holders = load_subparsers.add_parser("data-holders", help="Load raw market holders.")
    data_holders.add_argument("--force", action="store_true")
    data_holders.add_argument("--batch-size", type=int, default=10_000)
    data_holders.set_defaults(func=cmd_load_data_holders)

    data_market_positions = load_subparsers.add_parser(
        "data-market-positions", help="Load raw market positions."
    )
    data_market_positions.add_argument("--force", action="store_true")
    data_market_positions.add_argument("--batch-size", type=int, default=10_000)
    data_market_positions.set_defaults(func=cmd_load_data_market_positions)

    data_positions = load_subparsers.add_parser("data-positions", help="Load raw wallet positions.")
    data_positions.add_argument("--force", action="store_true")
    data_positions.add_argument("--batch-size", type=int, default=10_000)
    data_positions.set_defaults(func=cmd_load_data_positions)

    data_wallet_portfolio = load_subparsers.add_parser(
        "data-wallet-portfolio", help="Load raw wallet portfolio snapshots."
    )
    data_wallet_portfolio.add_argument("--force", action="store_true")
    data_wallet_portfolio.add_argument("--batch-size", type=int, default=10_000)
    data_wallet_portfolio.set_defaults(func=cmd_load_data_wallet_portfolio)

    data_user_pnl = load_subparsers.add_parser(
        "data-user-pnl", help="Load raw wallet PnL snapshots."
    )
    data_user_pnl.add_argument("--force", action="store_true")
    data_user_pnl.add_argument("--batch-size", type=int, default=10_000)
    data_user_pnl.set_defaults(func=cmd_load_data_user_pnl)

    data_oi = load_subparsers.add_parser("data-open-interest", help="Load raw open interest.")
    data_oi.add_argument("--force", action="store_true")
    data_oi.add_argument("--batch-size", type=int, default=10_000)
    data_oi.set_defaults(func=cmd_load_data_open_interest)

    chain_logs_load = load_subparsers.add_parser("chain-logs", help="Load raw Polygon logs.")
    chain_logs_load.add_argument("--force", action="store_true")
    chain_logs_load.add_argument("--batch-size", type=int, default=10_000)
    chain_logs_load.set_defaults(func=cmd_load_chain_logs)

    exchange_fills = load_subparsers.add_parser(
        "exchange-fills", help="Decode CTF exchange OrderFilled logs into fact_exchange_fill."
    )
    exchange_fills.add_argument("--force", action="store_true")
    exchange_fills.add_argument("--batch-size", type=int, default=10_000)
    exchange_fills.set_defaults(func=cmd_load_exchange_fills)

    orders_matched = load_subparsers.add_parser(
        "orders-matched", help="Decode CTF exchange OrdersMatched logs."
    )
    orders_matched.add_argument("--force", action="store_true")
    orders_matched.add_argument("--batch-size", type=int, default=10_000)
    orders_matched.set_defaults(func=cmd_load_orders_matched)

    fees_charged = load_subparsers.add_parser(
        "fees-charged", help="Decode CTF exchange FeeCharged logs."
    )
    fees_charged.add_argument("--force", action="store_true")
    fees_charged.add_argument("--batch-size", type=int, default=10_000)
    fees_charged.set_defaults(func=cmd_load_fees_charged)

    balance_movements = load_subparsers.add_parser(
        "balance-movements", help="Decode CTF ERC1155 TransferSingle/TransferBatch logs."
    )
    balance_movements.add_argument("--force", action="store_true")
    balance_movements.add_argument("--batch-size", type=int, default=10_000)
    balance_movements.set_defaults(func=cmd_load_balance_movements)

    lifecycle_events = load_subparsers.add_parser(
        "lifecycle-events", help="Decode CTF split, merge, and redeem lifecycle logs."
    )
    lifecycle_events.add_argument("--force", action="store_true")
    lifecycle_events.add_argument("--batch-size", type=int, default=10_000)
    lifecycle_events.set_defaults(func=cmd_load_lifecycle_events)

    build = subparsers.add_parser("build", help="Build analytical marts.")
    build_subparsers = build.add_subparsers(required=True)

    market_1m = build_subparsers.add_parser("market-1m", help="Build 1 minute market candles.")
    market_1m.set_defaults(func=cmd_build_market_1m)

    trader_profiles = build_subparsers.add_parser(
        "trader-profiles", help="Build trader profile mart."
    )
    trader_profiles.set_defaults(func=cmd_build_trader_profiles)

    wallet_trade_rollup = build_subparsers.add_parser(
        "wallet-trade-rollup", help="Build recent wallet trade rollup mart."
    )
    wallet_trade_rollup.add_argument("--since-hours", type=int, default=48)
    wallet_trade_rollup.set_defaults(func=cmd_build_wallet_trade_rollup)

    trader_chain_pnl = build_subparsers.add_parser(
        "trader-chain-pnl", help="Build chain-verified trader PnL mart."
    )
    trader_chain_pnl.set_defaults(func=cmd_build_trader_chain_pnl)

    event_wallet_pnl = build_subparsers.add_parser(
        "event-wallet-pnl", help="Build completed event wallet PnL mart."
    )
    event_wallet_pnl.set_defaults(func=cmd_build_event_wallet_pnl)

    live_wallet_positions = build_subparsers.add_parser(
        "live-wallet-positions", help="Build active event wallet position mart."
    )
    live_wallet_positions.set_defaults(func=cmd_build_live_wallet_positions)

    wallet_reputation = build_subparsers.add_parser(
        "wallet-reputation", help="Build wallet historical reputation mart."
    )
    wallet_reputation.set_defaults(func=cmd_build_wallet_reputation)

    wallet_screener = build_subparsers.add_parser(
        "wallet-screener", help="Build all-wallet whale and smart wallet screener mart."
    )
    wallet_screener.set_defaults(func=cmd_build_wallet_screener)

    wallet_fifa_24h_pnl = build_subparsers.add_parser(
        "wallet-fifa-24h-pnl", help="Build all-wallet FIFA 24h PnL mart."
    )
    wallet_fifa_24h_pnl.add_argument("--window-hours", type=int, default=24)
    wallet_fifa_24h_pnl.set_defaults(func=cmd_build_wallet_fifa_24h_pnl)

    fifa_trades = build_subparsers.add_parser(
        "fifa-trades", help="Build scoped FIFA trade cache mart."
    )
    fifa_trades.add_argument("--window-hours", type=int, default=72)
    fifa_trades.set_defaults(func=cmd_build_fifa_trades)

    event_anomaly_signals = build_subparsers.add_parser(
        "event-anomaly-signals", help="Build event anomaly signal mart."
    )
    event_anomaly_signals.add_argument("--large-trade-threshold", type=float, default=1_000.0)
    event_anomaly_signals.add_argument("--liquidity-ratio-threshold", type=float, default=0.10)
    event_anomaly_signals.add_argument("--coordinated-wallet-threshold", type=int, default=5)
    event_anomaly_signals.add_argument("--coordinated-notional-threshold", type=float, default=5_000.0)
    event_anomaly_signals.add_argument("--since-hours", type=int, default=168)
    event_anomaly_signals.set_defaults(func=cmd_build_event_anomaly_signals)

    analytics_core = build_subparsers.add_parser(
        "analytics-core", help="Build core event, wallet, live position, and anomaly marts."
    )
    analytics_core.set_defaults(func=cmd_build_analytics_core)

    alerts = build_subparsers.add_parser("alerts", help="Build alert mart.")
    alerts.add_argument("--price-move-threshold", type=float, default=0.10)
    alerts.add_argument("--spread-threshold", type=float, default=0.05)
    alerts.add_argument("--whale-notional-threshold", type=float, default=1_000.0)
    alerts.add_argument("--since-hours", type=int, default=24)
    alerts.set_defaults(func=cmd_build_alerts)

    trade_reconciliation = build_subparsers.add_parser(
        "trade-reconciliation", help="Build Data API vs chain fill reconciliation mart."
    )
    trade_reconciliation.set_defaults(func=cmd_build_trade_reconciliation)

    settlement_audit = build_subparsers.add_parser(
        "settlement-audit", help="Build market settlement and redeem audit mart."
    )
    settlement_audit.set_defaults(func=cmd_build_settlement_audit)

    collector_health = build_subparsers.add_parser(
        "collector-health", help="Build collector health mart from Postgres run history."
    )
    collector_health.set_defaults(func=cmd_build_collector_health)

    stream = subparsers.add_parser("stream", help="Publish raw and normalized events to Redpanda.")
    stream_subparsers = stream.add_subparsers(required=True)

    create_topic = stream_subparsers.add_parser("create-topic", help="Create one Redpanda topic.")
    create_topic.add_argument("--topic", required=True)
    create_topic.add_argument("--service", default="redpanda")
    create_topic.set_defaults(func=cmd_stream_create_topic)

    ws_market_raw = stream_subparsers.add_parser(
        "ws-market-raw", help="Publish raw CLOB WebSocket market events to Redpanda."
    )
    ws_market_raw.add_argument("--topic", default="zetta.polymarket.clob_ws.market.raw")
    ws_market_raw.add_argument("--service", default="redpanda")
    ws_market_raw.add_argument("--max-records", type=int)
    ws_market_raw.add_argument("--batch-size", type=int, default=500)
    ws_market_raw.add_argument("--skip-create-topic", action="store_true")
    ws_market_raw.set_defaults(func=cmd_stream_ws_market_raw)

    realtime = subparsers.add_parser("realtime", help="Inspect and reconcile real-time state.")
    realtime_subparsers = realtime.add_subparsers(required=True)

    rebuild_books = realtime_subparsers.add_parser(
        "rebuild-books", help="Rebuild in-memory order books from raw WebSocket events."
    )
    rebuild_books.add_argument("--max-records", type=int)
    rebuild_books.set_defaults(func=cmd_realtime_rebuild_books)

    reconcile_book = realtime_subparsers.add_parser(
        "reconcile-book", help="Compare reconstructed WebSocket book state with REST book."
    )
    reconcile_book.add_argument("--token-id", required=True)
    reconcile_book.add_argument("--max-records", type=int)
    reconcile_book.set_defaults(func=cmd_realtime_reconcile_book)

    trade_stream = realtime_subparsers.add_parser(
        "trade-stream", help="Serve live trade deltas over WebSocket."
    )
    trade_stream.add_argument("--host", default="127.0.0.1")
    trade_stream.add_argument("--port", type=int, default=8090)
    trade_stream.add_argument("--poll-seconds", type=float, default=2.0)
    trade_stream.add_argument("--limit", type=int, default=150)
    trade_stream.add_argument("--heartbeat-seconds", type=float, default=20.0)
    trade_stream.set_defaults(func=cmd_realtime_trade_stream)

    official_trade_feed = realtime_subparsers.add_parser(
        "official-trade-feed", help="Proxy Polymarket RTDS activity/trades over WebSocket."
    )
    official_trade_feed.add_argument("--host", default="127.0.0.1")
    official_trade_feed.add_argument("--port", type=int, default=8091)
    official_trade_feed.add_argument("--upstream-url", default=POLYMARKET_RTDS_URL)
    official_trade_feed.set_defaults(func=cmd_realtime_official_trade_feed)

    api = subparsers.add_parser("api", help="Serve product API endpoints.")
    api_subparsers = api.add_subparsers(required=True)
    serve = api_subparsers.add_parser("serve", help="Run the local product API server.")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8088)
    serve.set_defaults(func=cmd_api_serve)

    return parser


def add_gamma_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--page-limit", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=1, help="Use 0 to continue until exhausted.")
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--closed", choices=["true", "false"])
    parser.add_argument("--archived", choices=["true", "false"])
    parser.add_argument("--active", choices=["true", "false"])


def cmd_endpoints(_args: argparse.Namespace, app_settings: Settings) -> dict[str, str]:
    return {
        "gamma": app_settings.gamma_base_url,
        "data": app_settings.data_base_url,
        "clob": app_settings.clob_base_url,
        "clob_ws_market": app_settings.clob_ws_market_url,
        "polygon_rpc": app_settings.polygon_rpc_url,
        "clickhouse": (
            f"http://{app_settings.clickhouse_host}:{app_settings.clickhouse_port}/"
            f"{app_settings.clickhouse_database}"
        ),
        "postgres": app_settings.postgres_dsn,
        "raw_data_dir": str(app_settings.raw_data_dir),
        "state_dir": str(app_settings.state_dir),
    }


def cmd_db_ping(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return {"clickhouse": ClickHouseWriter(app_settings).ping()}


def cmd_db_migrate(args: argparse.Namespace, app_settings: Settings) -> Any:
    schema_path = Path(args.schema_path)
    statements = ClickHouseWriter(app_settings).execute_statements(
        schema_path.read_text(encoding="utf-8")
    )
    return {"schema_path": str(schema_path), "statements": statements}


def cmd_gamma(args: argparse.Namespace, app_settings: Settings, entity: str) -> Any:
    collector = GammaCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
    )
    return collector.collect_keyset(
        entity,  # type: ignore[arg-type]
        page_limit=args.page_limit,
        max_pages=args.max_pages,
        resume=args.resume,
        sleep_seconds=args.sleep_seconds,
        closed=parse_bool(args.closed),
        archived=parse_bool(args.archived),
        active=parse_bool(args.active),
    )


def cmd_trades(args: argparse.Namespace, app_settings: Settings) -> Any:
    collector = DataCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
    )
    return collector.collect_trades(
        page_limit=args.page_limit,
        max_pages=args.max_pages,
        resume=args.resume,
        user=args.user,
        market=args.market,
        event_id=args.event_id,
    )


def cmd_activity(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
    ).collect_activity(
        user=args.user,
        page_limit=args.page_limit,
        max_pages=args.max_pages,
        resume=args.resume,
    )


def cmd_holders(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
    ).collect_holders(market=args.market, limit=args.limit)


def cmd_market_positions(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
    ).collect_market_positions(market=args.market, limit=args.limit)


def cmd_positions(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
    ).collect_positions(user=args.user)


def cmd_wallet_portfolio(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
        rpc_client=PolygonRpcClient(app_settings),
    ).collect_wallet_portfolio(user=args.user)


def cmd_wallet_pnl(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
    ).collect_user_pnl(user=args.user, interval=args.interval, fidelity=args.fidelity)


def cmd_open_interest(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
        state_store=LocalStateStore(app_settings.state_dir),
    ).collect_open_interest(market=args.market)


def cmd_prices_history(args: argparse.Namespace, app_settings: Settings) -> Any:
    collector = ClobCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
    )
    return collector.collect_prices_history(
        token_id=args.token_id,
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        interval=args.interval,
        fidelity=args.fidelity,
    )


def cmd_prices_history_batch(args: argparse.Namespace, app_settings: Settings) -> Any:
    token_result = cmd_discover_tokens(args, app_settings)
    collector = ClobCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
    )
    return collector.collect_prices_history_batch(
        token_ids=token_result["tokens"],
        interval=args.interval,
        fidelity=args.fidelity,
        sleep_seconds=args.sleep_seconds,
    )


def cmd_book(args: argparse.Namespace, app_settings: Settings) -> Any:
    collector = ClobCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
    )
    return collector.collect_book(token_id=args.token_id)


def cmd_books_batch(args: argparse.Namespace, app_settings: Settings) -> Any:
    token_result = cmd_discover_tokens(args, app_settings)
    collector = ClobCollector(
        client=PolymarketClient(app_settings),
        raw_writer=raw_writer(app_settings),
    )
    return collector.collect_books_batch(
        token_ids=token_result["tokens"],
        sleep_seconds=args.sleep_seconds,
    )


def cmd_ws_market(args: argparse.Namespace, app_settings: Settings) -> Any:
    token_ids = parse_token_ids(args.token_ids)
    if not token_ids:
        token_result = cmd_discover_tokens(args, app_settings)
        token_ids = token_result["tokens"]
    collector = MarketWebSocketCollector(
        settings=app_settings,
        raw_writer=raw_writer(app_settings),
    )
    result = collector.collect(
        token_ids=token_ids,
        max_messages=args.max_messages,
        max_seconds=args.max_seconds,
        custom_feature_enabled=not args.disable_custom_feature,
        include_heartbeats=args.include_heartbeats,
    )
    return {**asdict(result), "tokens": len(token_ids)}


def cmd_chain_logs(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ChainCollector(
        client=PolygonRpcClient(app_settings),
        raw_writer=raw_writer(app_settings),
    ).collect_logs(
        from_block=args.from_block,
        to_block=args.to_block,
        addresses=args.addresses,
        topics=args.topics,
    )


def cmd_chain_block_number(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return {"block_number": PolygonRpcClient(app_settings).block_number()}


POLYMARKET_EXCHANGE_ADDRESSES = [
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
]
POLYMARKET_CTF_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
CHAIN_FRONTIER_STATE_KEY = "chain_frontier/polymarket"
EXCHANGE_LOG_TOPICS = [
    ORDER_FILLED_TOPIC,
    ORDERS_MATCHED_TOPIC,
    FEE_CHARGED_TOPIC,
]
CTF_LOG_TOPICS = [
    TRANSFER_SINGLE_TOPIC,
    TRANSFER_BATCH_TOPIC,
    POSITION_SPLIT_TOPIC,
    POSITIONS_MERGE_TOPIC,
    PAYOUT_REDEMPTION_TOPIC,
]


def cmd_chain_frontier(args: argparse.Namespace, app_settings: Settings) -> Any:
    if args.lookback_blocks <= 0:
        raise ValueError("--lookback-blocks must be positive")
    if args.confirmations < 0:
        raise ValueError("--confirmations cannot be negative")
    if args.overlap_blocks < 0:
        raise ValueError("--overlap-blocks cannot be negative")
    if args.step_blocks <= 0:
        raise ValueError("--step-blocks must be positive")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")

    client = PolygonRpcClient(app_settings)
    latest_block = client.block_number()
    to_block = latest_block - args.confirmations
    if to_block <= 0:
        raise ValueError("latest confirmed block is not positive")
    state_store = LocalStateStore(app_settings.state_dir)
    state = state_store.get(CHAIN_FRONTIER_STATE_KEY, {})
    last_to_block = int(state.get("to_block") or 0) if isinstance(state, dict) else 0
    if last_to_block >= to_block:
        return {
            "latest_block": latest_block,
            "from_block": None,
            "to_block": to_block,
            "previous_to_block": last_to_block,
            "skipped": True,
            "reason": "no_new_confirmed_blocks",
        }
    if last_to_block > 0:
        from_block = max(last_to_block - args.overlap_blocks + 1, to_block - args.lookback_blocks + 1)
    else:
        from_block = to_block - args.lookback_blocks + 1
    from_block = max(0, from_block)
    exchange_addresses = normalize_address_list(
        args.exchange_addresses or POLYMARKET_EXCHANGE_ADDRESSES
    )
    ctf_address = str(args.conditional_tokens_address or POLYMARKET_CTF_ADDRESS).lower()

    writer = raw_writer(app_settings)
    collector = ChainCollector(client=client, raw_writer=writer)
    collections = []
    for start in range(from_block, to_block + 1, args.step_blocks):
        end = min(start + args.step_blocks - 1, to_block)
        for topic in EXCHANGE_LOG_TOPICS:
            collections.append(
                asdict(
                    collector.collect_logs(
                        from_block=start,
                        to_block=end,
                        addresses=exchange_addresses,
                        topics=[topic],
                    )
                )
            )
        for topic in CTF_LOG_TOPICS:
            collections.append(
                asdict(
                    collector.collect_logs(
                        from_block=start,
                        to_block=end,
                        addresses=[ctf_address],
                        topics=[topic],
                    )
                )
            )

    writer.flush()
    loader = ChainRawLoader(clickhouse=ClickHouseWriter(app_settings))
    load_result = loader.load_logs(raw_root=app_settings.raw_data_dir, batch_size=args.batch_size)
    exchange_fills = loader.build_exchange_fills(batch_size=args.batch_size)
    orders_matched = loader.build_orders_matched(batch_size=args.batch_size)
    fees_charged = loader.build_fee_charged(batch_size=args.batch_size)
    balance_movements = loader.build_balance_movements(batch_size=args.batch_size)
    lifecycle_events = loader.build_lifecycle_events(batch_size=args.batch_size)
    state_store.set(
        CHAIN_FRONTIER_STATE_KEY,
        {
            "from_block": from_block,
            "to_block": to_block,
            "latest_block": latest_block,
            "updated_at": datetime.now(UTC).isoformat(),
        },
    )
    return {
        "latest_block": latest_block,
        "from_block": from_block,
        "to_block": to_block,
        "previous_to_block": last_to_block,
        "requests": len(collections),
        "collected_logs": sum(int(item.get("logs") or 0) for item in collections),
        "load": asdict(load_result),
        "decoded": {
            "exchange_fills": asdict(exchange_fills),
            "orders_matched": asdict(orders_matched),
            "fees_charged": asdict(fees_charged),
            "balance_movements": asdict(balance_movements),
            "lifecycle_events": asdict(lifecycle_events),
        },
    }


def normalize_address_list(values: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for value in values:
        for item in str(value).split(","):
            address = item.strip().lower()
            if address and address not in seen:
                normalized.append(address)
                seen.add(address)
    return normalized


def cmd_discover_tokens(args: argparse.Namespace, app_settings: Settings) -> Any:
    where = "where t.token_id != ''"
    if args.active_only:
        where += " and m.active = true and m.closed = false"
    query = (
        "select distinct t.token_id "
        "from dim_outcome_token as t "
        "left join dim_market as m on t.market_id = m.market_id "
        f"{where} "
        f"limit {args.limit} "
        "format JSONEachRow"
    )
    lines = ClickHouseWriter(app_settings).query_text(query).splitlines()
    tokens = [json.loads(line)["token_id"] for line in lines if line.strip()]
    return {"tokens": tokens, "count": len(tokens)}


def cmd_discover_markets(args: argparse.Namespace, app_settings: Settings) -> Any:
    where = "where condition_id != ''"
    if args.active_only:
        where += " and active = true and closed = false"
    query = (
        "select distinct condition_id "
        "from dim_market "
        f"{where} "
        f"limit {args.limit} "
        "format JSONEachRow"
    )
    lines = ClickHouseWriter(app_settings).query_text(query).splitlines()
    markets = [json.loads(line)["condition_id"] for line in lines if line.strip()]
    return {"markets": markets, "count": len(markets)}


def cmd_discover_wallets(args: argparse.Namespace, app_settings: Settings) -> Any:
    query = (
        "select user_address "
        "from mart_trader_profile final "
        "where user_address != '' "
        "order by traded_notional desc, last_trade_at desc "
        f"limit {args.limit} "
        "format JSONEachRow"
    )
    lines = ClickHouseWriter(app_settings).query_text(query).splitlines()
    wallets = [json.loads(line)["user_address"] for line in lines if line.strip()]
    return {"wallets": wallets, "count": len(wallets)}


def discover_missing_live_token_metadata(
    clickhouse: ClickHouseWriter,
    *,
    lookback_minutes: int,
    limit: int,
    min_notional: float,
) -> list[str]:
    if lookback_minutes <= 0:
        raise ValueError("lookback_minutes must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_notional < 0:
        raise ValueError("min_notional must be non-negative")

    query = f"""
        select fills.token_id
        from fact_exchange_fill as fills final
        left join dim_outcome_token as tokens final on fills.token_id = tokens.token_id
        where fills.ingested_at >= now64(3) - interval {lookback_minutes} minute
          and fills.token_id != ''
          and fills.notional >= {min_notional}
          and tokens.token_id = ''
        group by fills.token_id
        order by max(fills.ingested_at) desc, sum(fills.notional) desc
        limit {limit}
        format JSONEachRow
    """
    return [
        json.loads(line)["token_id"]
        for line in clickhouse.query_text(query).splitlines()
        if line.strip()
    ]


def cmd_refresh_live_token_metadata(args: argparse.Namespace, app_settings: Settings) -> Any:
    clickhouse = ClickHouseWriter(app_settings)
    token_ids = discover_missing_live_token_metadata(
        clickhouse,
        lookback_minutes=args.lookback_minutes,
        limit=args.limit,
        min_notional=args.min_notional,
    )
    client = PolymarketClient(app_settings)
    writer = raw_writer(app_settings)
    raw_paths: list[str] = []
    requests = 0
    markets_by_token: dict[str, int] = {}

    for token_id in token_ids:
        page = client.gamma_markets_by_clob_token_id(token_id=token_id)
        requests += 1
        markets_by_token[token_id] = len(page.items)
        if page.items:
            raw_path = writer.write(
                source="gamma",
                entity="markets",
                request_url=page.response.url,
                payload=page.response.body,
            )
            if not str(raw_path).endswith(".open"):
                raw_paths.append(str(raw_path))
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    raw_paths.extend(str(path) for path in writer.flush())
    raw_paths = list(dict.fromkeys(raw_paths))
    load_result = GammaRawLoader(clickhouse=clickhouse).load(
        raw_root=app_settings.raw_data_dir,
        batch_size=args.load_batch_size,
    )
    return {
        "candidate_token_ids": len(token_ids),
        "requests": requests,
        "markets": sum(markets_by_token.values()),
        "raw_paths": raw_paths,
        "markets_by_token": markets_by_token,
        "load": asdict(load_result),
    }


def cmd_refresh_polycop_wallet_signals(
    args: argparse.Namespace, app_settings: Settings
) -> Any:
    from zetta.polycop_wallets import refresh_polycop_wallet_signals

    row = refresh_polycop_wallet_signals(
        dsn=app_settings.postgres_dsn,
        page_size=args.page_size,
        max_pages=args.max_pages,
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        limit=args.limit,
        trigger_reason=args.trigger_reason,
    )
    return {
        "status": row.get("status"),
        "cache_key": row.get("cache_key"),
        "wallet_count": row.get("wallet_count"),
        "stable_count": row.get("stable_count"),
        "flow_count": row.get("flow_count"),
        "burst_count": row.get("burst_count"),
        "refreshed_at": row.get("refreshed_at"),
        "age_seconds": row.get("age_seconds"),
        "error": row.get("error"),
    }


def discover_condition_ids_for_history(
    clickhouse: ClickHouseWriter,
    *,
    event_limit: int = 0,
    active_only: bool = False,
) -> list[str]:
    markets_where = "where event_id != '' and condition_id != ''"
    if active_only:
        markets_where += " and active = true and closed = false and archived = false"
    events_limit = f"limit {event_limit}" if event_limit > 0 else ""
    query = f"""
        with selected_events as
        (
          select event_id
          from dim_market final
          {markets_where}
          order by updated_at desc, event_id
          {events_limit}
        )
        select distinct condition_id
        from dim_market final
        where event_id in selected_events
          and condition_id != ''
        order by condition_id
        format JSONEachRow
    """
    return [
        json.loads(line)["condition_id"]
        for line in clickhouse.query_text(query).splitlines()
        if line.strip()
    ]


def discover_token_ids_for_history(
    clickhouse: ClickHouseWriter,
    *,
    event_limit: int = 0,
    active_only: bool = False,
) -> list[str]:
    markets_where = "where event_id != '' and condition_id != ''"
    if active_only:
        markets_where += " and active = true and closed = false and archived = false"
    events_limit = f"limit {event_limit}" if event_limit > 0 else ""
    query = f"""
        with selected_events as
        (
          select event_id
          from dim_market final
          {markets_where}
          order by updated_at desc, event_id
          {events_limit}
        )
        select distinct token_id
        from dim_outcome_token final
        where token_id != ''
          and market_id in
          (
            select market_id
            from dim_market final
            where event_id in selected_events
          )
        order by token_id
        format JSONEachRow
    """
    return [
        json.loads(line)["token_id"]
        for line in clickhouse.query_text(query).splitlines()
        if line.strip()
    ]


def discover_frontier_events(
    clickhouse: ClickHouseWriter,
    *,
    event_limit: int,
    active_only: bool,
) -> list[str]:
    if event_limit <= 0:
        raise ValueError("event_limit must be positive")

    markets_where = "where event_id != '' and condition_id != ''"
    if active_only:
        markets_where += " and active = true and closed = false and archived = false"

    query = f"""
        select event_id
        from dim_market final
        {markets_where}
        group by event_id
        order by max(updated_at) desc, event_id
        limit {event_limit}
        format JSONEachRow
    """
    return [
        json.loads(line)["event_id"]
        for line in clickhouse.query_text(query).splitlines()
        if line.strip()
    ]


def discover_frontier_work(
    clickhouse: ClickHouseWriter,
    *,
    event_limit: int,
    condition_limit: int,
    token_limit: int,
    active_only: bool,
) -> tuple[list[str], list[str]]:
    if event_limit <= 0:
        raise ValueError("event_limit must be positive")
    if condition_limit < 0:
        raise ValueError("condition_limit must not be negative")
    if token_limit < 0:
        raise ValueError("token_limit must not be negative")

    markets_where = "where event_id != '' and condition_id != ''"
    if active_only:
        markets_where += " and active = true and closed = false and archived = false"

    condition_query = f"""
        with selected_events as
        (
          select distinct event_id
          from dim_market final
          {markets_where}
          group by event_id
          order by max(updated_at) desc, event_id
          limit {event_limit}
        )
        select distinct condition_id
        from dim_market final
        where event_id in selected_events
          and condition_id != ''
        order by condition_id
        limit {condition_limit}
        format JSONEachRow
    """
    token_query = f"""
        with selected_events as
        (
          select distinct event_id
          from dim_market final
          {markets_where}
          group by event_id
          order by max(updated_at) desc, event_id
          limit {event_limit}
        )
        select distinct token_id
        from dim_outcome_token final
        where token_id != ''
          and market_id in
          (
            select market_id
            from dim_market final
            where event_id in selected_events
          )
        order by token_id
        limit {token_limit}
        format JSONEachRow
    """
    condition_ids = [
        json.loads(line)["condition_id"]
        for line in clickhouse.query_text(condition_query).splitlines()
        if line.strip()
    ]
    token_ids = [
        json.loads(line)["token_id"]
        for line in clickhouse.query_text(token_query).splitlines()
        if line.strip()
    ]
    return condition_ids, token_ids


def discover_recent_wallets(
    clickhouse: ClickHouseWriter,
    *,
    wallet_limit: int,
    since_hours: int,
) -> list[str]:
    if wallet_limit <= 0:
        raise ValueError("wallet_limit must be positive")
    if since_hours <= 0:
        raise ValueError("since_hours must be positive")
    query = f"""
        select user_address
        from
        (
          select
            user_address,
            wallet_last_trade_at as source_last_trade_at,
            traded_notional_24h
          from
          (
            select
              user_address,
              max(last_trade_at) as wallet_last_trade_at,
              sum(traded_notional_24h) as traded_notional_24h
            from mart_wallet_trade_rollup final
            where user_address != ''
            group by user_address
          )
          where wallet_last_trade_at >= now64(3) - interval {since_hours} hour
          union all
          select
            user_address,
            profile_last_trade_at as source_last_trade_at,
            0.0 as traded_notional_24h
          from
          (
            select
              user_address,
              max(last_trade_at) as profile_last_trade_at
            from mart_trader_profile final
            where user_address != ''
            group by user_address
          )
          where profile_last_trade_at >= now64(3) - interval {since_hours} hour
        )
        group by user_address
        order by max(source_last_trade_at) desc, sum(traded_notional_24h) desc
        limit {wallet_limit}
        format JSONEachRow
    """
    return [
        json.loads(line)["user_address"]
        for line in clickhouse.query_text(query).splitlines()
        if line.strip()
    ]


def active_event_condition_query(*, event_limit: int, condition_limit: int) -> str:
    if event_limit < 0:
        raise ValueError("event_limit must not be negative")
    if condition_limit <= 0:
        raise ValueError("condition_limit must be positive")

    markets_where = (
        "where event_id != '' and condition_id != '' "
        "and active = true and closed = false and archived = false"
    )
    if event_limit <= 0:
        return f"""
            select distinct condition_id
            from dim_market final
            {markets_where}
            order by condition_id
            limit {condition_limit}
            format JSONEachRow
        """

    return f"""
        with selected_events as
        (
          select event_id
          from dim_market final
          {markets_where}
          group by event_id
          order by max(updated_at) desc, event_id
          limit {event_limit}
        )
        select distinct condition_id
        from dim_market final
        where condition_id != ''
          and active = true
          and closed = false
          and archived = false
          and event_id in selected_events
        order by condition_id
        limit {condition_limit}
        format JSONEachRow
    """


def discover_active_event_conditions(
    clickhouse: ClickHouseWriter,
    *,
    event_limit: int,
    condition_limit: int,
) -> list[str]:
    return [
        json.loads(line)["condition_id"]
        for line in clickhouse.query_text(
            active_event_condition_query(
                event_limit=event_limit,
                condition_limit=condition_limit,
            )
        ).splitlines()
        if line.strip()
    ]


def discover_active_event_wallets(
    clickhouse: ClickHouseWriter,
    *,
    wallet_limit: int,
    since_minutes: int,
    event_limit: int,
    condition_limit: int,
    min_notional: float,
) -> list[str]:
    if wallet_limit <= 0:
        raise ValueError("wallet_limit must be positive")
    if since_minutes <= 0:
        raise ValueError("since_minutes must be positive")
    if min_notional < 0:
        raise ValueError("min_notional must be non-negative")

    condition_query = active_event_condition_query(
        event_limit=event_limit,
        condition_limit=condition_limit,
    ).replace("format JSONEachRow", "")
    query = f"""
        with active_conditions as
        (
          {condition_query}
        )
        select user_address
        from fact_trade_by_time
        where user_address != ''
          and condition_id in active_conditions
          and timestamp >= now64(3) - interval {since_minutes} minute
        group by user_address
        having sum(abs(notional)) >= {min_notional}
        order by max(timestamp) desc, sum(abs(notional)) desc, user_address
        limit {wallet_limit}
        format JSONEachRow
    """
    return [
        json.loads(line)["user_address"]
        for line in clickhouse.query_text(query).splitlines()
        if line.strip()
    ]


def discover_unusual_betting_event_slugs(
    clickhouse: ClickHouseWriter,
    *,
    event_limit: int,
    recent_limit: int,
    since_minutes: int,
) -> list[str]:
    if event_limit < 0:
        raise ValueError("event_limit must not be negative")
    if recent_limit < 0:
        raise ValueError("recent_limit must not be negative")
    if since_minutes <= 0:
        raise ValueError("since_minutes must be positive")

    event_slugs: list[str] = []
    if recent_limit > 0:
        query = f"""
            select
              events.slug as event_slug,
              max(fills.ingested_at) as latest_fill_at,
              sum(toFloat64(fills.notional)) as recent_notional
            from fact_exchange_fill as fills final
            inner join dim_outcome_token as tokens final on fills.token_id = tokens.token_id
            inner join dim_market as markets final on tokens.market_id = markets.market_id
            inner join dim_event as events final on markets.event_id = events.event_id
            where events.slug like 'fifwc-%'
              and fills.ingested_at >= now64(3) - interval {since_minutes} minute
            group by events.slug
            order by latest_fill_at desc, recent_notional desc
            limit {recent_limit}
            format JSONEachRow
        """
        event_slugs.extend(
            str(json.loads(line).get("event_slug") or "")
            for line in clickhouse.query_text(query).splitlines()
            if line.strip()
        )

    if event_limit > 0:
        query = f"""
            select slug as event_slug
            from dim_event final
            where slug like 'fifwc-%'
              and active = true
              and closed = false
              and archived = false
            order by coalesce(start_time, updated_at) asc, slug
            limit {event_limit}
            format JSONEachRow
        """
        event_slugs.extend(
            str(json.loads(line).get("event_slug") or "")
            for line in clickhouse.query_text(query).splitlines()
            if line.strip()
        )

    normalized: list[str] = []
    seen: set[str] = set()
    for slug in event_slugs:
        base_slug = base_match_slug(str(slug or "").strip())
        if not base_slug or base_slug in seen:
            continue
            seen.add(base_slug)
            normalized.append(base_slug)
    return normalized


def discover_unusual_betting_event_slugs_from_active_wallets(
    clickhouse: ClickHouseWriter,
    *,
    event_limit: int,
    condition_limit: int,
    since_minutes: int,
    min_notional: float,
) -> list[str]:
    if event_limit < 0:
        raise ValueError("event_limit must not be negative")
    if event_limit == 0:
        return []
    if condition_limit <= 0:
        raise ValueError("condition_limit must be positive")
    if since_minutes <= 0:
        raise ValueError("since_minutes must be positive")
    if min_notional < 0:
        raise ValueError("min_notional must be non-negative")

    condition_query = active_event_condition_query(
        event_limit=0,
        condition_limit=condition_limit,
    ).replace("format JSONEachRow", "")
    query = f"""
        with active_conditions as
        (
          {condition_query}
        ),
        wallet_event_notional as
        (
          select
            events.slug as event_slug,
            trades.user_address as user_address,
            sum(abs(toFloat64(trades.notional))) as total_notional,
            max(trades.timestamp) as latest_trade_at
          from fact_trade_by_time as trades
          inner join dim_market as markets final on trades.condition_id = markets.condition_id
          inner join dim_event as events final on markets.event_id = events.event_id
          where trades.user_address != ''
            and trades.condition_id in active_conditions
            and events.slug like 'fifwc-%'
            and trades.timestamp >= now64(3) - interval {since_minutes} minute
          group by event_slug, user_address
          having total_notional >= {min_notional}
        )
        select
          event_slug,
          count() as signal_wallets,
          round(max(total_notional), 2) as max_wallet_notional,
          round(sum(total_notional), 2) as total_wallet_notional,
          max(latest_trade_at) as latest_trade_at
        from wallet_event_notional
        group by event_slug
        order by latest_trade_at desc, max_wallet_notional desc
        limit {event_limit}
        format JSONEachRow
    """
    event_slugs = [
        str(json.loads(line).get("event_slug") or "")
        for line in clickhouse.query_text(query).splitlines()
        if line.strip()
    ]
    return normalize_event_slug_list(event_slugs)


def discover_wallet_candidates(
    clickhouse: ClickHouseWriter,
    *,
    wallet_limit: int,
    mode: str,
    since_hours: int,
    min_notional: float = 10_000.0,
) -> list[str]:
    if mode == "recent":
        return discover_recent_wallets(
            clickhouse,
            wallet_limit=wallet_limit,
            since_hours=since_hours,
        )
    if wallet_limit <= 0:
        raise ValueError("wallet_limit must be positive")
    if min_notional < 0:
        raise ValueError("min_notional must be non-negative")

    if mode == "all":
        query = f"""
            select user_address
            from mart_wallet_screener final
            where user_address != ''
            order by
              isNull(portfolio_captured_at) desc,
              portfolio_captured_at asc,
              traded_notional desc,
              last_trade_at desc
            limit {wallet_limit}
            format JSONEachRow
        """
    elif mode == "smart-candidates":
        query = f"""
            select user_address
            from mart_wallet_screener final
            where user_address != ''
              and traded_notional >= {min_notional}
            order by
              isNull(pnl_captured_at) desc,
              pnl_captured_at asc,
              traded_notional desc,
              last_trade_at desc
            limit {wallet_limit}
            format JSONEachRow
        """
    elif mode == "whale-candidates":
        query = f"""
            select user_address
            from mart_wallet_screener final
            where user_address != ''
              and is_whale
            order by
              isNull(portfolio_captured_at) desc,
              portfolio_captured_at asc,
              traded_notional desc,
              max_single_trade_notional desc
            limit {wallet_limit}
            format JSONEachRow
        """
    else:
        raise ValueError(f"unsupported wallet candidate mode: {mode}")

    return [
        json.loads(line)["user_address"]
        for line in clickhouse.query_text(query).splitlines()
        if line.strip()
    ]


def merge_wallet_lists(primary: list[str], extra: list[str]) -> list[str]:
    wallets: list[str] = []
    seen: set[str] = set()
    for wallet in [*extra, *primary]:
        normalized = str(wallet or "").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        wallets.append(normalized)
    return wallets


def normalize_wallet_list(wallets: list[str]) -> list[str]:
    return merge_wallet_lists([], wallets)


def normalize_event_slug_list(values: list[str]) -> list[str]:
    slugs: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in str(value or "").split(","):
            slug = base_match_slug(item.strip())
            if not slug or slug in seen:
                continue
            seen.add(slug)
            slugs.append(slug)
    return slugs


def merge_event_slug_lists(primary: list[str], extra: list[str]) -> list[str]:
    slugs: list[str] = []
    seen: set[str] = set()
    for slug in [*extra, *primary]:
        normalized = base_match_slug(str(slug or "").strip())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        slugs.append(normalized)
    return slugs


def unusual_betting_task_dedupe_key(
    *,
    slug: str,
    wallet_limit: int,
    trade_limit: int,
    cold_price_threshold: float,
    large_threshold: float,
    very_large_threshold: float,
    extreme_threshold: float,
    include_related_markets: bool,
) -> str:
    return "|".join(
        [
            "unusual-betting",
            base_match_slug(slug),
            str(int(wallet_limit)),
            str(int(trade_limit)),
            f"{float(cold_price_threshold):.6f}",
            f"{float(large_threshold):.2f}",
            f"{float(very_large_threshold):.2f}",
            f"{float(extreme_threshold):.2f}",
            "related" if include_related_markets else "base",
        ]
    )


def tracked_wallet_addresses(app_settings: Settings) -> list[str]:
    try:
        return TrackedWalletStore(dsn=app_settings.postgres_dsn).tracked_addresses()
    except Exception as exc:
        print(f"tracked wallet discovery skipped: {exc}", file=sys.stderr)
        return []


def cmd_tasks_seed_basic(args: argparse.Namespace, app_settings: Settings) -> Any:
    store = task_store_for_args(args, app_settings)
    trade_max_pages = args.max_pages if args.max_pages > 0 else 1
    params = {
        "page_limit": args.page_limit,
        "max_pages": args.max_pages,
        "resume": True,
        "sleep_seconds": 0.0,
        "closed": None,
        "archived": None,
        "active": None,
    }
    added = store.add_many(
        [
            Task(kind="gamma-events", params=dict(params), priority=DISCOVERY_PRIORITY),
            Task(kind="gamma-markets", params=dict(params), priority=DISCOVERY_PRIORITY),
            Task(
                kind="trades",
                params={
                    "page_limit": min(args.page_limit, 500),
                    "max_pages": trade_max_pages,
                    "resume": False,
                    "user": None,
                    "market": None,
                    "event_id": None,
                },
                priority=DISCOVERY_PRIORITY,
            ),
        ]
    )
    return {"added": added, "summary": store.summary(), "task_store": args.task_store}


def cmd_tasks_seed_wallets(args: argparse.Namespace, app_settings: Settings) -> Any:
    if (
        not args.include_trades
        and not args.include_activity
        and not args.include_wallet_portfolio
        and not args.include_wallet_pnl
    ):
        raise ValueError(
            "at least one of include_trades/include_activity/include_wallet_portfolio/include_wallet_pnl must be enabled"
        )
    store = task_store_for_args(args, app_settings)
    refresh_run = args.refresh_run or datetime.now(UTC).replace(microsecond=0).isoformat()
    tracked_wallets = tracked_wallet_addresses(app_settings) if args.include_tracked_wallets else []
    explicit_wallets = normalize_wallet_list([*args.wallets, *tracked_wallets])
    candidate_wallets = []
    if args.wallet_limit > 0:
        candidate_wallets = discover_wallet_candidates(
            ClickHouseWriter(app_settings),
            wallet_limit=args.wallet_limit,
            mode=args.candidate_mode,
            since_hours=args.since_hours,
            min_notional=args.min_notional,
        )
    elif not explicit_wallets:
        raise ValueError("wallet_limit must be positive unless at least one --wallet is provided")
    wallets = merge_wallet_lists(candidate_wallets, explicit_wallets)
    explicit_wallet_set = set(explicit_wallets)
    tasks: list[Task] = []
    for wallet in wallets:
        is_explicit_wallet = wallet in explicit_wallet_set
        wallet_refresh_priority = (
            WALLET_EXPLICIT_REFRESH_PRIORITY if is_explicit_wallet else WALLET_REFRESH_PRIORITY
        )
        wallet_portfolio_priority = (
            WALLET_EXPLICIT_REFRESH_PRIORITY if is_explicit_wallet else WALLET_PORTFOLIO_PRIORITY
        )
        wallet_pnl_priority = WALLET_EXPLICIT_REFRESH_PRIORITY if is_explicit_wallet else WALLET_PNL_PRIORITY
        base_params = {
            "user": wallet,
            "page_limit": min(args.page_limit, 500),
            "max_pages": args.max_pages,
            "resume": False,
            "_refresh_run": refresh_run,
        }
        if args.requeue_done:
            base_params["_requeue_done"] = True
        if args.include_trades:
            tasks.append(
                Task(
                    kind="wallet-trades",
                    params={**base_params, "market": None, "event_id": None},
                    priority=wallet_refresh_priority,
                )
            )
        if args.include_activity:
            tasks.append(
                Task(
                    kind="wallet-activity",
                    params=dict(base_params),
                    priority=wallet_refresh_priority,
                )
            )
        if args.include_wallet_portfolio:
            portfolio_params = {"user": wallet, "_refresh_run": refresh_run}
            if args.requeue_done:
                portfolio_params["_requeue_done"] = True
            tasks.append(
                Task(
                    kind="wallet-portfolio",
                    params=portfolio_params,
                    priority=wallet_portfolio_priority,
                )
            )
        if args.include_wallet_pnl:
            pnl_params = {
                "user": wallet,
                "interval": "all",
                "fidelity": "1d",
                "_refresh_run": refresh_run,
            }
            if args.requeue_done:
                pnl_params["_requeue_done"] = True
            tasks.append(
                Task(
                    kind="wallet-pnl",
                    params=pnl_params,
                    priority=wallet_pnl_priority,
                )
            )

    added = store.add_many(tasks)
    return {
        "added": added,
        "candidate_tasks": len(tasks),
        "wallets": len(wallets),
        "refresh_run": refresh_run,
        "include_trades": args.include_trades,
        "include_activity": args.include_activity,
        "include_wallet_portfolio": args.include_wallet_portfolio,
        "include_wallet_pnl": args.include_wallet_pnl,
        "candidate_mode": args.candidate_mode,
        "explicit_wallets": len(explicit_wallets),
        "summary": store.summary(),
        "task_store": args.task_store,
    }


def cmd_tasks_seed_active_event_wallets(args: argparse.Namespace, app_settings: Settings) -> Any:
    if (
        not args.include_market_trades
        and not args.include_wallet_trades
        and not args.include_wallet_activity
        and not args.include_wallet_portfolio
        and not args.include_wallet_pnl
        and not getattr(args, "include_unusual_betting_refresh", False)
    ):
        raise ValueError("at least one active-event refresh target must be enabled")

    store = task_store_for_args(args, app_settings)
    clickhouse = ClickHouseWriter(app_settings)
    refresh_run = args.refresh_run or "active-event-wallets"

    condition_ids: list[str] = []
    if args.include_market_trades:
        condition_ids = discover_active_event_conditions(
            clickhouse,
            event_limit=args.event_limit,
            condition_limit=args.condition_limit,
        )

    discovered_wallets: list[str] = []
    if (
        args.include_wallet_trades
        or args.include_wallet_activity
        or args.include_wallet_portfolio
        or args.include_wallet_pnl
    ):
        discovered_wallets = discover_active_event_wallets(
            clickhouse,
            wallet_limit=args.wallet_limit,
            since_minutes=args.since_minutes,
            event_limit=args.event_limit,
            condition_limit=args.condition_limit,
            min_notional=args.min_notional,
        )

    tracked_wallets = tracked_wallet_addresses(app_settings) if args.include_tracked_wallets else []
    explicit_wallets = normalize_wallet_list([*args.wallets, *tracked_wallets])
    wallets = merge_wallet_lists(discovered_wallets, explicit_wallets)
    explicit_wallet_set = set(explicit_wallets)

    tasks: list[Task] = []
    if args.include_market_trades:
        for condition_id in condition_ids:
            params = {
                "page_limit": min(args.trade_page_limit, 500),
                "max_pages": args.trade_max_pages,
                "resume": False,
                "user": None,
                "market": condition_id,
                "event_id": None,
                "_refresh_run": refresh_run,
            }
            if args.requeue_done:
                params["_requeue_done"] = True
            tasks.append(Task(kind="trades", params=params, priority=FRONTIER_TRADES_PRIORITY))

    for wallet in wallets:
        is_explicit_wallet = wallet in explicit_wallet_set
        wallet_refresh_priority = (
            WALLET_EXPLICIT_REFRESH_PRIORITY if is_explicit_wallet else ACTIVE_EVENT_WALLET_PRIORITY
        )
        wallet_portfolio_priority = (
            WALLET_EXPLICIT_REFRESH_PRIORITY if is_explicit_wallet else WALLET_PORTFOLIO_PRIORITY
        )
        wallet_pnl_priority = WALLET_EXPLICIT_REFRESH_PRIORITY if is_explicit_wallet else WALLET_PNL_PRIORITY
        base_params = {
            "user": wallet,
            "page_limit": min(args.wallet_page_limit, 500),
            "max_pages": args.wallet_max_pages,
            "resume": False,
            "_refresh_run": refresh_run,
        }
        if args.requeue_done:
            base_params["_requeue_done"] = True
        if args.include_wallet_trades:
            tasks.append(
                Task(
                    kind="wallet-trades",
                    params={**base_params, "market": None, "event_id": None},
                    priority=wallet_refresh_priority,
                )
            )
        if args.include_wallet_activity:
            tasks.append(
                Task(
                    kind="wallet-activity",
                    params=dict(base_params),
                    priority=wallet_refresh_priority,
                )
            )
        if args.include_wallet_portfolio:
            portfolio_params = {"user": wallet, "_refresh_run": refresh_run}
            if args.requeue_done:
                portfolio_params["_requeue_done"] = True
            tasks.append(
                Task(
                    kind="wallet-portfolio",
                    params=portfolio_params,
                    priority=wallet_portfolio_priority,
                )
            )
        if args.include_wallet_pnl:
            pnl_params = {
                "user": wallet,
                "interval": "all",
                "fidelity": "1d",
                "_refresh_run": refresh_run,
            }
            if args.requeue_done:
                pnl_params["_requeue_done"] = True
            tasks.append(
                Task(
                    kind="wallet-pnl",
                    params=pnl_params,
                    priority=wallet_pnl_priority,
                )
            )

    unusual_betting_slugs: list[str] = []
    if getattr(args, "include_unusual_betting_refresh", False):
        unusual_betting_slugs = discover_unusual_betting_event_slugs_from_active_wallets(
            clickhouse,
            event_limit=getattr(args, "unusual_betting_event_limit", 20),
            condition_limit=args.condition_limit,
            since_minutes=args.since_minutes,
            min_notional=getattr(args, "unusual_betting_min_notional", 500_000.0),
        )
        for slug in unusual_betting_slugs:
            params = {
                "slug": slug,
                "wallet_limit": 100,
                "trade_limit": 100,
                "cold_price_threshold": 0.25,
                "large_threshold": 500_000.0,
                "very_large_threshold": 1_000_000.0,
                "extreme_threshold": 5_000_000.0,
                "include_related_markets": True,
                "refresh": True,
                "cache_ttl_seconds": 0,
                "trigger_reason": "active-event-wallets",
                "_refresh_run": refresh_run,
                "_dedupe_key": unusual_betting_task_dedupe_key(
                    slug=slug,
                    wallet_limit=100,
                    trade_limit=100,
                    cold_price_threshold=0.25,
                    large_threshold=500_000.0,
                    very_large_threshold=1_000_000.0,
                    extreme_threshold=5_000_000.0,
                    include_related_markets=True,
                ),
            }
            if args.requeue_done:
                params["_requeue_done"] = True
            tasks.append(
                Task(
                    kind="unusual-betting-refresh",
                    params=params,
                    priority=UNUSUAL_BETTING_REFRESH_PRIORITY,
                )
            )

    added = store.add_many(tasks)
    return {
        "added": added,
        "candidate_tasks": len(tasks),
        "condition_ids": len(condition_ids),
        "wallets": len(wallets),
        "discovered_wallets": len(discovered_wallets),
        "explicit_wallets": len(explicit_wallets),
        "since_minutes": args.since_minutes,
        "refresh_run": refresh_run,
        "include_market_trades": args.include_market_trades,
        "include_wallet_trades": args.include_wallet_trades,
        "include_wallet_activity": args.include_wallet_activity,
        "include_wallet_portfolio": args.include_wallet_portfolio,
        "include_wallet_pnl": args.include_wallet_pnl,
        "include_unusual_betting_refresh": getattr(
            args,
            "include_unusual_betting_refresh",
            False,
        ),
        "unusual_betting_min_notional": getattr(
            args,
            "unusual_betting_min_notional",
            500_000.0,
        ),
        "unusual_betting_events": len(unusual_betting_slugs),
        "summary": store.summary(),
        "task_store": args.task_store,
    }


def cmd_tasks_seed_unusual_betting(args: argparse.Namespace, app_settings: Settings) -> Any:
    store = task_store_for_args(args, app_settings)
    clickhouse = ClickHouseWriter(app_settings)
    refresh_run = args.refresh_run or "unusual-betting"
    explicit_slugs = normalize_event_slug_list(args.events)
    discovered_slugs = discover_unusual_betting_event_slugs(
        clickhouse,
        event_limit=args.event_limit,
        recent_limit=args.recent_limit,
        since_minutes=args.since_minutes,
    )
    slugs = merge_event_slug_lists(discovered_slugs, explicit_slugs)
    tasks: list[Task] = []
    for slug in slugs:
        params = {
            "slug": slug,
            "wallet_limit": args.wallet_limit,
            "trade_limit": args.trade_limit,
            "cold_price_threshold": args.cold_price_threshold,
            "large_threshold": args.large_threshold,
            "very_large_threshold": args.very_large_threshold,
            "extreme_threshold": args.extreme_threshold,
            "include_related_markets": args.include_related_markets,
            "refresh": True,
            "cache_ttl_seconds": 0,
            "trigger_reason": args.trigger_reason,
            "_refresh_run": refresh_run,
            "_dedupe_key": unusual_betting_task_dedupe_key(
                slug=slug,
                wallet_limit=args.wallet_limit,
                trade_limit=args.trade_limit,
                cold_price_threshold=args.cold_price_threshold,
                large_threshold=args.large_threshold,
                very_large_threshold=args.very_large_threshold,
                extreme_threshold=args.extreme_threshold,
                include_related_markets=args.include_related_markets,
            ),
        }
        if args.requeue_done:
            params["_requeue_done"] = True
        tasks.append(
            Task(
                kind="unusual-betting-refresh",
                params=params,
                priority=UNUSUAL_BETTING_REFRESH_PRIORITY,
            )
        )

    added = store.add_many(tasks)
    return {
        "added": added,
        "candidate_tasks": len(tasks),
        "events": len(slugs),
        "discovered_events": len(discovered_slugs),
        "explicit_events": len(explicit_slugs),
        "since_minutes": args.since_minutes,
        "refresh_run": refresh_run,
        "trigger_reason": args.trigger_reason,
        "summary": store.summary(),
        "task_store": args.task_store,
    }


def cmd_tasks_seed_frontier(args: argparse.Namespace, app_settings: Settings) -> Any:
    store = task_store_for_args(args, app_settings)
    refresh_run = args.refresh_run or datetime.now(UTC).replace(microsecond=0).isoformat()
    gamma_params = {
        "page_limit": args.gamma_page_limit,
        "max_pages": args.gamma_max_pages,
        "resume": args.gamma_resume,
        "sleep_seconds": args.gamma_sleep_seconds,
        "closed": None,
        "archived": None,
        "active": True if args.active_only else None,
        "_refresh_run": refresh_run,
    }
    tasks = []
    if args.include_gamma:
        tasks.extend(
            [
                Task(kind="gamma-events", params=dict(gamma_params), priority=FRONTIER_GAMMA_PRIORITY),
                Task(kind="gamma-markets", params=dict(gamma_params), priority=FRONTIER_GAMMA_PRIORITY),
            ]
        )

    event_ids: list[str] = []
    condition_ids: list[str] = []
    token_ids: list[str] = []
    if args.sync_mode == "event":
        event_ids = discover_frontier_events(
            ClickHouseWriter(app_settings),
            event_limit=args.event_limit,
            active_only=args.active_only,
        )
        tasks.extend(
            Task(
                kind="event-refresh",
                params={
                    "event_id": event_id,
                    "refresh_run": refresh_run,
                    "gamma_page_limit": args.gamma_page_limit,
                    "trade_page_limit": args.trade_page_limit,
                    "trade_max_pages": args.trade_max_pages,
                    "price_interval": args.price_interval,
                    "price_fidelity": args.price_fidelity,
                    "holders_limit": args.holders_limit,
                    "positions_limit": args.positions_limit,
                    "include_gamma": False,
                    "include_holders": args.include_holders,
                    "include_market_positions": args.include_market_positions,
                    "include_open_interest": args.include_open_interest,
                },
                priority=FRONTIER_EVENT_PRIORITY,
            )
            for event_id in event_ids
        )
    elif (
        args.include_trades
        or args.include_price_history
        or args.include_books
    ):
        condition_ids, token_ids = discover_frontier_work(
            ClickHouseWriter(app_settings),
            event_limit=args.event_limit,
            condition_limit=args.condition_limit,
            token_limit=args.token_limit,
            active_only=args.active_only,
        )

    if args.include_trades:
        trade_params = []
        if args.include_global_trades:
            params = {
                "page_limit": args.trade_page_limit,
                "max_pages": args.trade_max_pages,
                "resume": False,
                "user": None,
                "market": None,
                "event_id": None,
                "_refresh_run": refresh_run,
            }
            if args.requeue_done:
                params["_requeue_done"] = True
            trade_params.append(params)
        for condition_id in condition_ids:
            params = {
                "page_limit": args.trade_page_limit,
                "max_pages": args.trade_max_pages,
                "resume": False,
                "user": None,
                "market": condition_id,
                "event_id": None,
                "_refresh_run": refresh_run,
            }
            if args.requeue_done:
                params["_requeue_done"] = True
            trade_params.append(params)
        tasks.extend(
            Task(
                kind="trades",
                params=params,
                priority=FRONTIER_TRADES_PRIORITY,
            )
            for params in trade_params
        )
    if args.include_price_history:
        tasks.extend(
            Task(
                kind="prices-history",
                params={
                    "token_id": token_id,
                    "start_ts": None,
                    "end_ts": None,
                    "interval": args.price_interval,
                    "fidelity": args.price_fidelity,
                    "_refresh_run": refresh_run,
                },
                priority=FRONTIER_PRICE_HISTORY_PRIORITY,
            )
            for token_id in token_ids
        )
    if args.include_books:
        tasks.extend(
            Task(
                kind="book",
                params={"token_id": token_id, "_refresh_run": refresh_run},
                priority=FRONTIER_BOOK_PRIORITY,
            )
            for token_id in token_ids
        )

    added = store.add_many(tasks)
    return {
        "added": added,
        "candidate_tasks": len(tasks),
        "event_ids": len(event_ids),
        "condition_ids": len(condition_ids),
        "token_ids": len(token_ids),
        "refresh_run": refresh_run,
        "sync_mode": args.sync_mode,
        "include_gamma": args.include_gamma,
        "summary": store.summary(),
        "task_store": args.task_store,
    }


def cmd_tasks_seed_history(args: argparse.Namespace, app_settings: Settings) -> Any:
    store = task_store_for_args(args, app_settings)
    clickhouse = ClickHouseWriter(app_settings)
    condition_ids = discover_condition_ids_for_history(
        clickhouse,
        event_limit=args.event_limit,
        active_only=args.active_only,
    )
    token_ids = discover_token_ids_for_history(
        clickhouse,
        event_limit=args.event_limit,
        active_only=args.active_only,
    )

    tasks: list[Task] = []
    if args.include_trades:
        tasks.extend(
            Task(
                kind="trades",
                params={
                    "page_limit": args.trade_page_limit,
                    "max_pages": 0,
                    "resume": True,
                    "user": None,
                    "market": condition_id,
                    "event_id": None,
                },
                priority=HISTORY_BACKFILL_PRIORITY,
            )
            for condition_id in condition_ids
        )
    if args.include_price_history:
        tasks.extend(
            Task(
                kind="prices-history",
                params={
                    "token_id": token_id,
                    "start_ts": None,
                    "end_ts": None,
                    "interval": args.price_interval,
                    "fidelity": args.price_fidelity,
                },
                priority=HISTORY_BACKFILL_PRIORITY,
            )
            for token_id in token_ids
        )
    if args.include_books:
        tasks.extend(
            Task(
                kind="book",
                params={"token_id": token_id},
                priority=HISTORY_BACKFILL_PRIORITY,
            )
            for token_id in token_ids
        )
    if args.include_chain_logs:
        if args.chain_from_block is None or args.chain_to_block is None:
            raise ValueError("--chain-from-block and --chain-to-block are required for chain logs")
        if args.chain_block_step <= 0:
            raise ValueError("--chain-block-step must be positive")
        tasks.extend(
            Task(
                kind="chain-logs",
                params={
                    "from_block": start,
                    "to_block": min(start + args.chain_block_step - 1, args.chain_to_block),
                    "addresses": args.chain_addresses,
                    "topics": args.chain_topics,
                },
                priority=CHAIN_BACKFILL_PRIORITY,
            )
            for start in range(
                args.chain_from_block,
                args.chain_to_block + 1,
                args.chain_block_step,
            )
        )

    added = store.add_many(tasks)
    return {
        "added": added,
        "candidate_tasks": len(tasks),
        "condition_ids": len(condition_ids),
        "token_ids": len(token_ids),
        "summary": store.summary(),
        "task_store": args.task_store,
    }


def cmd_tasks_status(args: argparse.Namespace, app_settings: Settings) -> Any:
    store = task_store_for_args(args, app_settings)
    return {"summary": store.summary(), "task_store": args.task_store}


def cmd_tasks_progress(args: argparse.Namespace, app_settings: Settings) -> Any:
    store = task_store_for_args(args, app_settings)
    if not hasattr(store, "progress"):
        raise ValueError(f"{args.task_store} task store does not support progress")
    return {
        **store.progress(recent_limit=args.recent_limit),
        "task_store": args.task_store,
    }


def cmd_tasks_run_once(args: argparse.Namespace, app_settings: Settings) -> Any:
    store = task_store_for_args(args, app_settings)
    return TaskRunner(
        settings=app_settings,
        task_store=store,
        node_id=args.node_id,
        run_store=run_store_for_args(args, store),
    ).run_once()


def cmd_tasks_run_loop(args: argparse.Namespace, app_settings: Settings) -> Any:
    store = task_store_for_args(args, app_settings)
    return TaskRunner(
        settings=app_settings,
        task_store=store,
        node_id=args.node_id,
        run_store=run_store_for_args(args, store),
    ).run_loop(
        max_tasks=args.max_tasks,
        idle_sleep_seconds=args.idle_sleep_seconds,
        stop_on_idle=args.stop_on_idle,
    )


def task_store_for_args(args: argparse.Namespace, app_settings: Settings):
    allowed_kinds = parse_task_kinds(getattr(args, "task_kinds", []))
    if args.task_store == "postgres":
        return PostgresTaskStore(
            dsn=app_settings.postgres_dsn,
            node_id=args.node_id,
            lease_seconds=args.lease_seconds,
            allowed_kinds=allowed_kinds,
        )
    return LocalTaskStore(Path(args.task_file), allowed_kinds=allowed_kinds)


def run_store_for_args(args: argparse.Namespace, task_store):
    if args.task_store == "postgres":
        return task_store
    return LocalRunStore(Path(args.task_file).with_suffix(".runs.jsonl"))


def parse_task_kinds(values: list[str]) -> set[str] | None:
    kinds = {
        item.strip()
        for value in values
        for item in value.split(",")
        if item.strip()
    }
    return kinds or None


def cmd_load_gamma_raw(args: argparse.Namespace, app_settings: Settings) -> Any:
    return GammaRawLoader(clickhouse=ClickHouseWriter(app_settings)).load(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_clob_price_history(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ClobRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_price_history(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_clob_books(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ClobRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_books(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_clob_ws_market_books(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ClobRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_ws_market_books(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
        workers=args.workers,
    )


def cmd_load_data_trades(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_trades(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
        workers=args.workers,
        max_paths=args.max_paths,
        newest_first=args.newest_first,
    )


def cmd_load_data_activity(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_activity(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_data_holders(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_holders(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_data_market_positions(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_market_positions(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_data_positions(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_positions(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_data_wallet_portfolio(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_wallet_portfolio(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_data_user_pnl(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_user_pnl(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_data_open_interest(args: argparse.Namespace, app_settings: Settings) -> Any:
    return DataRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_open_interest(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_chain_logs(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ChainRawLoader(clickhouse=ClickHouseWriter(app_settings)).load_logs(
        raw_root=app_settings.raw_data_dir,
        force=args.force,
        batch_size=args.batch_size,
    )


def cmd_load_exchange_fills(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ChainRawLoader(clickhouse=ClickHouseWriter(app_settings)).build_exchange_fills(
        batch_size=args.batch_size,
        force=args.force,
    )


def cmd_load_orders_matched(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ChainRawLoader(clickhouse=ClickHouseWriter(app_settings)).build_orders_matched(
        batch_size=args.batch_size,
        force=args.force,
    )


def cmd_load_fees_charged(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ChainRawLoader(clickhouse=ClickHouseWriter(app_settings)).build_fee_charged(
        batch_size=args.batch_size,
        force=args.force,
    )


def cmd_load_balance_movements(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ChainRawLoader(clickhouse=ClickHouseWriter(app_settings)).build_balance_movements(
        batch_size=args.batch_size,
        force=args.force,
    )


def cmd_load_lifecycle_events(args: argparse.Namespace, app_settings: Settings) -> Any:
    return ChainRawLoader(clickhouse=ClickHouseWriter(app_settings)).build_lifecycle_events(
        batch_size=args.batch_size,
        force=args.force,
    )


def cmd_build_market_1m(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_market_1m()


def cmd_build_trader_profiles(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_trader_profiles()


def cmd_build_wallet_trade_rollup(args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_wallet_trade_rollup(
        since_hours=args.since_hours,
    )


def cmd_build_trader_chain_pnl(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_trader_chain_pnl()


def cmd_build_event_wallet_pnl(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_event_wallet_pnl()


def cmd_build_live_wallet_positions(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_live_wallet_positions()


def cmd_build_wallet_reputation(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_wallet_reputation()


def cmd_build_wallet_screener(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_wallet_screener()


def cmd_build_wallet_fifa_24h_pnl(args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_wallet_fifa_24h_pnl(
        window_hours=args.window_hours,
    )


def cmd_build_fifa_trades(args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_fifa_trades(
        window_hours=args.window_hours,
    )


def cmd_build_event_anomaly_signals(args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_event_anomaly_signals(
        large_trade_threshold=args.large_trade_threshold,
        liquidity_ratio_threshold=args.liquidity_ratio_threshold,
        coordinated_wallet_threshold=args.coordinated_wallet_threshold,
        coordinated_notional_threshold=args.coordinated_notional_threshold,
        since_hours=args.since_hours,
    )


def cmd_build_analytics_core(_args: argparse.Namespace, app_settings: Settings) -> Any:
    results = MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_analytics_core()
    return {"marts": [asdict(result) for result in results]}


def cmd_build_alerts(args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_alerts(
        price_move_threshold=args.price_move_threshold,
        spread_threshold=args.spread_threshold,
        whale_notional_threshold=args.whale_notional_threshold,
        since_hours=args.since_hours,
    )


def cmd_build_trade_reconciliation(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_trade_reconciliation()


def cmd_build_settlement_audit(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_settlement_audit()


def cmd_build_collector_health(_args: argparse.Namespace, app_settings: Settings) -> Any:
    return MartBuilder(clickhouse=ClickHouseWriter(app_settings)).build_collector_health(
        postgres_dsn=app_settings.postgres_dsn
    )


def cmd_stream_create_topic(args: argparse.Namespace, _app_settings: Settings) -> Any:
    created = RpkPublisher(service=args.service).create_topic(args.topic)
    return {"topic": args.topic, "created": created}


def cmd_stream_ws_market_raw(args: argparse.Namespace, app_settings: Settings) -> Any:
    return publish_ws_market_raw(
        raw_root=app_settings.raw_data_dir,
        publisher=RpkPublisher(service=args.service),
        topic=args.topic,
        max_records=args.max_records,
        batch_size=args.batch_size,
        create_topic=not args.skip_create_topic,
    )


def cmd_realtime_rebuild_books(args: argparse.Namespace, app_settings: Settings) -> Any:
    reconstructor = reconstruct_ws_market_raw(
        raw_root=app_settings.raw_data_dir,
        max_records=args.max_records,
    )
    return {"books": reconstructor.summaries(), "count": len(reconstructor.books)}


def cmd_realtime_reconcile_book(args: argparse.Namespace, app_settings: Settings) -> Any:
    reconstructor = reconstruct_ws_market_raw(
        raw_root=app_settings.raw_data_dir,
        max_records=args.max_records,
    )
    reconstructed = reconstructor.books.get(args.token_id)
    if reconstructed is None:
        raise ValueError(f"No reconstructed book found for token_id={args.token_id}")
    page = PolymarketClient(app_settings).clob_book(token_id=args.token_id)
    rest_book = page.items[0] if page.items else {}
    return reconciliation_diff(
        reconstructed=reconstructed.summary(),
        rest=rest_book_summary(rest_book, token_id=args.token_id),
    )


def cmd_realtime_trade_stream(args: argparse.Namespace, app_settings: Settings) -> Any:
    serve_trade_stream(
        settings=app_settings,
        clickhouse=ClickHouseWriter(app_settings),
        host=args.host,
        port=args.port,
        poll_seconds=args.poll_seconds,
        limit=args.limit,
        heartbeat_seconds=args.heartbeat_seconds,
    )
    return None


def cmd_realtime_official_trade_feed(args: argparse.Namespace, app_settings: Settings) -> Any:
    serve_official_trade_feed(
        host=args.host,
        port=args.port,
        upstream_url=args.upstream_url,
        state_dir=app_settings.state_dir,
    )
    return None


def cmd_api_serve(args: argparse.Namespace, app_settings: Settings) -> Any:
    serve_api(settings=app_settings, host=args.host, port=args.port)
    return None


def parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value == "true"


def parse_token_ids(values: list[str]) -> list[str]:
    token_ids: list[str] = []
    for value in values:
        token_ids.extend(token.strip() for token in value.split(",") if token.strip())
    return token_ids


def raw_writer(app_settings: Settings) -> RawJsonlWriter:
    return RawJsonlWriter(
        app_settings.raw_data_dir,
        chunk_records=app_settings.raw_chunk_records,
        chunk_seconds=app_settings.raw_chunk_seconds,
    )


def print_json(value: Any) -> None:
    if is_dataclass(value):
        value = asdict(value)
    print(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    raise SystemExit(main())
