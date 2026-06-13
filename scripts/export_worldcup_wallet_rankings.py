from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from zetta.config import Settings
from zetta.storage.clickhouse import ClickHouseWriter


INPUT_SLUGS = [
    "fifwc-mex-rsa-2026-06-11",
    "fifwc-mex-rsa-2026-06-11-exact-score",
    "fifwc-mex-rsa-2026-06-11-halftime-result",
    "fifwc-mex-rsa-2026-06-11-more-markets",
    "fifwc-kr-cze-2026-06-11",
    "fifwc-kr-cze-2026-06-11-exact-score",
    "fifwc-kr-cze-2026-06-11-halftime-result",
    "fifwc-kr-cze-2026-06-11-more-markets",
    "fifwc-can-bih-2026-06-12",
    "fifwc-can-bih-2026-06-12-exact-score",
    "fifwc-can-bih-2026-06-12-halftime-result",
    "fifwc-can-bih-2026-06-12-more-markets",
    "fifwc-usa-par-2026-06-12",
    "fifwc-usa-par-2026-06-12-exact-score",
    "fifwc-usa-par-2026-06-12-halftime-result",
    "fifwc-usa-par-2026-06-12-more-markets",
    "fifwc-qat-che-2026-06-13",
    "fifwc-qat-che-2026-06-13-exact-score",
    "fifwc-qat-che-2026-06-13-halftime-result",
    "fifwc-qat-che-2026-06-13-more-markets",
    "fifwc-bra-mar-2026-06-13",
    "fifwc-bra-mar-2026-06-13-exact-score",
    "fifwc-bra-mar-2026-06-13-halftime-result",
    "fifwc-bra-mar-2026-06-13-more-markets",
    "fifwc-hai-sco-2026-06-13",
    "fifwc-hai-sco-2026-06-13-exact-score",
    "fifwc-hai-sco-2026-06-13-halftime-result",
    "fifwc-hai-sco-2026-06-13-more-markets",
]

OUTPUT = Path("data/worldcup_wallet_per_wallet_rankings_20260613.json")
EPS = 1e-6
MIN_ROI_BUY_NOTIONAL = 100.0


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def rows_from(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def to_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def to_int(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def rounded(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        return round(value, 10)
    return value


def rounded_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {key: rounded(value) for key, value in row.items()}


def base_match_slug(event_slug: str) -> str:
    for suffix in ("-exact-score", "-halftime-result", "-more-markets"):
        if event_slug.endswith(suffix):
            return event_slug[: -len(suffix)]
    return event_slug


def metric_roi(pnl: float, buy_notional: float) -> float | None:
    if buy_notional <= EPS:
        return None
    return pnl / buy_notional


def first_non_empty(values: list[str]) -> str:
    for value in values:
        if value:
            return value
    return ""


def clickhouse() -> ClickHouseWriter:
    return ClickHouseWriter(
        Settings(
            clickhouse_host="127.0.0.1",
            clickhouse_port=8123,
            clickhouse_user="zetta",
            clickhouse_password="zetta",
            clickhouse_database="zetta",
            request_timeout_seconds=300,
        )
    )


def ranking_entry(
    wallet: dict[str, Any], metric_name: str, rank_match: dict[str, Any] | None = None
) -> dict[str, Any]:
    cumulative = wallet["wallet_cumulative"]
    if rank_match is None:
        rank_metric = {
            "name": metric_name,
            "value": cumulative["pnl"] if metric_name == "cumulative_pnl" else cumulative["roi"],
            "pnl": cumulative["pnl"],
            "roi": cumulative["roi"],
            "buy_notional": cumulative["buy_notional"],
            "traded_notional": cumulative["traded_notional"],
            "match_count": cumulative["match_count"],
        }
    else:
        rank_metric = {
            "name": metric_name,
            "value": rank_match["pnl"] if metric_name == "single_match_pnl" else rank_match["roi"],
            "pnl": rank_match["pnl"],
            "roi": rank_match["roi"],
            "buy_notional": rank_match["buy_notional"],
            "traded_notional": rank_match["traded_notional"],
            "match_slug": rank_match["match_slug"],
            "match_title": rank_match["match_title"],
        }
    return {
        "user_address": wallet["user_address"],
        "rank_metric": rounded_dict(rank_metric),
        "wallet_cumulative": cumulative,
        "rank_match": rank_match,
        "matches": wallet["matches"],
    }


def main() -> None:
    ch = clickhouse()
    slug_sql = ",".join(sql_string(slug) for slug in INPUT_SLUGS)

    metadata_rows = rows_from(
        ch.query_text(
            f"""
            select
              e.event_id as event_id,
              e.slug as event_slug,
              e.title as event_title,
              m.market_id as market_id,
              m.condition_id as condition_id,
              m.question as market_question,
              t.token_id as token_id,
              t.outcome as outcome
            from dim_event as e final
            left join dim_market as m final on m.event_id = e.event_id
            left join dim_outcome_token as t final on t.market_id = m.market_id
            where e.slug in ({slug_sql})
              and m.condition_id != ''
              and t.token_id != ''
            format JSONEachRow
            """
        )
    )
    if not metadata_rows:
        raise SystemExit("No event metadata found for input slugs")

    found_event_slugs = sorted({row["event_slug"] for row in metadata_rows})
    missing_event_slugs = [slug for slug in INPUT_SLUGS if slug not in set(found_event_slugs)]
    condition_ids = sorted({row["condition_id"] for row in metadata_rows if row.get("condition_id")})
    token_ids = sorted({row["token_id"] for row in metadata_rows if row.get("token_id")})
    if not condition_ids or not token_ids:
        raise SystemExit("No conditions/tokens found for input slugs")

    market_by_condition = {}
    token_meta = {}
    event_by_slug = {}
    match_titles: dict[str, str] = {}
    match_event_slugs: dict[str, set[str]] = defaultdict(set)
    coverage_by_match: dict[str, dict[str, Any]] = {}
    for row in metadata_rows:
        event_slug = row["event_slug"]
        match_slug = base_match_slug(event_slug)
        event_by_slug[event_slug] = {
            "event_id": row["event_id"],
            "event_slug": event_slug,
            "event_title": row["event_title"],
        }
        match_event_slugs[match_slug].add(event_slug)
        if event_slug == match_slug:
            match_titles[match_slug] = row["event_title"]
        market_by_condition[row["condition_id"]] = {
            "event_id": row["event_id"],
            "event_slug": event_slug,
            "event_title": row["event_title"],
            "match_slug": match_slug,
            "market_id": row["market_id"],
            "condition_id": row["condition_id"],
            "market_question": row["market_question"],
        }
        token_meta[row["token_id"]] = {
            "token_id": row["token_id"],
            "market_id": row["market_id"],
            "condition_id": row["condition_id"],
            "outcome": row["outcome"],
        }

    for row in metadata_rows:
        match_slug = base_match_slug(row["event_slug"])
        if match_slug not in match_titles and row["event_title"]:
            match_titles[match_slug] = row["event_title"]

    for match_slug in sorted(match_event_slugs):
        match_conditions = {
            row["condition_id"] for row in metadata_rows if base_match_slug(row["event_slug"]) == match_slug
        }
        match_tokens = {
            row["token_id"] for row in metadata_rows if base_match_slug(row["event_slug"]) == match_slug
        }
        coverage_by_match[match_slug] = {
            "title": match_titles.get(match_slug, ""),
            "event_slugs": sorted(match_event_slugs[match_slug]),
            "event_count": len(match_event_slugs[match_slug]),
            "condition_count": len(match_conditions),
            "token_count": len(match_tokens),
        }

    condition_sql = ",".join(sql_string(condition_id) for condition_id in condition_ids)
    token_sql = ",".join(sql_string(token_id) for token_id in token_ids)

    mark_rows = rows_from(
        ch.query_text(
            f"""
            select
              ids.token_id as token_id,
              multiIf(
                isNotNull(book.book_best_bid) and isNotNull(book.book_best_ask), (book.book_best_bid + book.book_best_ask) / 2,
                isNotNull(price.price), price.price,
                cast(null, 'Nullable(Float64)')
              ) as mark_price,
              multiIf(
                isNotNull(book.book_best_bid) and isNotNull(book.book_best_ask), 'orderbook_mid',
                isNotNull(price.price), 'price_history',
                'missing'
              ) as mark_price_source,
              if(
                isNotNull(book.book_best_bid) and isNotNull(book.book_best_ask),
                book.mark_at,
                price.mark_at
              ) as mark_price_at
            from (select arrayJoin([{token_sql}]) as token_id) as ids
            left join
            (
              select token_id, argMax(price, timestamp) as price, max(timestamp) as mark_at
              from fact_price_history
              where token_id in ({token_sql})
              group by token_id
            ) as price on ids.token_id = price.token_id
            left join
            (
              select
                token_id,
                argMax(best_bid, captured_at) as book_best_bid,
                argMax(best_ask, captured_at) as book_best_ask,
                max(captured_at) as mark_at
              from fact_orderbook_snapshot
              where token_id in ({token_sql})
                and best_bid is not null
                and best_ask is not null
              group by token_id
            ) as book on ids.token_id = book.token_id
            format JSONEachRow
            """
        )
    )
    marks = {row["token_id"]: row for row in mark_rows}

    trade_rows = rows_from(
        ch.query_text(
            f"""
            select
              condition_id,
              token_id,
              user_address,
              count() as trade_count,
              countIf(side = 'BUY') as buy_count,
              countIf(side = 'SELL') as sell_count,
              sumIf(size, side = 'BUY') as buy_size,
              sumIf(size, side = 'SELL') as sell_size,
              sumIf(notional, side = 'BUY') as buy_notional,
              sumIf(notional, side = 'SELL') as sell_notional,
              sum(notional) as traded_notional,
              min(timestamp) as first_trade_at,
              max(timestamp) as last_trade_at
            from
            (
              select
                dedupe_id,
                argMax(timestamp, ingested_at) as timestamp,
                argMax(condition_id, ingested_at) as condition_id,
                argMax(token_id, ingested_at) as token_id,
                lower(argMax(user_address, ingested_at)) as user_address,
                upper(argMax(side, ingested_at)) as side,
                argMax(size, ingested_at) as size,
                argMax(notional, ingested_at) as notional
              from
              (
                select
                  if(
                    trade_id != '',
                    concat(trade_id, ':', lower(user_address), ':', token_id, ':', side),
                    concat(transaction_hash, ':', toString(log_index), ':', lower(user_address), ':', token_id, ':', side)
                  ) as dedupe_id,
                  timestamp,
                  condition_id,
                  token_id,
                  user_address,
                  side,
                  size,
                  notional,
                  ingested_at
                from fact_trade
                where condition_id in ({condition_sql})
                  and user_address != ''
                  and token_id != ''
              )
              group by dedupe_id
            )
            group by condition_id, token_id, user_address
            format JSONEachRow
            """
        )
    )

    match_token_rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    raw_wallet_match_rows = set()
    raw_wallets = set()
    for row in trade_rows:
        user = row["user_address"].lower()
        condition_id = row["condition_id"]
        token_id = row["token_id"]
        market = market_by_condition.get(condition_id)
        if not market:
            continue
        mark = marks.get(token_id, {})
        mark_price = mark.get("mark_price")
        mark_price_float = None if mark_price is None else float(mark_price)
        buy_size = to_float(row["buy_size"])
        sell_size = to_float(row["sell_size"])
        position_size = buy_size - sell_size
        buy_notional = to_float(row["buy_notional"])
        sell_notional = to_float(row["sell_notional"])
        current_value = position_size * mark_price_float if position_size > EPS and mark_price_float is not None else 0.0
        token_pnl = sell_notional - buy_notional + current_value
        token_row = {
            "event_slug": market["event_slug"],
            "event_title": market["event_title"],
            "match_slug": market["match_slug"],
            "match_title": match_titles.get(market["match_slug"], market["event_title"]),
            "market_id": market["market_id"],
            "condition_id": condition_id,
            "market_question": market["market_question"],
            "token_id": token_id,
            "outcome": token_meta.get(token_id, {}).get("outcome", ""),
            "trade_count": to_int(row["trade_count"]),
            "buy_count": to_int(row["buy_count"]),
            "sell_count": to_int(row["sell_count"]),
            "buy_size": buy_size,
            "sell_size": sell_size,
            "position_size": position_size,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "traded_notional": to_float(row["traded_notional"]),
            "net_cashflow": sell_notional - buy_notional,
            "mark_price": mark_price_float,
            "mark_price_source": mark.get("mark_price_source", "missing"),
            "mark_price_at": mark.get("mark_price_at"),
            "current_value": current_value,
            "pnl": token_pnl,
            "roi": metric_roi(token_pnl, buy_notional),
            "first_trade_at": row.get("first_trade_at"),
            "last_trade_at": row.get("last_trade_at"),
            "missing_mark": position_size > EPS and mark_price_float is None,
            "negative_position": position_size < -EPS,
        }
        key = (user, market["match_slug"])
        raw_wallet_match_rows.add(key)
        raw_wallets.add(user)
        match_token_rows[key].append(token_row)

    wallet_matches: dict[str, list[dict[str, Any]]] = defaultdict(list)
    excluded_negative_rows = 0
    excluded_negative_wallets = set()
    for (user, match_slug), token_rows in match_token_rows.items():
        negative_tokens = [row for row in token_rows if row["negative_position"]]
        if negative_tokens:
            excluded_negative_rows += 1
            excluded_negative_wallets.add(user)
            continue
        buy_notional = sum(row["buy_notional"] for row in token_rows)
        sell_notional = sum(row["sell_notional"] for row in token_rows)
        current_value = sum(row["current_value"] for row in token_rows)
        pnl = sell_notional - buy_notional + current_value
        event_slugs = sorted({row["event_slug"] for row in token_rows})
        token_ids_for_match = sorted({row["token_id"] for row in token_rows})
        match_row = {
            "match_slug": match_slug,
            "match_title": match_titles.get(
                match_slug, first_non_empty([row["event_title"] for row in token_rows])
            ),
            "event_slugs": event_slugs,
            "event_count": len(event_slugs),
            "market_count": len({row["market_id"] for row in token_rows}),
            "token_count": len(token_ids_for_match),
            "trade_count": sum(row["trade_count"] for row in token_rows),
            "buy_count": sum(row["buy_count"] for row in token_rows),
            "sell_count": sum(row["sell_count"] for row in token_rows),
            "buy_size": sum(row["buy_size"] for row in token_rows),
            "sell_size": sum(row["sell_size"] for row in token_rows),
            "position_size": sum(row["position_size"] for row in token_rows),
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "traded_notional": sum(row["traded_notional"] for row in token_rows),
            "net_cashflow": sell_notional - buy_notional,
            "current_value": current_value,
            "pnl": pnl,
            "roi": metric_roi(pnl, buy_notional),
            "first_trade_at": min(row["first_trade_at"] for row in token_rows if row.get("first_trade_at")),
            "last_trade_at": max(row["last_trade_at"] for row in token_rows if row.get("last_trade_at")),
            "missing_mark_tokens": sum(1 for row in token_rows if row["missing_mark"]),
            "negative_position_tokens": 0,
            "tokens": [
                rounded_dict(row)
                for row in sorted(
                    token_rows,
                    key=lambda row: (
                        row["event_slug"],
                        row["market_question"],
                        row["outcome"],
                        row["token_id"],
                    ),
                )
            ],
        }
        wallet_matches[user].append(rounded_dict(match_row))

    wallet_records = []
    for user, matches in wallet_matches.items():
        buy_notional = sum(match["buy_notional"] for match in matches)
        sell_notional = sum(match["sell_notional"] for match in matches)
        current_value = sum(match["current_value"] for match in matches)
        pnl = sell_notional - buy_notional + current_value
        cumulative = {
            "user_address": user,
            "match_count": len({match["match_slug"] for match in matches}),
            "event_count": sum(match["event_count"] for match in matches),
            "market_count": sum(match["market_count"] for match in matches),
            "token_count": sum(match["token_count"] for match in matches),
            "trade_count": sum(match["trade_count"] for match in matches),
            "buy_count": sum(match["buy_count"] for match in matches),
            "sell_count": sum(match["sell_count"] for match in matches),
            "buy_size": sum(match["buy_size"] for match in matches),
            "sell_size": sum(match["sell_size"] for match in matches),
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "traded_notional": sum(match["traded_notional"] for match in matches),
            "net_cashflow": sell_notional - buy_notional,
            "current_value": current_value,
            "pnl": pnl,
            "roi": metric_roi(pnl, buy_notional),
            "first_trade_at": min(match["first_trade_at"] for match in matches if match.get("first_trade_at")),
            "last_trade_at": max(match["last_trade_at"] for match in matches if match.get("last_trade_at")),
            "missing_mark_tokens": sum(match["missing_mark_tokens"] for match in matches),
            "negative_position_tokens": 0,
        }
        sorted_matches = sorted(matches, key=lambda match: match["pnl"], reverse=True)
        roi_matches = [
            match
            for match in matches
            if match.get("roi") is not None and match["buy_notional"] >= MIN_ROI_BUY_NOTIONAL
        ]
        wallet_records.append(
            {
                "user_address": user,
                "wallet_cumulative": rounded_dict(cumulative),
                "best_single_match_by_profit": sorted_matches[0] if sorted_matches else None,
                "best_single_match_by_roi_min_buy_100": (
                    sorted(roi_matches, key=lambda match: match["roi"], reverse=True)[0]
                    if roi_matches
                    else None
                ),
                "matches": sorted(matches, key=lambda match: (match["match_slug"], match["first_trade_at"] or "")),
            }
        )

    # User-facing ranking rule: every wallet in every ranking must be net-positive across
    # the supplied World Cup match scope, so a negative cumulative ROI cannot appear.
    eligible_positive_cumulative = [
        wallet
        for wallet in wallet_records
        if wallet["wallet_cumulative"]["pnl"] > EPS
        and wallet["wallet_cumulative"]["roi"] is not None
        and wallet["wallet_cumulative"]["roi"] > EPS
    ]

    single_profit_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["best_single_match_by_profit"] is not None
            and wallet["best_single_match_by_profit"]["pnl"] > EPS
        ],
        key=lambda wallet: wallet["best_single_match_by_profit"]["pnl"],
        reverse=True,
    )[:100]
    single_roi_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["best_single_match_by_roi_min_buy_100"] is not None
            and wallet["best_single_match_by_roi_min_buy_100"]["roi"] is not None
            and wallet["best_single_match_by_roi_min_buy_100"]["roi"] > EPS
        ],
        key=lambda wallet: wallet["best_single_match_by_roi_min_buy_100"]["roi"],
        reverse=True,
    )[:100]
    multi_profit_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["wallet_cumulative"]["match_count"] >= 2
            and wallet["wallet_cumulative"]["pnl"] > EPS
        ],
        key=lambda wallet: wallet["wallet_cumulative"]["pnl"],
        reverse=True,
    )[:100]
    multi_roi_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["wallet_cumulative"]["match_count"] >= 2
            and wallet["wallet_cumulative"]["buy_notional"] >= MIN_ROI_BUY_NOTIONAL
            and wallet["wallet_cumulative"]["roi"] is not None
            and wallet["wallet_cumulative"]["roi"] > EPS
        ],
        key=lambda wallet: wallet["wallet_cumulative"]["roi"],
        reverse=True,
    )[:100]

    mark_source_counts = {
        source: sum(1 for mark in marks.values() if mark.get("mark_price_source") == source)
        for source in {mark.get("mark_price_source", "missing") for mark in marks.values()}
    }
    output = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "method": (
            "wallet-centric mark-to-market from deduplicated fact_trade rows; "
            "token pnl=sell_notional-buy_notional+positive_position_size*mark_price; "
            "roi=pnl/buy_notional; marks use latest orderbook mid when available, otherwise latest price_history; "
            "wallet-match rows with negative token positions are excluded; "
            "all ranking entries require cumulative wallet pnl > 0 and cumulative wallet roi > 0 within the supplied match scope"
        ),
        "ranking_definitions": {
            "single_match_profit_wallets": (
                "Wallets ranked by their best single-match PnL; rank_match contains the ranking match; "
                "wallet_cumulative must also be positive."
            ),
            "single_match_roi_wallets_min_buy_100": (
                "Wallets ranked by their best single-match ROI with that match buy_notional >= 100; "
                "wallet_cumulative must also be positive."
            ),
            "multi_match_profit_wallets": (
                "Wallets with at least 2 matches ranked by cumulative PnL across the supplied match scope; "
                "cumulative PnL and ROI must be positive."
            ),
            "multi_match_roi_wallets_min_buy_100": (
                "Wallets with at least 2 matches and cumulative buy_notional >= 100 ranked by cumulative ROI; "
                "cumulative PnL and ROI must be positive."
            ),
        },
        "scope": {
            "input_slugs": INPUT_SLUGS,
            "input_slug_count": len(INPUT_SLUGS),
            "match_count": len(coverage_by_match),
            "found_event_slugs": found_event_slugs,
            "missing_event_slugs": missing_event_slugs,
            "condition_count": len(condition_ids),
            "token_count": len(token_ids),
            "coverage_by_match": coverage_by_match,
        },
        "quality": {
            "trade_token_rows": len(trade_rows),
            "raw_wallet_match_rows": len(raw_wallet_match_rows),
            "raw_wallet_count": len(raw_wallets),
            "excluded_wallet_match_rows_with_negative_positions": excluded_negative_rows,
            "excluded_wallets_with_negative_positions": len(excluded_negative_wallets),
            "wallet_count_after_quality_filters": len(wallet_records),
            "positive_cumulative_wallet_count": len(eligible_positive_cumulative),
            "missing_mark_token_count": sum(
                1
                for wallet in wallet_records
                for match in wallet["matches"]
                for token in match["tokens"]
                if token["missing_mark"]
            ),
            "mark_source_counts": dict(sorted(mark_source_counts.items())),
        },
        "wallet_count": len(wallet_records),
        "positive_cumulative_wallet_count": len(eligible_positive_cumulative),
        "single_match_profit_wallets": [
            {
                "rank": index + 1,
                **ranking_entry(wallet, "single_match_pnl", wallet["best_single_match_by_profit"]),
            }
            for index, wallet in enumerate(single_profit_ranked)
        ],
        "single_match_roi_wallets_min_buy_100": [
            {
                "rank": index + 1,
                **ranking_entry(wallet, "single_match_roi", wallet["best_single_match_by_roi_min_buy_100"]),
            }
            for index, wallet in enumerate(single_roi_ranked)
        ],
        "multi_match_profit_wallets": [
            {"rank": index + 1, **ranking_entry(wallet, "cumulative_pnl")}
            for index, wallet in enumerate(multi_profit_ranked)
        ],
        "multi_match_roi_wallets_min_buy_100": [
            {"rank": index + 1, **ranking_entry(wallet, "cumulative_roi")}
            for index, wallet in enumerate(multi_roi_ranked)
        ],
    }

    for list_name in (
        "single_match_profit_wallets",
        "single_match_roi_wallets_min_buy_100",
        "multi_match_profit_wallets",
        "multi_match_roi_wallets_min_buy_100",
    ):
        for entry in output[list_name]:
            cumulative = entry["wallet_cumulative"]
            if cumulative["roi"] is None or cumulative["roi"] <= EPS or cumulative["pnl"] <= EPS:
                raise AssertionError(
                    (list_name, entry["rank"], entry["user_address"], cumulative["pnl"], cumulative["roi"])
                )
            metric = entry["rank_metric"]
            if metric["value"] is None or metric["value"] <= EPS:
                raise AssertionError((list_name, entry["rank"], entry["user_address"], metric))

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    print("wrote", OUTPUT)
    print("wallet_count_after_quality_filters", len(wallet_records))
    print("positive_cumulative_wallet_count", len(eligible_positive_cumulative))
    for list_name in (
        "single_match_profit_wallets",
        "single_match_roi_wallets_min_buy_100",
        "multi_match_profit_wallets",
        "multi_match_roi_wallets_min_buy_100",
    ):
        rows = output[list_name]
        print(list_name, len(rows))
        if rows:
            first = rows[0]
            print(
                "  #1",
                first["user_address"],
                first["rank_metric"],
                "cumulative",
                {key: first["wallet_cumulative"][key] for key in ("pnl", "roi", "match_count", "buy_notional")},
            )


if __name__ == "__main__":
    main()
