from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any


EPS = 1e-6
MIN_ROI_BUY_NOTIONAL = 100.0
MIN_VOLUME_NOTIONAL = 10_000.0
TOKEN_MARK_QUERY_CHUNK_SIZE = 200
EVENT_VARIANT_SUFFIXES = ("-exact-score", "-halftime-result", "-more-markets")
RANKING_LIST_NAMES = (
    "cumulative_profit_wallets",
    "cumulative_roi_wallets_min_buy_100",
    "cumulative_profit_wallets_min_volume_10000",
    "cumulative_roi_wallets_min_volume_10000",
    "single_match_profit_wallets",
    "single_match_roi_wallets_min_buy_100",
    "multi_match_profit_wallets",
    "multi_match_roi_wallets_min_buy_100",
)

WORLD_CUP_BASE_MATCH_SLUGS = (
    "fifwc-mex-rsa-2026-06-11",
    "fifwc-kr-cze-2026-06-11",
    "fifwc-can-bih-2026-06-12",
    "fifwc-usa-par-2026-06-12",
    "fifwc-qat-che-2026-06-13",
    "fifwc-bra-mar-2026-06-13",
    "fifwc-hai-sco-2026-06-13",
    "fifwc-aus-tur-2026-06-14",
    "fifwc-ger-kor-2026-06-14",
    "fifwc-nld-jpn-2026-06-14",
    "fifwc-civ-ecu-2026-06-14",
    "fifwc-swe-tun-2026-06-14",
    "fifwc-esp-cvi-2026-06-15",
    "fifwc-bel-egy-2026-06-15",
    "fifwc-ksa-ury-2026-06-15",
    "fifwc-irn-nzl-2026-06-15",
    "fifwc-fra-sen-2026-06-16",
    "fifwc-irq-nor-2026-06-16",
    "fifwc-arg-alg-2026-06-16",
    "fifwc-aut-jor-2026-06-17",
    "fifwc-prt-cdr-2026-06-17",
    "fifwc-eng-hrv-2026-06-17",
    "fifwc-gha-pan-2026-06-17",
    "fifwc-uzb-col-2026-06-17",
    "fifwc-cze-rsa-2026-06-18",
    "fifwc-che-bih-2026-06-18",
    "fifwc-can-qat-2026-06-18",
    "fifwc-mex-kr-2026-06-18",
    "fifwc-usa-aus-2026-06-19",
    "fifwc-sco-mar-2026-06-19",
    "fifwc-bra-hai-2026-06-19",
    "fifwc-tur-par-2026-06-19",
    "fifwc-nld-swe-2026-06-20",
    "fifwc-ger-civ-2026-06-20",
    "fifwc-ecu-kor-2026-06-20",
    "fifwc-tun-jpn-2026-06-21",
    "fifwc-esp-ksa-2026-06-21",
    "fifwc-bel-irn-2026-06-21",
    "fifwc-ury-cvi-2026-06-21",
    "fifwc-nzl-egy-2026-06-21",
    "fifwc-arg-aut-2026-06-22",
    "fifwc-fra-irq-2026-06-22",
    "fifwc-nor-sen-2026-06-22",
    "fifwc-jor-alg-2026-06-22",
    "fifwc-prt-uzb-2026-06-23",
    "fifwc-eng-gha-2026-06-23",
    "fifwc-pan-hrv-2026-06-23",
    "fifwc-col-cdr-2026-06-23",
    "fifwc-bih-qat-2026-06-24",
    "fifwc-che-can-2026-06-24",
    "fifwc-mar-hai-2026-06-24",
    "fifwc-sco-bra-2026-06-24",
    "fifwc-cze-mex-2026-06-24",
    "fifwc-rsa-kr-2026-06-24",
    "fifwc-kor-civ-2026-06-25",
    "fifwc-ecu-ger-2026-06-25",
    "fifwc-jpn-swe-2026-06-25",
    "fifwc-tun-nld-2026-06-25",
    "fifwc-par-aus-2026-06-25",
    "fifwc-tur-usa-2026-06-25",
    "fifwc-nor-fra-2026-06-26",
    "fifwc-sen-irq-2026-06-26",
    "fifwc-cvi-ksa-2026-06-26",
    "fifwc-ury-esp-2026-06-26",
    "fifwc-egy-irn-2026-06-26",
    "fifwc-nzl-bel-2026-06-26",
    "fifwc-hrv-gha-2026-06-27",
    "fifwc-pan-eng-2026-06-27",
    "fifwc-col-prt-2026-06-27",
    "fifwc-cdr-uzb-2026-06-27",
    "fifwc-alg-aut-2026-06-27",
    "fifwc-jor-arg-2026-06-27",
)

DEFAULT_WORLD_CUP_EVENT_SLUGS = tuple(
    event_slug
    for match_slug in WORLD_CUP_BASE_MATCH_SLUGS
    for event_slug in (match_slug, *(f"{match_slug}{suffix}" for suffix in EVENT_VARIANT_SUFFIXES))
)

MATCH_DATE_RE = re.compile(
    r"-(\d{4}-\d{2}-\d{2})(?:-(?:exact-score|halftime-result|more-markets))?$"
)


def rows_from(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def query_clickhouse(clickhouse: Any, query: str) -> str:
    if hasattr(clickhouse, "query_body_text"):
        return clickhouse.query_body_text(query)
    return clickhouse.query_text(query)


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


def not_empty(value: Any) -> bool:
    return value is not None and value != ""


def is_fresh(mark_at: Any, last_trade_at: Any) -> bool:
    if not not_empty(mark_at) or not not_empty(last_trade_at):
        return False
    return str(mark_at) >= str(last_trade_at)


def base_match_slug(event_slug: str) -> str:
    for suffix in EVENT_VARIANT_SUFFIXES:
        if event_slug.endswith(suffix):
            return event_slug[: -len(suffix)]
    return event_slug


def match_slug_date(event_slug: str) -> str:
    match = MATCH_DATE_RE.search(event_slug)
    return match.group(1) if match else ""


def expand_match_slugs(slugs: list[str] | tuple[str, ...]) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()
    for slug in slugs:
        normalized = str(slug or "").strip()
        if not normalized:
            continue
        base_slug = base_match_slug(normalized)
        variants = (base_slug, *(f"{base_slug}{suffix}" for suffix in EVENT_VARIANT_SUFFIXES))
        for event_slug in variants:
            if event_slug not in seen:
                expanded.append(event_slug)
                seen.add(event_slug)
    return expanded


def parse_slug_values(query: dict[str, list[str]], *keys: str) -> list[str]:
    slugs: list[str] = []
    for key in keys:
        for raw_value in query.get(key, []):
            for value in str(raw_value or "").replace("\n", ",").split(","):
                slug = value.strip()
                if slug:
                    slugs.append(slug)
    return slugs


def worldcup_event_slugs_for_scope(
    slugs: list[str] | tuple[str, ...] | None = None,
    *,
    date_from: str = "",
    date_to: str = "",
    expand_variants: bool = True,
) -> list[str]:
    source = list(WORLD_CUP_BASE_MATCH_SLUGS if slugs is None else slugs)
    if date_from or date_to:
        source = [
            slug
            for slug in source
            if (not date_from or match_slug_date(slug) >= date_from)
            and (not date_to or match_slug_date(slug) <= date_to)
        ]
    if expand_variants:
        return expand_match_slugs(source)
    seen: set[str] = set()
    scoped: list[str] = []
    for slug in source:
        normalized = str(slug or "").strip()
        if normalized and normalized not in seen:
            scoped.append(normalized)
            seen.add(normalized)
    return scoped


def metric_roi(pnl: float, buy_notional: float) -> float | None:
    if buy_notional <= EPS:
        return None
    return pnl / buy_notional


def first_non_empty(values: list[str]) -> str:
    for value in values:
        if value:
            return value
    return ""


def min_non_empty(values: list[Any]) -> Any:
    present = [value for value in values if value]
    return min(present) if present else None


def max_non_empty(values: list[Any]) -> Any:
    present = [value for value in values if value]
    return max(present) if present else None


def chunked(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    return [values[index : index + size] for index in range(0, len(values), size)]


def ranking_definitions() -> dict[str, str]:
    return {
        "cumulative_profit_wallets": (
            "Wallets ranked by cumulative mark-to-market PnL across all supplied World Cup events."
        ),
        "cumulative_roi_wallets_min_buy_100": (
            "Wallets ranked by cumulative ROI across all supplied World Cup events with "
            "cumulative buy_notional >= 100."
        ),
        "cumulative_profit_wallets_min_volume_10000": (
            "Wallets with traded_notional >= 10000 ranked by cumulative mark-to-market PnL."
        ),
        "cumulative_roi_wallets_min_volume_10000": (
            "Wallets with traded_notional >= 10000 and buy_notional >= 100 "
            "ranked by cumulative ROI."
        ),
        "single_match_profit_wallets": (
            "Wallets ranked by their best single-match PnL; rank_match contains the ranking match; "
            "wallet_cumulative must also be positive."
        ),
        "single_match_roi_wallets_min_buy_100": (
            "Wallets ranked by their best single-match ROI with that match buy_notional >= 100; "
            "wallet_cumulative must also be positive."
        ),
        "multi_match_profit_wallets": (
            "Wallets with at least 2 matches ranked by cumulative PnL across the supplied "
            "match scope; "
            "cumulative PnL and ROI must be positive."
        ),
        "multi_match_roi_wallets_min_buy_100": (
            "Wallets with at least 2 matches and cumulative buy_notional >= 100 ranked "
            "by cumulative ROI; "
            "cumulative PnL and ROI must be positive."
        ),
    }


def ranking_entry(
    wallet: dict[str, Any], metric_name: str, rank_match: dict[str, Any] | None = None
) -> dict[str, Any]:
    cumulative = wallet["wallet_cumulative"]
    if rank_match is None:
        metric_value = (
            cumulative["pnl"] if metric_name.startswith("cumulative_pnl") else cumulative["roi"]
        )
        rank_metric = {
            "name": metric_name,
            "value": metric_value,
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


def empty_worldcup_wallet_rankings(
    input_slugs: list[str],
    *,
    data_status: str,
    found_event_slugs: list[str] | None = None,
) -> dict[str, Any]:
    found = found_event_slugs or []
    found_set = set(found)
    output: dict[str, Any] = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_status": data_status,
        "method": worldcup_wallet_rankings_method(),
        "ranking_definitions": ranking_definitions(),
        "scope": {
            "input_slugs": input_slugs,
            "input_slug_count": len(input_slugs),
            "match_count": len({base_match_slug(slug) for slug in input_slugs}),
            "found_event_slugs": found,
            "missing_event_slugs": [slug for slug in input_slugs if slug not in found_set],
            "condition_count": 0,
            "token_count": 0,
            "coverage_by_match": {},
        },
        "quality": {
            "trade_token_rows": 0,
            "raw_wallet_match_rows": 0,
            "raw_wallet_count": 0,
            "excluded_wallet_match_rows_with_negative_positions": 0,
            "excluded_wallets_with_negative_positions": 0,
            "wallet_count_after_quality_filters": 0,
            "positive_cumulative_wallet_count": 0,
            "roi_min_buy_notional": MIN_ROI_BUY_NOTIONAL,
            "volume_min_notional": MIN_VOLUME_NOTIONAL,
            "missing_mark_token_count": 0,
            "mark_source_counts": {},
        },
        "wallet_count": 0,
        "positive_cumulative_wallet_count": 0,
    }
    for name in RANKING_LIST_NAMES:
        output[name] = []
    return output


def worldcup_wallet_rankings_method() -> str:
    return (
        "wallet-centric mark-to-market from deduplicated fact_trade rows; "
        "token pnl=sell_notional-buy_notional+positive_position_size*mark_price; "
        "roi=pnl/buy_notional; marks use fresh orderbook mid or fresh price_history only when "
        "the mark timestamp is not older than the token's latest trade in scope, otherwise the "
        "token's latest trade price; wallet-match rows with negative token positions are excluded; "
        "all ranking entries require cumulative wallet pnl > 0 and cumulative wallet roi > 0 "
        "within the supplied match scope"
    )


def worldcup_wallet_rankings(
    clickhouse: Any,
    input_slugs: list[str] | tuple[str, ...] | None = None,
    *,
    rank_limit: int = 100,
) -> dict[str, Any]:
    slugs = list(DEFAULT_WORLD_CUP_EVENT_SLUGS if input_slugs is None else input_slugs)
    if not slugs:
        return empty_worldcup_wallet_rankings([], data_status="no_slugs")
    rank_limit = max(1, rank_limit)
    slug_sql = ",".join(sql_string(slug) for slug in slugs)

    metadata_rows = rows_from(
        query_clickhouse(
            clickhouse,
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
            """,
        )
    )
    if not metadata_rows:
        return empty_worldcup_wallet_rankings(slugs, data_status="no_event_metadata")

    found_event_slugs = sorted({row["event_slug"] for row in metadata_rows})
    missing_event_slugs = [slug for slug in slugs if slug not in set(found_event_slugs)]
    condition_ids = sorted(
        {row["condition_id"] for row in metadata_rows if row.get("condition_id")}
    )
    token_ids = sorted({row["token_id"] for row in metadata_rows if row.get("token_id")})
    if not condition_ids or not token_ids:
        return empty_worldcup_wallet_rankings(
            slugs,
            data_status="no_conditions_or_tokens",
            found_event_slugs=found_event_slugs,
        )

    market_by_condition: dict[str, dict[str, Any]] = {}
    token_meta: dict[str, dict[str, Any]] = {}
    match_titles: dict[str, str] = {}
    match_event_slugs: dict[str, set[str]] = defaultdict(set)
    coverage_by_match: dict[str, dict[str, Any]] = {}
    for row in metadata_rows:
        event_slug = row["event_slug"]
        match_slug = base_match_slug(event_slug)
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
            row["condition_id"]
            for row in metadata_rows
            if base_match_slug(row["event_slug"]) == match_slug
        }
        match_tokens = {
            row["token_id"]
            for row in metadata_rows
            if base_match_slug(row["event_slug"]) == match_slug
        }
        coverage_by_match[match_slug] = {
            "title": match_titles.get(match_slug, ""),
            "event_slugs": sorted(match_event_slugs[match_slug]),
            "event_count": len(match_event_slugs[match_slug]),
            "condition_count": len(match_conditions),
            "token_count": len(match_tokens),
        }

    condition_sql = ",".join(sql_string(condition_id) for condition_id in condition_ids)

    trade_rows = rows_from(
        query_clickhouse(
            clickhouse,
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
              argMax(price, timestamp) as last_trade_price,
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
                argMax(price, ingested_at) as price,
                argMax(size, ingested_at) as size,
                argMax(notional, ingested_at) as notional
              from
              (
                select
                  if(
                    trade_id != '',
                    concat(trade_id, ':', lower(user_address), ':', token_id, ':', side),
                    concat(
                      transaction_hash, ':', toString(log_index), ':',
                      lower(user_address), ':', token_id, ':', side
                    )
                  ) as dedupe_id,
                  timestamp,
                  condition_id,
                  token_id,
                  user_address,
                  side,
                  price,
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
            """,
        )
    )

    latest_trade_by_token = {
        row["token_id"]: {
            "last_trade_at": row["last_trade_at"],
            "last_trade_price": to_float(row["last_trade_price"]),
        }
        for row in rows_from(
            query_clickhouse(
                clickhouse,
                f"""
                select
                  token_id,
                  max(timestamp) as last_trade_at,
                  argMax(price, timestamp) as last_trade_price
                from
                (
                  select
                    dedupe_id,
                    argMax(timestamp, ingested_at) as timestamp,
                    argMax(token_id, ingested_at) as token_id,
                    argMax(price, ingested_at) as price
                  from
                  (
                    select
                      if(
                        trade_id != '',
                        concat(trade_id, ':', lower(user_address), ':', token_id, ':', side),
                        concat(
                          transaction_hash, ':', toString(log_index), ':',
                          lower(user_address), ':', token_id, ':', side
                        )
                      ) as dedupe_id,
                      timestamp,
                      token_id,
                      price,
                      ingested_at
                    from fact_trade
                    where condition_id in ({condition_sql})
                      and user_address != ''
                      and token_id != ''
                  )
                  group by dedupe_id
                )
                group by token_id
                format JSONEachRow
                """,
            )
        )
    }

    mark_rows = []
    for token_chunk in chunked(token_ids, TOKEN_MARK_QUERY_CHUNK_SIZE):
        chunk_token_sql = ",".join(sql_string(token_id) for token_id in token_chunk)
        mark_rows.extend(
            rows_from(
                query_clickhouse(
                    clickhouse,
                    f"""
                    select
                      ids.token_id as token_id,
                      book.book_best_bid as book_best_bid,
                      book.book_best_ask as book_best_ask,
                      book.mark_at as book_mark_at,
                      price.price as price_history_price,
                      price.mark_at as price_history_at
                    from (select arrayJoin([{chunk_token_sql}]) as token_id) as ids
                    left join
                    (
                      select
                        token_id,
                        argMax(price, timestamp) as price,
                        max(timestamp) as mark_at
                      from fact_price_history final
                      where token_id in ({chunk_token_sql})
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
                      where token_id in ({chunk_token_sql})
                        and best_bid is not null
                        and best_ask is not null
                      group by token_id
                    ) as book on ids.token_id = book.token_id
                    format JSONEachRow
                    """,
                )
            )
        )
    marks: dict[str, dict[str, Any]] = {}
    for row in mark_rows:
        token_id = row["token_id"]
        latest_trade = latest_trade_by_token.get(token_id, {})
        last_trade_at = latest_trade.get("last_trade_at")
        if (
            not_empty(row.get("book_best_bid"))
            and not_empty(row.get("book_best_ask"))
            and is_fresh(row.get("book_mark_at"), last_trade_at)
        ):
            mark_price = (to_float(row["book_best_bid"]) + to_float(row["book_best_ask"])) / 2
            marks[token_id] = {
                "token_id": token_id,
                "mark_price": mark_price,
                "mark_price_source": "orderbook_mid",
                "mark_price_at": row.get("book_mark_at"),
            }
        elif not_empty(row.get("price_history_price")) and is_fresh(
            row.get("price_history_at"), last_trade_at
        ):
            marks[token_id] = {
                "token_id": token_id,
                "mark_price": to_float(row["price_history_price"]),
                "mark_price_source": "price_history",
                "mark_price_at": row.get("price_history_at"),
            }
        elif not_empty(latest_trade.get("last_trade_price")):
            marks[token_id] = {
                "token_id": token_id,
                "mark_price": to_float(latest_trade["last_trade_price"]),
                "mark_price_source": "last_trade_price",
                "mark_price_at": last_trade_at,
            }
        else:
            marks[token_id] = {
                "token_id": token_id,
                "mark_price": None,
                "mark_price_source": "missing",
                "mark_price_at": None,
            }

    match_token_rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    raw_wallet_match_rows: set[tuple[str, str]] = set()
    raw_wallets: set[str] = set()
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
        current_value = (
            position_size * mark_price_float
            if position_size > EPS and mark_price_float is not None
            else 0.0
        )
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
    excluded_negative_wallets: set[str] = set()
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
            "first_trade_at": min_non_empty([row["first_trade_at"] for row in token_rows]),
            "last_trade_at": max_non_empty([row["last_trade_at"] for row in token_rows]),
            "missing_mark_tokens": sum(1 for row in token_rows if row["missing_mark"]),
            "negative_position_tokens": 0,
            "tokens": [
                rounded_dict(row)
                for row in sorted(
                    token_rows,
                    key=lambda item: (
                        item["event_slug"],
                        item["market_question"],
                        item["outcome"],
                        item["token_id"],
                    ),
                )
            ],
        }
        wallet_matches[user].append(rounded_dict(match_row))

    wallet_records: list[dict[str, Any]] = []
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
            "first_trade_at": min_non_empty([match["first_trade_at"] for match in matches]),
            "last_trade_at": max_non_empty([match["last_trade_at"] for match in matches]),
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
                "matches": sorted(
                    matches,
                    key=lambda match: (match["match_slug"], match["first_trade_at"] or ""),
                ),
            }
        )

    eligible_positive_cumulative = [
        wallet
        for wallet in wallet_records
        if wallet["wallet_cumulative"]["pnl"] > EPS
        and wallet["wallet_cumulative"]["roi"] is not None
        and wallet["wallet_cumulative"]["roi"] > EPS
    ]

    cumulative_profit_ranked = sorted(
        eligible_positive_cumulative,
        key=lambda wallet: wallet["wallet_cumulative"]["pnl"],
        reverse=True,
    )[:rank_limit]
    cumulative_roi_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["wallet_cumulative"]["buy_notional"] >= MIN_ROI_BUY_NOTIONAL
        ],
        key=lambda wallet: wallet["wallet_cumulative"]["roi"],
        reverse=True,
    )[:rank_limit]
    cumulative_profit_min_volume_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["wallet_cumulative"]["traded_notional"] >= MIN_VOLUME_NOTIONAL
        ],
        key=lambda wallet: wallet["wallet_cumulative"]["pnl"],
        reverse=True,
    )[:rank_limit]
    cumulative_roi_min_volume_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["wallet_cumulative"]["traded_notional"] >= MIN_VOLUME_NOTIONAL
            and wallet["wallet_cumulative"]["buy_notional"] >= MIN_ROI_BUY_NOTIONAL
        ],
        key=lambda wallet: wallet["wallet_cumulative"]["roi"],
        reverse=True,
    )[:rank_limit]
    single_profit_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["best_single_match_by_profit"] is not None
            and wallet["best_single_match_by_profit"]["pnl"] > EPS
        ],
        key=lambda wallet: wallet["best_single_match_by_profit"]["pnl"],
        reverse=True,
    )[:rank_limit]
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
    )[:rank_limit]
    multi_profit_ranked = sorted(
        [
            wallet
            for wallet in eligible_positive_cumulative
            if wallet["wallet_cumulative"]["match_count"] >= 2
            and wallet["wallet_cumulative"]["pnl"] > EPS
        ],
        key=lambda wallet: wallet["wallet_cumulative"]["pnl"],
        reverse=True,
    )[:rank_limit]
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
    )[:rank_limit]

    mark_source_counts = {
        source: sum(1 for mark in marks.values() if mark.get("mark_price_source") == source)
        for source in {mark.get("mark_price_source", "missing") for mark in marks.values()}
    }
    output: dict[str, Any] = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_status": "ok",
        "method": worldcup_wallet_rankings_method(),
        "ranking_definitions": ranking_definitions(),
        "scope": {
            "input_slugs": slugs,
            "input_slug_count": len(slugs),
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
            "roi_min_buy_notional": MIN_ROI_BUY_NOTIONAL,
            "volume_min_notional": MIN_VOLUME_NOTIONAL,
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
        "cumulative_profit_wallets": [
            {"rank": index + 1, **ranking_entry(wallet, "cumulative_pnl")}
            for index, wallet in enumerate(cumulative_profit_ranked)
        ],
        "cumulative_roi_wallets_min_buy_100": [
            {"rank": index + 1, **ranking_entry(wallet, "cumulative_roi")}
            for index, wallet in enumerate(cumulative_roi_ranked)
        ],
        "cumulative_profit_wallets_min_volume_10000": [
            {"rank": index + 1, **ranking_entry(wallet, "cumulative_pnl_min_volume_10000")}
            for index, wallet in enumerate(cumulative_profit_min_volume_ranked)
        ],
        "cumulative_roi_wallets_min_volume_10000": [
            {"rank": index + 1, **ranking_entry(wallet, "cumulative_roi_min_volume_10000")}
            for index, wallet in enumerate(cumulative_roi_min_volume_ranked)
        ],
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
                **ranking_entry(
                    wallet,
                    "single_match_roi",
                    wallet["best_single_match_by_roi_min_buy_100"],
                ),
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

    assert_positive_rankings(output)
    return output


def assert_positive_rankings(output: dict[str, Any]) -> None:
    for list_name in RANKING_LIST_NAMES:
        for entry in output[list_name]:
            cumulative = entry["wallet_cumulative"]
            if cumulative["roi"] is None or cumulative["roi"] <= EPS or cumulative["pnl"] <= EPS:
                raise AssertionError(
                    (
                        list_name,
                        entry["rank"],
                        entry["user_address"],
                        cumulative["pnl"],
                        cumulative["roi"],
                    )
                )
            metric = entry["rank_metric"]
            if metric["value"] is None or metric["value"] <= EPS:
                raise AssertionError((list_name, entry["rank"], entry["user_address"], metric))


def compact_worldcup_wallet_rankings(output: dict[str, Any]) -> dict[str, Any]:
    compact = dict(output)
    for list_name in RANKING_LIST_NAMES:
        compact[list_name] = [compact_ranking_entry(entry) for entry in output.get(list_name, [])]
    return compact


def compact_ranking_entry(entry: dict[str, Any]) -> dict[str, Any]:
    compact = {
        "rank": entry.get("rank"),
        "user_address": entry.get("user_address"),
        "rank_metric": entry.get("rank_metric"),
        "wallet_cumulative": entry.get("wallet_cumulative"),
        "rank_match": None,
    }
    rank_match = entry.get("rank_match")
    if isinstance(rank_match, dict):
        compact["rank_match"] = {
            key: value for key, value in rank_match.items() if key != "tokens"
        }
    return compact
