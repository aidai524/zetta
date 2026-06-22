from __future__ import annotations

import json
import math
import time
from bisect import bisect_left
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from zetta.unusual_betting_cache import (
    age_seconds,
    import_psycopg,
    iso_or_none,
    parse_datetime_or_none,
)


POLYCOP_TRADE_URL = "https://polycop.ai/v1/web/trade"
POLYCOP_SIGNAL_CACHE_KEY = "latest"
DEFAULT_PAGE_SIZE = 50
DEFAULT_MAX_PAGES = 25
DEFAULT_LIMIT = 500


POLYCOP_WALLET_SIGNAL_CACHE_SCHEMA_SQL = """
create table if not exists polycop_wallet_signal_cache (
  cache_key text primary key,
  status text not null default '',
  source text not null default 'polycop',
  wallet_count integer not null default 0,
  stable_count integer not null default 0,
  flow_count integer not null default 0,
  burst_count integer not null default 0,
  parameters jsonb not null default '{}'::jsonb,
  summary jsonb not null default '{}'::jsonb,
  detail jsonb not null default '{}'::jsonb,
  trigger_reason text not null default '',
  generated_at timestamptz,
  refreshed_at timestamptz not null default now(),
  error text
);

create index if not exists idx_polycop_wallet_signal_cache_refreshed
  on polycop_wallet_signal_cache (refreshed_at desc);
"""


class PolycopWalletSignalCacheStore:
    def __init__(self, *, dsn: str) -> None:
        self.dsn = dsn

    def ensure_schema(self) -> None:
        psycopg, _Jsonb = import_psycopg()
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(POLYCOP_WALLET_SIGNAL_CACHE_SCHEMA_SQL)

    def get(
        self,
        cache_key: str = POLYCOP_SIGNAL_CACHE_KEY,
        *,
        max_age_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        psycopg, _Jsonb = import_psycopg()
        self.ensure_schema()
        where = "cache_key = %s"
        params: list[Any] = [cache_key]
        if max_age_seconds is not None and max_age_seconds > 0:
            where += " and refreshed_at >= now() - (%s::text || ' seconds')::interval"
            params.append(max_age_seconds)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    select
                      cache_key,
                      status,
                      source,
                      wallet_count,
                      stable_count,
                      flow_count,
                      burst_count,
                      parameters,
                      summary,
                      detail,
                      trigger_reason,
                      generated_at,
                      refreshed_at,
                      error
                    from polycop_wallet_signal_cache
                    where {where}
                    limit 1
                    """,
                    params,
                )
                row = cursor.fetchone()
        return polycop_wallet_signal_cache_row(row) if row else None

    def upsert(
        self,
        *,
        cache_key: str,
        status: str,
        source: str,
        wallet_count: int,
        stable_count: int,
        flow_count: int,
        burst_count: int,
        parameters: dict[str, Any],
        summary: dict[str, Any],
        detail: dict[str, Any],
        trigger_reason: str,
        generated_at: Any = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        psycopg, Jsonb = import_psycopg()
        self.ensure_schema()
        generated_dt = parse_datetime_or_none(generated_at)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into polycop_wallet_signal_cache
                      (
                        cache_key,
                        status,
                        source,
                        wallet_count,
                        stable_count,
                        flow_count,
                        burst_count,
                        parameters,
                        summary,
                        detail,
                        trigger_reason,
                        generated_at,
                        refreshed_at,
                        error
                      )
                    values
                      (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                    on conflict (cache_key) do update
                    set status = excluded.status,
                        source = excluded.source,
                        wallet_count = excluded.wallet_count,
                        stable_count = excluded.stable_count,
                        flow_count = excluded.flow_count,
                        burst_count = excluded.burst_count,
                        parameters = excluded.parameters,
                        summary = excluded.summary,
                        detail = excluded.detail,
                        trigger_reason = excluded.trigger_reason,
                        generated_at = excluded.generated_at,
                        refreshed_at = now(),
                        error = excluded.error
                    returning
                      cache_key,
                      status,
                      source,
                      wallet_count,
                      stable_count,
                      flow_count,
                      burst_count,
                      parameters,
                      summary,
                      detail,
                      trigger_reason,
                      generated_at,
                      refreshed_at,
                      error
                    """,
                    (
                        cache_key,
                        status,
                        source,
                        wallet_count,
                        stable_count,
                        flow_count,
                        burst_count,
                        Jsonb(parameters),
                        Jsonb(summary),
                        Jsonb(detail),
                        trigger_reason,
                        generated_dt,
                        error,
                    ),
                )
                row = cursor.fetchone()
        return polycop_wallet_signal_cache_row(row)


def fetch_polycop_wallet_signal_rows(
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    sleep_seconds: float = 0.1,
    timeout_seconds: float = 20.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    total_pages: int | None = None
    total_raw: int | None = None
    pages_fetched = 0
    page_size = max(1, min(int(page_size or DEFAULT_PAGE_SIZE), 100))
    max_pages = max(1, min(int(max_pages or DEFAULT_MAX_PAGES), 100))
    for page_number in range(1, max_pages + 1):
        page = fetch_polycop_wallet_signal_page(
            page_number=page_number,
            page_size=page_size,
            timeout_seconds=timeout_seconds,
        )
        portfolio = page.get("portfolio", [])
        if not isinstance(portfolio, list):
            portfolio = []
        rows.extend(row for row in portfolio if isinstance(row, dict))
        pages_fetched += 1
        pagination = page.get("pagination") if isinstance(page.get("pagination"), dict) else {}
        total_pages = to_int(pagination.get("totalPages"), total_pages or 0) or total_pages
        total_raw = to_int(pagination.get("total"), total_raw or 0) or total_raw
        if not portfolio:
            break
        if total_pages is not None and page_number >= total_pages:
            break
        if sleep_seconds > 0 and page_number < max_pages:
            time.sleep(sleep_seconds)
    metadata = {
        "request_url": POLYCOP_TRADE_URL,
        "page_size": page_size,
        "max_pages": max_pages,
        "pages_fetched": pages_fetched,
        "total_pages": total_pages,
        "total_raw": total_raw,
    }
    return rows, metadata


def fetch_polycop_wallet_signal_page(
    *,
    page_number: int,
    page_size: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    payload = {
        "sort_options": [{"field": "score", "descending": True}],
        "pagination": {"page_number": page_number, "page_size": page_size},
    }
    data = json.dumps(payload).encode("utf-8")
    request = Request(
        POLYCOP_TRADE_URL,
        data=data,
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": "https://polycop.ai",
            "referer": "https://polycop.ai/leaderboard",
            "x-requested-with": "XMLHttpRequest",
            "user-agent": "Mozilla/5.0 zetta-polycop-wallet-signals",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"polycop_http_error:{exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"polycop_url_error:{exc.reason}") from exc
    except TimeoutError as exc:
        raise RuntimeError("polycop_timeout") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("polycop_invalid_json") from exc
    if not isinstance(body, dict):
        raise RuntimeError("polycop_invalid_response")
    code = body.get("code", 0)
    if code not in (0, "0", None):
        message = str(body.get("message") or "unknown")
        raise RuntimeError(f"polycop_error:{message}")
    data_body = body.get("data") if isinstance(body.get("data"), dict) else {}
    return {
        "portfolio": data_body.get("portfolio", []),
        "pagination": data_body.get("pagination", {}),
    }


def build_polycop_wallet_signal_result(
    rows: list[dict[str, Any]],
    *,
    metadata: dict[str, Any] | None = None,
    limit: int = DEFAULT_LIMIT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated_dt = generated_at or datetime.now(UTC).replace(microsecond=0)
    metadata = dict(metadata or {})
    ranked_wallets = score_polycop_wallets(rows)
    limit = max(1, min(int(limit or DEFAULT_LIMIT), 2000))
    stable = [wallet for wallet in ranked_wallets if "stable" in wallet["segments"]]
    flow = [wallet for wallet in ranked_wallets if "flow" in wallet["segments"]]
    burst = [wallet for wallet in ranked_wallets if "burst" in wallet["segments"]]
    detail = {
        "wallets": ranked_wallets[:limit],
        "segments": {
            "ai_top": ranked_wallets[:limit],
            "stable": stable[:limit],
            "flow": flow[:limit],
            "burst": burst[:limit],
        },
    }
    summary = {
        "wallet_count": len(ranked_wallets),
        "raw_wallet_count": len(rows),
        "stable_count": len(stable),
        "flow_count": len(flow),
        "burst_count": len(burst),
        "top_wallets": compact_wallets(ranked_wallets[:10]),
        "notes": [
            "totalVolume from the current Polycop response is not used because it contains an address value.",
            "Scores are percentile-ranked across the fetched Polycop leaderboard rows and penalize tiny sample size, high recent concentration, high slippage, and heavy hedging.",
        ],
    }
    parameters = {
        "source_url": POLYCOP_TRADE_URL,
        "sort_options": [{"field": "score", "descending": True}],
        "page_size": metadata.get("page_size"),
        "max_pages": metadata.get("max_pages"),
        "pages_fetched": metadata.get("pages_fetched"),
        "result_limit": limit,
    }
    return {
        "status": "ok",
        "source": "polycop",
        "generated_at": generated_dt.isoformat(),
        "parameters": parameters,
        "summary": summary,
        "detail": detail,
        "metadata": metadata,
    }


def refresh_polycop_wallet_signals(
    *,
    dsn: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    sleep_seconds: float = 0.1,
    timeout_seconds: float = 20.0,
    limit: int = DEFAULT_LIMIT,
    trigger_reason: str = "scheduled",
    cache_key: str = POLYCOP_SIGNAL_CACHE_KEY,
) -> dict[str, Any]:
    store = PolycopWalletSignalCacheStore(dsn=dsn)
    generated_at = datetime.now(UTC).replace(microsecond=0)
    try:
        rows, metadata = fetch_polycop_wallet_signal_rows(
            page_size=page_size,
            max_pages=max_pages,
            sleep_seconds=sleep_seconds,
            timeout_seconds=timeout_seconds,
        )
        result = build_polycop_wallet_signal_result(
            rows,
            metadata=metadata,
            limit=limit,
            generated_at=generated_at,
        )
        summary = result["summary"]
        detail = result["detail"]
        return store.upsert(
            cache_key=cache_key,
            status="ok",
            source="polycop",
            wallet_count=int(summary.get("wallet_count") or 0),
            stable_count=int(summary.get("stable_count") or 0),
            flow_count=int(summary.get("flow_count") or 0),
            burst_count=int(summary.get("burst_count") or 0),
            parameters=result["parameters"],
            summary=summary,
            detail=detail,
            trigger_reason=trigger_reason,
            generated_at=generated_at,
            error=None,
        )
    except Exception as exc:
        existing: dict[str, Any] | None = None
        try:
            existing = store.get(cache_key=cache_key)
        except Exception:
            existing = None
        summary = dict(existing.get("summary") or {}) if existing else {}
        detail = dict(existing.get("detail") or {}) if existing else {}
        previous_generated_at = existing.get("generated_at") if existing else None
        return store.upsert(
            cache_key=cache_key,
            status="stale_error" if existing else "error",
            source="polycop",
            wallet_count=int(existing.get("wallet_count") or 0) if existing else 0,
            stable_count=int(existing.get("stable_count") or 0) if existing else 0,
            flow_count=int(existing.get("flow_count") or 0) if existing else 0,
            burst_count=int(existing.get("burst_count") or 0) if existing else 0,
            parameters={
                "source_url": POLYCOP_TRADE_URL,
                "page_size": page_size,
                "max_pages": max_pages,
                "result_limit": limit,
            },
            summary=summary,
            detail=detail,
            trigger_reason=trigger_reason,
            generated_at=previous_generated_at or generated_at,
            error=str(exc),
        )


def score_polycop_wallets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared_by_address: dict[str, dict[str, Any]] = {}
    for row in rows:
        prepared = prepare_polycop_wallet_row(row)
        address = prepared["address"]
        if not address:
            continue
        current = prepared_by_address.get(address)
        if current is None or wallet_preference_key(prepared) > wallet_preference_key(current):
            prepared_by_address[address] = prepared
    prepared_rows = list(prepared_by_address.values())
    percentile_sets = {
        "score": percentile_lookup([row["source_score"] for row in prepared_rows]),
        "pnl_log": percentile_lookup([safe_log1p(row["actual_total_pnl"]) for row in prepared_rows]),
        "recent_pnl_log": percentile_lookup(
            [safe_log1p(row["recent20_pnl"]) for row in prepared_rows]
        ),
        "win_rate": percentile_lookup([row["win_rate"] for row in prepared_rows]),
        "recent_win_rate": percentile_lookup([row["recent20_win_rate"] for row in prepared_rows]),
        "markets_log": percentile_lookup([safe_log1p(row["total_markets"]) for row in prepared_rows]),
        "profit_loss_ratio": percentile_lookup(
            [min(row["avg_profit_loss_ratio"], 10.0) for row in prepared_rows]
        ),
        "slippage": percentile_lookup([row["slippage_cost_rate"] for row in prepared_rows]),
    }

    wallets: list[dict[str, Any]] = []
    for row in prepared_rows:
        source_score_pct = percentile_sets["score"](row["source_score"])
        pnl_pct = percentile_sets["pnl_log"](safe_log1p(row["actual_total_pnl"]))
        recent_pnl_pct = percentile_sets["recent_pnl_log"](safe_log1p(row["recent20_pnl"]))
        win_rate_pct = percentile_sets["win_rate"](row["win_rate"])
        recent_win_rate_pct = percentile_sets["recent_win_rate"](row["recent20_win_rate"])
        markets_pct = percentile_sets["markets_log"](safe_log1p(row["total_markets"]))
        plr_pct = percentile_sets["profit_loss_ratio"](min(row["avg_profit_loss_ratio"], 10.0))
        low_slippage_pct = 1.0 - percentile_sets["slippage"](row["slippage_cost_rate"])
        score = (
            0.16 * source_score_pct
            + 0.18 * pnl_pct
            + 0.16 * recent_pnl_pct
            + 0.12 * win_rate_pct
            + 0.12 * recent_win_rate_pct
            + 0.10 * markets_pct
            + 0.08 * plr_pct
            + 0.08 * low_slippage_pct
        )
        score -= sample_size_penalty(row["total_markets"])
        if row["recent_pnl_share"] > 0.75:
            score -= 0.12
        elif row["recent_pnl_share"] > 0.55:
            score -= 0.06
        if row["slippage_cost_rate"] > 12:
            score -= 0.08
        if row["hedge_ratio"] > 0.25:
            score -= 0.05
        if row["backtest_gap_ratio"] > 0.25:
            score -= 0.04
        ai_score = round(max(0.0, min(100.0, score * 100)), 2)
        segments = wallet_segments(row)
        wallets.append(
            {
                "rank": 0,
                "address": row["address"],
                "user_name": row["user_name"],
                "x_name": row["x_name"],
                "profile_image": row["profile_image"],
                "ai_score": ai_score,
                "source_score": round(row["source_score"], 4),
                "segments": segments,
                "primary_segment": primary_segment(segments),
                "reasons": wallet_reasons(row, segments),
                "metrics": {
                    "balance": round(row["balance"], 4),
                    "available": round(row["available"], 4),
                    "actual_total_pnl": round(row["actual_total_pnl"], 4),
                    "backtest_total_pnl": round(row["backtest_total_pnl"], 4),
                    "recent20_pnl": round(row["recent20_pnl"], 4),
                    "recent20_backtest_pnl": round(row["recent20_backtest_pnl"], 4),
                    "win_rate": round(row["win_rate"], 4),
                    "recent20_win_rate": round(row["recent20_win_rate"], 4),
                    "avg_profit_loss_ratio": round(row["avg_profit_loss_ratio"], 4),
                    "avg_market_roi": round(row["avg_market_roi"], 4),
                    "avg_market_profit_rate": round(row["avg_market_profit_rate"], 4),
                    "slippage_cost_rate": round(row["slippage_cost_rate"], 4),
                    "recent20_slippage_cost_rate": round(row["recent20_slippage_cost_rate"], 4),
                    "total_markets": row["total_markets"],
                    "hedged_markets": row["hedged_markets"],
                },
                "derived": {
                    "hedge_ratio": round(row["hedge_ratio"], 4),
                    "recent_pnl_share": round(row["recent_pnl_share"], 4),
                    "backtest_gap_ratio": round(row["backtest_gap_ratio"], 4),
                },
            }
        )
    wallets.sort(
        key=lambda wallet: (
            wallet["ai_score"],
            wallet["metrics"]["actual_total_pnl"],
            wallet["metrics"]["recent20_pnl"],
            wallet["metrics"]["total_markets"],
        ),
        reverse=True,
    )
    for index, wallet in enumerate(wallets, start=1):
        wallet["rank"] = index
    return wallets


def prepare_polycop_wallet_row(row: dict[str, Any]) -> dict[str, Any]:
    actual_pnl = to_float(row.get("actualTotalPnl"))
    recent_pnl = to_float(row.get("recent20Pnl"))
    markets = to_int(row.get("totalMarkets"), 0)
    hedged_markets = to_int(row.get("hedgedMarkets"), 0)
    backtest_pnl = to_float(row.get("backtestTotalPnl"))
    return {
        "address": normalize_address(row.get("address")),
        "user_name": str(row.get("userName") or ""),
        "x_name": str(row.get("xName") or ""),
        "profile_image": str(row.get("profileImage") or ""),
        "balance": to_float(row.get("balance")),
        "available": to_float(row.get("available")),
        "source_score": to_float(row.get("score")),
        "actual_total_pnl": actual_pnl,
        "backtest_total_pnl": backtest_pnl,
        "recent20_pnl": recent_pnl,
        "recent20_backtest_pnl": to_float(row.get("recent20BacktestPnl")),
        "win_rate": to_float(row.get("winRate")),
        "recent20_win_rate": to_float(row.get("recent20WinRate")),
        "avg_profit_loss_ratio": to_float(row.get("avgProfitLossRatio")),
        "avg_market_roi": to_float(row.get("avgMarketRoi")),
        "avg_market_profit_rate": to_float(row.get("avgMarketProfitRate")),
        "slippage_cost_rate": to_float(row.get("slippageCostRate")),
        "recent20_slippage_cost_rate": to_float(row.get("recent20SlippageCostRate")),
        "total_markets": markets,
        "hedged_markets": hedged_markets,
        "hedge_ratio": (hedged_markets / markets) if markets > 0 else 0.0,
        "recent_pnl_share": (recent_pnl / actual_pnl) if actual_pnl > 0 else 0.0,
        "backtest_gap_ratio": abs(actual_pnl - backtest_pnl) / max(abs(actual_pnl), 1.0),
    }


def wallet_segments(row: dict[str, Any]) -> list[str]:
    segments: list[str] = []
    if (
        row["total_markets"] >= 50
        and row["actual_total_pnl"] >= 10_000
        and row["recent20_pnl"] >= 1_000
        and row["win_rate"] >= 50
        and row["recent20_win_rate"] >= 55
        and row["avg_profit_loss_ratio"] >= 1.5
        and row["slippage_cost_rate"] <= 12
        and row["recent_pnl_share"] <= 0.7
    ):
        segments.append("stable")
    if (
        row["total_markets"] >= 500
        and row["actual_total_pnl"] >= 30_000
        and row["recent20_pnl"] > 1_000
        and row["win_rate"] >= 45
    ):
        segments.append("flow")
    if row["recent20_pnl"] >= 20_000 and (
        row["recent_pnl_share"] >= 0.45 or row["total_markets"] < 50
    ):
        segments.append("burst")
    if not segments:
        segments.append("watch")
    return segments


def primary_segment(segments: list[str]) -> str:
    for segment in ("stable", "flow", "burst", "watch"):
        if segment in segments:
            return segment
    return segments[0] if segments else "watch"


def wallet_reasons(row: dict[str, Any], segments: list[str]) -> list[str]:
    reasons: list[str] = []
    if "stable" in segments:
        reasons.append("stable_profit_sample")
    if "flow" in segments:
        reasons.append("large_market_sample")
    if "burst" in segments:
        reasons.append("recent_profit_spike")
    if row["slippage_cost_rate"] <= 6:
        reasons.append("low_slippage")
    if row["win_rate"] >= 60:
        reasons.append("high_win_rate")
    if row["total_markets"] < 25:
        reasons.append("small_sample")
    if row["recent_pnl_share"] > 0.75:
        reasons.append("recent_pnl_concentrated")
    if row["hedge_ratio"] > 0.25:
        reasons.append("hedged_activity")
    return reasons


def sample_size_penalty(total_markets: int) -> float:
    if total_markets < 25:
        return 0.18
    if total_markets < 50:
        return 0.07
    return 0.0


def wallet_preference_key(row: dict[str, Any]) -> tuple[float, float, float]:
    return (row["source_score"], row["actual_total_pnl"], row["recent20_pnl"])


def percentile_lookup(values: list[float]):
    ordered = sorted(float(value or 0.0) for value in values)
    if not ordered:
        return lambda _value: 0.0
    if len(ordered) == 1:
        return lambda _value: 1.0

    def lookup(value: float) -> float:
        index = bisect_left(ordered, float(value or 0.0))
        return index / (len(ordered) - 1)

    return lookup


def compact_wallets(wallets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for wallet in wallets:
        metrics = wallet.get("metrics", {})
        compact.append(
            {
                "rank": wallet.get("rank"),
                "address": wallet.get("address"),
                "user_name": wallet.get("user_name"),
                "x_name": wallet.get("x_name"),
                "ai_score": wallet.get("ai_score"),
                "primary_segment": wallet.get("primary_segment"),
                "segments": wallet.get("segments"),
                "actual_total_pnl": metrics.get("actual_total_pnl"),
                "recent20_pnl": metrics.get("recent20_pnl"),
                "win_rate": metrics.get("win_rate"),
                "total_markets": metrics.get("total_markets"),
            }
        )
    return compact


def polycop_wallet_signal_cache_row(row: Any) -> dict[str, Any]:
    (
        cache_key,
        status,
        source,
        wallet_count,
        stable_count,
        flow_count,
        burst_count,
        parameters,
        summary,
        detail,
        trigger_reason,
        generated_at,
        refreshed_at,
        error,
    ) = row
    return {
        "cache_key": str(cache_key or ""),
        "status": str(status or ""),
        "source": str(source or "polycop"),
        "wallet_count": int(wallet_count or 0),
        "stable_count": int(stable_count or 0),
        "flow_count": int(flow_count or 0),
        "burst_count": int(burst_count or 0),
        "parameters": dict(parameters or {}),
        "summary": dict(summary or {}),
        "detail": dict(detail or {}),
        "trigger_reason": str(trigger_reason or ""),
        "generated_at": iso_or_none(generated_at),
        "refreshed_at": iso_or_none(refreshed_at),
        "age_seconds": age_seconds(refreshed_at),
        "error": str(error) if error else None,
    }


def normalize_address(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    return text


def safe_log1p(value: float) -> float:
    if value <= 0 or not math.isfinite(value):
        return 0.0
    return math.log1p(value)


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(round(to_float(value, float(default))))
    except (TypeError, ValueError, OverflowError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else default
    text = str(value).strip()
    if not text:
        return default
    text = text.replace(",", "").replace("$", "").replace("%", "")
    try:
        number = float(text)
    except ValueError:
        return default
    return number if math.isfinite(number) else default
