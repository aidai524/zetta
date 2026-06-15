from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from zetta.config import Settings
from zetta.storage.clickhouse import ClickHouseWriter
from zetta.worldcup_wallets import (
    RANKING_LIST_NAMES,
    parse_slug_values,
    worldcup_event_slugs_for_scope,
    worldcup_wallet_rankings,
)


DEFAULT_OUTPUT = Path("data/worldcup_wallet_per_wallet_rankings_latest.json")


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export high-profit World Cup wallet rankings from ClickHouse."
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--date-from", default="")
    parser.add_argument("--date-to", default="")
    parser.add_argument(
        "--slug",
        action="append",
        dest="slugs",
        default=[],
        help="Base match slug or event slug. Repeat or pass comma-separated values.",
    )
    parser.add_argument(
        "--no-expand-variants",
        action="store_true",
        help="Use supplied slugs exactly instead of adding exact-score/halftime/more-markets variants.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    supplied_slugs = parse_slug_values({"slug": args.slugs}, "slug")
    input_slugs = worldcup_event_slugs_for_scope(
        supplied_slugs or None,
        date_from=args.date_from,
        date_to=args.date_to,
        expand_variants=not args.no_expand_variants,
    )
    output = worldcup_wallet_rankings(clickhouse(), input_slugs, rank_limit=args.limit)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str) + "\n")
    print_summary(output_path, output)


def print_summary(output_path: Path, output: dict[str, Any]) -> None:
    print("wrote", output_path)
    print("data_status", output.get("data_status"))
    print("wallet_count_after_quality_filters", output.get("wallet_count"))
    print("positive_cumulative_wallet_count", output.get("positive_cumulative_wallet_count"))
    for list_name in RANKING_LIST_NAMES:
        rows = output.get(list_name, [])
        print(list_name, len(rows))
        if rows:
            first = rows[0]
            print(
                "  #1",
                first["user_address"],
                first["rank_metric"],
                "cumulative",
                {
                    key: first["wallet_cumulative"][key]
                    for key in ("pnl", "roi", "match_count", "buy_notional")
                },
            )


if __name__ == "__main__":
    main()
