from datetime import UTC, datetime

from zetta.models.normalize import normalize_market, normalize_outcome_tokens, parse_dt


def test_parse_dt_handles_iso_z() -> None:
    parsed = parse_dt("2026-06-04T03:00:00Z")

    assert parsed == datetime(2026, 6, 4, 3, 0, tzinfo=UTC)


def test_normalize_outcome_tokens_handles_json_strings() -> None:
    market = {
        "id": "m1",
        "conditionId": "c1",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["101","102"]',
    }

    rows = normalize_outcome_tokens(market, ingested_at=datetime.now(UTC))

    assert rows[0]["token_id"] == "101"
    assert rows[0]["outcome"] == "Yes"
    assert rows[1]["token_id"] == "102"
    assert rows[1]["outcome"] == "No"


def test_normalize_market_defaults_missing_numbers() -> None:
    row = normalize_market({"id": "m1", "question": "Will it rain?"}, ingested_at=datetime.now(UTC))

    assert row["market_id"] == "m1"
    assert row["volume"] == 0.0
    assert row["liquidity"] == 0.0

