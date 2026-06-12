from datetime import UTC, datetime, timezone, timedelta

from zetta.storage.clickhouse import json_default


def test_json_default_serializes_datetimes_as_utc() -> None:
    value = datetime(2026, 6, 11, 19, 0, tzinfo=timezone(timedelta(hours=8)))

    assert json_default(value) == "2026-06-11 11:00:00.000"


def test_json_default_keeps_utc_datetimes_in_utc() -> None:
    value = datetime(2026, 6, 11, 11, 0, tzinfo=UTC)

    assert json_default(value) == "2026-06-11 11:00:00.000"
