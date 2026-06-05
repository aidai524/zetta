from zetta.loaders.gamma import GammaRawLoader
from zetta.storage.raw import RawJsonlWriter


class FakeClickHouse:
    def __init__(self) -> None:
        self.tables = {}
        self.loaded_hashes = set()

    def insert(self, table, rows):
        self.tables[table] = list(rows)
        if table == "raw_ingest_log":
            self.loaded_hashes.update(row["payload_hash"] for row in rows)
        return len(rows)

    def query_text(self, query):
        if "raw_ingest_log" in query:
            return "\n".join(sorted(self.loaded_hashes))
        return ""


def test_gamma_raw_loader_loads_events_and_nested_markets(tmp_path) -> None:
    RawJsonlWriter(tmp_path).write(
        source="gamma",
        entity="events",
        request_url="https://example.test/events/keyset",
        payload={
            "data": [
                {
                    "id": "e1",
                    "title": "Election",
                    "markets": [
                        {
                            "id": "m1",
                            "conditionId": "c1",
                            "question": "Will A win?",
                            "outcomes": '["Yes","No"]',
                            "clobTokenIds": '["t1","t2"]',
                        }
                    ],
                }
            ],
            "next_cursor": "cursor",
        },
    )
    fake = FakeClickHouse()

    result = GammaRawLoader(clickhouse=fake).load(raw_root=tmp_path, batch_size=1)

    assert result.raw_records == 1
    assert result.skipped_raw_records == 0
    assert result.events == 1
    assert result.markets == 1
    assert result.outcome_tokens == 2
    assert result.series == 0
    assert result.tags == 0
    assert result.event_markets == 1
    assert fake.tables["dim_event"][0]["event_id"] == "e1"
    assert fake.tables["dim_market"][0]["market_id"] == "m1"
    assert fake.tables["dim_outcome_token"][1]["token_id"] == "t2"


def test_gamma_raw_loader_flushes_multiple_raw_records(tmp_path) -> None:
    for index in range(2):
        RawJsonlWriter(tmp_path).write(
            source="gamma",
            entity="markets",
            request_url=f"https://example.test/markets/keyset?page={index}",
            payload={
                "markets": [
                    {
                        "id": f"m{index}",
                        "conditionId": f"c{index}",
                        "question": "Will it work?",
                        "outcomes": '["Yes","No"]',
                        "clobTokenIds": f'["t{index}y","t{index}n"]',
                    }
                ]
            },
        )
    fake = FakeClickHouse()

    result = GammaRawLoader(clickhouse=fake).load(raw_root=tmp_path, batch_size=2)

    assert result.raw_records == 2
    assert result.markets == 2
    assert result.outcome_tokens == 4
    assert result.ingest_logs == 2


def test_gamma_raw_loader_skips_duplicate_payloads_in_same_run(tmp_path) -> None:
    payload = {
        "markets": [
            {
                "id": "m1",
                "conditionId": "c1",
                "question": "Will it work?",
                "outcomes": '["Yes","No"]',
                "clobTokenIds": '["ty","tn"]',
            }
        ]
    }
    RawJsonlWriter(tmp_path).write(
        source="gamma",
        entity="markets",
        request_url="https://example.test/markets/keyset",
        payload=payload,
    )
    RawJsonlWriter(tmp_path).write(
        source="gamma",
        entity="markets",
        request_url="https://example.test/markets/keyset?bad_cursor=same",
        payload=payload,
    )
    fake = FakeClickHouse()

    result = GammaRawLoader(clickhouse=fake).load(raw_root=tmp_path)

    assert result.raw_records == 2
    assert result.skipped_raw_records == 1
    assert result.markets == 1
    assert result.ingest_logs == 1


def test_gamma_raw_loader_loads_series_tags_and_skips_duplicates(tmp_path) -> None:
    RawJsonlWriter(tmp_path).write(
        source="gamma",
        entity="events",
        request_url="https://example.test/events/keyset",
        payload={
            "events": [
                {
                    "id": "e1",
                    "title": "Election",
                    "markets": [],
                    "series": [{"id": "s1", "ticker": "pol", "slug": "politics", "title": "Politics"}],
                    "tags": [{"id": "t1", "label": "All", "slug": "all"}],
                }
            ],
        },
    )
    fake = FakeClickHouse()

    first = GammaRawLoader(clickhouse=fake).load(raw_root=tmp_path)
    second = GammaRawLoader(clickhouse=fake).load(raw_root=tmp_path)
    replay = GammaRawLoader(clickhouse=fake).load(raw_root=tmp_path, force=True)

    assert first.series == 1
    assert first.tags == 1
    assert first.event_series == 1
    assert first.event_tags == 1
    assert second.skipped_raw_records == 1
    assert second.events == 0
    assert replay.skipped_raw_records == 0
    assert replay.ingest_logs == 0
    assert replay.events == 1
