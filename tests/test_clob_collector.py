from zetta.collectors.clob import ClobCollector
from zetta.http import HttpClientError


class FakeRawWriter:
    def write(self, **_kwargs):
        raise AssertionError("missing orderbooks should not write raw records")


class MissingOrderbookClient:
    def clob_book(self, *, token_id):
        raise HttpClientError(
            f"GET https://clob.test/book?token_id={token_id} failed with 404: "
            '{"error":"No orderbook exists for the requested token id"}'
        )


def test_collect_book_treats_missing_orderbook_as_empty_success() -> None:
    result = ClobCollector(
        client=MissingOrderbookClient(),
        raw_writer=FakeRawWriter(),
    ).collect_book(token_id="token-1")

    assert result.entity == "book"
    assert result.items == 0
    assert result.output_path == ""
