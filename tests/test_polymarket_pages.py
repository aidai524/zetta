from zetta.http import HttpResponse
from zetta.polymarket import PolymarketClient, _keyset_page


def test_keyset_page_parses_data_and_cursor() -> None:
    response = HttpResponse(
        url="https://example.test/events/keyset",
        status=200,
        headers={},
        body={"data": [{"id": "1"}, {"id": "2"}], "next_cursor": "abc"},
    )

    page = _keyset_page(response)

    assert [item["id"] for item in page.items] == ["1", "2"]
    assert page.next_cursor == "abc"


def test_keyset_page_supports_legacy_list_body() -> None:
    response = HttpResponse(
        url="https://example.test/events",
        status=200,
        headers={},
        body=[{"id": "1"}],
    )

    page = _keyset_page(response)

    assert page.items == [{"id": "1"}]
    assert page.next_cursor is None


def test_gamma_keyset_uses_after_cursor_param() -> None:
    class FakeHttp:
        def __init__(self) -> None:
            self.params = None

        def get(self, _url, params):
            self.params = params
            return HttpResponse(
                url="https://example.test/events/keyset",
                status=200,
                headers={},
                body={"events": [], "next_cursor": None},
            )

    client = PolymarketClient.__new__(PolymarketClient)
    client.settings = type("Settings", (), {"gamma_base_url": "https://example.test"})()
    client.http = FakeHttp()

    client.gamma_events_keyset(limit=10, next_cursor="cursor-1")

    assert client.http.params["after_cursor"] == "cursor-1"
    assert "next_cursor" not in client.http.params
