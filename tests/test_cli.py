from zetta.cli import parse_token_ids


def test_parse_token_ids_accepts_repeated_and_comma_separated_values() -> None:
    assert parse_token_ids(["token-1, token-2", "token-3"]) == [
        "token-1",
        "token-2",
        "token-3",
    ]
