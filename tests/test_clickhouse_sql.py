from zetta.storage.clickhouse import split_sql_statements


def test_split_sql_statements_ignores_blank_and_comment_only_lines() -> None:
    sql = """
    -- comment
    create table one (id String);

    create table two (id String);
    """

    assert split_sql_statements(sql) == [
        "create table one (id String)",
        "create table two (id String)",
    ]
