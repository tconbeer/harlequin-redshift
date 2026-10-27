"""The adapter's own catalog queries bind every name, and search matches literally.

A recording cursor stands in for the driver: what matters here is that a name
travels as a parameter and never as SQL text, which needs no cluster to see.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import pytest
from sql_text import AWKWARD_NAMES, literals

from harlequin_redshift import catalog_queries
from harlequin_redshift.adapter import HarlequinRedshiftConnection
from harlequin_redshift.completions import _routine_completions
from harlequin_redshift.quoting import like_contains


class _RecordingCursor:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple[str, list[str]]] = []

    def execute(self, sql: str, params: list[str] | None = None) -> None:
        self.calls.append((sql, list(params or [])))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def close(self) -> None:
        pass


def _assert_bound(sql: str, params: list[str], *names: str) -> None:
    """Each of `names` is a parameter, and the SQL holds a placeholder per one."""
    assert sql.count("%s") == len(params)
    for name in names:
        assert name in params
        assert name not in sql
    # the only literal the statement spells out is the LIKE escape character
    assert set(literals(sql)) <= {"!"}


@pytest.mark.parametrize("all_databases", [False, True])
@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_catalog_queries_bind_every_name(name: str, all_databases: bool) -> None:
    database, schema, relation = f"d {name}", f"s {name}", f"r {name}"
    cur = _RecordingCursor()

    catalog_queries.schemas(cur, database, all_databases=all_databases)  # type: ignore[arg-type]
    catalog_queries.relations(
        cur,  # type: ignore[arg-type]
        database,
        all_databases=all_databases,
        schema=schema,
    )
    catalog_queries.columns(
        cur,  # type: ignore[arg-type]
        database,
        all_databases=all_databases,
        schema=schema,
        relation=relation,
    )
    catalog_queries.columns(
        cur,  # type: ignore[arg-type]
        database,
        all_databases=all_databases,
        name_like=name,
    )

    schemas_call, relations_call, columns_call, search_call = cur.calls
    assert schemas_call[1] == [database]
    assert relations_call[1] == [database, schema]
    assert columns_call[1] == [database, schema, relation]
    assert search_call[1] == [database, name]
    for sql, params in cur.calls:
        _assert_bound(sql, params, database, *params)


def _connection(cursor: _RecordingCursor) -> HarlequinRedshiftConnection:
    """A connection stub whose catalog queries run against `cursor`."""
    conn = HarlequinRedshiftConnection.__new__(HarlequinRedshiftConnection)
    conn.database = "dev"
    conn.all_databases = False
    conn.read_only = False
    conn._session_read_only = True
    conn._use_show_metadata = False

    @contextmanager
    def _metadata_cursor() -> Iterator[_RecordingCursor]:
        yield cursor

    conn._metadata_cursor = _metadata_cursor  # type: ignore[assignment,method-assign]
    return conn


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_the_tree_loads_awkward_names_exactly(name: str) -> None:
    """Without the SHOW path, each level asks for the exact name it was given."""
    cursor = _RecordingCursor(rows=[("dev", name, name, "TABLE")])
    conn = _connection(cursor)

    assert conn._get_relations("dev", name) == [(name, "TABLE")]
    [(sql, params)] = cursor.calls
    assert params == ["dev", name]
    _assert_bound(sql, params, name)

    cursor.rows = [("dev", name, name, name, "varchar", 16, None, 1)]
    assert conn._get_columns("dev", name, name) == [(name, "varchar(16)")]
    sql, params = cursor.calls[-1]
    assert params == ["dev", name, name]
    _assert_bound(sql, params, name)


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_search_binds_the_term_as_a_literal_pattern(name: str) -> None:
    cursor = _RecordingCursor()
    conn = _connection(cursor)

    for kind in ("relations", "columns"):
        conn.search_catalog(name, kind=kind)

    assert cursor.calls
    for sql, params in cursor.calls:
        assert "like %s escape '!'" in sql
        [_database, pattern] = params
        _assert_bound(sql, params, pattern)
        assert pattern in (like_contains(name), like_contains(name.lower()))


def test_search_finds_mixed_case_and_folded_names() -> None:
    cursor = _RecordingCursor()
    conn = _connection(cursor)
    conn.search_catalog("Mixed_Case", kind="relations")
    assert [params[1] for _sql, params in cursor.calls] == [
        "%Mixed!_Case%",
        "%mixed!_case%",
    ]


def test_search_results_carry_quoted_identifiers() -> None:
    name = 'we"ird name'
    cursor = _RecordingCursor(rows=[("dev", "public", name, "TABLE")])
    conn = _connection(cursor)

    [result] = conn.search_catalog("we", kind="relations")

    assert result.item.label == name
    assert result.item.query_name == '"public"."we""ird name"'


def test_routine_completions_quote_names_that_need_it() -> None:
    cursor = _RecordingCursor(
        rows=[
            ("sum", "pg_catalog", True),
            ("MyFunc", "public", False),
            ("my func", "public", False),
        ]
    )

    class _Conn:
        def cursor(self) -> _RecordingCursor:
            return cursor

    completions = {
        c.label: c.value
        for c in _routine_completions(_Conn())  # type: ignore[arg-type]
    }
    assert completions == {
        "sum": "sum",
        "MyFunc": '"MyFunc"',
        "my func": '"my func"',
    }
