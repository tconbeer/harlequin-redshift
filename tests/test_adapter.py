"""Adapter-level tests.

The ones that need a cluster take the `connection` fixture and only read.
"""

from __future__ import annotations

from importlib.metadata import entry_points

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import create_backend

from harlequin_redshift import HarlequinRedshiftAdapter
from harlequin_redshift.adapter import (
    HarlequinRedshiftConnection,
    _format_type,
    _is_canceled,
    _is_user_schema,
)
from harlequin_redshift.cli_options import REDSHIFT_OPTIONS


def rows(cur: HarlequinCursor) -> list:
    """The cursor's rows, as a list.

    `fetchall()` is typed as optional on the base class; this adapter always
    returns a sequence, and asserting that here keeps every caller readable.
    """
    data = cur.fetchall()
    assert data is not None
    return list(data)


# -- discovery and configuration (no cluster needed) --------------------------


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "redshift"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinRedshiftAdapter


def test_declares_its_optional_features() -> None:
    assert HarlequinRedshiftAdapter.IMPLEMENTS_CANCEL is True
    assert HarlequinRedshiftAdapter.IMPLEMENTS_CATALOG_SEARCH is True
    assert HarlequinRedshiftAdapter.IMPLEMENTS_READ_ONLY is True
    assert HarlequinRedshiftAdapter.ADAPTER_OPTIONS is REDSHIFT_OPTIONS
    adapter = HarlequinRedshiftAdapter(conn_str=())
    assert adapter.provides_details is True
    assert adapter.provides_driver_details is True


def test_secrets_are_marked_secret() -> None:
    """An option holding a credential must never be printed back."""
    secret_names = {
        option.name for option in REDSHIFT_OPTIONS if getattr(option, "secret", False)
    }
    assert secret_names == {
        "password",
        "access_key_id",
        "secret_access_key",
        "session_token",
        "web_identity_token",
    }


def test_init_ignores_unknown_kwargs() -> None:
    adapter = HarlequinRedshiftAdapter(
        conn_str=("redshift://host/dev",), foo=1, bar="b"
    )
    assert "foo" not in adapter.options
    assert "bar" not in adapter.options


def test_all_databases_is_translated_for_the_driver() -> None:
    """`all_databases` is this adapter's spelling; the driver wants the inverse."""
    from harlequin_redshift.adapter import HarlequinRedshiftConnection

    conn = HarlequinRedshiftConnection.__new__(HarlequinRedshiftConnection)
    conn.connect_kwargs = {"all_databases": True}
    conn.all_databases = bool(conn.connect_kwargs.pop("all_databases", False))
    conn.connect_kwargs["database_metadata_current_db_only"] = not conn.all_databases
    assert conn.connect_kwargs == {"database_metadata_current_db_only": False}
    assert "all_databases" not in conn.connect_kwargs


def test_all_databases_defaults_to_the_connected_database_only() -> None:
    adapter = HarlequinRedshiftAdapter(conn_str=("redshift://host/dev",))
    assert "all_databases" not in adapter.options


def test_read_only_is_not_a_driver_option() -> None:
    """`read_only` is this adapter's, not the driver's; it must not be passed on."""
    adapter = HarlequinRedshiftAdapter(
        conn_str=("redshift://host/dev",), read_only=True
    )
    assert adapter.read_only is True
    assert "read_only" not in adapter.options


def test_read_only_defaults_to_false() -> None:
    assert (
        HarlequinRedshiftAdapter(conn_str=("redshift://host/dev",)).read_only is False
    )


@pytest.mark.parametrize(
    "conn_str,options,expected",
    [
        ((), {}, "localhost:5439/dev"),
        (("redshift://my-host",), {}, "my-host:5439/dev"),
        (("redshift://my-host:15439/analytics",), {}, "my-host:15439/analytics"),
        (("redshift://my-host",), {"port": "15439"}, "my-host:15439/dev"),
        (
            ("redshift://awsuser:secret@my-host:15439/analytics",),
            {},
            "my-host:15439/analytics",
        ),
        (("host=my-host dbname=analytics",), {}, "my-host:5439/analytics"),
    ],
)
def test_connection_id(conn_str: tuple, options: dict, expected: str) -> None:
    adapter = HarlequinRedshiftAdapter(conn_str=conn_str, **options)
    assert adapter.connection_id == expected


def test_connection_id_is_none_for_an_unreadable_conn_str() -> None:
    adapter = HarlequinRedshiftAdapter(conn_str=("mysql://host/db",))
    assert adapter.connection_id is None


def test_multiple_conn_strs_raise() -> None:
    with pytest.raises(HarlequinConnectionError):
        HarlequinRedshiftAdapter(conn_str=("redshift://a", "redshift://b")).connect()


def test_bad_conn_str_raises_connection_error() -> None:
    with pytest.raises(HarlequinConnectionError):
        HarlequinRedshiftAdapter(conn_str=("mysql://host/db",)).connect()


# -- pure helpers -------------------------------------------------------------


@pytest.mark.parametrize(
    "type_name,size,digits,expected",
    [
        ("varchar", 256, None, "varchar(256)"),
        ("character varying", 64, None, "character varying(64)"),
        ("bpchar", 8, None, "bpchar(8)"),
        ("numeric", 18, 2, "numeric(18,2)"),
        ("numeric", 18, None, "numeric(18)"),
        # a size the server reports for a fixed-width type is not part of it
        ("int4", 10, None, "int4"),
        ("timestamp", 29, 6, "timestamp"),
        ("super", None, None, "super"),
        (None, None, None, ""),
    ],
)
def test_format_type(
    type_name: str | None, size: object, digits: object, expected: str
) -> None:
    assert _format_type(type_name, size, digits) == expected


@pytest.mark.parametrize(
    "type_name,expected",
    [
        ("varchar(256)", "s"),
        ("VARCHAR", "s"),
        ("numeric(18,2)", "#.#"),
        ("int8", "##"),
        ("timestamp without time zone", "ts"),
        ("super", "{}"),
        ("geometry", "geo"),
        ("no such type", "?"),
    ],
)
def test_short_column_type(type_name: str, expected: str) -> None:
    assert HarlequinRedshiftConnection._short_column_type(type_name) == expected


@pytest.mark.parametrize(
    "oid,expected", [(23, "#"), (1043, "s"), (1700, "#.#"), (4000, "{}"), (99999, "?")]
)
def test_short_column_type_from_oid(oid: int, expected: str) -> None:
    assert HarlequinRedshiftConnection._short_column_type_from_oid(oid) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ("public", True),
        ("analytics", True),
        ("pg_catalog", False),
        ("pg_temp_3", False),
        ("information_schema", False),
        ("catalog_history", False),
        (None, False),
        ("", False),
    ],
)
def test_is_user_schema(schema: str | None, expected: bool) -> None:
    assert _is_user_schema(schema) is expected


def test_is_canceled_reads_the_sqlstate() -> None:
    canceled = Exception({"C": "57014", "M": "Query cancelled on user's request"})
    assert _is_canceled(canceled) is True


def test_is_canceled_falls_back_to_the_message() -> None:
    assert _is_canceled(Exception("ERROR: Query cancelled on user's request")) is True


def test_a_plain_error_is_not_a_cancellation() -> None:
    assert _is_canceled(Exception({"C": "42601", "M": "syntax error"})) is False
    assert _is_canceled(ValueError("boom")) is False


# -- against a real cluster (read-only) ---------------------------------------


def test_connect(connection: HarlequinRedshiftConnection) -> None:
    assert isinstance(connection, HarlequinConnection)


def test_execute_select(connection: HarlequinRedshiftConnection) -> None:
    cur = connection.execute("select 1 as a")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [("a", "#")]
    backend = create_backend(cur.fetchall())
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_execute_select_types(connection: HarlequinRedshiftConnection) -> None:
    cur = connection.execute(
        "select 1::int as i, 1.5::numeric(4,2) as n, 'x'::varchar as v, "
        "true as b, current_date as d, getdate() as ts"
    )
    assert cur is not None
    assert cur.columns() == [
        ("i", "#"),
        ("n", "#.#"),
        ("v", "s"),
        ("b", "t/f"),
        ("d", "d"),
        ("ts", "ts"),
    ]
    assert len(rows(cur)) == 1


def test_execute_select_dupe_cols(connection: HarlequinRedshiftConnection) -> None:
    cur = connection.execute("select 1 as a, 2 as a, 3 as a")
    assert cur is not None
    assert len(cur.columns()) == 3
    backend = create_backend(cur.fetchall())
    assert backend.column_count == 3
    assert backend.row_count == 1


def test_set_limit(connection: HarlequinRedshiftConnection) -> None:
    cur = connection.execute("select 1 as a union all select 2 union all select 3")
    assert cur is not None
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    backend = create_backend(cur.fetchall())
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_returning_no_rows(connection: HarlequinRedshiftConnection) -> None:
    cur = connection.execute("select 1 as a where false")
    assert cur is not None
    assert rows(cur) == []


def test_execute_raises_query_error(connection: HarlequinRedshiftConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        connection.execute("sel;")


def test_connection_recovers_from_a_query_error(
    connection: HarlequinRedshiftConnection,
) -> None:
    """A failed statement must not poison the session for the next one."""
    with pytest.raises(HarlequinQueryError):
        connection.execute("select * from no_such_table_ac9f1")
    cur = connection.execute("select 1 as a")
    assert cur is not None
    assert rows(cur) == [[1]]


def test_get_catalog(connection: HarlequinRedshiftConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)
    assert connection.database in [item.label for item in catalog.items]


def test_get_completions(connection: HarlequinRedshiftConnection) -> None:
    completions = connection.get_completions()
    labels = {c.label for c in completions}
    # keywords ship with the adapter
    assert {"select", "sortkey", "distkey", "unload"} <= labels
    # functions come from the cluster
    assert {"listagg", "getdate"} & labels


def test_completions_are_sorted(connection: HarlequinRedshiftConnection) -> None:
    completions = connection.get_completions()
    assert completions == sorted(completions)


def test_transaction_modes_toggle(connection: HarlequinRedshiftConnection) -> None:
    start = connection.transaction_mode.label
    assert start in ("Auto", "Manual")
    assert connection.toggle_transaction_mode().label != start
    assert connection.toggle_transaction_mode().label == start


def test_manual_mode_commits_and_rolls_back(
    connection: HarlequinRedshiftConnection,
) -> None:
    """Manual mode keeps one transaction open across statements."""
    while connection.transaction_mode.label != "Manual":
        connection.toggle_transaction_mode()
    try:
        cur = connection.execute("select 1 as a")
        assert cur is not None
        cur.fetchall()
        assert connection._main_conn.in_transaction is True
        connection.rollback()
        assert connection._main_conn.in_transaction is False
        cur = connection.execute("select 2 as a")
        assert cur is not None
        cur.fetchall()
        connection.commit()
        assert connection._main_conn.in_transaction is False
    finally:
        while connection.transaction_mode.label != "Auto":
            connection.toggle_transaction_mode()


def test_auto_mode_leaves_no_open_transaction(
    connection: HarlequinRedshiftConnection,
) -> None:
    assert connection.transaction_mode.label == "Auto"
    cur = connection.execute("select 1 as a")
    assert cur is not None
    cur.fetchall()
    assert connection._main_conn.in_transaction is False


def test_cancel_stops_a_running_query(
    connection: HarlequinRedshiftConnection,
) -> None:
    """A cancelled query comes back as no result, not as an error."""
    import threading

    assert connection._backend_pid is not None
    canceller = threading.Timer(1.5, connection.cancel)
    canceller.start()
    try:
        # a self-join over a system table, long enough to be cancelled
        cur = connection.execute(
            "select count(*) from stv_blocklist a, stv_blocklist b, stv_blocklist c"
        )
        if cur is not None:
            cur.fetchall()
    except HarlequinQueryError as e:  # pragma: no cover - depends on timing
        pytest.fail(f"A cancelled query raised instead of returning: {e}")
    finally:
        canceller.cancel()

    # the connection still works afterwards
    after = connection.execute("select 1 as a")
    assert after is not None
    assert rows(after) == [[1]]


def test_cancel_while_idle_sends_nothing(
    connection: HarlequinRedshiftConnection,
) -> None:
    """CANCEL names a session, not a statement.

    One sent while the session is idle can land on whatever it runs next, so an
    idle cancel must not reach the server at all.
    """
    assert connection._query_running is False
    connection.cancel()
    cur = connection.execute("select 1 as a")
    assert cur is not None
    assert rows(cur) == [[1]]


def test_get_completions_is_one_round_trip(
    connection: HarlequinRedshiftConnection,
) -> None:
    """Startup must not cost a SHOW per schema.

    The driver's get_functions() issues one per schema; this reads the same
    names with a single query, and the whole call should be quick even on a
    cluster with dozens of schemas.
    """
    import time

    start = time.monotonic()
    completions = connection.get_completions()
    elapsed = time.monotonic() - start
    assert completions
    assert elapsed < 30, f"get_completions() took {elapsed:.1f}s"


def test_pool_is_reused_and_not_exhausted(
    connection: HarlequinRedshiftConnection,
) -> None:
    """Every borrower has to give its connection back, or the pool runs dry."""
    for _ in range(connection.pool.max_size + 3):
        connection._get_databases()
    assert connection.pool._num_connections <= connection.pool.max_size
    assert connection._get_databases()


def test_a_failed_metadata_call_does_not_leak_connections(
    connection: HarlequinRedshiftConnection,
) -> None:
    for _ in range(connection.pool.max_size + 3):
        with pytest.raises(HarlequinQueryError):
            connection._metadata(
                lambda cur: cur.execute("select not_a_column"), "the catalog"
            )
    assert connection._get_databases()


def test_read_only_connection_can_read(
    read_only_connection: HarlequinRedshiftConnection,
) -> None:
    assert read_only_connection.read_only is True
    cur = read_only_connection.execute("select 1 as a")
    assert cur is not None
    assert rows(cur) == [[1]]


def test_read_only_connection_rejects_writes(
    read_only_connection: HarlequinRedshiftConnection,
) -> None:
    """A temp table is the least invasive write there is, and it must still fail.

    It would live only in this session and vanish when it ends, so a cluster
    that let it through is unharmed -- but the guarantee this adapter makes
    would be broken, which is what this checks.
    """
    with pytest.raises(HarlequinQueryError):
        read_only_connection.execute(
            "create temporary table harlequin_read_only_probe (a int)"
        )


def test_read_only_survives_transaction_mode_toggle(
    read_only_connection: HarlequinRedshiftConnection,
) -> None:
    start = read_only_connection.transaction_mode.label
    try:
        for _ in range(4):
            with pytest.raises(HarlequinQueryError):
                read_only_connection.execute(
                    "create temporary table harlequin_read_only_probe (a int)"
                )
            read_only_connection.toggle_transaction_mode()
    finally:
        while read_only_connection.transaction_mode.label != start:
            read_only_connection.toggle_transaction_mode()


def test_read_only_connection_reads_the_catalog(
    read_only_connection: HarlequinRedshiftConnection,
) -> None:
    catalog = read_only_connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items


# -- the driver's metadata fallback -------------------------------------------


class _BrokenCursor:
    """A cursor whose fast metadata path fails the way the driver's does.

    `redshift_connector` caches a column-name to index map per cursor and can
    look a name up in a map built from a different result set; the symptom is a
    KeyError naming a column that is not in it.
    """

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error or KeyError("database_name")
        self.legacy_calls = 0

    def get_schemas(self, **kwargs: object) -> list:
        raise self.error

    def get_schemas_legacy_hardcoded_query(self, **kwargs: object) -> list:
        self.legacy_calls += 1
        return [("public", "dev")]


def _connection_with_cursor(cursor: object) -> HarlequinRedshiftConnection:
    """A connection stub whose metadata calls run against `cursor`."""
    conn = HarlequinRedshiftConnection.__new__(HarlequinRedshiftConnection)
    conn.database = "dev"
    conn.read_only = False
    conn._session_read_only = True
    conn._use_show_metadata = True
    conn._query_running = False

    from contextlib import contextmanager

    @contextmanager
    def _metadata_cursor():  # type: ignore[no-untyped-def]
        yield cursor

    conn._metadata_cursor = _metadata_cursor  # type: ignore[method-assign]
    return conn


def test_metadata_falls_back_to_the_legacy_query() -> None:
    cursor = _BrokenCursor()
    conn = _connection_with_cursor(cursor)

    assert conn._get_schemas("dev") == ["public"]
    assert cursor.legacy_calls == 1


def test_a_broken_show_path_is_only_tried_once() -> None:
    """Every later call goes straight to the query, not through a doomed SHOW."""
    cursor = _BrokenCursor()
    conn = _connection_with_cursor(cursor)

    assert conn._use_show_metadata is True
    conn._get_schemas("dev")
    assert conn._use_show_metadata is False

    for _ in range(5):
        assert conn._get_schemas("dev") == ["public"]
    assert cursor.legacy_calls == 6


@pytest.mark.parametrize(
    "error", [KeyError("database_name"), TypeError("not subscriptable"), IndexError(3)]
)
def test_every_index_error_falls_back(error: Exception) -> None:
    cursor = _BrokenCursor(error=error)
    conn = _connection_with_cursor(cursor)

    assert conn._get_schemas("dev") == ["public"]
    assert cursor.legacy_calls == 1


def test_a_server_error_is_not_retried_on_the_legacy_query() -> None:
    """A permission or connection failure must surface, not be worked around."""
    cursor = _BrokenCursor(
        error=Exception({"C": "42501", "M": "permission denied for database dev"})
    )
    conn = _connection_with_cursor(cursor)

    with pytest.raises(HarlequinQueryError) as excinfo:
        conn._get_schemas("dev")
    assert "permission denied" in str(excinfo.value)
    assert cursor.legacy_calls == 0


def test_without_a_legacy_query_the_error_explains_itself() -> None:
    cursor = _BrokenCursor()
    conn = _connection_with_cursor(cursor)

    with pytest.raises(HarlequinQueryError) as excinfo:
        conn._metadata(lambda cur: cur.get_schemas(), "the schemas in dev")
    assert "database-metadata-current-db-only" in str(excinfo.value)
