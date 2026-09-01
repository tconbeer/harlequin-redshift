"""The Harlequin adapter for Amazon Redshift, built on ``redshift_connector``.

Redshift speaks the Postgres wire protocol but is not Postgres: psycopg cannot
talk to it, its catalog spans databases through datashares, and its DDL, tuning
views, and cancellation are its own. This adapter uses Amazon's own driver, and
reads the catalog through that driver's metadata calls rather than through
hand-written system-table queries, so a cluster answers with whichever path it
supports -- server-side SHOW discovery on current clusters, the cross-database
SVV_ALL_* views, or the driver's legacy pg_catalog queries on older ones.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from itertools import cycle
from typing import Any, Callable, Iterator, Sequence, cast

import redshift_connector
from harlequin import (
    HarlequinAdapter,
    HarlequinCompletion,
    HarlequinConnection,
    HarlequinCursor,
    HarlequinTransactionMode,
)
from harlequin.catalog import (
    Catalog,
    CatalogItem,
    CatalogSearchKind,
    CatalogSearchResult,
)
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from redshift_connector import Connection, Cursor
from textual_fastdatatable.backend import AutoBackendType

from harlequin_redshift.catalog import (
    ColumnCatalogItem,
    DatabaseCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
    relation_item,
)
from harlequin_redshift.cli_options import REDSHIFT_OPTIONS
from harlequin_redshift.completions import _get_completions
from harlequin_redshift.conn_str import ConnStrError, build_connect_kwargs
from harlequin_redshift.pool import RedshiftConnectionPool

logger = logging.getLogger(__name__)

MetadataRows = Sequence[Sequence[Any]]
"""The rows one of the driver's metadata calls returns.

The driver annotates its legacy query methods as returning a single row's tuple
rather than a tuple of rows, so results are narrowed to this at the one place
they are read instead of at each call site.
"""

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5439
DEFAULT_DATABASE = "dev"
DEFAULT_TIMEOUT = 30

_POOL_SIZE = 5
"""Connections the catalog, completions, search, and cancel share.

The connection the user's queries run on is not one of these; it is held for the
life of the session so that a transaction can stay open across statements.
"""

_SYSTEM_SCHEMAS = frozenset({"information_schema", "catalog_history"})
"""Schemas the catalog does not show, alongside everything named ``pg_*``."""

_CANCELED_SQLSTATE = "57014"
"""``query_canceled``, which the server reports for a cancelled statement."""

_DRIVER_INDEXING_ERRORS = (KeyError, IndexError, TypeError)
"""What the driver raises when its own column-index map does not fit its rows.

For the server-side SHOW metadata path, `redshift_connector` caches a map of
column name to column index on the cursor, built once from whatever
`cursor.description` holds at the time, and then looks names up in it. When the
map is built from a different result set than the one being read, the lookup
raises -- `KeyError: 'database_name'` from `get_catalog_list()` is the case seen
in the wild, on a cluster with multi-database catalog metadata enabled.

Every affected call has a legacy sibling that queries the catalog tables
directly and never builds that map, so these are worth retrying there rather
than handing the user a broken catalog. A server-side error is not in this
tuple: a permission or connection failure must still surface.
"""

_SIZED_TYPES = frozenset(
    {
        "bpchar",
        "char",
        "character",
        "character varying",
        "nchar",
        "nvarchar",
        "varbyte",
        "varchar",
    }
)
"""Types whose declared length belongs in the type name shown in the catalog."""

_PRECISION_TYPES = frozenset({"decimal", "numeric"})

# JDBC-shaped result offsets for the driver's metadata calls
_TABLE_SCHEM, _TABLE_NAME, _TABLE_TYPE = 1, 2, 3
_COLUMN_NAME, _COLUMN_TYPE_NAME = 3, 5
_COLUMN_SIZE, _DECIMAL_DIGITS, _ORDINAL_POSITION = 6, 8, 16


def _error_detail(e: BaseException) -> dict[str, str]:
    """The server's error fields, when the driver attached them.

    ``redshift_connector`` raises its errors with the wire protocol's
    ErrorResponse as a dict in ``args[0]``: ``C`` is the SQLSTATE and ``M`` the
    message.
    """
    if e.args and isinstance(e.args[0], dict):
        return e.args[0]
    return {}


def _error_message(e: BaseException) -> str:
    """An error as a single line worth showing the user."""
    detail = _error_detail(e)
    message = detail.get("M")
    if not message:
        return f"{e.__class__.__name__}: {e}"
    hint = detail.get("H")
    return f"{message}\n\n{hint}" if hint else message


def _is_canceled(e: BaseException) -> bool:
    """True if the server ended the statement because it was cancelled.

    A cancellation is not a query error -- the user asked for it -- so it must
    not reach the user as one.
    """
    detail = _error_detail(e)
    if detail.get("C") == _CANCELED_SQLSTATE:
        return True
    message = (detail.get("M") or str(e)).lower()
    return "cancel" in message and ("user" in message or "request" in message)


def _close_quietly(cur: Cursor) -> None:
    try:
        cur.close()
    except Exception:
        logger.debug("Could not close cursor", exc_info=True)


def _format_type(
    type_name: str | None, size: Any = None, decimal_digits: Any = None
) -> str:
    """A column's type, spelled the way Redshift would spell it.

    The driver reports the bare type name and its size separately, so they are
    put back together here for the types where the size is part of how the
    column was declared.
    """
    if not type_name:
        return ""
    name = type_name.strip()
    lowered = name.lower()
    if lowered in _SIZED_TYPES and isinstance(size, int) and size > 0:
        return f"{name}({size})"
    if lowered in _PRECISION_TYPES and isinstance(size, int) and size > 0:
        if isinstance(decimal_digits, int):
            return f"{name}({size},{decimal_digits})"
        return f"{name}({size})"
    return name


def _text(value: Any) -> str:
    """A metadata row's cell as text.

    The driver types its metadata results as heterogeneous tuples, so the cells
    this adapter reads as names and types are narrowed here in one place.
    """
    return "" if value is None else str(value)


MetadataCalls = tuple[Callable[[Cursor], Any], Callable[[Cursor], Any]]
"""One metadata call and the legacy query that stands in for it."""


def _schemas_matching(database: str, pattern: str) -> MetadataCalls:
    """The driver calls that find schemas whose name matches `pattern`."""
    return (
        lambda cur: cur.get_schemas(catalog=database, schema_pattern=pattern),
        lambda cur: cur.get_schemas_legacy_hardcoded_query(
            catalog=database, schema_pattern=pattern
        ),
    )


def _relations_matching(database: str, pattern: str) -> MetadataCalls:
    """The driver calls that find relations whose name matches `pattern`."""
    return (
        lambda cur: cur.get_tables(catalog=database, table_name_pattern=pattern),
        lambda cur: cur.get_tables_legacy_hardcoded_query(
            catalog=database, table_name_pattern=pattern
        ),
    )


def _columns_matching(database: str, pattern: str) -> MetadataCalls:
    """The driver calls that find columns whose name matches `pattern`."""
    return (
        lambda cur: cur.get_columns(catalog=database, columnname_pattern=pattern),
        lambda cur: cur.get_columns_legacy_hardcoded_query(
            catalog=database, columnname_pattern=pattern
        ),
    )


def _is_user_schema(schema: str | None) -> bool:
    """True for a schema the catalog shows.

    The system catalogs and each session's temp schema are left out, the same
    filter the Postgres adapter applies, so the tree holds what a user wrote.
    """
    if not schema:
        return False
    return schema not in _SYSTEM_SCHEMAS and not schema.startswith("pg_")


class HarlequinRedshiftCursor(HarlequinCursor):
    def __init__(self, conn: "HarlequinRedshiftConnection", cur: Cursor) -> None:
        self.conn = conn
        self.cur = cur
        # copy the description now: the cursor is closed once its rows are
        # fetched, which may happen before columns() is called.
        description = cur.description
        assert description is not None
        self.description = list(description)
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        return [
            (str(col[0]), self.conn._short_column_type_from_oid(col[1]))
            for col in self.description
        ]

    def set_limit(self, limit: int) -> "HarlequinRedshiftCursor":
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                rows = self.cur.fetchall()
            else:
                rows = self.cur.fetchmany(self._limit)
        except Exception as e:
            if _is_canceled(e):
                return []
            raise HarlequinQueryError(
                msg=_error_message(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        finally:
            _close_quietly(self.cur)
            self.conn._end_implicit_transaction()
        return list(rows)


class HarlequinRedshiftConnection(HarlequinConnection):
    def __init__(
        self,
        conn_str: Sequence[str],
        *_: Any,
        init_message: str = "",
        options: dict[str, Any],
        read_only: bool = False,
    ) -> None:
        self.init_message = init_message
        self.read_only = bool(read_only)
        try:
            self.connect_kwargs = build_connect_kwargs(
                conn_str[0] if conn_str else "", options
            )
        except ConnStrError as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title=(
                    "Harlequin could not connect to Redshift. "
                    "Invalid connection string."
                ),
            ) from e
        self.connect_kwargs.setdefault("host", DEFAULT_HOST)
        self.connect_kwargs.setdefault("port", DEFAULT_PORT)
        self.connect_kwargs.setdefault("database", DEFAULT_DATABASE)
        self.connect_kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        self.connect_kwargs.setdefault("application_name", "harlequin")
        # `all_databases` is this adapter's option, spelled the way a user thinks
        # about it; the driver wants the inverse, and never sees the original.
        self.all_databases = bool(self.connect_kwargs.pop("all_databases", False))
        self.connect_kwargs[
            "database_metadata_current_db_only"
        ] = not self.all_databases

        self.database: str = str(self.connect_kwargs["database"])
        self._use_show_metadata = True
        """False once the driver's server-side metadata path has been seen to fail.

        That path is faster for a single node of the tree, but on a cluster
        where its cached column-index map is wrong it raises every time. The
        first failure switches this off so the rest of the session goes straight
        to the catalog queries instead of paying for a doomed attempt per node.
        """
        self._query_running = False
        """True while a statement is on the wire on the main connection."""
        self._session_read_only = False
        """True once the server accepts a session-wide read-only default.

        When it does not, each transaction is opened ``read only`` instead.
        """

        self.pool = RedshiftConnectionPool(
            connect_kwargs=self.connect_kwargs,
            max_size=_POOL_SIZE,
            timeout=float(self.connect_kwargs.get("timeout") or DEFAULT_TIMEOUT),
            configure=self._configure_connection,
        )
        try:
            self._main_conn: Connection = self.pool.open_connection()
        except HarlequinConnectionError:
            raise
        except Exception as e:
            self.pool.close()
            raise HarlequinConnectionError(
                msg=_error_message(e),
                title="Harlequin could not connect to Redshift.",
            ) from e

        self._backend_pid: int | None = self._get_backend_pid()

        self._transaction_modes = cycle(
            [
                HarlequinTransactionMode(label="Auto"),
                HarlequinTransactionMode(
                    label="Manual",
                    commit=self.commit,
                    rollback=self.rollback,
                ),
            ]
        )
        self.toggle_transaction_mode()

    # -- connection setup -----------------------------------------------------

    def _configure_connection(self, conn: Connection) -> None:
        """Prepares a freshly-opened connection, pooled or main.

        Every connection is left in autocommit: this adapter opens and ends its
        own transactions, both to honor the Manual transaction mode and, in
        read-only mode, to open them ``read only``. Leaving the driver to inject
        its own ``begin transaction`` would open a read-write one first.
        """
        conn.autocommit = True
        self._clear_read_timeout(conn)
        if not self.read_only:
            return
        self._session_read_only = self._try_session_read_only(conn)
        if not self._session_read_only:
            self._assert_read_only_transactions(conn)

    @staticmethod
    def _clear_read_timeout(conn: Connection) -> None:
        """Stops the connect timeout from also cutting off long queries.

        The driver calls ``settimeout(timeout)`` on the socket while connecting
        and never clears it, so ``timeout`` becomes a ceiling on how long *any*
        read may block -- a query that runs longer than it dies with "The read
        operation timed out", which for a query IDE is the wrong behavior. The
        timeout is dropped once the connection is up; TCP keepalives, which this
        adapter enables, are what notice a peer that has actually gone away.
        """
        try:
            conn._usock.settimeout(None)
        except Exception:
            logger.debug("Could not clear the socket read timeout", exc_info=True)

    def _try_session_read_only(self, conn: Connection) -> bool:
        """Sets, and confirms, a read-only default for the whole session.

        Returns False if this server has no such setting, in which case the
        caller falls back to opening each transaction ``read only``.
        """
        cur = conn.cursor()
        try:
            cur.execute("set session characteristics as transaction read only")
            cur.execute("show default_transaction_read_only")
            result = cur.fetchone()
        except Exception:
            logger.debug(
                "Server does not support a session-wide read-only default; "
                "opening each transaction read only instead.",
                exc_info=True,
            )
            return False
        finally:
            _close_quietly(cur)
        setting = str(result[0]).strip().lower() if result else ""
        return setting in ("on", "true", "1")

    def _assert_read_only_transactions(self, conn: Connection) -> None:
        """Confirms the server honors ``begin read only``.

        Harlequin was asked for a read-only connection, so refusing to open one
        beats handing back a connection that would happily write.
        """
        cur = conn.cursor()
        try:
            cur.execute("begin read only")
            cur.execute("show transaction_read_only")
            result = cur.fetchone()
            setting = str(result[0]).strip().lower() if result else ""
            if setting not in ("on", "true", "1"):
                raise HarlequinConnectionError(
                    msg=(
                        "Harlequin requested a read-only connection, but this "
                        "server reports transaction_read_only is "
                        f"{setting!r} inside a transaction opened READ ONLY. "
                        "Refusing to connect, since writes would not be "
                        "prevented."
                    ),
                    title=(
                        "Harlequin could not open a read-only connection to Redshift."
                    ),
                )
        except HarlequinConnectionError:
            raise
        except Exception as e:
            raise HarlequinConnectionError(
                msg=_error_message(e),
                title="Harlequin could not open a read-only connection to Redshift.",
            ) from e
        finally:
            _close_quietly(cur)
            try:
                conn.rollback()
            except Exception:
                logger.debug("Could not end the read-only probe", exc_info=True)

    def _get_backend_pid(self) -> int | None:
        """The server-side process id of the main connection.

        Redshift cancels a running statement by process id, so this is looked up
        once, while the connection is idle, and kept for ``cancel()``.
        """
        cur = self._main_conn.cursor()
        try:
            cur.execute("select pg_backend_pid()")
            result = cur.fetchone()
        except Exception:
            logger.debug("Could not read the backend pid", exc_info=True)
            return None
        finally:
            _close_quietly(cur)
        return int(result[0]) if result else None

    # -- transactions ---------------------------------------------------------

    @property
    def transaction_mode(self) -> HarlequinTransactionMode:
        return self._transaction_mode

    def toggle_transaction_mode(self) -> HarlequinTransactionMode:
        self._transaction_mode = next(self._transaction_modes)
        # a transaction left open under the old mode would outlive it
        self.rollback()
        return self._transaction_mode

    @property
    def _needs_explicit_begin(self) -> bool:
        """True when a statement must be run inside a transaction opened here.

        Manual mode needs one so the user's commit and rollback have something
        to act on. Read-only mode needs one on a server with no session-wide
        read-only default, because ``begin read only`` is then the only thing
        making the statement read-only.
        """
        return self.transaction_mode.label != "Auto" or (
            self.read_only and not self._session_read_only
        )

    def _begin(self, conn: Connection) -> None:
        if conn.in_transaction:
            return
        statement = "begin read only" if self.read_only else "begin"
        cur = conn.cursor()
        try:
            cur.execute(statement)
        finally:
            _close_quietly(cur)

    def _end_implicit_transaction(self) -> None:
        """Ends a transaction this adapter opened only to scope one statement.

        In Auto mode the user never asked for a transaction, so a read-only one
        opened to enforce read-only mode is rolled back as soon as the statement
        that needed it is done -- otherwise its snapshot would go stale and the
        catalog would stop seeing new objects.
        """
        if self.transaction_mode.label != "Auto":
            return
        self.rollback()

    def commit(self) -> None:
        try:
            self._main_conn.commit()
        except Exception as e:
            raise HarlequinQueryError(
                msg=_error_message(e),
                title="Harlequin could not commit the transaction.",
            ) from e

    def rollback(self) -> None:
        try:
            if self._main_conn.in_transaction:
                self._main_conn.rollback()
        except Exception:
            logger.debug("Could not roll back the transaction", exc_info=True)

    # -- queries --------------------------------------------------------------

    def execute(self, query: str) -> HarlequinCursor | None:
        conn = self._main_conn
        if self._needs_explicit_begin:
            try:
                self._begin(conn)
            except Exception as e:
                raise HarlequinQueryError(
                    msg=_error_message(e),
                    title="Harlequin could not open a transaction.",
                ) from e

        cur = conn.cursor()
        self._query_running = True
        try:
            cur.execute(query)
        except Exception as e:
            _close_quietly(cur)
            if _is_canceled(e):
                self.rollback()
                return None
            msg_suffix = ""
            try:
                self.rollback()
            except Exception:
                # likely the connection is closed; error messages can be
                # cryptic, so help the user.
                msg_suffix = (
                    "\n\nYou may need to restart Harlequin to reconnect to the "
                    "database."
                )
            raise HarlequinQueryError(
                msg=f"{_error_message(e)}{msg_suffix}",
                title="Harlequin encountered an error while executing your query.",
            ) from e
        finally:
            # the driver reads the whole result set before execute() returns, so
            # nothing is left on the wire once it does
            self._query_running = False

        if cur.description is not None:
            # the transaction stays open until the rows are handed over
            return HarlequinRedshiftCursor(self, cur)
        _close_quietly(cur)
        self._end_implicit_transaction()
        return None

    def cancel(self) -> None:
        """Asks the server to stop whatever the main connection is running.

        Redshift's own ``CANCEL`` statement does this, and it has to be sent on
        a second connection, because the one being cancelled is busy.
        """
        pid = self._backend_pid
        if pid is None or not self._query_running:
            # CANCEL names a session, not a statement, so one sent while the
            # session is idle can land on whatever it runs next
            return
        try:
            with self.pool.connection(timeout=5.0) as conn:
                cur = conn.cursor()
                try:
                    cur.execute(f"cancel {pid}")
                finally:
                    _close_quietly(cur)
        except Exception:
            # the query may well have finished on its own in the meantime
            logger.debug("Could not cancel the running query", exc_info=True)

    def close(self) -> None:
        try:
            self.rollback()
        finally:
            try:
                self._main_conn.close()
            except Exception:
                logger.debug("Could not close the main connection", exc_info=True)
            self.pool.close()

    # -- catalog --------------------------------------------------------------

    @contextmanager
    def _metadata_cursor(self) -> Iterator[Cursor]:
        """A cursor on a pooled connection, for the driver's metadata calls.

        Harlequin loads the catalog on worker threads while a query may be
        running, and a ``redshift_connector`` connection cannot be shared
        between threads, so these never touch the main connection.
        """
        with self.pool.connection() as conn:
            if self.read_only and not self._session_read_only:
                self._begin(conn)
            cur = conn.cursor()
            try:
                yield cur
            finally:
                _close_quietly(cur)
                if conn.in_transaction:
                    conn.rollback()

    def _metadata(
        self,
        call: Callable[[Cursor], Any],
        what: str,
        legacy: Callable[[Cursor], Any] | None = None,
    ) -> MetadataRows:
        """Runs one of the driver's metadata calls.

        `legacy` is the same call spelled as a direct catalog query. It is used
        only when the fast path fails the way the driver's cached column-index
        map makes it fail, so that a catalog still loads on a cluster where that
        path is broken. Anything the server itself refused is raised as it is.
        """
        if legacy is not None and not self._use_show_metadata:
            return self._catalog_query(legacy, what)
        try:
            with self._metadata_cursor() as cur:
                return cast(MetadataRows, call(cur))
        except HarlequinQueryError:
            raise
        except _DRIVER_INDEXING_ERRORS:
            if legacy is None:
                raise HarlequinQueryError(
                    msg=(
                        "The Redshift driver could not read its own metadata "
                        f"result while reading {what}. This is a driver "
                        "limitation on this cluster; try connecting with "
                        "--database-metadata-current-db-only."
                    ),
                    title=f"Redshift raised an error reading {what}:",
                ) from None
            logger.warning(
                "The driver's server-side metadata path failed reading %s; "
                "using catalog queries for the rest of this session.",
                what,
                exc_info=True,
            )
            self._use_show_metadata = False
        except Exception as e:
            raise HarlequinQueryError(
                msg=_error_message(e),
                title=f"Redshift raised an error reading {what}:",
            ) from e

        return self._catalog_query(legacy, what)

    def _catalog_query(self, call: Callable[[Cursor], Any], what: str) -> MetadataRows:
        """Reads the catalog with one query, not the driver's server-side path.

        The driver answers an unqualified metadata call by walking the cluster
        one SHOW at a time -- a column search issues a SHOW COLUMNS for every
        table in every schema of every database -- so anything unqualified goes
        here instead, where the pattern is pushed into a single statement.
        """
        try:
            with self._metadata_cursor() as cur:
                return cast(MetadataRows, call(cur))
        except HarlequinQueryError:
            raise
        except Exception as e:
            raise HarlequinQueryError(
                msg=_error_message(e),
                title=f"Redshift raised an error reading {what}:",
            ) from e

    def _get_databases(self) -> list[str]:
        rows = self._metadata(
            lambda cur: cur.get_catalogs(),
            "the list of databases",
            legacy=lambda cur: cur.get_catalogs_legacy_hardcoded_query(),
        )
        return sorted({_text(row[0]) for row in rows if row[0]})

    def _get_schemas(self, database: str) -> list[str]:
        rows = self._metadata(
            lambda cur: cur.get_schemas(catalog=database),
            f"the schemas in {database}",
            legacy=lambda cur: cur.get_schemas_legacy_hardcoded_query(catalog=database),
        )
        return sorted({_text(row[0]) for row in rows if _is_user_schema(_text(row[0]))})

    def _get_relations(self, database: str, schema: str) -> list[tuple[str, str]]:
        rows = self._metadata(
            lambda cur: cur.get_tables(catalog=database, schema_pattern=schema),
            f"the relations in {database}.{schema}",
            legacy=lambda cur: cur.get_tables_legacy_hardcoded_query(
                catalog=database, schema_pattern=schema
            ),
        )
        # the driver's schema filter is a LIKE pattern, so a schema whose name
        # holds a wildcard character matches more than itself; the exact name is
        # what this level of the catalog shows.
        relations = {
            (_text(row[_TABLE_NAME]), _text(row[_TABLE_TYPE]))
            for row in rows
            if row[_TABLE_SCHEM] == schema and row[_TABLE_NAME]
        }
        return sorted(relations)

    def _get_columns(
        self, database: str, schema: str, relation: str
    ) -> list[tuple[str, str]]:
        rows = self._metadata(
            lambda cur: cur.get_columns(
                catalog=database, schema_pattern=schema, tablename_pattern=relation
            ),
            f"the columns of {database}.{schema}.{relation}",
            legacy=lambda cur: cur.get_columns_legacy_hardcoded_query(
                catalog=database, schema_pattern=schema, tablename_pattern=relation
            ),
        )
        matching = [
            row
            for row in rows
            if row[_TABLE_SCHEM] == schema and row[_TABLE_NAME] == relation
        ]
        matching.sort(key=lambda row: row[_ORDINAL_POSITION] or 0)
        return [
            (
                _text(row[_COLUMN_NAME]),
                _format_type(
                    _text(row[_COLUMN_TYPE_NAME]),
                    row[_COLUMN_SIZE],
                    row[_DECIMAL_DIGITS],
                ),
            )
            for row in matching
        ]

    def get_catalog(self) -> Catalog:
        db_items: list[CatalogItem] = [
            DatabaseCatalogItem.from_label(label=db, connection=self)
            for db in self._get_databases()
        ]
        return Catalog(items=db_items)

    # -- catalog search -------------------------------------------------------

    def _search_patterns(self, term: str) -> list[str]:
        """The LIKE patterns that find `term` anywhere in a name.

        Redshift folds unquoted identifiers to lower case unless the cluster
        turns on case-sensitive identifiers, and its metadata calls match with
        LIKE, which is case-sensitive. Searching for the lower-cased term as
        well as the term as typed covers both.
        """
        patterns = [f"%{term}%"]
        lowered = f"%{term.lower()}%"
        if lowered != patterns[0]:
            patterns.append(lowered)
        return patterns

    def _search_schemas(
        self, term: str, databases: dict[str, DatabaseCatalogItem]
    ) -> dict[tuple[str, str], tuple[SchemaCatalogItem, tuple[str, ...]]]:
        found: dict[tuple[str, str], tuple[SchemaCatalogItem, tuple[str, ...]]] = {}
        for pattern in self._search_patterns(term):
            _show, query = _schemas_matching(self.database, pattern)
            rows = self._catalog_query(query, "the catalog")
            for row in rows:
                schema = _text(row[0])
                database = _text(row[1]) or self.database
                if not _is_user_schema(schema):
                    continue
                key = (database, schema)
                if key in found:
                    continue
                parent = self._database_item(database, databases)
                found[key] = (
                    SchemaCatalogItem.from_parent(parent=parent, label=schema),
                    (database,),
                )
        return found

    def _database_item(
        self, database: str, databases: dict[str, DatabaseCatalogItem]
    ) -> DatabaseCatalogItem:
        """The item for `database`, built once and shared by its descendants."""
        item = databases.get(database)
        if item is None:
            item = DatabaseCatalogItem.from_label(label=database, connection=self)
            databases[database] = item
        return item

    def _schema_item(
        self,
        database: str,
        schema: str,
        databases: dict[str, DatabaseCatalogItem],
        schemas: dict[tuple[str, str], SchemaCatalogItem],
    ) -> SchemaCatalogItem:
        item = schemas.get((database, schema))
        if item is None:
            item = SchemaCatalogItem.from_parent(
                parent=self._database_item(database, databases), label=schema
            )
            schemas[(database, schema)] = item
        return item

    def search_catalog(
        self, term: str, kind: CatalogSearchKind = "all"
    ) -> list[CatalogSearchResult]:
        """Every catalog item whose label contains `term`.

        Each level is matched by the same driver metadata call that builds it in
        the tree, so a match is the item the user would have reached by opening
        nodes, and it can be used the same way.

        Schemas, relations, and columns come from the connected database. The
        other databases on the cluster are matched by name, which is all the
        catalog's top level shows for them: searching every database's columns
        means a cross-database scan of SVV_ALL_COLUMNS, which does not finish
        quickly enough to sit behind a search box.
        """
        if not term:
            return []
        databases: dict[str, DatabaseCatalogItem] = {}
        schemas: dict[tuple[str, str], SchemaCatalogItem] = {}
        # keyed by the item's path, so that a parent sorts before its children
        # and the same object found by two patterns is reported once
        results: dict[tuple[str, ...], CatalogSearchResult] = {}

        if kind == "all":
            lowered = term.lower()
            for database in self._get_databases():
                if lowered in database.lower():
                    results[(database,)] = CatalogSearchResult(
                        item=self._database_item(database, databases)
                    )
            for key, (schema_item, parents) in self._search_schemas(
                term, databases
            ).items():
                schemas[key] = schema_item
                results[(key[0], key[1])] = CatalogSearchResult(
                    item=schema_item, parents=parents
                )

        if kind in ("all", "relations"):
            for pattern in self._search_patterns(term):
                _show, query = _relations_matching(self.database, pattern)
                rows = self._catalog_query(query, "the catalog")
                for row in rows:
                    database = _text(row[0]) or self.database
                    schema = _text(row[_TABLE_SCHEM])
                    name = _text(row[_TABLE_NAME])
                    if not name or not _is_user_schema(schema):
                        continue
                    path = (database, schema, name)
                    if path in results:
                        continue
                    results[path] = CatalogSearchResult(
                        item=relation_item(
                            parent=self._schema_item(
                                database, schema, databases, schemas
                            ),
                            label=name,
                            type_name=_text(row[_TABLE_TYPE]),
                        ),
                        parents=(database, schema),
                    )

        if kind in ("all", "columns"):
            relations: dict[tuple[str, str, str], RelationCatalogItem] = {}
            for pattern in self._search_patterns(term):
                _show, query = _columns_matching(self.database, pattern)
                rows = self._catalog_query(query, "the catalog")
                for row in rows:
                    database = _text(row[0]) or self.database
                    schema = _text(row[_TABLE_SCHEM])
                    table = _text(row[_TABLE_NAME])
                    column = _text(row[_COLUMN_NAME])
                    if not column or not _is_user_schema(schema):
                        continue
                    column_path = (database, schema, table, column)
                    if column_path in results:
                        continue
                    relation_key = (database, schema, table)
                    parent = relations.get(relation_key)
                    if parent is None:
                        # get_columns does not report the relation's type, and
                        # a column's identifiers do not depend on it
                        parent = relation_item(
                            parent=self._schema_item(
                                database, schema, databases, schemas
                            ),
                            label=table,
                            type_name=None,
                        )
                        relations[relation_key] = parent
                    type_name = _format_type(
                        _text(row[_COLUMN_TYPE_NAME]),
                        row[_COLUMN_SIZE],
                        row[_DECIMAL_DIGITS],
                    )
                    results[column_path] = CatalogSearchResult(
                        item=ColumnCatalogItem.from_parent(
                            parent=parent,
                            label=column,
                            type_label=self._short_column_type(type_name),
                            type_name=type_name,
                        ),
                        parents=(database, schema, table),
                    )

        return [results[path] for path in sorted(results)]

    # -- completions ----------------------------------------------------------

    def get_completions(self) -> list[HarlequinCompletion]:
        with self.pool.connection() as conn:
            return _get_completions(conn)

    # -- type labels ----------------------------------------------------------

    @staticmethod
    def _short_column_type(type_name: str) -> str:
        MAPPING = {
            "bigint": "##",
            "bigserial": "##",
            "bit": "010",
            "bool": "t/f",
            "boolean": "t/f",
            "bpchar": "s",
            "bytea": "b",
            "char": "s",
            "character": "s",
            "date": "d",
            "decimal": "#.#",
            "double": "#.#",
            "float": "#.#",
            "float4": "#.#",
            "float8": "#.#",
            "geography": "geo",
            "geometry": "geo",
            "hllsketch": "hll",
            "int": "#",
            "int2": "#",
            "int4": "#",
            "int8": "##",
            "integer": "#",
            "interval": "|-|",
            "json": "{}",
            "jsonb": "b{}",
            "nchar": "s",
            "numeric": "#.#",
            "nvarchar": "s",
            "real": "#.#",
            "smallint": "#",
            "super": "{}",
            "text": "s",
            "time": "t",
            "timestamp": "ts",
            "timestamptz": "ts",
            "timetz": "t",
            "uuid": "uid",
            "varbinary": "b",
            "varbyte": "b",
            "varchar": "s",
        }
        # the driver reports "timestamp without time zone" and "varchar(256)"
        # alike, so the base name is what is looked up
        base = type_name.split("(")[0].strip().lower()
        if base in MAPPING:
            return MAPPING[base]
        return MAPPING.get(base.split(" ")[0], "?")

    @staticmethod
    def _short_column_type_from_oid(oid: int) -> str:
        MAPPING = {
            16: "t/f",  # bool
            17: "b",  # bytea
            18: "s",  # char
            19: "s",  # name
            20: "##",  # int8
            21: "#",  # int2
            23: "#",  # int4
            25: "s",  # text
            26: "oid",
            114: "{}",  # json
            600: "•",  # point
            700: "#.#",  # float4
            701: "#.#",  # float8
            702: "ts",  # abstime
            790: "$$",  # money
            829: "mac",  # macaddr
            869: "ip",  # inet
            650: "ip",  # cidr
            1000: "[t/f]",
            1001: "[b]",
            1002: "[s]",
            1003: "[s]",
            1005: "[#]",
            1007: "[#]",
            1009: "[s]",
            1014: "[s]",
            1015: "[s]",
            1016: "[##]",
            1021: "[#.#]",
            1022: "[#.#]",
            1028: "[oid]",
            1042: "s",  # bpchar
            1043: "s",  # varchar
            1082: "d",  # date
            1083: "t",  # time
            1114: "ts",  # timestamp
            1115: "[ts]",
            1182: "[d]",
            1183: "[t]",
            1184: "ts",  # timestamptz
            1185: "[ts]",
            1186: "|-|",  # interval
            1187: "[|-|]",
            1188: "|-|",  # intervaly2m
            1190: "|-|",  # intervald2s
            1231: "[#.#]",
            1266: "t",  # timetz
            1700: "#.#",  # numeric
            2950: "uid",  # uuid
            3000: "geo",  # geometry
            3001: "geo",  # geography
            3802: "b{}",  # jsonb
            3999: "geo",  # geometryhex
            4000: "{}",  # super
            6551: "b",  # varbyte
        }
        return MAPPING.get(oid, "?")


class HarlequinRedshiftAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = REDSHIFT_OPTIONS
    IMPLEMENTS_CANCEL = True
    IMPLEMENTS_CATALOG_SEARCH = True
    IMPLEMENTS_READ_ONLY = True
    ADAPTER_DETAILS = (
        "**harlequin-redshift** connects to Amazon Redshift with "
        "[`redshift_connector`](https://github.com/aws/amazon-redshift-python-driver)"
        ", Amazon's own Python driver.\n\n"
        "- The data catalog is read through the driver's metadata calls, so it "
        "follows whichever path the cluster supports and spans datashare "
        "databases where they are enabled.\n"
        "- `CANCEL` stops a running query; `--read-only` opens read-only "
        "transactions.\n"
        "- IAM, Redshift Serverless, and federated identity providers are "
        "supported through the driver's connection options."
    )
    ADAPTER_DRIVER_DETAILS = (
        f"`redshift_connector` version `{redshift_connector.__version__}`."
    )

    def __init__(
        self,
        conn_str: Sequence[str],
        read_only: bool = False,
        **options: Any,
    ) -> None:
        self.conn_str = conn_str
        self.read_only = bool(read_only)
        # Harlequin passes only the options the user set, and may pass extras;
        # the connection keeps whatever this adapter declared and ignores the
        # rest, so an unknown key never reaches the driver.
        declared = {option.name for option in REDSHIFT_OPTIONS}
        self.options: dict[str, Any] = {
            key: value for key, value in options.items() if key in declared
        }

    @property
    def connection_id(self) -> str | None:
        """A stable id for the cluster and database this adapter connects to.

        Harlequin caches the catalog and query history against this, so it names
        the endpoint and database and nothing that varies between sessions.
        """
        try:
            kwargs = build_connect_kwargs(
                self.conn_str[0] if self.conn_str else "", self.options
            )
        except ConnStrError:
            return None
        host = kwargs.get("host", DEFAULT_HOST)
        port = kwargs.get("port", DEFAULT_PORT)
        database = kwargs.get("database", DEFAULT_DATABASE)
        return f"{host}:{port}/{database}"

    def connect(self) -> HarlequinRedshiftConnection:
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide multiple connection strings to the Redshift "
                f"adapter. {self.conn_str}"
            )
        return HarlequinRedshiftConnection(
            self.conn_str, options=self.options, read_only=self.read_only
        )
