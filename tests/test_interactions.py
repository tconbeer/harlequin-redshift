"""Every catalog interaction names the object it was opened on, however it is named.

No cluster is needed: the SQL each action inserts or executes is read back with
Redshift's lexical rules, and the names found in it must be exactly the item's.
"""

from __future__ import annotations

from typing import Any, Callable

import pytest
from sql_text import AWKWARD_NAMES, dotted_name, identifiers, literals, unquoted_text

from harlequin_redshift.catalog import (
    DatabaseCatalogItem,
    ExternalTableCatalogItem,
    MaterializedViewCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
    TableCatalogItem,
    ViewCatalogItem,
)
from harlequin_redshift.interactions import (
    execute_drop_database_statement,
    execute_drop_external_table_statement,
    execute_drop_materialized_view_statement,
    execute_drop_schema_statement,
    execute_drop_table_statement,
    execute_drop_view_statement,
    execute_use_statement,
    insert_columns_at_cursor,
    show_describe_relation,
    show_describe_table_constraints,
    show_describe_table_design,
    show_external_table_ddl,
    show_external_table_definition,
    show_list_objects,
    show_list_schemas,
    show_materialized_view_info,
    show_select_star,
    show_storage_summary,
    show_table_definition,
    show_table_info,
    show_view_definition,
)

DATABASE = 'my "db"'


class _Cursor:
    def fetchall(self) -> list:
        return [["create table t (a int)"]]


class _Connection:
    """Records what an interaction executes; serves a fixed catalog below it."""

    def __init__(self, column_name: str = "a") -> None:
        self.database = DATABASE
        self.executed: list[str] = []
        self.column_name = column_name

    def execute(self, query: str) -> _Cursor:
        self.executed.append(query)
        return _Cursor()

    @staticmethod
    def _short_column_type(type_name: str) -> str:
        return "#"

    def _get_columns(self, database: str, schema: str, relation: str) -> list:
        return [(self.column_name, "int4"), ("b", "int4")]

    def _get_relations(self, database: str, schema: str) -> list:
        return [("t", "TABLE")]

    def _get_schemas(self, database: str) -> list:
        return ["public"]


class _Driver:
    """Records what an interaction hands back to Harlequin."""

    def __init__(self) -> None:
        self.inserted: list[str] = []
        self.notifications: list[tuple[str, str]] = []

    def insert_text_in_new_buffer(self, text: str) -> None:
        self.inserted.append(text)

    def insert_text_at_selection(self, text: str) -> None:
        self.inserted.append(text)

    def notify(self, message: str, severity: str = "information") -> None:
        self.notifications.append((message, severity))

    def confirm_and_execute(self, callback: Callable[[], None]) -> None:
        callback()

    def refresh_catalog(self) -> None:
        pass


def _run(
    interaction: Callable[[Any, Any], None], item: object, driver: _Driver
) -> None:
    """Runs `interaction` the way Harlequin would, with the recording driver."""
    interaction(item, driver)


def _database(connection: _Connection, label: str = DATABASE) -> DatabaseCatalogItem:
    return DatabaseCatalogItem.from_label(
        label=label,
        connection=connection,  # type: ignore[arg-type]
    )


def _schema(connection: _Connection, name: str) -> SchemaCatalogItem:
    return SchemaCatalogItem.from_parent(parent=_database(connection), label=name)


def _relation(
    connection: _Connection,
    item_class: type[RelationCatalogItem],
    schema: str,
    name: str,
) -> RelationCatalogItem:
    return item_class.from_parent(parent=_schema(connection, schema), label=name)


def _assert_no_leak(sql: str, *names: str) -> None:
    """None of `names` appears in `sql` outside a quoted literal or identifier."""
    words = unquoted_text(sql)
    for name in names:
        if name not in ("plain", "t", "public"):
            assert name not in words


# -- items name themselves ----------------------------------------------------


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_item_identifiers_round_trip(name: str) -> None:
    connection = _Connection(column_name=name)
    database = _database(connection, label=name)
    assert dotted_name(database.qualified_identifier) == [name]

    schema = SchemaCatalogItem.from_parent(parent=database, label=name)
    assert dotted_name(schema.qualified_identifier) == [name, name]
    # not the connected database, so the query name carries it
    assert dotted_name(schema.query_name) == [name, name]

    table = TableCatalogItem.from_parent(parent=schema, label=name)
    assert dotted_name(table.qualified_identifier) == [name, name, name]
    assert dotted_name(table.query_name) == [name, name, name]

    [column, _b] = table.fetch_children()
    assert dotted_name(column.qualified_identifier) == [name] * 4
    assert dotted_name(column.query_name) == [name]


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_connected_database_items_are_two_part(name: str) -> None:
    connection = _Connection()
    table = _relation(connection, TableCatalogItem, name, name)
    assert dotted_name(table.query_name) == [name, name]
    assert dotted_name(table.qualified_identifier) == [DATABASE, name, name]


# -- actions that write SQL into the editor -----------------------------------


@pytest.mark.parametrize("name", AWKWARD_NAMES)
@pytest.mark.parametrize(
    "interaction,item_class,expected",
    [
        (
            show_describe_relation,
            TableCatalogItem,
            lambda schema, name: [DATABASE, schema, name],
        ),
        (
            show_describe_table_design,
            TableCatalogItem,
            lambda schema, name: [DATABASE, schema, name],
        ),
        (show_table_info, TableCatalogItem, lambda schema, name: [schema, name]),
        (
            show_describe_table_constraints,
            TableCatalogItem,
            lambda schema, name: [name, schema],
        ),
        (
            show_external_table_definition,
            ExternalTableCatalogItem,
            lambda schema, name: [schema, name],
        ),
        (
            show_materialized_view_info,
            MaterializedViewCatalogItem,
            lambda schema, name: [schema, name],
        ),
    ],
)
def test_relation_descriptions_filter_on_the_exact_names(
    name: str,
    interaction: Callable[[Any, Any], None],
    item_class: type[RelationCatalogItem],
    expected: Callable[[str, str], list[str]],
) -> None:
    schema = f"s {name}"
    driver = _Driver()
    interaction(_relation(_Connection(), item_class, schema, name), driver)

    [sql] = driver.inserted
    # the query's own literals (constraint-type labels) precede the filters
    expected_names = expected(schema, name)
    assert literals(sql)[-len(expected_names) :] == expected_names
    _assert_no_leak(sql, name, schema)


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_preview_selects_from_the_relation(name: str) -> None:
    driver = _Driver()
    _run(
        show_select_star, _relation(_Connection(), TableCatalogItem, name, name), driver
    )
    [sql] = driver.inserted
    assert identifiers(sql) == [DATABASE, name, name]
    _assert_no_leak(sql, name)


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_insert_columns_quotes_each_column(name: str) -> None:
    driver = _Driver()
    connection = _Connection(column_name=name)
    _run(
        insert_columns_at_cursor,
        _relation(connection, TableCatalogItem, "public", "t"),
        driver,
    )
    [text] = driver.inserted
    assert identifiers(text) == [name, "b"]
    assert unquoted_text(text) == ",\n"


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_schema_listings_filter_on_the_exact_names(name: str) -> None:
    connection = _Connection()
    schema = _schema(connection, name)

    driver = _Driver()
    _run(show_list_objects, schema, driver)
    _run(show_storage_summary, schema, driver)
    list_objects, storage_summary = driver.inserted
    assert literals(list_objects) == [DATABASE, name]
    assert literals(storage_summary) == [name]
    _assert_no_leak(list_objects, name)
    _assert_no_leak(storage_summary, name)


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_database_listings_filter_on_the_exact_name(name: str) -> None:
    database = _database(_Connection(), label=name)

    driver = _Driver()
    _run(show_list_objects, database, driver)
    _run(show_list_schemas, database, driver)
    _run(show_storage_summary, database, driver)
    list_objects, list_schemas, storage_summary = driver.inserted
    assert literals(list_objects) == [name]
    assert literals(list_schemas) == [name]
    assert literals(storage_summary) == []
    _assert_no_leak(list_objects, name)
    _assert_no_leak(list_schemas, name)


# -- actions that execute -----------------------------------------------------


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_set_search_path_names_only_the_schema(name: str) -> None:
    connection = _Connection()
    driver = _Driver()
    _run(execute_use_statement, _schema(connection, name), driver)

    [sql] = connection.executed
    prefix = "set search_path to "
    assert sql.startswith(prefix)
    assert dotted_name(sql[len(prefix) :]) == [name]


def test_set_search_path_refuses_another_database() -> None:
    connection = _Connection()
    other = _database(connection, label="other")
    schema = SchemaCatalogItem.from_parent(parent=other, label="public")
    driver = _Driver()

    _run(execute_use_statement, schema, driver)

    assert connection.executed == []
    [(_message, severity)] = driver.notifications
    assert severity == "error"


@pytest.mark.parametrize("name", AWKWARD_NAMES)
@pytest.mark.parametrize(
    "interaction,item_class,command",
    [
        (show_table_definition, TableCatalogItem, "show table "),
        (show_view_definition, ViewCatalogItem, "show view "),
        (show_external_table_ddl, ExternalTableCatalogItem, "show external table "),
    ],
)
def test_show_definition_names_the_relation(
    name: str,
    interaction: Callable[[Any, Any], None],
    item_class: type[RelationCatalogItem],
    command: str,
) -> None:
    connection = _Connection()
    driver = _Driver()
    interaction(_relation(connection, item_class, name, name), driver)

    [sql] = connection.executed
    assert sql.startswith(command)
    assert dotted_name(sql[len(command) :]) == [name, name]
    # the header that quotes the query back stays a comment, whatever the name
    [text] = driver.inserted
    assert unquoted_text(text).strip() == "create table t (a int)"


@pytest.mark.parametrize("name", AWKWARD_NAMES)
@pytest.mark.parametrize(
    "interaction,item_class,command",
    [
        (execute_drop_table_statement, TableCatalogItem, "drop table "),
        (execute_drop_view_statement, ViewCatalogItem, "drop view "),
        (
            execute_drop_materialized_view_statement,
            MaterializedViewCatalogItem,
            "drop materialized view ",
        ),
        (
            execute_drop_external_table_statement,
            ExternalTableCatalogItem,
            "drop external table ",
        ),
    ],
)
def test_drop_relation_names_the_relation(
    name: str,
    interaction: Callable[[Any, Any], None],
    item_class: type[RelationCatalogItem],
    command: str,
) -> None:
    connection = _Connection()
    interaction(_relation(connection, item_class, name, name), _Driver())

    [sql] = connection.executed
    assert sql.startswith(command)
    assert dotted_name(sql[len(command) :]) == [name, name]


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_drop_schema_names_the_schema(name: str) -> None:
    connection = _Connection()
    _run(execute_drop_schema_statement, _schema(connection, name), _Driver())

    [sql] = connection.executed
    command, suffix = "drop schema ", " cascade"
    assert sql.startswith(command) and sql.endswith(suffix)
    assert dotted_name(sql[len(command) : -len(suffix)]) == [name]


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_drop_database_names_the_database(name: str) -> None:
    connection = _Connection()
    _run(execute_drop_database_statement, _database(connection, label=name), _Driver())

    [sql] = connection.executed
    command = "drop database "
    assert sql.startswith(command)
    assert dotted_name(sql[len(command) :]) == [name]
