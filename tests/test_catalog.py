"""Catalog tests: the tree, its interactions, and search.

Everything here reads, except `test_awkward_names_load_search_and_interact`,
which creates a uniquely named schema and drops it. The relations the other
tests walk are found on the cluster at run time by the `a_relation` fixture,
so the suite does not assume any particular object exists.
"""

from __future__ import annotations

import uuid
from typing import Any, Generator

import pytest
from harlequin.catalog import CatalogSearchResult, InteractiveCatalogItem
from recording_driver import RecordingDriver, run_interaction

from harlequin_redshift.adapter import HarlequinRedshiftConnection
from harlequin_redshift.catalog import (
    ColumnCatalogItem,
    DatabaseCatalogItem,
    ExternalTableCatalogItem,
    MaterializedViewCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
    TableCatalogItem,
    TempTableCatalogItem,
    ViewCatalogItem,
    relation_class,
    relation_item,
)
from harlequin_redshift.interactions import (
    execute_drop_schema_statement,
    execute_drop_table_statement,
    execute_drop_view_statement,
    execute_use_statement,
    insert_columns_at_cursor,
    show_describe_relation,
    show_describe_table_constraints,
    show_describe_table_design,
    show_list_objects,
    show_list_schemas,
    show_select_star,
    show_storage_summary,
    show_table_definition,
    show_table_info,
    show_view_definition,
)
from harlequin_redshift.quoting import qualified_name, quote_identifier, quote_literal

# -- item construction (no cluster needed) ------------------------------------


class _FakeConnection:
    database = "dev"

    @staticmethod
    def _short_column_type(type_name: str) -> str:
        return "s"

    def _get_columns(self, database: str, schema: str, relation: str) -> list:
        return [("a", "int4"), ("b", "varchar(16)")]

    def _get_relations(self, database: str, schema: str) -> list:
        return [("t", "TABLE"), ("v", "VIEW")]

    def _get_schemas(self, database: str) -> list:
        return ["public"]


@pytest.fixture
def db_item() -> DatabaseCatalogItem:
    return DatabaseCatalogItem.from_label(
        label="dev",
        connection=_FakeConnection(),  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    "type_name,expected",
    [
        ("TABLE", TableCatalogItem),
        ("BASE TABLE", TableCatalogItem),
        ("VIEW", ViewCatalogItem),
        ("MATERIALIZED VIEW", MaterializedViewCatalogItem),
        ("EXTERNAL TABLE", ExternalTableCatalogItem),
        ("LOCAL TEMPORARY", TempTableCatalogItem),
        ("materialized view", MaterializedViewCatalogItem),
        # an unfamiliar type still shows up, and is still queryable
        ("SOMETHING NEW", TableCatalogItem),
        (None, TableCatalogItem),
    ],
)
def test_relation_class(type_name: str | None, expected: type) -> None:
    assert relation_class(type_name) is expected


def test_items_nest_and_name_themselves(db_item: DatabaseCatalogItem) -> None:
    assert db_item.type_label == "db"
    assert db_item.qualified_identifier == '"dev"'
    assert db_item.is_connected_database is True

    [schema] = db_item.fetch_children()
    assert isinstance(schema, SchemaCatalogItem)
    assert schema.qualified_identifier == '"dev"."public"'
    # the connected database is addressed with two parts
    assert schema.query_name == '"public"'

    table, view = schema.fetch_children()
    assert isinstance(table, TableCatalogItem)
    assert isinstance(view, ViewCatalogItem)
    assert table.qualified_identifier == '"dev"."public"."t"'
    assert table.query_name == '"public"."t"'
    assert (table.type_label, table.type_name) == ("t", "TABLE")
    assert (view.type_label, view.type_name) == ("v", "VIEW")

    columns = table.fetch_children()
    assert all(isinstance(c, ColumnCatalogItem) for c in columns)
    assert [c.label for c in columns] == ["a", "b"]
    assert columns[0].qualified_identifier == '"dev"."public"."t"."a"'
    assert columns[0].query_name == '"a"'
    assert columns[0].type_name == "int4"
    # a column has no children, and asking does not raise
    assert columns[0].fetch_children() == []
    assert columns[0].loaded is True


def test_another_database_is_addressed_with_three_parts() -> None:
    """Redshift can read another database on the cluster, so a query says which."""
    other = DatabaseCatalogItem.from_label(
        label="shared",
        connection=_FakeConnection(),  # type: ignore[arg-type]
    )
    assert other.is_connected_database is False
    schema = SchemaCatalogItem.from_parent(parent=other, label="public")
    assert schema.query_name == '"shared"."public"'
    table = relation_item(parent=schema, label="t", type_name="TABLE")
    assert table.query_name == '"shared"."public"."t"'
    assert table.qualified_identifier == '"shared"."public"."t"'


def test_children_are_not_loaded_until_asked(db_item: DatabaseCatalogItem) -> None:
    assert not db_item.children
    assert not db_item.loaded
    [schema] = db_item.fetch_children()
    assert not schema.children
    assert not schema.loaded
    table, _view = schema.fetch_children()
    assert not table.children
    assert not table.loaded


def test_an_orphaned_item_fetches_nothing() -> None:
    """An item with no parent cannot say what to query, and must not raise."""
    assert (
        RelationCatalogItem(
            qualified_identifier='"t"', query_name='"t"', label="t", type_label="t"
        ).fetch_children()
        == []
    )
    assert (
        SchemaCatalogItem(
            qualified_identifier='"s"', query_name='"s"', label="s", type_label="sch"
        ).fetch_children()
        == []
    )
    assert (
        DatabaseCatalogItem(
            qualified_identifier='"d"', query_name='"d"', label="d", type_label="db"
        ).fetch_children()
        == []
    )


@pytest.mark.parametrize(
    "item_class,expected",
    [
        (TableCatalogItem, {"Show DDL (SHOW TABLE)", "Drop Table"}),
        (ViewCatalogItem, {"Show DDL (SHOW VIEW)", "Drop View"}),
        (MaterializedViewCatalogItem, {"Show Refresh Info"}),
        (ExternalTableCatalogItem, {"Show Location & Format"}),
        (SchemaCatalogItem, {"Set Search Path", "Drop Schema"}),
        (DatabaseCatalogItem, {"List Schemas", "Drop Database"}),
    ],
)
def test_interactions_are_declared(
    item_class: type[InteractiveCatalogItem], expected: set[str]
) -> None:
    labels = {label for label, _callable in item_class.INTERACTIONS or []}
    assert expected <= labels
    # every base relation action is inherited
    if issubclass(item_class, RelationCatalogItem):
        assert {"Preview Data", "Insert Columns at Cursor"} <= labels


def test_interactions_are_callable() -> None:
    for item_class in (
        TableCatalogItem,
        ViewCatalogItem,
        MaterializedViewCatalogItem,
        ExternalTableCatalogItem,
        SchemaCatalogItem,
        DatabaseCatalogItem,
    ):
        for _label, interaction in item_class.INTERACTIONS or []:
            assert callable(interaction)


# -- against a real cluster (read-only) ---------------------------------------


def test_catalog_walks_to_columns(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    database, schema, relation = a_relation
    catalog = connection.get_catalog()

    [db_item] = [i for i in catalog.items if i.label == database]
    assert isinstance(db_item, InteractiveCatalogItem)
    assert isinstance(db_item, DatabaseCatalogItem)
    assert not db_item.children and not db_item.loaded

    schema_items = db_item.fetch_children()
    assert all(isinstance(i, SchemaCatalogItem) for i in schema_items)
    [schema_item] = [i for i in schema_items if i.label == schema]
    assert not schema_item.children and not schema_item.loaded

    relation_items = schema_item.fetch_children()
    assert all(isinstance(i, RelationCatalogItem) for i in relation_items)
    [relation_item_] = [i for i in relation_items if i.label == relation]
    assert not relation_item_.children and not relation_item_.loaded

    assert isinstance(relation_item_, RelationCatalogItem)
    column_items = relation_item_.fetch_children()
    assert column_items
    assert all(isinstance(i, ColumnCatalogItem) for i in column_items)
    # every column names its full type and carries a short label for it
    for column in column_items:
        assert column.type_name
        assert column.type_label


def test_system_schemas_are_hidden(connection: HarlequinRedshiftConnection) -> None:
    schemas = connection._get_schemas(connection.database)
    assert schemas
    assert "pg_catalog" not in schemas
    assert "information_schema" not in schemas


def test_columns_come_back_in_ordinal_order(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    database, schema, relation = a_relation
    from_adapter = connection._get_columns(database, schema, relation)
    cur = connection.execute(
        f"select * from {qualified_name(schema, relation)} where false"
    )
    assert cur is not None
    cur.fetchall()
    assert [name for name, _type in from_adapter] == [
        name for name, _type in cur.columns()
    ]


def test_search_finds_a_relation(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    database, schema, relation = a_relation
    results = connection.search_catalog(relation, kind="relations")

    matching = [
        r
        for r in results
        if r.item.label == relation and r.parents == (database, schema)
    ]
    assert matching, f"search did not find {database}.{schema}.{relation}"
    [found] = matching
    assert isinstance(found.item, RelationCatalogItem)


def test_search_results_match_the_tree(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    """A searched item is the item the user would have reached by walking."""
    database, schema, relation = a_relation
    [db_item] = [
        i
        for i in connection.get_catalog().items
        if isinstance(i, DatabaseCatalogItem) and i.label == database
    ]
    [schema_item] = [i for i in db_item.fetch_children() if i.label == schema]
    [walked] = [i for i in schema_item.fetch_children() if i.label == relation]

    [searched] = [
        r
        for r in connection.search_catalog(relation, kind="relations")
        if r.item.label == relation and r.parents == (database, schema)
    ]

    assert type(searched.item) is type(walked)
    assert searched.item.query_name == walked.query_name
    assert searched.item.qualified_identifier == walked.qualified_identifier
    assert searched.item.type_label == walked.type_label
    assert searched.item.type_name == walked.type_name


def test_search_finds_a_column(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    database, schema, relation = a_relation
    columns = connection._get_columns(database, schema, relation)
    column_name, column_type = columns[0]

    results = connection.search_catalog(column_name, kind="columns")

    matching = [
        r
        for r in results
        if r.item.label == column_name and r.parents == (database, schema, relation)
    ]
    assert matching
    [found] = matching
    assert isinstance(found.item, ColumnCatalogItem)
    assert found.item.query_name == quote_identifier(column_name)
    assert found.item.type_name == column_type


def test_search_kinds_are_scoped(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    _database, _schema, relation = a_relation
    relations = connection.search_catalog(relation, kind="relations")
    assert relations
    assert all(isinstance(r.item, RelationCatalogItem) for r in relations)

    columns = connection.search_catalog(relation, kind="columns")
    assert all(isinstance(r.item, ColumnCatalogItem) for r in columns)


def test_search_all_includes_every_level(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    database, schema, relation = a_relation
    results = connection.search_catalog(relation, kind="all")
    assert any(
        r.item.label == relation and r.parents == (database, schema) for r in results
    )


def test_search_reports_a_parent_before_its_children(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    """Results arrive in catalog order, so a path reads top-down."""
    _database, _schema, relation = a_relation
    results = connection.search_catalog(relation, kind="all")
    paths = [(*r.parents, r.item.label) for r in results]
    assert paths == sorted(paths)


def test_search_is_case_insensitive_for_folded_identifiers(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    _database, _schema, relation = a_relation
    if relation != relation.lower():
        pytest.skip("This cluster uses case-sensitive identifiers.")
    upper = [
        (r.item.label, r.parents)
        for r in connection.search_catalog(relation.upper(), kind="relations")
    ]
    lower = [
        (r.item.label, r.parents)
        for r in connection.search_catalog(relation, kind="relations")
    ]
    assert upper == lower


def test_search_matches_a_substring(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    database, schema, relation = a_relation
    if len(relation) < 3:
        pytest.skip("Relation name is too short to search by a substring.")
    results = connection.search_catalog(relation[1:-1], kind="relations")
    assert any(
        r.item.label == relation and r.parents == (database, schema) for r in results
    )


def test_search_without_a_match_is_empty(
    connection: HarlequinRedshiftConnection,
) -> None:
    assert connection.search_catalog("no_such_object_ac9f1b2d") == []


def test_search_does_not_walk_the_cluster(
    connection: HarlequinRedshiftConnection,
) -> None:
    """A search is a query per level, not a SHOW per object.

    Answering an unqualified column search through the driver's server-side
    path means one SHOW COLUMNS for every table in every schema; this asserts
    the search stays fast enough that it cannot be doing that.
    """
    import time

    start = time.monotonic()
    connection.search_catalog("no_such_object_ac9f1b2d", kind="all")
    elapsed = time.monotonic() - start
    assert elapsed < 60, f"search_catalog() took {elapsed:.1f}s"


def test_search_for_an_empty_term_is_empty(
    connection: HarlequinRedshiftConnection,
) -> None:
    assert connection.search_catalog("") == []


def test_search_returns_search_results(
    connection: HarlequinRedshiftConnection, a_relation: tuple[str, str, str]
) -> None:
    _database, _schema, relation = a_relation
    results = connection.search_catalog(relation)
    assert results
    assert all(isinstance(r, CatalogSearchResult) for r in results)


# -- against a real cluster: awkward names (creates and drops its own schema) --

_AWKWARD_TABLES = ("a\\b", 'we"ird', "x'y")
_AWKWARD_VIEW = "v;w"


def _rows(connection: HarlequinRedshiftConnection, sql: str) -> list[list[Any]]:
    cur = connection.execute(sql)
    assert cur is not None, sql
    data = cur.fetchall()
    assert data is not None
    return [list(row) for row in data]


@pytest.fixture
def awkward_schema(
    connection: HarlequinRedshiftConnection,
) -> Generator[SchemaCatalogItem, None, None]:
    """A schema of its own, with tables and a view whose names need quoting.

    Every name is lower case, so the test holds whether or not the cluster
    turns on case-sensitive identifiers. The schema is dropped afterwards.
    """
    schema = f"hq_{uuid.uuid4().hex[:8]} a\\b\"'"
    connection.execute(f"create schema {quote_identifier(schema)}")
    try:
        for name in _AWKWARD_TABLES:
            table = qualified_name(schema, name)
            connection.execute(
                f"create table {table} ({quote_identifier(name)} varchar(16))"
            )
            connection.execute(f"insert into {table} values ({quote_literal(name)})")
        connection.execute(
            f"create view {qualified_name(schema, _AWKWARD_VIEW)} as "
            f"select * from {qualified_name(schema, _AWKWARD_TABLES[0])}"
        )
        [database] = [
            item
            for item in connection.get_catalog().items
            if isinstance(item, DatabaseCatalogItem)
            and item.label == connection.database
        ]
        [schema_item] = [i for i in database.fetch_children() if i.label == schema]
        yield schema_item
    finally:
        connection.execute(f"drop schema if exists {quote_identifier(schema)} cascade")


def test_awkward_names_load_search_and_interact(
    connection: HarlequinRedshiftConnection, awkward_schema: SchemaCatalogItem
) -> None:
    """Redshift's own answer to every quoting rule this adapter relies on."""
    schema = awkward_schema.label
    database = connection.database

    # a literal holding a backslash reads back as written
    for name in _AWKWARD_TABLES:
        assert _rows(connection, f"select * from {qualified_name(schema, name)}") == [
            [name]
        ]

    # the tree
    relations = {item.label: item for item in awkward_schema.fetch_children()}
    assert set(relations) == {*_AWKWARD_TABLES, _AWKWARD_VIEW}
    for name in _AWKWARD_TABLES:
        assert [c.label for c in relations[name].fetch_children()] == [name]
        assert isinstance(relations[name], TableCatalogItem)
    assert isinstance(relations[_AWKWARD_VIEW], ViewCatalogItem)

    # search
    for name in (*_AWKWARD_TABLES, _AWKWARD_VIEW):
        found = connection.search_catalog(name, kind="relations")
        assert any(
            r.item.label == name and r.parents == (database, schema) for r in found
        ), name
    found_schemas = connection.search_catalog(schema, kind="all")
    assert any(r.item.label == schema for r in found_schemas)

    # editor-inserted SQL runs and targets each table
    for name in _AWKWARD_TABLES:
        table = relations[name]
        driver = RecordingDriver()
        for interaction in (
            show_select_star,
            insert_columns_at_cursor,
            show_describe_relation,
            show_describe_table_design,
            show_table_info,
            show_describe_table_constraints,
        ):
            run_interaction(interaction, table, driver)
        (
            select_star,
            column_list,
            describe,
            design,
            table_info,
            constraints,
        ) = driver.inserted
        assert _rows(connection, select_star) == [[name]]
        assert _rows(connection, f"select {column_list} from {table.query_name}") == [
            [name]
        ]
        assert [row[0] for row in _rows(connection, describe)] == [name]
        assert [row[0] for row in _rows(connection, design)] == [name]
        assert all(row[1] == name for row in _rows(connection, table_info))
        assert _rows(connection, constraints) == []

        run_interaction(show_table_definition, table, driver)
        assert driver.notifications == []
        _header, _blank, definition = driver.inserted[-1].partition("\n\n")
        assert definition.strip()

    driver = RecordingDriver()
    run_interaction(show_view_definition, relations[_AWKWARD_VIEW], driver)
    assert driver.notifications == []

    run_interaction(show_list_objects, awkward_schema, driver)
    listed = _rows(connection, driver.inserted[-1])
    assert {row[2] for row in listed} == {*_AWKWARD_TABLES, _AWKWARD_VIEW}
    assert awkward_schema.parent is not None
    run_interaction(show_list_schemas, awkward_schema.parent, driver)
    assert schema in {row[1] for row in _rows(connection, driver.inserted[-1])}
    run_interaction(show_storage_summary, awkward_schema, driver)
    assert all(row[0] == schema for row in _rows(connection, driver.inserted[-1]))

    # the actions that execute
    try:
        run_interaction(execute_use_statement, awkward_schema, driver)
        assert _rows(connection, "select current_schema()") == [[schema]]
    finally:
        connection.execute("reset search_path")

    run_interaction(execute_drop_view_statement, relations[_AWKWARD_VIEW], driver)
    run_interaction(execute_drop_table_statement, relations["a\\b"], driver)
    assert {item.label for item in awkward_schema.fetch_children()} == {
        'we"ird',
        "x'y",
    }
    run_interaction(execute_drop_schema_statement, awkward_schema, driver)
    assert awkward_schema.parent is not None
    assert schema not in {i.label for i in awkward_schema.parent.fetch_children()}
