"""Context-menu actions for the items in the Redshift data catalog.

Most of these write SQL into a new editor buffer rather than running it, so the
user sees what will hit the cluster and can edit it first. The ones that do
execute -- the DDL definitions and the drops -- say so, and the drops go through
Harlequin's confirmation modal.
"""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Literal, Sequence

from harlequin.catalog import CatalogItem
from harlequin.exception import HarlequinQueryError

if TYPE_CHECKING:
    from harlequin.driver import HarlequinDriver

    from harlequin_redshift.catalog import (
        ColumnCatalogItem,
        DatabaseCatalogItem,
        ExternalTableCatalogItem,
        MaterializedViewCatalogItem,
        RelationCatalogItem,
        SchemaCatalogItem,
        TableCatalogItem,
        ViewCatalogItem,
    )


def _literal(value: str) -> str:
    """A string as a SQL literal, with its quotes escaped.

    These queries are built by interpolation rather than by binding, because
    they are handed to the user as text to read and run, not executed with
    parameters.
    """
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _database_predicate(item: "RelationCatalogItem | SchemaCatalogItem") -> str:
    """The SVV_ALL_* filter that scopes a query to one item's database.

    The SVV_ALL_* views span every database the cluster exposes, including the
    ones a datashare brings in, so a query against them has to name the database
    the catalog item actually came from.
    """
    database = _root_label(item)
    return f"database_name = {_literal(database)}" if database else "true"


def _root_label(item: CatalogItem) -> str | None:
    """The label of the database an item sits under."""
    node: CatalogItem | None = item
    label: str | None = None
    while node is not None:
        label = node.label
        node = getattr(node, "parent", None)
    return label


# -- Editor / session ---------------------------------------------------------


def execute_use_statement(
    item: "SchemaCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    if item.connection is None:
        return
    try:
        item.connection.execute(f"set search_path to {item.qualified_identifier}")
    except HarlequinQueryError:
        driver.notify("Could not switch context", severity="error")
        raise
    else:
        driver.notify(f"Editor context switched to {item.label}")


def insert_columns_at_cursor(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    if item.loaded:
        cols: Sequence["CatalogItem" | "ColumnCatalogItem"] = item.children
    else:
        cols = item.fetch_children()
    driver.insert_text_at_selection(text=",\n".join(c.query_name for c in cols))


def show_select_star(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select *
            from {item.qualified_identifier}
            limit 100
            """.strip("\n")
        )
    )


# -- Descriptions -------------------------------------------------------------


def show_describe_relation(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    """Every column of the relation, from the catalog view that spans databases."""
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                column_name as "Column",
                ordinal_position as "#",
                data_type as "Type",
                column_default as "Default",
                is_nullable as "Nullable",
                remarks as "Description"
            from svv_all_columns
            where
                {_database_predicate(item)}
                and schema_name = {_literal(item.parent.label)}
                and table_name = {_literal(item.label)}
            order by ordinal_position
            """.strip("\n")
        )
    )


def show_describe_table_design(
    item: "TableCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    """The Redshift-specific physical design of a table's columns.

    Distribution key, sort key, and compression encoding are what make a
    Redshift table fast or slow, and none of them appear in a generic column
    description. SVV_REDSHIFT_COLUMNS carries them for local tables.
    """
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                column_name as "Column",
                ordinal_position as "#",
                data_type as "Type",
                encoding as "Encoding",
                distkey as "Is Dist Key",
                sortkey as "Sort Key Position",
                is_nullable as "Nullable",
                column_default as "Default"
            from svv_redshift_columns
            where
                {_database_predicate(item)}
                and schema_name = {_literal(item.parent.label)}
                and table_name = {_literal(item.label)}
            order by ordinal_position
            """.strip("\n")
        )
    )


def show_table_info(
    item: "TableCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    """Size, skew, sortedness, and stats staleness for the table.

    SVV_TABLE_INFO is the view Redshift's own tuning guidance is written
    against; it only reports tables in the connected database.
    """
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                "schema" as "Schema",
                "table" as "Table",
                diststyle as "Dist Style",
                sortkey1 as "Sort Key",
                size as "Size (MB)",
                pct_used as "Pct of Disk",
                tbl_rows as "Rows",
                skew_rows as "Row Skew",
                skew_sortkey1 as "Sort Key Skew",
                unsorted as "Pct Unsorted",
                vacuum_sort_benefit as "Vacuum Benefit",
                stats_off as "Stats Staleness",
                encoded as "Encoded"
            from svv_table_info
            where
                "schema" = {_literal(item.parent.label)}
                and "table" = {_literal(item.label)}
            """.strip("\n")
        )
    )


def show_describe_table_constraints(
    item: "TableCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    """The table's declared keys.

    Redshift does not enforce primary, foreign, or unique keys -- it only
    records them, and the query planner uses them -- so this reads them out of
    pg_constraint the way the planner sees them.
    """
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            -- Redshift records these constraints for the query planner but
            -- does not enforce them; data is not validated against them.
            select
                c.relname as "Table",
                con.conname as "Constraint Name",
                case con.contype
                    when 'p' then 'Primary Key'
                    when 'f' then 'Foreign Key'
                    when 'u' then 'Unique'
                    when 'c' then 'Check'
                    else con.contype::text
                end as "Constraint Type",
                fc.relname as "References",
                pg_catalog.pg_get_constraintdef(con.oid) as "Definition"
            from pg_catalog.pg_constraint con
            join pg_catalog.pg_class c on con.conrelid = c.oid
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            left join pg_catalog.pg_class fc on con.confrelid = fc.oid
            where
                c.relname = {_literal(item.label)}
                and n.nspname = {_literal(item.parent.label)}
            order by con.conname
            """.strip("\n")
        )
    )


def show_external_table_definition(
    item: "ExternalTableCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    """Where a Spectrum table's data lives and how it is serialized."""
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                schemaname as "Schema",
                tablename as "Table",
                location as "Location",
                input_format as "Input Format",
                output_format as "Output Format",
                serialization_lib as "SerDe",
                serde_parameters as "SerDe Parameters",
                parameters as "Parameters"
            from svv_external_tables
            where
                schemaname = {_literal(item.parent.label)}
                and tablename = {_literal(item.label)}
            """.strip("\n")
        )
    )


def show_materialized_view_info(
    item: "MaterializedViewCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    """Refresh state and autorefresh settings for a materialized view."""
    if item.parent is None:
        driver.notify(
            f"Could not describe {item.label} due to missing schema reference.",
            severity="error",
        )
        return
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                database_name as "Database",
                schema_name as "Schema",
                name as "Name",
                autorefresh as "Autorefresh",
                autorewrite as "Autorewrite",
                is_stale as "Is Stale",
                state as "State",
                updated_upto_xid as "Updated Up To XID"
            from svv_mv_info
            where
                schema_name = {_literal(item.parent.label)}
                and name = {_literal(item.label)}
            """.strip("\n")
        )
    )


# -- Listings -----------------------------------------------------------------


def show_list_objects(
    item: "SchemaCatalogItem | DatabaseCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    """Every relation under this item, local, external, and shared alike."""
    # can't use isinstance due to circular reference
    if type(item).__name__ == "SchemaCatalogItem":
        parent = getattr(item, "parent", None)
        database = parent.label if parent is not None else None
        scope = f"and schema_name = {_literal(item.label)}"
    else:
        database = item.label
        scope = ""
    database_filter = f"database_name = {_literal(database)}" if database else "true"
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                database_name as "Database",
                schema_name as "Schema",
                table_name as "Name",
                table_type as "Type",
                remarks as "Description"
            from svv_all_tables
            where
                {database_filter}
                {scope}
            order by 1, 2, 3
            """.strip("\n")
        )
    )


def show_list_schemas(
    item: "DatabaseCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                database_name as "Database",
                schema_name as "Schema",
                schema_owner as "Owner",
                schema_type as "Type",
                source_database as "Source Database",
                schema_acl as "Privileges"
            from svv_all_schemas
            where database_name = {_literal(item.label)}
            order by 1, 2
            """.strip("\n")
        )
    )


def show_storage_summary(
    item: "DatabaseCatalogItem | SchemaCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    """The largest tables under this item, by disk footprint.

    SVV_TABLE_INFO covers only the connected database, so this is scoped by
    schema when the item names one and left unscoped otherwise.
    """
    # can't use isinstance due to circular reference
    if type(item).__name__ == "SchemaCatalogItem":
        scope = f'where "schema" = {_literal(item.label)}'
    else:
        scope = ""
    driver.insert_text_in_new_buffer(
        dedent(
            f"""
            select
                "schema" as "Schema",
                "table" as "Table",
                diststyle as "Dist Style",
                sortkey1 as "Sort Key",
                size as "Size (MB)",
                pct_used as "Pct of Disk",
                tbl_rows as "Rows",
                unsorted as "Pct Unsorted",
                stats_off as "Stats Staleness"
            from svv_table_info
            {scope}
            order by size desc nulls last
            limit 100
            """.strip("\n")
        )
    )


# -- Definitions (these run a query) ------------------------------------------


def _show_definition(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
    show_command: str,
    what: str,
) -> None:
    """Runs one of Redshift's SHOW commands and puts its DDL in a new buffer.

    SHOW TABLE / SHOW VIEW / SHOW EXTERNAL TABLE return the object's DDL as a
    single value, which is the whole reason to run the statement here instead of
    handing it to the user: what they want is the text it returns.
    """
    if item.connection is None or item.parent is None:
        return
    query = f"{show_command} {item.qualified_identifier}"
    try:
        cur = item.connection.execute(query)
    except HarlequinQueryError:
        driver.notify(
            f"Could not get the definition of {what} {item.label}.", severity="error"
        )
        raise
    if cur is None:
        driver.notify(f"{item.label} did not report a definition.", severity="warning")
        return
    result = cur.fetchall()
    if not result:
        driver.notify(f"{item.label} did not report a definition.", severity="warning")
        return
    definition = str(result[0][0])
    driver.insert_text_in_new_buffer(
        f"-- {what.capitalize()} definition for {item.query_name}\n"
        f"-- from: {query}\n\n"
        f"{definition}"
    )


def show_table_definition(item: "TableCatalogItem", driver: "HarlequinDriver") -> None:
    _show_definition(item, driver, show_command="show table", what="table")


def show_view_definition(item: "ViewCatalogItem", driver: "HarlequinDriver") -> None:
    _show_definition(item, driver, show_command="show view", what="view")


def show_external_table_ddl(
    item: "ExternalTableCatalogItem", driver: "HarlequinDriver"
) -> None:
    _show_definition(
        item, driver, show_command="show external table", what="external table"
    )


# -- Drops (these run DDL, behind a confirmation) -----------------------------


def execute_drop_relation_statement(
    item: "RelationCatalogItem",
    driver: "HarlequinDriver",
    relation_type: Literal["view", "table", "materialized view", "external table"],
) -> None:
    def _drop_relation() -> None:
        if item.connection is None:
            return
        try:
            item.connection.execute(f"drop {relation_type} {item.qualified_identifier}")
        except HarlequinQueryError:
            driver.notify(
                f"Could not drop {relation_type} {item.label}", severity="error"
            )
            raise
        else:
            driver.notify(f"Dropped {relation_type} {item.label}")
            driver.refresh_catalog()

    driver.confirm_and_execute(callback=_drop_relation)


def execute_drop_table_statement(
    item: "RelationCatalogItem", driver: "HarlequinDriver"
) -> None:
    execute_drop_relation_statement(item=item, driver=driver, relation_type="table")


def execute_drop_view_statement(
    item: "RelationCatalogItem", driver: "HarlequinDriver"
) -> None:
    execute_drop_relation_statement(item=item, driver=driver, relation_type="view")


def execute_drop_materialized_view_statement(
    item: "RelationCatalogItem", driver: "HarlequinDriver"
) -> None:
    execute_drop_relation_statement(
        item=item, driver=driver, relation_type="materialized view"
    )


def execute_drop_external_table_statement(
    item: "RelationCatalogItem", driver: "HarlequinDriver"
) -> None:
    execute_drop_relation_statement(
        item=item, driver=driver, relation_type="external table"
    )


def execute_drop_schema_statement(
    item: "SchemaCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    def _drop_schema() -> None:
        if item.connection is None:
            return
        try:
            item.connection.execute(f"drop schema {item.qualified_identifier} cascade")
        except HarlequinQueryError:
            driver.notify(f"Could not drop schema {item.label}", severity="error")
            raise
        else:
            driver.notify(f"Dropped schema {item.label}")
            driver.refresh_catalog()

    if item.children or item.fetch_children():
        driver.confirm_and_execute(callback=_drop_schema)
    else:
        _drop_schema()


def execute_drop_database_statement(
    item: "DatabaseCatalogItem",
    driver: "HarlequinDriver",
) -> None:
    def _drop_database() -> None:
        if item.connection is None:
            return
        try:
            item.connection.execute(f"drop database {item.qualified_identifier}")
        except HarlequinQueryError:
            driver.notify(f"Could not drop database {item.label}", severity="error")
            raise
        else:
            driver.notify(f"Dropped database {item.label}")
            driver.refresh_catalog()

    if item.children or item.fetch_children():
        driver.confirm_and_execute(callback=_drop_database)
    else:
        _drop_database()
