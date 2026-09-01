"""The items Harlequin shows in the Redshift data catalog.

Each level lazy-loads the one below it through ``fetch_children()``, so opening
a node is one round trip and a cluster with thousands of relations costs nothing
until the user goes looking for one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from harlequin.catalog import InteractiveCatalogItem

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

if TYPE_CHECKING:
    from harlequin_redshift.adapter import HarlequinRedshiftConnection

TABLE = "TABLE"
VIEW = "VIEW"
MATERIALIZED_VIEW = "MATERIALIZED VIEW"
EXTERNAL_TABLE = "EXTERNAL TABLE"
EXTERNAL_VIEW = "EXTERNAL VIEW"
SHARED_TABLE = "SHARED TABLE"
LOCAL_TEMPORARY = "LOCAL TEMPORARY"


@dataclass
class ColumnCatalogItem(InteractiveCatalogItem["HarlequinRedshiftConnection"]):
    parent: "RelationCatalogItem | None" = None

    @classmethod
    def from_parent(
        cls,
        parent: "RelationCatalogItem",
        label: str,
        type_label: str,
        type_name: str | None = None,
    ) -> "ColumnCatalogItem":
        return cls(
            qualified_identifier=f'{parent.qualified_identifier}."{label}"',
            query_name=f'"{label}"',
            label=label,
            type_label=type_label,
            type_name=type_name,
            connection=parent.connection,
            parent=parent,
            loaded=True,
        )


@dataclass
class RelationCatalogItem(InteractiveCatalogItem["HarlequinRedshiftConnection"]):
    INTERACTIONS = [
        ("Insert Columns at Cursor", insert_columns_at_cursor),
        ("Preview Data", show_select_star),
        ("Describe Columns", show_describe_relation),
    ]
    TYPE_LABEL: ClassVar[str] = "rel"
    DEFAULT_TYPE_NAME: ClassVar[str] = TABLE
    parent: "SchemaCatalogItem | None" = None

    @classmethod
    def from_parent(
        cls,
        parent: "SchemaCatalogItem",
        label: str,
        type_name: str | None = None,
    ) -> "RelationCatalogItem":
        return cls(
            qualified_identifier=f'{parent.qualified_identifier}."{label}"',
            query_name=f'{parent.query_name}."{label}"',
            label=label,
            type_label=cls.TYPE_LABEL,
            type_name=type_name or cls.DEFAULT_TYPE_NAME,
            connection=parent.connection,
            parent=parent,
        )

    def fetch_children(self) -> list[ColumnCatalogItem]:
        if self.parent is None or self.parent.parent is None or self.connection is None:
            return []
        result = self.connection._get_columns(
            self.parent.parent.label, self.parent.label, self.label
        )
        return [
            ColumnCatalogItem.from_parent(
                parent=self,
                label=column_name,
                type_label=self.connection._short_column_type(column_type),
                type_name=column_type,
            )
            for column_name, column_type in result
        ]


class TableCatalogItem(RelationCatalogItem):
    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Show DDL (SHOW TABLE)", show_table_definition),
        ("Describe Design (Dist/Sort/Encoding)", show_describe_table_design),
        ("Show Table Info", show_table_info),
        ("Describe Constraints", show_describe_table_constraints),
        ("Drop Table", execute_drop_table_statement),
    ]
    TYPE_LABEL = "t"
    DEFAULT_TYPE_NAME = TABLE


class TempTableCatalogItem(TableCatalogItem):
    TYPE_LABEL = "tmp"
    DEFAULT_TYPE_NAME = LOCAL_TEMPORARY


class SharedTableCatalogItem(RelationCatalogItem):
    """A table this cluster reads through a datashare.

    It lives in another cluster, so the local physical-design and drop actions
    do not apply to it.
    """

    TYPE_LABEL = "shr"
    DEFAULT_TYPE_NAME = SHARED_TABLE


class ViewCatalogItem(RelationCatalogItem):
    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Show DDL (SHOW VIEW)", show_view_definition),
        ("Drop View", execute_drop_view_statement),
    ]
    TYPE_LABEL = "v"
    DEFAULT_TYPE_NAME = VIEW


class MaterializedViewCatalogItem(RelationCatalogItem):
    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Show DDL (SHOW VIEW)", show_view_definition),
        ("Show Refresh Info", show_materialized_view_info),
        ("Drop Materialized View", execute_drop_materialized_view_statement),
    ]
    TYPE_LABEL = "mv"
    DEFAULT_TYPE_NAME = MATERIALIZED_VIEW


class ExternalTableCatalogItem(RelationCatalogItem):
    """A Redshift Spectrum table, backed by files in S3."""

    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Show DDL (SHOW EXTERNAL TABLE)", show_external_table_ddl),
        ("Show Location & Format", show_external_table_definition),
        ("Drop External Table", execute_drop_external_table_statement),
    ]
    TYPE_LABEL = "ext"
    DEFAULT_TYPE_NAME = EXTERNAL_TABLE


class ExternalViewCatalogItem(RelationCatalogItem):
    TYPE_LABEL = "extv"
    DEFAULT_TYPE_NAME = EXTERNAL_VIEW


_RELATION_CLASSES: dict[str, type[RelationCatalogItem]] = {
    TABLE: TableCatalogItem,
    "BASE TABLE": TableCatalogItem,
    "PARTITIONED TABLE": TableCatalogItem,
    "FOREIGN TABLE": ExternalTableCatalogItem,
    VIEW: ViewCatalogItem,
    MATERIALIZED_VIEW: MaterializedViewCatalogItem,
    EXTERNAL_TABLE: ExternalTableCatalogItem,
    EXTERNAL_VIEW: ExternalViewCatalogItem,
    SHARED_TABLE: SharedTableCatalogItem,
    "SHARED VIEW": SharedTableCatalogItem,
    LOCAL_TEMPORARY: TempTableCatalogItem,
    "TEMPORARY TABLE": TempTableCatalogItem,
    "TEMPORARY VIEW": ViewCatalogItem,
    "SYSTEM TABLE": TableCatalogItem,
    "SYSTEM VIEW": ViewCatalogItem,
}
"""The item class for each relation type the server reports.

``get_tables()`` spells the type differently depending on whether the cluster
answers metadata from SHOW discovery, from SVV_ALL_TABLES, or from the driver's
legacy pg_catalog query, so every spelling that reaches us is mapped here.
"""


def relation_class(type_name: str | None) -> type[RelationCatalogItem]:
    """The item class that represents a relation of this type.

    An unfamiliar type falls back to a plain table, so a relation the server
    grows a new name for still appears in the catalog and can still be queried.
    """
    if not type_name:
        return TableCatalogItem
    return _RELATION_CLASSES.get(type_name.strip().upper(), TableCatalogItem)


def relation_item(
    parent: "SchemaCatalogItem", label: str, type_name: str | None
) -> RelationCatalogItem:
    """A relation item of the class its type calls for."""
    return relation_class(type_name).from_parent(
        parent=parent, label=label, type_name=type_name
    )


@dataclass
class SchemaCatalogItem(InteractiveCatalogItem["HarlequinRedshiftConnection"]):
    INTERACTIONS = [
        ("Set Search Path", execute_use_statement),
        ("List Relations", show_list_objects),
        ("Show Storage Summary", show_storage_summary),
        ("Drop Schema", execute_drop_schema_statement),
    ]
    parent: "DatabaseCatalogItem | None" = None

    @classmethod
    def from_parent(
        cls,
        parent: "DatabaseCatalogItem",
        label: str,
    ) -> "SchemaCatalogItem":
        return cls(
            qualified_identifier=f'{parent.qualified_identifier}."{label}"',
            # a schema in another database is only addressable three-part, so
            # the query name carries the database only when it has to
            query_name=(
                f'"{label}"'
                if parent.is_connected_database
                else f'{parent.query_name}."{label}"'
            ),
            label=label,
            type_label="sch",
            type_name="SCHEMA",
            connection=parent.connection,
            parent=parent,
        )

    def fetch_children(self) -> list[RelationCatalogItem]:
        if self.parent is None or self.connection is None:
            return []
        return [
            relation_item(parent=self, label=label, type_name=type_name)
            for label, type_name in self.connection._get_relations(
                self.parent.label, self.label
            )
        ]


@dataclass
class DatabaseCatalogItem(InteractiveCatalogItem["HarlequinRedshiftConnection"]):
    INTERACTIONS = [
        ("List Schemas", show_list_schemas),
        ("List Relations", show_list_objects),
        ("Show Storage Summary", show_storage_summary),
        ("Drop Database", execute_drop_database_statement),
    ]
    is_connected_database: bool = False
    """True for the database this session is connected to.

    Redshift can read the other databases on the cluster with a three-part name,
    but the connected one is addressed with two parts, so the items below this
    one need to know which this is to spell their query names.
    """

    @classmethod
    def from_label(
        cls, label: str, connection: "HarlequinRedshiftConnection"
    ) -> "DatabaseCatalogItem":
        database_identifier = f'"{label}"'
        return cls(
            qualified_identifier=database_identifier,
            query_name=database_identifier,
            label=label,
            type_label="db",
            type_name="DATABASE",
            connection=connection,
            is_connected_database=label == connection.database,
        )

    def fetch_children(self) -> list[SchemaCatalogItem]:
        if self.connection is None:
            return []
        return [
            SchemaCatalogItem.from_parent(parent=self, label=label)
            for label in self.connection._get_schemas(self.label)
        ]
