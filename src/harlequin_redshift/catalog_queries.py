"""Catalog queries this adapter runs itself, with every name bound as a parameter.

The driver's own catalog queries strip quotes, spaces, and ``-;/`` from the
names they are given and inline what is left, so an object whose name holds one
of those cannot be found through them; these bind each name instead.

Each function returns rows in the shape the adapter reads, whichever view
answered: a schema is ``(schema, database)``, a relation is ``(database,
schema, name, type)``, and a column is ``(database, schema, relation, column,
type, size, decimal digits, ordinal position)``.
"""

from __future__ import annotations

from typing import Any, Sequence

from redshift_connector import Cursor

from harlequin_redshift.quoting import LIKE_ESCAPE

Rows = Sequence[Sequence[Any]]

_LIKE = f"like %s escape '{LIKE_ESCAPE}'"

_DRIVER_TYPE_NAMES = {
    "boolean": "bool",
    "character varying": "varchar",
    '"char"': "char",
    "smallint": "int2",
    "integer": "int4",
    "bigint": "int8",
    "real": "float4",
    "double precision": "float8",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
}
"""SQL-standard type names, spelled the way the driver's metadata calls spell them.

The catalog shows the same type name whichever path loaded a column.
"""

_SCHEMAS_IN_CONNECTED_DATABASE = """
select nspname, current_database()
from pg_catalog.pg_namespace
where current_database() = %s
"""

_SCHEMAS_IN_ANY_DATABASE = """
select schema_name, database_name
from svv_all_schemas
where database_name = %s
"""

_RELATIONS_IN_CONNECTED_DATABASE = """
select current_database(), table_schema, table_name, table_type
from svv_tables
where current_database() = %s
"""

_RELATIONS_IN_ANY_DATABASE = """
select database_name, schema_name, table_name, table_type
from svv_all_tables
where database_name = %s
"""

_COLUMNS_IN_CONNECTED_DATABASE = """
select
    current_database(),
    table_schema,
    table_name,
    column_name,
    data_type,
    coalesce(character_maximum_length, numeric_precision),
    numeric_scale,
    ordinal_position
from svv_columns
where current_database() = %s
"""

_COLUMNS_IN_ANY_DATABASE = """
select
    database_name,
    schema_name,
    table_name,
    column_name,
    data_type,
    coalesce(character_maximum_length, numeric_precision),
    numeric_scale,
    ordinal_position
from svv_all_columns
where database_name = %s
"""


def _run(
    cur: Cursor, query: str, database: str, filters: list[tuple[str, str]]
) -> Rows:
    """Runs `query` for `database`, with each ``(predicate, value)`` appended.

    Every predicate is one of this module's fixed fragments; the values,
    `database` included, travel as bound parameters.
    """
    sql = query + "".join(f"  and {predicate}\n" for predicate, _value in filters)
    cur.execute(sql, [database, *(value for _predicate, value in filters)])
    return cur.fetchall() or ()


def schemas(
    cur: Cursor,
    database: str,
    *,
    all_databases: bool,
    name_like: str | None = None,
) -> Rows:
    """The schemas in `database`, or only those whose name is LIKE `name_like`."""
    if all_databases:
        query, name_column = _SCHEMAS_IN_ANY_DATABASE, "schema_name"
    else:
        query, name_column = _SCHEMAS_IN_CONNECTED_DATABASE, "nspname"
    filters: list[tuple[str, str]] = []
    if name_like is not None:
        filters.append((f"{name_column} {_LIKE}", name_like))
    return _run(cur, query, database, filters)


def relations(
    cur: Cursor,
    database: str,
    *,
    all_databases: bool,
    schema: str | None = None,
    name_like: str | None = None,
) -> Rows:
    """The relations in `database`, narrowed to one `schema` and a name pattern."""
    if all_databases:
        query, schema_column = _RELATIONS_IN_ANY_DATABASE, "schema_name"
    else:
        query, schema_column = _RELATIONS_IN_CONNECTED_DATABASE, "table_schema"
    filters: list[tuple[str, str]] = []
    if schema is not None:
        filters.append((f"{schema_column} = %s", schema))
    if name_like is not None:
        filters.append((f"table_name {_LIKE}", name_like))
    return _run(cur, query, database, filters)


def columns(
    cur: Cursor,
    database: str,
    *,
    all_databases: bool,
    schema: str | None = None,
    relation: str | None = None,
    name_like: str | None = None,
) -> Rows:
    """The columns in `database`, narrowed to one relation and a name pattern."""
    if all_databases:
        query, schema_column = _COLUMNS_IN_ANY_DATABASE, "schema_name"
    else:
        query, schema_column = _COLUMNS_IN_CONNECTED_DATABASE, "table_schema"
    filters: list[tuple[str, str]] = []
    if schema is not None:
        filters.append((f"{schema_column} = %s", schema))
    if relation is not None:
        filters.append(("table_name = %s", relation))
    if name_like is not None:
        filters.append((f"column_name {_LIKE}", name_like))
    return [
        (*row[:4], _DRIVER_TYPE_NAMES.get(row[4], row[4]), *row[5:])
        for row in _run(cur, query, database, filters)
    ]
