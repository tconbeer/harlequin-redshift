"""Extra autocomplete entries for the Redshift query editor.

Harlequin builds completions for catalog objects itself; this adds the parts of
the dialect it cannot see: Redshift's keywords, and the functions and stored
procedures the cluster actually has.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from harlequin import HarlequinCompletion
from redshift_connector import Connection

logger = logging.getLogger(__name__)

_SYSTEM_SCHEMAS = frozenset({"pg_catalog", "information_schema"})
"""Schemas whose routines are built-ins, so their names need no qualifier."""


def _keyword_completions() -> list[HarlequinCompletion]:
    # source: the reserved words in the Amazon Redshift SQL reference, plus the
    # non-reserved words its statements use
    keyword_path = Path(__file__).parent / "keywords.tsv"
    completions: list[HarlequinCompletion] = []
    with keyword_path.open("r", encoding="utf-8") as f:
        keyword_reader = csv.reader(f, delimiter="\t")
        _header = next(keyword_reader)
        for keyword, kind in keyword_reader:
            completions.append(
                HarlequinCompletion(
                    label=keyword.lower(),
                    type_label="kw",
                    value=keyword.lower(),
                    priority=100 if kind.startswith("reserved") else 1000,
                    context=None,
                )
            )
    return completions


_ROUTINES = r"""
select distinct
    p.proname as label,
    n.nspname as schema_name,
    p.proisagg as is_aggregate
from pg_catalog.pg_proc p
join pg_catalog.pg_namespace n on n.oid = p.pronamespace
where
    length(p.proname) < 37
    and p.proname not like '\\_%'
    and p.proname not like 'pg\\_%'
"""
"""Every routine the connected database has, in one statement.

The driver's ``get_functions()`` answers by issuing a SHOW FUNCTIONS per schema,
which on a cluster with dozens of schemas costs dozens of round trips for a list
that is wanted once, at startup. pg_proc holds the same names -- built-ins,
UDFs, and stored procedures alike -- and reads in one.
"""


def _routine_completions(conn: Connection) -> list[HarlequinCompletion]:
    """Every function and stored procedure in the connected database.

    Completions are a convenience, so a cluster that will not answer this query
    costs the user their function names, not their session.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(_ROUTINES)
        rows = cursor.fetchall()
    except Exception:
        logger.warning("Could not read routine names for autocomplete", exc_info=True)
        return []
    finally:
        try:
            cursor.close()
        except Exception:
            pass

    completions: list[HarlequinCompletion] = []
    seen: set[tuple[str, str | None]] = set()
    for row in rows:
        name, schema = str(row[0]), str(row[1])
        is_aggregate = bool(row[2]) if len(row) > 2 else False
        context = None if schema in _SYSTEM_SCHEMAS else schema
        if (name, context) in seen:
            continue
        seen.add((name, context))
        completions.append(
            HarlequinCompletion(
                label=name,
                type_label="agg" if is_aggregate else "fn",
                value=name,
                priority=1000,
                context=context,
            )
        )
    return completions


def _get_completions(conn: Connection) -> list[HarlequinCompletion]:
    completions = _keyword_completions()
    completions.extend(_routine_completions(conn))
    return sorted(completions)
