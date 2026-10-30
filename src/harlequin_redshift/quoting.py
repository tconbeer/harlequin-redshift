"""Quoting for the SQL text this adapter writes but cannot bind.

Values that reach a query this adapter executes are bound as parameters; these
are for what cannot be: identifiers, and SQL handed to the user's editor.
"""

from __future__ import annotations

import re

_PLAIN_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_$]*")
"""A name Redshift reads back unchanged without quotes, keywords aside."""

LIKE_ESCAPE = "!"
"""The escape character for patterns built by `like_contains()`.

Redshift gives ``!`` no meaning inside a string literal, so a pattern escaped
with it means the same thing whether it is bound or written into SQL text.
"""


def quote_identifier(name: str) -> str:
    """A name as a Redshift delimited identifier, with ``"`` doubled."""
    return '"' + name.replace('"', '""') + '"'


def qualified_name(*parts: str) -> str:
    """A dotted name, each part quoted on its own."""
    return ".".join(quote_identifier(part) for part in parts)


def quote_identifier_if_needed(name: str) -> str:
    """A name as-is when it is already a plain lower-case identifier, else quoted.

    Unquoted identifiers fold to lower case, so anything with upper case or a
    character outside ``[a-z0-9_$]`` needs quotes to mean the same object.
    """
    if _PLAIN_IDENTIFIER.fullmatch(name):
        return name
    return quote_identifier(name)


def quote_literal(value: str) -> str:
    """A string as a Redshift string literal.

    Redshift reads a backslash inside ``'...'`` as an escape, so both ``\\`` and
    ``'`` are doubled, the same as its own ``QUOTE_LITERAL()``.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def like_contains(term: str) -> str:
    """A LIKE pattern matching `term` anywhere, its wildcards taken literally.

    Use it with ``ESCAPE '!'`` (`LIKE_ESCAPE`).
    """
    escaped = (
        term.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2)
        .replace("%", f"{LIKE_ESCAPE}%")
        .replace("_", f"{LIKE_ESCAPE}_")
    )
    return f"%{escaped}%"
