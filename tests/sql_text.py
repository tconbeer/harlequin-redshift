"""A reader for the SQL these tests assert on, following Redshift's lexical rules.

Inside ``'...'`` a backslash escapes the next character and ``''`` is one quote;
inside ``"..."`` only ``""`` is special. That is enough to recover, from SQL the
adapter wrote, exactly which names it put in it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Token:
    kind: str
    """``literal``, ``identifier``, or ``text``."""
    value: str


def tokens(sql: str) -> list[Token]:
    """`sql` split into string literals, delimited identifiers, and other text.

    ``--`` comments are dropped. Raises ValueError on an unterminated quote.
    """
    found: list[Token] = []
    text: list[str] = []
    position = 0

    def flush_text() -> None:
        if text:
            found.append(Token("text", "".join(text)))
            text.clear()

    while position < len(sql):
        char = sql[position]
        if sql.startswith("--", position):
            end_of_line = sql.find("\n", position)
            position = len(sql) if end_of_line == -1 else end_of_line
            continue
        if char not in "'\"":
            text.append(char)
            position += 1
            continue
        flush_text()
        value: list[str] = []
        position += 1
        while True:
            if position >= len(sql):
                raise ValueError(f"unterminated {char} in {sql!r}")
            current = sql[position]
            if char == "'" and current == "\\":
                value.append(sql[position + 1])
                position += 2
            elif current == char and sql[position + 1 : position + 2] == char:
                value.append(char)
                position += 2
            elif current == char:
                position += 1
                break
            else:
                value.append(current)
                position += 1
        found.append(Token("literal" if char == "'" else "identifier", "".join(value)))
    flush_text()
    return found


def literals(sql: str) -> list[str]:
    """Every string literal's value, in order."""
    return [token.value for token in tokens(sql) if token.kind == "literal"]


def identifiers(sql: str) -> list[str]:
    """Every delimited identifier's name, in order."""
    return [token.value for token in tokens(sql) if token.kind == "identifier"]


def unquoted_text(sql: str) -> str:
    """Everything outside quotes and comments: the SQL's own words."""
    return "".join(token.value for token in tokens(sql) if token.kind == "text")


def dotted_name(sql: str) -> list[str]:
    """The parts of a name like ``"a"."b"."c"``, which must be all `sql` holds."""
    parsed = tokens(sql)
    assert [token.kind for token in parsed[::2]] == ["identifier"] * len(parsed[::2])
    assert all(token == Token("text", ".") for token in parsed[1::2])
    return [token.value for token in parsed[::2]]


AWKWARD_NAMES = [
    "plain",
    "Mixed Case",
    'double"quote',
    "single'quote",
    "back`tick",
    "]bracket[",
    "back\\slash",
    "trailing\\",
    "dotted.name",
    "semi;colon",
    "'; drop table users; --",
    '"; drop table users; --',
    "per%cent_under",
    "new\nline",
    "x\n    y",
    "x\n                y",
]
"""Names a catalog can hold that SQL built by interpolation would get wrong."""
