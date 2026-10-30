"""The quoting module, checked against Redshift's lexical rules."""

from __future__ import annotations

import pytest
from sql_text import AWKWARD_NAMES, dotted_name, identifiers, literals, tokens

from harlequin_redshift.quoting import (
    LIKE_ESCAPE,
    like_contains,
    qualified_name,
    quote_identifier,
    quote_identifier_if_needed,
    quote_literal,
)


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_quote_identifier_round_trips(name: str) -> None:
    quoted = quote_identifier(name)
    assert [token.kind for token in tokens(quoted)] == ["identifier"]
    assert identifiers(quoted) == [name]


@pytest.mark.parametrize(
    "name,expected",
    [
        ("plain", '"plain"'),
        ('a"b', '"a""b"'),
        ('""', '""""""'),
        # nothing but a double quote is special inside a delimited identifier
        ("a\\b'c", '"a\\b\'c"'),
    ],
)
def test_quote_identifier(name: str, expected: str) -> None:
    assert quote_identifier(name) == expected


@pytest.mark.parametrize("name", AWKWARD_NAMES)
def test_quote_literal_round_trips(name: str) -> None:
    quoted = quote_literal(name)
    assert [token.kind for token in tokens(quoted)] == ["literal"]
    assert literals(quoted) == [name]


@pytest.mark.parametrize(
    "value,expected",
    [
        ("plain", "'plain'"),
        ("it's", "'it''s'"),
        # a backslash escapes inside a Redshift string literal, so it is doubled,
        # as Redshift's own QUOTE_LITERAL() doubles it
        ("a\\b", "'a\\\\b'"),
        ("trailing\\", "'trailing\\\\'"),
        ("\\'", "'\\\\'''"),
    ],
)
def test_quote_literal(value: str, expected: str) -> None:
    assert quote_literal(value) == expected


def test_qualified_name_quotes_each_part() -> None:
    parts = ["my db", "sch.ema", 'ta"ble']
    assert qualified_name(*parts) == '"my db"."sch.ema"."ta""ble"'
    assert dotted_name(qualified_name(*parts)) == parts


@pytest.mark.parametrize(
    "name,expected",
    [
        ("sum", "sum"),
        ("_private$1", "_private$1"),
        ("MyFunc", '"MyFunc"'),
        ("my func", '"my func"'),
        ("1st", '"1st"'),
        ('a"b', '"a""b"'),
        ("", '""'),
    ],
)
def test_quote_identifier_if_needed(name: str, expected: str) -> None:
    assert quote_identifier_if_needed(name) == expected


@pytest.mark.parametrize(
    "term,expected",
    [
        ("orders", "%orders%"),
        ("per%cent", "%per!%cent%"),
        ("under_score", "%under!_score%"),
        ("bang!", "%bang!!%"),
        ("back\\slash", "%back\\slash%"),
    ],
)
def test_like_contains_takes_wildcards_literally(term: str, expected: str) -> None:
    assert LIKE_ESCAPE == "!"
    assert like_contains(term) == expected
