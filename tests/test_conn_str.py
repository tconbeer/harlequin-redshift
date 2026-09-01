from __future__ import annotations

import pytest

from harlequin_redshift.conn_str import (
    ConnStrError,
    build_connect_kwargs,
    conn_str_to_dict,
)


@pytest.mark.parametrize(
    "conn_str,expected",
    [
        ("", {}),
        (None, {}),
        ("   ", {}),
        (
            "redshift://my-cluster.abc.us-east-1.redshift.amazonaws.com:5439/dev",
            {
                "host": "my-cluster.abc.us-east-1.redshift.amazonaws.com",
                "port": 5439,
                "database": "dev",
            },
        ),
        (
            "postgresql://awsuser:hunter2@localhost:15439/dev",
            {
                "host": "localhost",
                "port": 15439,
                "database": "dev",
                "user": "awsuser",
                "password": "hunter2",
            },
        ),
        (
            "postgres://localhost/analytics",
            {"host": "localhost", "database": "analytics"},
        ),
        (
            "host=localhost port=15439 dbname=dev user=awsuser",
            {
                "host": "localhost",
                "port": 15439,
                "database": "dev",
                "user": "awsuser",
            },
        ),
        (
            "host=localhost connect_timeout=10 password='with space'",
            {"host": "localhost", "timeout": 10, "password": "with space"},
        ),
    ],
)
def test_conn_str_to_dict(conn_str: str | None, expected: dict) -> None:
    assert conn_str_to_dict(conn_str) == expected


def test_url_query_string_carries_driver_options() -> None:
    parsed = conn_str_to_dict(
        "redshift://localhost:15439/dev?iam=true&region=us-east-1"
        "&cluster_identifier=my-cluster&ssl=false"
    )
    assert parsed == {
        "host": "localhost",
        "port": 15439,
        "database": "dev",
        "iam": True,
        "region": "us-east-1",
        "cluster_identifier": "my-cluster",
        "ssl": False,
    }


def test_url_components_beat_the_query_string() -> None:
    parsed = conn_str_to_dict("redshift://real-host:15439/dev?host=decoy&port=1")
    assert parsed["host"] == "real-host"
    assert parsed["port"] == 15439


def test_percent_encoded_credentials_are_decoded() -> None:
    parsed = conn_str_to_dict("redshift://user%40corp:p%40ss%2Fword@host/dev")
    assert parsed["user"] == "user@corp"
    assert parsed["password"] == "p@ss/word"


def test_db_groups_parses_as_a_list() -> None:
    parsed = conn_str_to_dict("redshift://host/dev?db_groups=analysts,readers")
    assert parsed["db_groups"] == ["analysts", "readers"]


@pytest.mark.parametrize(
    "conn_str",
    [
        "mysql://localhost/dev",
        "redshift://localhost:not-a-port/dev",
        "host=localhost bare-word",
        "host=localhost port=abc",
        "host=localhost iam=maybe",
    ],
)
def test_bad_conn_str_raises(conn_str: str) -> None:
    with pytest.raises(ConnStrError):
        conn_str_to_dict(conn_str)


def test_options_override_the_conn_str() -> None:
    kwargs = build_connect_kwargs(
        "redshift://localhost:15439/dev",
        {"port": "5439", "database": "analytics", "user": "awsuser"},
    )
    assert kwargs == {
        "host": "localhost",
        "port": 5439,
        "database": "analytics",
        "user": "awsuser",
    }


def test_unset_options_do_not_override_the_conn_str() -> None:
    kwargs = build_connect_kwargs(
        "redshift://localhost:15439/dev",
        {"port": None, "database": None, "db_groups": (), "user": "awsuser"},
    )
    assert kwargs == {
        "host": "localhost",
        "port": 15439,
        "database": "dev",
        "user": "awsuser",
    }


def test_options_alone_are_enough() -> None:
    kwargs = build_connect_kwargs(None, {"host": "localhost", "port": "15439"})
    assert kwargs == {"host": "localhost", "port": 15439}
