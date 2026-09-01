"""Fixtures for the harlequin-redshift tests.

The integration tests here run against a real Amazon Redshift cluster and never
write to it: they create nothing, drop nothing, and read only what the cluster
already has. Everything they assert on is discovered from the catalog at run
time, so the suite works against any cluster without being told what is in it.

Point them at a cluster with a Harlequin profile. By default they look for a
profile named ``redshift-test`` in the config files Harlequin itself discovers::

    # .harlequin.toml
    [profiles.redshift-test]
    adapter = "redshift"
    host = "localhost"
    port = 15439
    database = "dev"
    user = "..."
    password = "..."

Override the profile with ``HARLEQUIN_REDSHIFT_TEST_PROFILE`` and the config
file with ``HARLEQUIN_REDSHIFT_TEST_CONFIG``, or skip the config entirely and
set ``HARLEQUIN_REDSHIFT_TEST_DSN`` to a connection string. With none of those
set, every integration test is skipped and the unit tests still run.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Generator

import pytest
from harlequin.config import load_profile

from harlequin_redshift.adapter import (
    HarlequinRedshiftAdapter,
    HarlequinRedshiftConnection,
)
from harlequin_redshift.cli_options import REDSHIFT_OPTIONS

DEFAULT_PROFILE = "redshift-test"

_DECLARED_OPTIONS = frozenset(option.name for option in REDSHIFT_OPTIONS)
"""The options this adapter declares.

A profile also carries Harlequin's own settings -- `read_only`, `limit`,
`viewer_max_rows` -- so it is filtered down to what the adapter declared, the
same way the adapter itself filters what Harlequin hands it. `read_only` is
this adapter's own argument rather than one of its options, so the fixtures
below pass it separately.
"""


def _profile_options() -> tuple[tuple[str, ...], dict[str, Any]] | None:
    """The connection string and options the tests should connect with."""
    dsn = os.environ.get("HARLEQUIN_REDSHIFT_TEST_DSN")
    if dsn:
        return (dsn,), {}

    raw_config_path = os.environ.get("HARLEQUIN_REDSHIFT_TEST_CONFIG")
    config_path = Path(raw_config_path) if raw_config_path else None
    profile_name = os.environ.get("HARLEQUIN_REDSHIFT_TEST_PROFILE", DEFAULT_PROFILE)
    try:
        profile = load_profile(config_path, profile_name)
    except Exception:
        return None
    if not profile:
        return None

    conn_str = profile.get("conn_str") or ()
    if isinstance(conn_str, str):
        conn_str = (conn_str,)
    options = {
        key.replace("-", "_"): value
        for key, value in profile.items()
        if key.replace("-", "_") in _DECLARED_OPTIONS
    }
    return tuple(conn_str), options


@pytest.fixture(scope="session")
def connection_options() -> tuple[tuple[str, ...], dict[str, Any]]:
    options = _profile_options()
    if options is None:
        pytest.skip(
            "No Redshift test cluster configured. Set HARLEQUIN_REDSHIFT_TEST_DSN, "
            f"or define a [profiles.{DEFAULT_PROFILE}] profile in a Harlequin "
            "config file."
        )
    return options


@pytest.fixture(scope="session")
def adapter(
    connection_options: tuple[tuple[str, ...], dict[str, Any]],
) -> HarlequinRedshiftAdapter:
    conn_str, options = connection_options
    return HarlequinRedshiftAdapter(conn_str=conn_str, **options)


@pytest.fixture(scope="session")
def connection(
    adapter: HarlequinRedshiftAdapter,
) -> Generator[HarlequinRedshiftConnection, None, None]:
    """One connection shared by the read-only integration tests.

    It is session-scoped because opening a connection to a real cluster is slow
    and none of these tests change anything another one could see.
    """
    try:
        conn = adapter.connect()
    except Exception as e:
        pytest.skip(f"Could not connect to the Redshift test cluster: {e}")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def read_only_connection(
    connection_options: tuple[tuple[str, ...], dict[str, Any]],
) -> Generator[HarlequinRedshiftConnection, None, None]:
    conn_str, options = connection_options
    try:
        conn = HarlequinRedshiftAdapter(
            conn_str=conn_str, read_only=True, **options
        ).connect()
    except Exception as e:
        pytest.skip(f"Could not open a read-only connection: {e}")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def a_relation(connection: HarlequinRedshiftConnection) -> tuple[str, str, str]:
    """One relation that exists on the cluster, as (database, schema, relation).

    The tests that need a real object to describe find it here rather than
    naming one, so they do not depend on what this particular cluster holds.
    """
    database = connection.database
    for schema in connection._get_schemas(database):
        relations = connection._get_relations(database, schema)
        for name, _type_name in relations:
            if connection._get_columns(database, schema, name):
                return database, schema, name
    pytest.skip(f"No readable relation found in {database}.")
