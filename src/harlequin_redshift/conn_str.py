"""Turns a connection string into ``redshift_connector.connect()`` keyword args.

``redshift_connector.connect()`` takes keywords only, but Harlequin passes a
connection string as its first argument, and Redshift users spell one either as
a URL (``redshift://user@host:5439/dev``) or, since the server speaks the
Postgres wire protocol, as a libpq keyword string (``host=... dbname=...``).
Both are accepted here and normalized onto the driver's own parameter names.
"""

from __future__ import annotations

import shlex
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

_SCHEMES = frozenset(
    {"redshift", "redshift+redshift_connector", "postgres", "postgresql"}
)
"""URL schemes a connection string may use.

The Postgres ones are here because Redshift speaks that protocol, so the DSNs
users already have for a cluster are usually spelled with them.
"""

_ALIASES = {
    # libpq spells these differently than redshift_connector does
    "dbname": "database",
    "username": "user",
    "connect_timeout": "timeout",
    "passwd": "password",
    "pwd": "password",
    "sslrootcert": "sslmode",
    "options": None,
    # accepted spellings of driver options
    "serverless": "is_serverless",
    "workgroup": "serverless_work_group",
    "clusterid": "cluster_identifier",
    "aws_profile": "profile",
    "aws_region": "region",
}
"""Other spellings mapped onto a driver keyword. ``None`` drops the key."""

_INT_KEYS = frozenset(
    {
        "port",
        "timeout",
        "max_prepared_statements",
        "client_protocol_version",
        "idp_response_timeout",
        "listen_port",
        "tcp_keepalive_idle",
        "tcp_keepalive_interval",
        "tcp_keepalive_count",
    }
)

_BOOL_KEYS = frozenset(
    {
        "ssl",
        "ssl_insecure",
        "iam",
        "auto_create",
        "allow_db_user_override",
        "force_lowercase",
        "database_metadata_current_db_only",
        "enable_table_types",
        "numeric_to_float",
        "is_serverless",
        "group_federation",
        "iam_disable_cache",
        "tcp_keepalive",
    }
)

_LIST_KEYS = frozenset({"db_groups"})

_TRUE = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSE = frozenset({"0", "false", "f", "no", "n", "off"})


class ConnStrError(ValueError):
    """The connection string could not be read."""


def _coerce(key: str, value: Any) -> Any:
    """A raw option value as the type ``connect()`` expects for ``key``.

    Values arrive as strings from a URL, a keyword string, or a config file, so
    every option that is not a string is converted here rather than at each of
    those call sites.
    """
    if value is None or not isinstance(value, str):
        return value
    if key in _INT_KEYS:
        try:
            return int(value)
        except ValueError as e:
            raise ConnStrError(f"{key} must be an integer; got {value!r}.") from e
    if key in _BOOL_KEYS:
        lowered = value.strip().lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
        raise ConnStrError(f"{key} must be a boolean; got {value!r}.")
    if key in _LIST_KEYS:
        return [item for item in (v.strip() for v in value.split(",")) if item]
    return value


def _normalize(items: Any) -> dict[str, Any]:
    """Option pairs with their keys renamed and their values typed."""
    normalized: dict[str, Any] = {}
    for raw_key, value in items:
        key = raw_key.strip().lower()
        if key in _ALIASES:
            aliased = _ALIASES[key]
            if aliased is None:
                continue
            key = aliased
        normalized[key] = _coerce(key, value)
    return normalized


def _parse_url(conn_str: str) -> dict[str, Any]:
    parts = urlsplit(conn_str)
    options: dict[str, Any] = _normalize(
        parse_qsl(parts.query, keep_blank_values=False)
    )
    # a URL's own components outrank its query string, which is where the
    # extra driver options live
    if parts.hostname:
        options["host"] = unquote(parts.hostname)
    try:
        if parts.port is not None:
            options["port"] = parts.port
    except ValueError as e:
        raise ConnStrError(f"Invalid port in connection string: {conn_str!r}") from e
    if parts.username:
        options["user"] = unquote(parts.username)
    if parts.password:
        options["password"] = unquote(parts.password)
    database = parts.path.lstrip("/")
    if database:
        options["database"] = unquote(database)
    return options


def _parse_keyword_string(conn_str: str) -> dict[str, Any]:
    try:
        tokens = shlex.split(conn_str)
    except ValueError as e:
        raise ConnStrError(f"Invalid connection string: {conn_str!r}") from e
    pairs = []
    for token in tokens:
        if "=" not in token:
            raise ConnStrError(
                f"Invalid connection string: {conn_str!r}. Expected a URL, or "
                f"space-separated key=value pairs; got the bare word {token!r}."
            )
        key, _, value = token.partition("=")
        pairs.append((key, value))
    return _normalize(pairs)


def conn_str_to_dict(conn_str: str | None) -> dict[str, Any]:
    """The ``connect()`` keywords a connection string spells out.

    Raises:
        ConnStrError: if the string is neither a URL with a scheme this adapter
            knows nor a well-formed keyword string.
    """
    if not conn_str or not conn_str.strip():
        return {}
    conn_str = conn_str.strip()
    scheme, separator, _ = conn_str.partition("://")
    if separator:
        if scheme.lower() not in _SCHEMES:
            raise ConnStrError(
                f"Unrecognized scheme {scheme!r} in connection string. Expected "
                f"one of: {', '.join(sorted(_SCHEMES))}."
            )
        return _parse_url(conn_str)
    return _parse_keyword_string(conn_str)


def build_connect_kwargs(
    conn_str: str | None, options: dict[str, Any]
) -> dict[str, Any]:
    """Everything ``connect()`` needs, from a connection string plus CLI options.

    An option set on the command line, in a profile, or in the environment wins
    over the same option in the connection string, which matches how Harlequin's
    other adapters resolve the two.
    """
    kwargs = conn_str_to_dict(conn_str)
    kwargs.update(
        {
            key: _coerce(key, value)
            for key, value in options.items()
            if value is not None and value != () and value != []
        }
    )
    return kwargs
