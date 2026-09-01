from __future__ import annotations

from harlequin.options import (
    FlagOption,
    ListOption,
    SelectOption,
    TextOption,
)


def _int_validator(s: str | None) -> tuple[bool, str]:
    if s is None:
        return True, ""
    try:
        _ = int(s)
    except ValueError:
        return False, f"Cannot convert {s} to an int!"
    else:
        return True, ""


# -- Basic connection ---------------------------------------------------------

host = TextOption(
    name="host",
    description=(
        "The hostname of the Amazon Redshift cluster or Serverless workgroup "
        "endpoint, e.g. my-cluster.abc123.us-east-1.redshift.amazonaws.com"
    ),
    short_decls=["-h"],
    default="localhost",
)

port = TextOption(
    name="port",
    description="The port the Redshift endpoint is listening on.",
    short_decls=["-p"],
    default="5439",
    validator=_int_validator,
)

database = TextOption(
    name="database",
    description="The name of the database to connect to.",
    short_decls=["-d", "--dbname"],
    default="dev",
)

user = TextOption(
    name="user",
    description="The database user name to connect as.",
    short_decls=["-u", "--username", "-U"],
)

password = TextOption(
    name="password",
    description="The password for the database user.",
    secret=True,
)

# -- Driver behavior ----------------------------------------------------------

timeout = TextOption(
    name="timeout",
    description=(
        "The number of seconds before a connection attempt to the server times "
        "out (write as an integer, e.g., 30)."
    ),
    validator=_int_validator,
)

application_name = TextOption(
    name="application_name",
    description=(
        "The name of the application shown in Redshift's system tables (STL_QUERY, "
        "SVL_QLOG) for queries run by this connection."
    ),
    default="harlequin",
)

max_prepared_statements = TextOption(
    name="max_prepared_statements",
    description="The maximum number of prepared statements the driver caches.",
    validator=_int_validator,
)

client_protocol_version = SelectOption(
    name="client_protocol_version",
    description=(
        "The wire protocol version the driver negotiates with the server. Lower "
        "this only if the server rejects the default (EXTENDED_RESULT_METADATA)."
    ),
    choices=[
        ("BASE_SERVER (0)", "0"),
        ("EXTENDED_RESULT_METADATA (1)", "1"),
        ("BINARY (2)", "2"),
    ],
)

numeric_to_float = FlagOption(
    name="numeric_to_float",
    description=(
        "Convert NUMERIC/DECIMAL values to Python floats instead of Decimals. "
        "Faster, but lossy; off by default so exact values are preserved."
    ),
)

tcp_keepalive = FlagOption(
    name="tcp_keepalive",
    description="Enable TCP keepalives on the connection socket.",
    default=True,
)

# -- SSL ----------------------------------------------------------------------

ssl = FlagOption(
    name="ssl",
    description="Use SSL for the connection. Enabled by default; see --sslmode.",
    default=True,
)

sslmode = SelectOption(
    name="sslmode",
    description=(
        "The SSL mode used when --ssl is enabled. 'verify-ca' checks the server "
        "certificate against Amazon's bundled CA certificates."
    ),
    choices=["verify-ca", "verify-full"],
    default="verify-ca",
)

ssl_insecure = FlagOption(
    name="ssl_insecure",
    description=(
        "Skip verification of the server certificate. Off by default; only use "
        "this against an endpoint you control, e.g. a local SSH tunnel."
    ),
)

# -- Catalog / metadata -------------------------------------------------------

all_databases = FlagOption(
    name="all_databases",
    description=(
        "Show every database the cluster exposes metadata for, including "
        "datashare databases, instead of only the connected one. This asks the "
        "server for cross-database catalog metadata, which is answered by the "
        "SVV_ALL_* views and is markedly slower; some clusters also cannot "
        "serve it, in which case the adapter falls back to direct catalog "
        "queries. Off by default."
    ),
)

enable_table_types = FlagOption(
    name="enable_table_types",
    description=(
        "Report detailed relation types (EXTERNAL TABLE, MATERIALIZED VIEW, ...) "
        "from the server. On by default; turning this off collapses them to "
        "TABLE and VIEW."
    ),
    default=True,
)

# -- IAM / identity -----------------------------------------------------------

iam = FlagOption(
    name="iam",
    description=(
        "Authenticate with IAM credentials instead of a database password. "
        "Requires --cluster-identifier and --region (or --is-serverless with "
        "--serverless-work-group), plus credentials from --profile, the "
        "--access-key-id/--secret-access-key pair, or the environment."
    ),
)

profile = TextOption(
    name="profile",
    description="The name of a profile in the AWS credentials or config file.",
)

region = TextOption(
    name="region",
    description="The AWS region the Redshift cluster or workgroup is in.",
)

cluster_identifier = TextOption(
    name="cluster_identifier",
    description="The identifier of the provisioned Redshift cluster.",
)

access_key_id = TextOption(
    name="access_key_id",
    description="The AWS access key id used for IAM authentication.",
    secret=True,
)

secret_access_key = TextOption(
    name="secret_access_key",
    description="The AWS secret access key used for IAM authentication.",
    secret=True,
)

session_token = TextOption(
    name="session_token",
    description="The AWS session token used with temporary IAM credentials.",
    secret=True,
)

db_user = TextOption(
    name="db_user",
    description=(
        "The Redshift database user that IAM authentication creates credentials "
        "for. Used with --iam."
    ),
)

auto_create = FlagOption(
    name="auto_create",
    description=(
        "Create the database user named by --db-user if it does not already "
        "exist. Used with --iam."
    ),
)

db_groups = ListOption(
    name="db_groups",
    description=(
        "A database group the IAM user joins for the duration of the session. "
        "Repeat the option to join more than one group."
    ),
)

# -- Redshift Serverless ------------------------------------------------------

is_serverless = FlagOption(
    name="is_serverless",
    description="Connect to a Redshift Serverless workgroup instead of a cluster.",
)

serverless_acct_id = TextOption(
    name="serverless_acct_id",
    description="The AWS account id that owns the Redshift Serverless workgroup.",
)

serverless_work_group = TextOption(
    name="serverless_work_group",
    description="The name of the Redshift Serverless workgroup.",
)

# -- Federated identity -------------------------------------------------------

credentials_provider = TextOption(
    name="credentials_provider",
    description=(
        "The fully-qualified name of an identity provider plugin, e.g. "
        "AzureCredentialsProvider, OktaCredentialsProvider, "
        "BrowserSamlCredentialsProvider, or BrowserAzureCredentialsProvider."
    ),
)

idp_host = TextOption(
    name="idp_host",
    description="The hostname of the identity provider used for federated login.",
)

login_url = TextOption(
    name="login_url",
    description="The SSO login URL used by the browser-based credentials providers.",
)

preferred_role = TextOption(
    name="preferred_role",
    description=(
        "The ARN of the IAM role to assume from the identity provider's assertion."
    ),
)

role_arn = TextOption(
    name="role_arn",
    description=(
        "The ARN of the IAM role to assume for JWT or web identity authentication."
    ),
)

web_identity_token = TextOption(
    name="web_identity_token",
    description=(
        "The OIDC/JWT token used with --role-arn for web identity authentication."
    ),
    secret=True,
)

auth_profile = TextOption(
    name="auth_profile",
    description=(
        "The name of a Redshift authentication profile holding connection settings."
    ),
)

endpoint_url = TextOption(
    name="endpoint_url",
    description=(
        "An override for the Redshift API endpoint, for testing or private endpoints."
    ),
)

group_federation = FlagOption(
    name="group_federation",
    description=(
        "Use Redshift IdP groups when authenticating a federated user against a "
        "provisioned cluster."
    ),
)

REDSHIFT_OPTIONS = [
    host,
    port,
    database,
    user,
    password,
    timeout,
    application_name,
    max_prepared_statements,
    client_protocol_version,
    numeric_to_float,
    tcp_keepalive,
    ssl,
    sslmode,
    ssl_insecure,
    all_databases,
    enable_table_types,
    iam,
    profile,
    region,
    cluster_identifier,
    access_key_id,
    secret_access_key,
    session_token,
    db_user,
    auto_create,
    db_groups,
    is_serverless,
    serverless_acct_id,
    serverless_work_group,
    credentials_provider,
    idp_host,
    login_url,
    preferred_role,
    role_arn,
    web_identity_token,
    auth_profile,
    endpoint_url,
    group_federation,
]
