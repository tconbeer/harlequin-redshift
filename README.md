# harlequin-redshift

A [Harlequin](https://harlequin.sh) adapter for Amazon Redshift, built on
[`redshift_connector`](https://github.com/aws/amazon-redshift-python-driver),
Amazon's own Python driver.

Harlequin's Postgres adapter uses `psycopg`, which
[cannot talk to Redshift](https://github.com/tconbeer/harlequin-postgres/issues/43).
This adapter uses the official driver instead, and leans on what that driver and
the server offer: cross-database catalog metadata, `CANCEL`, `SHOW TABLE` /
`SHOW VIEW` DDL, the `SVV_*` tuning views, IAM and Redshift Serverless
authentication, and federated identity providers.

## Installation

Install `harlequin-redshift` into the same environment as `harlequin`:

```bash
uv tool install 'harlequin[redshift]'
```

Or, from a checkout of this repository:

```bash
uv tool install harlequin --with .
```

## Connecting

Pass a connection string with `-a redshift`:

```bash
harlequin -a redshift "redshift://my-user:my-pass@my-cluster.abc123.us-east-1.redshift.amazonaws.com:5439/dev"
```

A connection string may be a URL, with a `redshift://`, `postgres://`, or
`postgresql://` scheme, or a libpq-style keyword string:

```bash
harlequin -a redshift "host=localhost port=5439 dbname=dev user=awsuser"
```

You can also pass all or part of it as separate options. This is equivalent to
the URL above:

```bash
harlequin -a redshift -h my-cluster.abc123.us-east-1.redshift.amazonaws.com \
    -p 5439 -d dev -u my-user --password my-pass
```

Options set on the command line, in a profile, or in the environment override
the same setting in the connection string. Extra driver options can also ride
along in a URL's query string:

```bash
harlequin -a redshift "redshift://my-cluster:5439/dev?iam=true&region=us-east-1"
```

For every option and its description, run `harlequin --help`.

### IAM authentication

```bash
harlequin -a redshift --iam --cluster-identifier my-cluster --region us-east-1 \
    --db-user analyst -d dev
```

Credentials come from `--profile`, from `--access-key-id` / `--secret-access-key`
(plus `--session-token`), or from the environment, in the driver's usual order.
Add `--auto-create` to create `--db-user` if it does not exist, and `--db-groups`
to join groups for the session.

### Redshift Serverless

```bash
harlequin -a redshift --iam --is-serverless \
    --serverless-work-group my-workgroup --region us-east-1 -d dev
```

### Federated identity providers

Set `--credentials-provider` to a plugin the driver ships, such as
`AzureCredentialsProvider`, `OktaCredentialsProvider`,
`BrowserSamlCredentialsProvider`, or `BrowserAzureCredentialsProvider`, along
with that plugin's options (`--idp-host`, `--login-url`, `--preferred-role`, and
so on).

## Data catalog

The catalog is four levels: database, schema, relation, column. Each level is
loaded only when you open the one above it, so a cluster with thousands of
relations costs nothing until you go looking for one.

Every level is read through the driver's own metadata calls, so a cluster
answers with whichever path it supports: server-side `SHOW` discovery on current
clusters, the cross-database `SVV_ALL_*` views, or the driver's legacy
`pg_catalog` queries on older ones. That means datashare databases and external
(Spectrum) schemas appear in the tree wherever the cluster exposes them.

By default the catalog shows the connected database. Pass `--all-databases` to
show every database the cluster exposes metadata for, including the ones a
datashare brings in.

That flag is off by default because it is not free. It asks the server for
cross-database catalog metadata, which is answered by the `SVV_ALL_*` views and
is markedly slower, and on some clusters the driver's server-side metadata path
cannot serve it at all (see [Driver notes](#driver-notes)). With it off, the
catalog is read through the fast path and every level is a single round trip.

Relations in another database are given three-part query names, which is how
Redshift's cross-database queries address them; relations in the connected
database get two-part names.

System schemas (`pg_*` and `information_schema`) are not shown.

### Interactions

Right-click (or press the context-menu key on) a catalog item:

| Item | Actions |
| --- | --- |
| Database | List Schemas, List Relations, Show Storage Summary, Drop Database |
| Schema | Set Search Path, List Relations, Show Storage Summary, Drop Schema |
| Table | Insert Columns at Cursor, Preview Data, Describe Columns, Show DDL (`SHOW TABLE`), Describe Design (dist key, sort key, encoding), Show Table Info (`SVV_TABLE_INFO`), Describe Constraints, Drop Table |
| View | ... plus Show DDL (`SHOW VIEW`), Drop View |
| Materialized view | ... plus Show DDL, Show Refresh Info (`SVV_MV_INFO`), Drop Materialized View |
| External table | ... plus Show DDL (`SHOW EXTERNAL TABLE`), Show Location & Format, Drop External Table |

Most of these write SQL into a new buffer rather than running it, so you see
what will hit the cluster before it does. The `Show DDL` actions run their
`SHOW` statement, because the DDL is what they return. The drops go through
Harlequin's confirmation modal.

## Catalog search

This adapter implements `search_catalog()`, so you can find an object without
walking the catalog a level at a time:

```bash
hsql -a redshift "redshift://my-cluster:5439/dev" --catalog-search orders
```

A term matches a database, schema, relation, or column whose name contains it.
Each level is matched with the same metadata call that builds it in the tree, so
a result is the item you would have reached by opening nodes, and it can be used
the same way.

Schemas, relations, and columns come from the connected database. The other
databases on the cluster are matched by name, which is all the catalog's top
level shows for them: searching every database's columns means a cross-database
scan of `SVV_ALL_COLUMNS`, which does not finish quickly enough to sit behind a
search box.

Redshift folds unquoted identifiers to lower case unless the cluster sets
`enable_case_sensitive_identifier`, and the server matches these names with
`LIKE`, which is case-sensitive. A search therefore tries both the term as typed
and its lower-cased form. On a cluster that does use case-sensitive identifiers,
a term must match the stored case.

## Cancelling a query

Press <kbd>ctrl</kbd>+<kbd>c</kbd> while a query is running. The adapter sends
Redshift's `CANCEL <pid>` statement on a second connection, and the cancelled
query returns no result instead of raising an error.

## Read-only mode

```bash
harlequin --read-only -a redshift "redshift://my-cluster:5439/dev"
```

The adapter asks the server for a session-wide read-only default first, and
confirms the server reports it as on. If the server has no such setting, it
opens every transaction with `BEGIN READ ONLY` instead, and confirms the server
reports `transaction_read_only` as on inside one. If neither holds, Harlequin
refuses to start rather than hand back a connection that would happily write.

Read-only mode applies to both Auto and Manual transaction modes.

## Driver notes

Two `redshift_connector` behaviors this adapter works around, both of which you
may want to know about if you use the driver directly:

**The connect timeout is also a read timeout.** The driver sets a timeout on the
socket while connecting and never clears it, so `timeout` becomes a ceiling on
how long *any* read may block -- a query that runs longer than it dies with
"The read operation timed out". This adapter clears the socket timeout once the
connection is up, so `--timeout` means what it says: a limit on connecting, not
on your queries. TCP keepalives, which are on by default, are what notice a peer
that has actually gone away.

**The server-side metadata path can mis-index its own results.** For its
`SHOW`-based metadata calls the driver caches a column-name-to-index map on the
cursor, builds it once from whatever result set is current, then looks names up
in it. On a cluster with cross-database catalog metadata enabled, this raises
`KeyError: 'database_name'` from `get_catalog_list()`. When that happens, this
adapter logs it, stops using that path for the rest of the session, and reads
the catalog with the driver's direct catalog queries instead -- so the catalog
loads either way. A genuine server error, such as a permission failure, is
reported rather than worked around.

Note also that the driver answers an *unqualified* metadata call by walking the
cluster one `SHOW` at a time: `get_columns()` with only a column pattern issues
one `SHOW COLUMNS` for every table in every schema of every database, and
`get_functions()` one `SHOW FUNCTIONS` per schema. This adapter uses single
catalog queries wherever a call would otherwise fan out that way.

## Transaction modes

Click the `Tx:` label in the Run Query Bar to switch between `Auto` and
`Manual`. In `Manual`, one transaction stays open across statements and
Harlequin shows Commit and Rollback buttons.

## Autocomplete

Beyond the catalog objects Harlequin completes on its own, this adapter provides
Redshift's reserved and non-reserved keywords, and the functions and stored
procedures the connected cluster reports.

## Development

```bash
make init      # uv sync
make check     # format, lint, typecheck, test
make test
```

The tests that need a cluster read only, and are skipped when no cluster is
configured. To run them, define a Harlequin profile named `redshift-test`:

```toml
# .harlequin.toml
[profiles.redshift-test]
adapter = "redshift"
host = "localhost"
port = 15439
database = "dev"
user = "..."
password = "..."
```

Or set `HARLEQUIN_REDSHIFT_TEST_DSN` to a connection string.
`HARLEQUIN_REDSHIFT_TEST_PROFILE` and `HARLEQUIN_REDSHIFT_TEST_CONFIG` override
the profile name and config path.
