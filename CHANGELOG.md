# harlequin-redshift CHANGELOG

All notable changes to this project will be documented in this file.

## [Unreleased]

- Initial release: a Harlequin adapter for Amazon Redshift, built on
  `redshift_connector`.
- Lazy-loading data catalog (database → schema → relation → column) read
  through the driver's metadata calls, so it spans datashare databases and
  external schemas where the cluster exposes them.
- Catalog interactions, including `SHOW TABLE` / `SHOW VIEW` DDL, the
  `SVV_TABLE_INFO` and `SVV_MV_INFO` tuning views, and Redshift's distribution
  key, sort key, and column encoding.
- `search_catalog()` across databases, schemas, relations, and columns.
- Query cancellation with Redshift's `CANCEL`.
- Read-only mode, verified against the server before connecting.
- Auto and Manual transaction modes.
- IAM, Redshift Serverless, and federated identity provider authentication.
- `--all-databases` to extend the catalog to datashare and other databases;
  off by default, since cross-database metadata is served by the slower
  `SVV_ALL_*` views.

### Driver workarounds

- The connect timeout is no longer also a ceiling on how long a query may run:
  `redshift_connector` sets a socket timeout while connecting and never clears
  it, so a query that ran longer than `--timeout` died with "The read operation
  timed out". The socket timeout is cleared once the connection is up.
- When the driver's server-side metadata path mis-indexes its own results
  (`KeyError: 'database_name'` from `get_catalog_list()`, seen with
  cross-database catalog metadata enabled), the catalog falls back to the
  driver's direct catalog queries for the rest of the session instead of
  failing to load. Server errors are still reported.
- Catalog search and autocomplete use single catalog queries rather than the
  driver's unqualified metadata calls, which walk the cluster one `SHOW` at a
  time -- one `SHOW COLUMNS` per table for a column search, one
  `SHOW FUNCTIONS` per schema for function names.
- `CANCEL` is only sent while a statement is actually running: it names a
  session rather than a statement, so one sent while the session is idle would
  land on whatever it ran next.
