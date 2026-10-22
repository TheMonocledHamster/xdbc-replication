# Paper datasets

Each directory contains `schema.sql`, `transfer.json`, `dataset.json`, and `sample.csv`. The sample has 128 data records plus a header using actual PostgreSQL column names. Normal local and CI checks use only these samples.

| Selection | Source table | Columns | Original archive member |
|---|---|---|---|
| `lineitem` | `lineitem_sf10` | 16 | `lineitem.tbl` |
| `acs` | `ss13husallm` | 230 | `ss13husallm.csv` |
| `iot` | `iotm` | 85 | `iotm.csv` |
| `icu` | `inputeventsm` | 26 | `inputeventsm.csv` |

## Provenance

Schemas and data locations come from the authors' [artifact at revision b5092474d1bb9f8dda139d97ca2941507a78e321](https://github.com/polydbms/xdbc-sigmod-ari/tree/b5092474d1bb9f8dda139d97ca2941507a78e321). Samples were extracted on 2026-10-22 from the beginnings of the corresponding files. IoT uses the existing local copy of that archive's CSV.

`dataset.json` records the archive URL, member name, full-file header setting, sample row count, and sample SHA-256. Schemas are copied from the artifact's `sql_scripts` directory. Row field names are lowercased to match the DDL.

The files represent the authors' prepared workloads, rather than arbitrary raw TPC-H, Census, IoT, or ICU downloads. Use the documented archives or provide files with the same column order and database-compatible values.

## Lineitem differences

The artifact's Lineitem JSON schema disagrees with its PostgreSQL DDL. Our configuration follows the DDL: BIGINT order keys, exact NUMERIC(12,2) values, and DATE columns. The numeric row slots are 16 bytes wide. This preserves values without silently narrowing or converting decimals through floating point; the intermediate layout therefore differs from the paper's simplified type summary.

The artifact's comma-delimited Lineitem CSV contains unquoted commas inside comments. We use the pipe-delimited `lineitem.tbl` instead. This archive has 16 fields per row with no trailing pipe. `make fetch DATASET=lineitem` writes standard CSV with correctly quoted comments. Unexpected extra data is rejected, not truncated.

## Loading and verification

All four full inputs are headerless. Checked-in samples have canonical column headers. `make seed` loads the sample; `make load` loads `/data/<dataset>.csv` using the full-file setting. The native Lineitem archive is converted by `make fetch`; supply the resulting CSV to `make load`, not a raw `.tbl` file.

`make verify` compares canonical CSV streams from both databases, ordered across all selected columns, with duplicates preserved. It reports row counts and a SHA-256 digest. `make export` writes that canonical sink CSV to `exports/<dataset>.csv`.

Full input, download, and export files are excluded from Git and Docker images. Archive downloads are explicit; the samples do not require network access. If an author's archive becomes unavailable, use an authorized local copy with `DATA_DIR` and `FILE`.
