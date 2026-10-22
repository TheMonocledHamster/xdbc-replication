# IoT fixture

`sample.csv` contains the header and first 128 records from the authors' `iotm.csv`: 85 columns of non-NULL integers, doubles, and strings. Record order and values are preserved; CSV quoting and line endings were normalized with Python's CSV writer.

| File | Purpose |
|---|---|
| `sample.csv` | Small real-data input for local checks and CI |
| `schema.sql` | PostgreSQL table definition used on both sides |
| `transfer.json` | Ordered row schema and runtime settings |

`make seed` creates the tables if needed, clears both tables, and imports the source through PostgreSQL CSV COPY. `make e2e` requires exactly 128 fixture records and compares all columns independently of the XDBC codec. See the [project README](../../README.md) for commands.

## Provenance

The schema comes from the authors' [artifact repository at revision b5092474d1bb9f8dda139d97ca2941507a78e321](https://github.com/polydbms/xdbc-sigmod-ari/tree/b5092474d1bb9f8dda139d97ca2941507a78e321):

- DDL: `sql_scripts/create_iot.sql`, copied unchanged.
- Fields: `schemas/iotm.json`, with column names lowercased to match the PostgreSQL DDL. Runtime settings were added for this harness.
- Data: [the authors' IoT archive](https://tubcloud.tu-berlin.de/s/M3aeptL8R5ekWSD/download?path=%2F&files=iotm.csv.tar.gz), sampled by streaming on 2026-10-22.

The full archive is not checked in or downloaded by normal runs. This sample is for correctness checks, not performance measurement.

Verify the checked-in CSV from the repository root:

```sh
sha256sum testdata/iot/sample.csv
```

Expected SHA-256:

```text
b063b67f8f5ae3eff2c473f0bc92b84bb0575a1e4e870af4c52fbc27ab341633
```
