# IoT fixture

The sample has 128 data records, IDs 0 through 127, and 85 columns. Its header uses actual PostgreSQL column names. The full file at `data/iot.csv` (archive member `iotm.csv`) is headerless, so its first data row must not be skipped.

The configuration and DDL match the authors' prepared IoT workload. `dataset.json` records the pinned source archive, sample checksum, and full-file loading settings.

See the [dataset guide](../README.md) for provenance and formats, and the [project README](../../README.md) for sample and full-file commands.
