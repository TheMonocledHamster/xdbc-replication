# CSV sink

`internal/target` provides a CSV writer alongside its PostgreSQL writer. The
CSV writer consumes the same fixed-width row payloads and uses the same scalar
decoder as the PostgreSQL writer. It supports every V1 row scalar type and
NULL. CSV uses PostgreSQL's default `COPY` convention: NULL is an unquoted
empty field, while an empty non-NULL string is written as `""`. Fields
containing commas, quotes, or line breaks are quoted and escaped.

The sink is selected using `SINK_TYPE=csv` and requires
`CSV_OUTPUT_PATH`. Data arrives after the normal receive and optional
decompression stages; the CSV writer validates each batch and releases its
buffer after use.

For local Compose runs, the client writes to `/exports/<dataset>.csv`, mounted
as `exports/<dataset>.csv` in the repository. `make verify SINK_TYPE=csv`
loads this file into the harness target table and uses the existing row-count
and checksum comparison to validate the transfer.
