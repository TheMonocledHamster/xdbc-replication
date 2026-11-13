# Fig. 9b benchmark

Times how fast our Go XDBC, PostgreSQL's built-in `postgres_fdw` and (optionally) JDBC copy a table
between two PostgreSQL databases (Fig. 9b of the XDBC paper). Runs entirely in Docker.

## Setup

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Mac, Windows) or Docker Engine with Compose (Linux), and Git. Start Docker.
2. Get the code and run:
   ```sh
   git clone https://github.com/TheMonocledHamster/xdbc-replication.git
   cd xdbc-replication
   docker compose run --build --rm bench
   ```
3. Read the summary table at the end, or drop `bench-results/<run id>.jsonl` on
   <https://claude.ai/artifact/6d7yED3FfpTFSj7dHxYGBa> to see it as the Fig. 9b chart.

The first run builds the containers and downloads the dataset (iot, ~3 GB); a full iot run takes about 15 minutes.

That runs ours and native. To add JDBC, put one of these before `bench`:
- `-e JDBC=stream`: fetches 10,000 rows at a time. Works on any machine, but isn't how the original ran it.
- `-e JDBC=faithful`: the whole table at once, exactly like the original. Needs a 32 GB+ machine.

---

## Details

### Machine needs

- **Disk:** ~15 GB free per dataset, ~60 GB for all four.
- **Memory:** 8 GB+ RAM; on Docker Desktop give Docker at least 8 GB (Settings → Resources).
  JDBC done the original way needs much more (see [JDBC needs a lot of memory](#jdbc-needs-a-lot-of-memory)).
- **Stay awake:** keep the machine from sleeping on long runs (Mac: `caffeinate -i docker compose run ...`;
  Windows: Settings → Power → never sleep while plugged in).

### Options

Add options with `-e` (same in bash, zsh and PowerShell):

```sh
docker compose run --build --rm -e DATASETS=all -e JDBC=faithful -e MACHINE="xl170 64GB" bench
```

| Option | Default | Meaning |
|---|---|---|
| `DATASETS` | `iot` | `lineitem`, `acs`, `iot`, `icu`, several separated by spaces or commas, or `all` |
| `JDBC` | `off` | `faithful`: jdbc_fdw exactly like the original (loads the whole table into Java memory). `stream`: fetches 10,000 rows at a time, works on small machines, **not** faithful |
| `ROUNDS` | `3` | Rounds per system and sink |
| `SINKS` | `pg,csv` | `pg` (into a target table), `csv` (into a CSV file), or both |
| `MACHINE` | empty | A label for your machine, shown on the chart |
| `JDBC_HEAP_MB` | Java's default (¼ of RAM) | Java memory cap for JDBC, e.g. `16000` |
| `QUERY_TIMEOUT` | `20m` | Limit for each native/JDBC query; slower runs are recorded as "timeout" |
| `BENCH_DATA` | Docker volume | A folder with existing `<dataset>.csv` files, to skip the download |

Rough times on an 8-core laptop: iot ≈ 15 min, `all` ≈ 1.5–2 h (lineitem is 60M rows).

### Results

- The run prints one line per timed run (`round 1  native PG -> PG  17.96 s  1476313 rows  ok`),
  the SHA-256 check of our copy, and a table of medians.
- `bench-results/<run id>.jsonl` has one line per run: dataset, system, sink, round, seconds,
  rows, whether all rows arrived, plus the machine label, CPUs and memory Docker can see.

### Troubleshooting

- *Cannot connect to the Docker daemon:* Docker isn't running; open Docker Desktop.
- *A run stopped partway:* the machine probably slept, or ran out of memory (JDBC).
- *Start fresh:* `docker compose --profile bench down -v` deletes the benchmark's databases and downloaded data.

### What runs, and how it matches the original

Four containers on one machine: `bench-source` and `bench-target` (PostgreSQL 17, separate
from the dev stack's databases) and `bench`, which starts our server and both clients as
processes and drives the timing.

| System | What is timed | Original setup it mirrors |
|---|---|---|
| Ours (our Go XDBC) | `POST /transfer` on the PG client and the CSV client | — |
| Native (`postgres_fdw`, default options, 100 rows per fetch) | `INSERT INTO t SELECT * FROM native.t` and `COPY (SELECT * FROM native.t) TO '/tmp/bench.out' WITH CSV HEADER` | [`setup_fdw.sql` L19–27](https://github.com/polydbms/pg_xdbc_fdw/blob/45c70e529742f339e090d8a52b14ef71e34d5c54/experiments/setup_fdw.sql#L19-L27), [timed query L156–159](https://github.com/polydbms/xdbc-sigmod-ari/blob/b5092474d1bb9f8dda139d97ca2941507a78e321/experiment_files/run_experiments_for_datasets.sh#L156-L159) |
| JDBC (`jdbc_fdw` + PostgreSQL JDBC 42.7.4, `querytimeout 1200`) | the same two queries on `jdbc.t` | [`Dockerfile_client` L96–130](https://github.com/polydbms/pg_xdbc_fdw/blob/45c70e529742f339e090d8a52b14ef71e34d5c54/docker/Dockerfile_client#L96-L130), [`setup_fdw.sql` L37–50](https://github.com/polydbms/pg_xdbc_fdw/blob/45c70e529742f339e090d8a52b14ef71e34d5c54/experiments/setup_fdw.sql#L37-L50) |

### JDBC needs a lot of memory

`jdbc_fdw` never asks the driver to fetch in chunks, so the driver loads the whole table
into Java memory before returning a row. Java's default cap is ¼ of RAM. Rough needs:
icu ~8 GB, acs ~10 GB, iot ~13 GB, lineitem ~30 GB. So `JDBC=faithful` needs a 32 GB+
machine for icu/acs/iot (with `JDBC_HEAP_MB=16000`) and ~64 GB for lineitem. On a 16 GB
laptop it fails (iot crashed after 49 s, acs failed after 259 s).

`JDBC=stream` adds `defaultRowFetchSize=10000` to the JDBC URL; `jdbc/fetch-size.patch`
turns autocommit off only in that case, because the driver ignores a fetch size in
autocommit mode. With `JDBC=faithful` the patch does nothing.

The original script never runs JDBC on lineitem: it records 0
([L124–133](https://github.com/polydbms/xdbc-sigmod-ari/blob/b5092474d1bb9f8dda139d97ca2941507a78e321/experiment_files/run_experiments_for_datasets.sh#L124-L133)),
and the plotting script turns 0 into the "Timeout after 20m" label. Their older results
show a real attempt that hit a 1-hour timeout.

### Deviations from the original

- PostgreSQL 17 instead of 13 (native times were the same on both in our tests), JDK 21 instead of 8.
- Everything on one machine. The authors' cluster had a network between the databases;
  see `docs/fig9b-investigation.md` for why that matters for native.
