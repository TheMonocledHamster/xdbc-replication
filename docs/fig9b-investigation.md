# Fig. 9b investigation

Fig. 9b of the XDBC paper (SIGMOD 2025) times how long it takes to copy a large table
from one PostgreSQL database to another. We reproduced it with our Go version of XDBC
and found two places where our numbers don't match the paper:

1. **Native is fast for us and very slow in the paper** (the big one). The likely cause is
   network delay on the authors' cluster, not anything in our code or theirs.
2. **JDBC can't run the original way on a 16 GB laptop** (minor). It needs a machine with
   32 GB+ of RAM.

To rerun everything: `docker compose run --build --rm bench` (see [`bench/README.md`](../bench/README.md)).

## Who's who in the chart

| Name | What it is | Who wrote it |
|---|---|---|
| **Ours** (xdbc) | Our Go version of XDBC, the system the paper proposes | Our team |
| **Native** | PostgreSQL's built-in way to read another Postgres database (`postgres_fdw`), with no XDBC involved. The baseline XDBC is supposed to beat | The PostgreSQL project |
| **JDBC** | A Java connector (`jdbc_fdw`) | pgspider |

All three ran on our own machine, set up the same way as the authors' benchmark.
"PG → PG" means copying into a table in the target database; "PG → CSV" means copying into
a CSV file, which is what the paper's benchmark actually measured.

## 1. Native: fast for us, very slow in the paper

| iot (5.8M rows) | Paper | Ours (Brad's M5) |
|---|---|---|
| xdbc | ~48 s | 45.1 s (PG → CSV), 25.6 s (PG → PG) |
| native | **~258 s** | **44.4 s** (PG → CSV), 40.5 s (PG → PG) |

XDBC matches the paper. Native is about 6× faster for us.

**In plain terms:** native carries the table over in small loads, 100 rows per trip, so
iot takes 58,331 trips. Each trip is a request and a reply. On one laptop a trip is nearly
instant. On a cluster, where the two databases are on different machines, every trip
crosses a network and waits a few milliseconds. 58,331 short waits add up to minutes. XDBC
sends big buffers without waiting for a reply each time, so network delay barely affects it.

**How we got there:**

1. **Same configuration.** The authors' native setup is plain `postgres_fdw` with default
   options ([`setup_fdw.sql` L19–27](https://github.com/polydbms/pg_xdbc_fdw/blob/45c70e529742f339e090d8a52b14ef71e34d5c54/experiments/setup_fdw.sql#L19-L27)).
   Ours is the same: no `fetch_size`, so both use the default of 100 rows per trip.
2. **Their own code is fast on our machine too.** We built and ran the authors' artifact
   (their C++ XDBC and their benchmark containers) on the M5. iot PG → CSV: xdbc 24.2 s,
   native **47.3 s**, not 258 s. (It took 9 build-only patches to compile on ARM; no
   behavior changes.)
3. **Not the PostgreSQL version.** The paper used PG13; we use PG17. Native took 52.8 s on
   PG13 and 53.7 s on PG17.
4. **Adding network delay reproduces their number.** We added 3.5 ms of artificial delay
   between the databases (Linux `tc netem`). Native went from 50 s to **387 s**; XDBC
   stayed at about 45 s. Drawing a line between those points, about 2 ms of delay gives
   their 258 s (estimated, not measured directly).
5. **The authors didn't add delay on purpose.** Their experiment scheduler has a delay
   setting, but it is commented out and set to 0
   (`xdbc-client/experiments/experiment_scheduler/job_runner.py` L44–55,
   `configuration.py` L23), and the Fig. 9b script doesn't shape the network at all. So
   any delay came from their real cluster network.

**Caveat:** the paper doesn't report its network latency, so this is our best explanation,
consistent with every test we ran, but not confirmed from their side.

**Next step:** run on two CloudLab machines (real network between the databases), plus a
latency sweep (0, 0.5, 1, 2, 5 ms) to show native's time climbing while XDBC stays flat.

## 2. JDBC needs a machine with 32 GB+ of RAM

The original JDBC setup (`jdbc_fdw` + PostgreSQL JDBC 42.7.4) loads the **entire table into
Java memory** before writing a single row; `jdbc_fdw` never asks the driver to fetch in
chunks. Java's default memory cap is ¼ of RAM, about 3 GB on a 16 GB laptop. On Brad's M5:
iot crashed after 49 s, acs failed after 259 s.

Rough memory needs: icu ~8 GB, acs ~10 GB, iot ~13 GB, lineitem ~30 GB. So a **32 GB+
machine** can run icu/acs/iot the original way, and lineitem needs ~64 GB. A CloudLab xl170
(64 GB) should run all four:

```sh
docker compose run --build --rm -e DATASETS=all -e JDBC=faithful -e JDBC_HEAP_MB=40000 bench
```

Meanwhile we measured JDBC with one change: fetching 10,000 rows at a time
(`-e JDBC=stream`). It works on the laptop but is **not** faithful, and it's likely faster
than the original would be. One run each, Brad's M5:

| | lineitem | acs | iot | icu |
|---|---|---|---|---|
| JDBC PG → PG | 404.3 s | 71.4 s | 136.1 s | 80.7 s |
| JDBC PG → CSV | 347.5 s | 61.4 s | 122.4 s | 59.9 s |
| Paper JDBC | timeout | ~235 s | ~435 s | ~210 s |

**About lineitem:** the authors' final script never runs JDBC on lineitem. It records 0
([L124–133](https://github.com/polydbms/xdbc-sigmod-ari/blob/b5092474d1bb9f8dda139d97ca2941507a78e321/experiment_files/run_experiments_for_datasets.sh#L124-L133)),
and the plotting script turns that 0 into the "Timeout after 20m" label. Their older
results (`pg_xdbc_fdw/experiment_results4`, October 2024) do show a real attempt that hit a
1-hour timeout, and lineitem at 1/10 the size took ~341 s, so a timeout is believable.

## The investigation path

1. **Spike on iot.** Our first PG → PG run took 76.7 s, slower than native. Per-step timing
   showed the writer was the bottleneck: it committed every batch separately (54,011
   commits). PR #47 (one transaction + binary COPY) fixed it: **81.5 s → 27.2 s** in the
   same session.
2. **The native mystery.** Native took ~40 s for us but 258 s in the paper. We checked the
   hardware, the PostgreSQL version and the configuration, then ran the authors' own
   artifact on the M5 (native 47.3 s), and finally added network delay (387 s at 3.5 ms).
   See section 1.
3. **All four datasets** with PR #47, 3 rounds each, every run moving the full row count and
   our copies matching the source by SHA-256.
4. **JDBC** set up like the original ran out of memory; the 10,000-rows-per-fetch version
   works. See section 2.
5. **One command.** Everything above now runs with `docker compose run --build --rm bench`
   on any machine with Docker.

## Results on Brad's M5

Apple M5, 16 GB RAM (12 GB to Docker via Colima, 8 CPUs), PostgreSQL 17, PR #47 build,
plugged in. Median of 3 rounds, seconds. Bold = faster of ours and native.

| Dataset | Ours PG → PG | Native PG → PG | Ours PG → CSV | Native PG → CSV | SHA-256 |
|---|---|---|---|---|---|
| lineitem (60M rows) | 274 | **241** | 238 | **203** | ✓ |
| acs (230 columns) | 20.3 | **14.5** | 20.1 | **16.2** | ✓ |
| iot (5.8M rows) | **25.6** | 40.5 | 45.1 | 44.4 | ✓ |
| icu (9.0M rows) | **16.4** | 26.0 | 34.6 | **25.6** | ✓ |

- Ours beats native PG → PG on iot and icu by about 1.6×, and loses on acs (230 columns)
  and lineitem (NUMERIC and DATE columns), where we convert values one at a time.
- On PG → CSV, ours never clearly wins, so the CSV sink is worth a look.
- Lineitem times rose each round (225 → 297 s for ours) while the laptop was low on memory;
  treat them as rough.
