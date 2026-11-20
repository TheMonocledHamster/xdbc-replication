# Benchmark target database: PostgreSQL 17 + jdbc_fdw, mirroring the original's JDBC baseline
# (xdbc-sigmod-ari/postgres/Dockerfile:46, pg_xdbc_fdw/docker/Dockerfile_client:96-130):
# pgspider/jdbc_fdw built with PGXS, libjvm linked into /usr/lib, PostgreSQL JDBC driver 42.7.4.
# Deviations: PG17 instead of PG13 and JDK 21 (JDK 8 isn't packaged for Debian trixie).
# fetch-size.patch is inert unless the jdbc server URL has defaultRowFetchSize (JDBC=stream).
FROM postgres:17
ARG JDBC_FDW_REF=02f245b99fe9d574ac4e74c3ebd5039f551bf9fd
RUN apt-get update && apt-get install -qy --no-install-recommends \
      openjdk-21-jdk-headless git make gcc libc6-dev curl ca-certificates postgresql-server-dev-17 \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(find /usr/lib/jvm -name libjvm.so | head -1)")/libjvm.so" /usr/lib/libjvm.so \
 && mkdir -p /opt/jdbc && curl -fsSL -o /opt/jdbc/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
 && git clone https://github.com/pgspider/jdbc_fdw.git /jdbc_fdw && cd /jdbc_fdw && git checkout "$JDBC_FDW_REF"
COPY fetch-size.patch /tmp/fetch-size.patch
RUN cd /jdbc_fdw && git apply /tmp/fetch-size.patch && make clean USE_PGXS=1 && make install USE_PGXS=1
