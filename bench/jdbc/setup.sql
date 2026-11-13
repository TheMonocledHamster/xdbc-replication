-- JDBC baseline setup on target-db, mirroring pg_xdbc_fdw/experiments/setup_fdw.sql:37-50
-- (same driver, querytimeout 1200, one foreign table per dataset in schema "jdbc").
CREATE EXTENSION IF NOT EXISTS jdbc_fdw;
DROP SERVER IF EXISTS postgres_jdbc_server CASCADE;
CREATE SERVER postgres_jdbc_server FOREIGN DATA WRAPPER jdbc_fdw OPTIONS (
  drivername 'org.postgresql.Driver',
  url 'jdbc:postgresql://source-db:5432/xdbc_test',
  querytimeout '1200',
  jarfile '/opt/jdbc/postgresql-42.7.4.jar');
CREATE USER MAPPING FOR CURRENT_USER SERVER postgres_jdbc_server
  OPTIONS (username 'postgres', password 'postgres');
CREATE SCHEMA IF NOT EXISTS jdbc;

-- Foreign tables copy the column list of the local target table (created by `tools load`).
DO $$
DECLARE t text; cols text;
BEGIN
  FOREACH t IN ARRAY ARRAY['iotm','inputeventsm','ss13husallm','lineitem_sf10'] LOOP
    IF to_regclass('public.' || t) IS NULL THEN CONTINUE; END IF;
    SELECT string_agg(format('%I %s', attname, format_type(atttypid, atttypmod)), ', ' ORDER BY attnum)
      INTO cols FROM pg_attribute
      WHERE attrelid = ('public.' || t)::regclass AND attnum > 0 AND NOT attisdropped;
    EXECUTE format('DROP FOREIGN TABLE IF EXISTS jdbc.%I', t);
    EXECUTE format('CREATE FOREIGN TABLE jdbc.%I (%s) SERVER postgres_jdbc_server OPTIONS (table_name %L)', t, cols, t);
  END LOOP;
END $$;
