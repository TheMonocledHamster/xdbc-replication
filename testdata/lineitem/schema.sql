CREATE TABLE IF NOT EXISTS lineitem_sf10
(
    l_orderkey      bigint  NOT NULL,
    l_partkey       integer,
    l_suppkey       integer,
    l_linenumber    integer NOT NULL,
    l_quantity      numeric(12, 2),
    l_extendedprice numeric(12, 2),
    l_discount      numeric(12, 2),
    l_tax           numeric(12, 2),
    l_returnflag    character(1),
    l_linestatus    character(1),
    l_shipdate      date,
    l_commitdate    date,
    l_receiptdate   date,
    l_shipinstruct  character(25),
    l_shipmode      character(10),
    l_comment       character varying(44)
);