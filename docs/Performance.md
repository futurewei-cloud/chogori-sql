# TPC-C Benchmark

## Description

The Chogori cluster was configured to use RDMA and only one Chogori server core was used. We used the Java TPC-C client from the open-source oltpbench project, and configured it to load five TPC-C warehouses. Minor, functionally-equivalent changes were needed for compatibilty with the oltpbench TPC-C benchmark. Namely, converting function updates (e.g. SET COUNT = COUNT + 1) into explicity read-modify-write operations, and handling conflict aborts correctly.

## Single server core throughput

Running with six parallel client connections.

All TPC-C transactions types (NewOrder, Payment, OrderStatus, Delivery, StockLevel) in 45/43/4/4/4 
ratio. 

June 30, 2021: **66.9 transactions / second**, 1806.3 NewOrder transactions / minute (tpmC).
March 1, 2021: 62.6 transactions / second, 1690.5 NewOrder transactions / minute (tpmC).

## Competitor Comparison

The same hardware was used as in the Chogori SQL tests, and the same TPC-C scale and number of clients were used.

### Apache Ignite

Apache Ignite does not fully support TPC-C, modifications required were:

- Tables without primary key are not supported, removed history table
- Problems with decimal types, converted to floats
- "FOR UPDATE" clauses removed

It was configured with two backup servers and "LOG\_ONLY" WAL.

**147.32417110756072 requests/sec**

### YugabyteDB

Configured with one server.

**34.91646379590163 requests/sec**

### CockroachDB

Configured with three servers. Required auto-commit to be on for the load phase.

**180.1567748991625 requests/sec**

## Single client latency

Running with a single client connection and only one transaction type at a time. Results form March 1, 2021 initial release.

|Transaction|Median latency (ms)|95th perc. (ms)|99th perc. (ms)|
|---|---|---|---|
|NewOrder|20.5|28|29|
|Payment|5.2|6|7|
|OrderStatus|1.79|2.5|3.7|
|Delivery|8.3|9.6|11.7|
|StockLevel|1276|1304|1320|

