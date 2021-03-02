# TPC-C Benchmark

## Description

Performance as of initial internal release on 3/1/2021. The Chogori cluster was configured to use 
RDMA and only one Chogori server core was used. We used the Java TPC-C client from chogori-oltpbench, 
and configured it to load five TPC-C warehouses.

## Single server core throughput

Running with six parallel client connections.
All TPC-C transactions types (NewOrder, Payment, OrderStatus, Delivery, StockLevel) in 45/43/4/4/4 
ratio: 62.6 transactions / second, 1690.5 NewOrder transactions / minute (tpmC).

## Single client latency

Running with a single client connection and only one transaction type at a time.

|Transaction|Median latency (ms)|95th perc. (ms)|99th perc. (ms)|
|---|---|---|---|
|NewOrder|20.5|28|29|
|Payment|5.2|6|7|
|OrderStatus|1.79|2.5|3.7|
|Delivery|8.3|9.6|11.7|
|StockLevel|1276|1304|1320|

