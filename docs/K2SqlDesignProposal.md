This document is a design proposal for the K2 SQL layer. It is a working in progress. 

# Introduction
## Assumptions 
K2 platform is an in-memory document storage system. We introduce a SQL layer on top of so that users could interactive with our system using SQLs.

The design is based on the following Assumptions:
* K2 Platform provides document APIs so that the SQL layers could access the table rows and columns with column projections and predicate pushdown
* For the first version of prototype, we will integrate postgres (PG) database engine for query parsing, planning, and execution. That is to say, the query
execution is on a single PG instance, which might not be ideal for complex queries, but should work for most OLTP queries. 
* However, we should make our design to be extensible to be able to run queries in a distributed fasion, i.e., a physical query plan could be split
into multiple segements to run on different nodes, which is useful for HTAP (Hybrid transaction/analytical processing) and OLAP in the future.
* Data persistency and consistency is guaranteed by the K2 storage layer
* Each table has a primary key and we need to specify one if it is not provided in schema definition. 
* Primary keys that are used to partition tables cannot be changed once they are defined. 

## Design Goals
We like to achieve the following goals for the first version of prototype.
* Support Catalog management by DDLs, i.e., create, update, and delete databasse and tables
* Support secondary indexes 
* Support select with column projection and predication pushdown. Aggregation pushdown could be implemented in later versions.
* Support insert, update, delete, and simple joins
* Allow users to submit queries using PG libpq interface. JDBC and ODBC drivers could be provided in later versions. 

# System Design Proposal 

This is a high level design. Details will be provided in separate sections.

## Architecture 

The system architecture is as follows and it consists of the following components.
* The storage layer provides document APIs so that we could storage data with data type information into it.
* On the top of the storage layer, we have SQL executor, which could run separately on different hosts or the same host of the storage layer.
* There is a SQL coordinator so that it could manage catalog, index, and data type updates on k2 storage nodes. 

![Architecture](./images/K2SqlSystemArchitecture01.png)

## SQL Coordinator 

The SQL coordinator has the following responsiblities
* Manage database and table schemas while user submits DDLs such as create, update, and delete tables.
* Manage secondary indexes for tables.
* Both table schema and indexes are stored as document in storage layer by calling K2 storage APIs.
* The catalog manager needs to handle schema update on the storage layer. 
* Initialize PG system databases and system tables globally so that SQL executor could be stateless without creating its own local system databases/tables,
which are difficult to be consistent. 
* Manage the table to collection/partition mapping in storage layer so that we could update the the document schema version and types 
on K2 storage nodes when a user alters tables.
* Provide service APIs for the SQL executor to get table schema and index information
* Could provides the initial connection for user to get the SQL executor to submit a query so that we can load balance the query load on 
SQL executors. If we colocate the SQL executor with a K2 storage node, the coordinator could assign the query to the proper SQL executor 
to gain data locality
*  In the future, we could add SQL parsing, planning, and optimization logic to the coordinator so that it could generate SQL plan segements 
to submit to different SQL executors to do parallel executions and keep track of the Query life cycle.
* For prototype, we could use a single instance coordinator. We could add a distributed coordinators by using raft consensus protocol. 

## SQL Executor 
The SQL executor run SQL queries locally. For the first version, it is implemented by integrating with postgres, i.e., it is a single instance
query executor. 
* It is stateless, i.e., it won't create any local system databases and tables.
* It provide libpq APIs for users to submit queries and get back query results.
* It consists of an embedded PG with the following modifications
  * Foreign Data Wrapper is used to access table data on K2 storage layer instead of from local memory and disk, this is mainly for table scan
with expressions for column projection and predicate pushdown.
  * SQL grammar is updated to only support a subset of Queries. Will add more query types incrementally.
  * PG command APIs are updated to call catalog manager instead of updating local catalogs 
  * Index update and scan are rerouted to K2 storage layer 
  * The above functions are implemented by calling a connector as a glue layer between PG and K2 storage layer 
  * The executor in PG manages the life cycle of a query, i.e., from parsing, planning, optimizing, and execution. 
*  The connector layer is used by PG to interact with SQL coordinator and K2 storage layer
    * PG calls its PG gate APIs for DDLs and DMLs
    * It calls catalog manager in SQL coordinator for user databases and tables
    * It calls Index manager in SQL coordinator for secondary indexes 
    * It scans table data from K2 storage layer with column projection and predication pushdown. Paginationg might be used to fetch result data. 
    * It scans table indexes for Index based scan or join.
    * It could call multiple K2 storate nodes if the SQL involves multiple tables on different K2 collections/partions

## K2 Storage Layer 
K2 storage layer functions consist of 
* Table data are stored in K2 storage layer. The collection holds schema information.
* PG system tables, user tables, and table secondary indexes are stored as regular data in k2 storage layer.
* It provides document style APIs for the connector in SQL executor to access table data or secondary index
* SQL executor might call the transaction protocol in K2 storage layer for a SQL transaction with multiple collection/partition updates
* The collection schemas for a partition need to be updated by SQL coordinator with a different schema version id.

# Open Questions

* Do we support composite primary keys?
* Should we colocate SQL executor with K2 storage node?
* How to integrate PG transactions with K2S3 transaction protocol?
