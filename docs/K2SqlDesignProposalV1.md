This document is a design proposal for the K2 SQL layer. It is a working in progress. 

# Introduction
## Assumptions 
K2 platform is an in-memory document storage system. We introduce a SQL layer on top of so that users could interactive with our system using SQLs.

The design is based on the following Assumptions:
* K2 Platform provides document APIs so that the SQL layers could access the table rows and columns with column projections and predicate pushdown
* For the first version of prototype, we will integrate Postgres (PG) database engine for query parsing, planning, and execution. That is to say, the query
execution is on a single PG instance, which might not be ideal for complex queries, but should work for most OLTP queries. 
* However, we should make our design to be extensible to be able to run queries in a distributed fashion, i.e., a physical query plan could be split
into multiple segments to run on different nodes, which is useful for HTAP (Hybrid transaction/analytical processing) and OLAP in the future.
* Data persistency and consistency is guaranteed by the K2 storage layer
* Each table has a primary key and we need to specify one if it is not provided in schema definition. 
* Primary keys that are used to partition tables cannot be changed once they are defined. 
* *DDL schema updates are transactional, but schema migration are not transactional*.

## Design Goals
We like to achieve the following goals for the first version of prototype.
* Support Catalog management by DDLs, i.e., create, update, and delete database and tables
* Support secondary indexes 
* Support select with column projection and predication pushdown. Aggregation pushdown could be implemented in later versions.
* Support insert, update, delete, truncate, and simple joins
* Allow users to submit queries using PG libpq interface. JDBC and ODBC drivers could be provided in later versions. 

# System Design Proposal 

This is a high level design. Details will be provided in separate sections.

## Architecture 

The system architecture is as follows and it consists of the following components.
* The storage layer provides document APIs so that we could storage data with data type information, table schemas, and secondary indexes into it.
* On the top of the storage layer, we have SQL executor, which could run separately on different hosts.

![Architecture](./images/K2SqlSystemDiagram021.png)

When the SQL system first starts, one of the SQL executors creates the template0 and the template1 template databases for Postgres, it also creates a default database called postgres. A user could create a new database, for example, demo, by copying it from the template1 database.  

## SQL Executor 

A more detailed view of SQL executor is shown by the following diagram.

![SQL Executor](./images/K2SqlExecutorDiagram021.png)

### Postgres 

We could adopt the modified version of PG from [YugaByteDB](https://github.com/futurewei-cloud/chogori-sql/blob/master/docs/YugabyteDb.md), which has the following customization
* Modified the initdb.c to not create system databases and tables locally and relies on catalog manager in the connector instead. In this way, the postgres is stateless.
* Wired in a Foreign Data Wrapper (DFW) to scan data from an external data source instead of local memory and disk.
* Changed PG commands to call external catalog manager 
* Changed Index Scan to read from external data source.
* Updated caching logic for external data sources 
* Implemented sequence support 
* Implemented column projection and predicate pushdown to external data sources
* Implemented aggression pushdown for min, max, sum, and count
* Used the [PG Gate APIs](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/include/yb/yql/pggate/ybc_pggate.h) to interact
with external catalog manager and data sources.
However, we need to change PG for our own use, which means we might need to change the PG Gate APIs as well.
* Update the SQL grammar [gram.y](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/backend/parser/gram.y) to only enable
the SQL syntax that we support
* Remove/disable the colocated table logic
* Changed the tablet logic, which was part of YugaByteDB's storage component
* Disable aggression push down for now until we support them in K2 storage layer
* Update the transaction logic to integrate with K2S3 transaction protocol
* Need to check if we could support sequence  

### K2 Connector 

The K2 connector behaves similar to the PG Gate in YugaByteDB and it is a glue layer among catalog service and storage layer. However, we need 
rewrite our own connector logic since YugaByteDB's PG Gate is heavily coupled with its catalog system, tablet service, and its own transaction control 
logic. We need to implement the PG Gate APIs using our own logic.

Whenever the K2 connector receives API calls from PG, it dispatches the calls to DDL handler or DML handler depending on the call type. The 
DDL or DML handle creates a session for a SQL statement to allocate memory to cache schema and other data and set up client connection to the K2 storage layer. They also have logic to bind columns and expressions to table and then make calls to catalog manager if necessary. Some operations are done in-memory, for example, column bindings. The session is closed and cache is validated once a SQL statement finishes.

#### Catalog Manager

The catalog manager runs on each SQL executor and it is responsible for 
* initializes, creates, and saves PG system databases and tables to K2 storage layer, for example, all the [system catalog](https://www.postgresql.org/docs/11/catalogs.html) for template1, template0, and Postgres
* manages user databases and tables such as create, insert, update, delete, and truncate.
* provides catalog APIs for DML handler and DDL handler 
* caches schemas locally to avoid fetching them from remote
* Call K2 document APIs to store/update/delete documents on K2 storage layers for system/user databases, tables, and indexes. 

When user creates a database, the catalog manager generates a database oid (object id) and the tables in a database are assigned with table oids, which could be used to map a collection in K2 storage layer. The table primary key(s) should be used to locate the record in a K2 collection. 

##### Initialization

When the SQL Executor first starts up, it needs to initialize the catalog system and internal data.
* System catalogs
* Existing user database tables
* Internal states such as whether initDB is finished or not

Postgres consists of a set of system catalogs as described in [PG doc](https://www.postgresql.org/docs/current/catalogs.html). All the catalogs start with the 
"pg_" prefix. Meanwhile, The [information schema](https://www.postgresql.org/docs/11/information-schema.html) consists of a set of views that contain information about the objects defined in the current database. The information schema is defined in the SQL standard and can therefore be expected to be portable and remain stable — unlike the system catalogs, which are specific to PostgreSQL and are modeled after implementation concerns. The information schema views do not, however, contain information about PostgreSQL-specific features. The views are defined in this [sql](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/backend/catalog/information_schema.sql).

For Postgres, it always creates a new database from templates. The two templates are [template0 and template1](https://www.postgresql.org/docs/11/manage-ag-templatedbs.html), where template0 is never changed after the PG cluster starts and template1 includes other changes. By default, PG copies the standard system database named template1. As a result, when a cluster starts up, the SQL coordinator needs to create template0, template1, and Postgres (and other default databases) databases. Each database consists a subset of system catalogs.

Since we have multiple SQL executors with a PG process inside each one, we need to initialize the default databases inside SQL coordinator instead of the PG instance inside the SQL executor to make each SQL executor stateless and keep schema data consistency. The SQL coordinator stores the schema data on K2 storage layer to keep the data consistent and durable, it also cache the database (namespace), table, and index data locally. 

For the cache, we would keep a map of namespace to a collection that includes id-table, name-table, id-indexTable maps.

![Coordinator Cache](./images/K2SqlCoordinatorCache01.png)

The default databases could be generated directly when SQL coordinator starts for the first time. They could also be created by loading from a snapshot in some storage format. After that, an internal isInitDBDone flag is set to true so that SQL executors could check this flag and become ready only
after this flag is true.

When we store system catalogs, we store the schema of the tables into a table, for example, a "sys_catalog" table with each row represents a table schema. 
Apart from that, we need a table to store the namespaces (databases). The data for system catalogs are stored on the same collection/partitions of the system table schema and we don't partition them since normally the table size is small. 

When user creates a database (namespace), a subset of system catalogs are copied into the new database. When a new table is created, the metadata is saved into
the *sys_catalog* table inside the namespace. However, user table data and indexes are partitioned and saved into different K2 data nodes based on database id/name and partition/range keys.

##### Schema Entities

The entities for SQL schemas are shown in the following class diagram.
* Namespace: database
* DataType: data types that support [PG SQL data types](https://www.postgresql.org/docs/11/datatype.html)
* TypeInfo: used internally to resolve a SQL type to a physical type, i.e., c/c++ data type
* ColumnId: consists of column name and column id, which is generated by the system
* SortingOrder: the column sorting order, which could be used during index build
* ColumnSchema: the [column definition](https://www.postgresql.org/docs/11/infoschema-columns.html), where isKey indicates if this column is part of the primary key
* PartitionType: PG supports [Hash, Range, and List partitions](https://www.postgresql.org/docs/11/ddl-partitioning.html). We need to decide how do we 
support table partitions since storage layer might partition the table with split and join automatically
* PartitionSchema: partition schema for a table
* TableProperties: we need to decide what properties that we need to introduce
* Schema: the schema for a table row with table properties. Schema is versioned.
* ExprOperator: all expression types in SQL where clauses and index definitions
* Condition: a typical conditional expression
* Expression: it consists of different types of expression, for example, Condition, or a build-in function such as lower() or floor(). We need to decide whether we support them in K2 storage layer
* IndexColumn: the column involved in an index
* IndexTable: PG stores the [secondary indexes](https://www.postgresql.org/docs/11/indexes.html) as separate IndexTables. An index has its own system generated id, name, version, the table id that the index is built on, and whether it is unique. An index could include one or more columns
* Table: the table information that is maintained by the catalog manger and it consists of table name, system generated table id, table schema, partition schema, and an index map for secondary indexes

![SQL Schema](./images/K2SqlSchemaEntity02.png)

##### Catalog APIs 

Catalog APIs are used to manage databases, tables, and indexes. In the response object, an error code is returned if the API call fails.
* createNamespace(): create a database
* createTable(): create a table
* createIndex(): create an index
* *cloneTable(): clone a table from an existing table*
* alterTable(): change a table schema by adding columns, renaming columns, or dropping columns. The schema version is increased for each update
* getTableSchema(): get table schema including the index ids if they are available
* getIndex(): get index information
* listNamespaces(): return all databases for a user
* listTables(): return all tables in a database
* dropNamespace(): delete a database
* dropTable(): delete a table
* dropIndex(): delete an index
* isInitDBDone(): whether the system initialization is done or not

![Catalog Service](./images/K2SqlCatalogService021.png)

### K2 Storage

The SQL layer stores table schema, indexes, and data on K2 storage layer and it needs to scan the data or index on data nodes. Both SQL executors and SQL coordinator need to call K2 storage layer APIs.

#### SQL Document APIs

The K2 storage layer provides document APIs so that we could the K2 storage layer knows the data schema. As a result, we propose the following SQL document APIs, which is a wrapper layer in the SQL layer to call the native K2 storage APIs and are used by both the SQL coordinator and the connector in SQL executors. The SQL document APIs could be used to update schema or fetch/update/delete records. Filters are used to filter out data during Index or data scan. A pagination token could be used to fetch records in pages. 

Multiple SQL Executor might try to initialize the system in parallel, to avoid that, we introduce a conditionalUpdateRecord() API so that only one SQL executor could start the initialization, for example, by setting a flag. 

Whenever Postgres creates a database, it first copies all the system catalogs from the template1 template database. To make this database copy more efficient,
we could introduce a clonePartition() API to clone the partition inside K2 storage to avoid reading data and writing data in and out of the K2 storage layer.

![SQL Document APIs](./images/K2SqlDocumentAPIs021.png)

#### K2 Storage APIs

We need to convert the SQL document APIs to native K2 storage APIs to access data on K2 storage layer. Apart from that, we also need to encode the record
properly so that the storage layer could decode the data. We like to adopt the [encoding schema](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md) in cockroachDB.

#### Persistence

The following diagram illustrates the persistence of SQL schema and data in system.
* Map a database to a K2 collection, table id is part of the partition key
* The SQL executor initializes the system by first creating two template databases, i.e., template0 and template1. We also persist configuration such as system catalog version
* The SQL executor then creates a default database postgres such as all users use this database by default
* User table schemas are stored in system catalog in the database. User table data and index data are stored in the same collection
* If a user creates a new database, a new collection is created and system catalog in database template1 is copied into the new collection. New table schemas, data, and indexes are stored into the same collection.

![SQL Schema and data persistence](./images/K2SqlSchemaDataPersistence021.png)
