This document is a design proposal for the K2 SQL layer. It is a working in progress. 

# Introduction
## Assumptions 
K2 platform is an in-memory document storage system. We introduce a SQL layer on top of so that users could interactive with our system using SQLs.

The design is based on the following Assumptions:
* K2 Platform provides document APIs so that the SQL layers could access the table rows and columns with column projections and predicate pushdown
* For the first version of prototype, we will integrate postgres (PG) database engine for query parsing, planning, and execution. That is to say, the query
execution is on a single PG instance, which might not be ideal for complex queries, but should work for most OLTP queries. 
* However, we should make our design to be extensible to be able to run queries in a distributed fashion, i.e., a physical query plan could be split
into multiple segments to run on different nodes, which is useful for HTAP (Hybrid transaction/analytical processing) and OLAP in the future.
* Data persistency and consistency is guaranteed by the K2 storage layer
* Each table has a primary key and we need to specify one if it is not provided in schema definition. 
* Primary keys that are used to partition tables cannot be changed once they are defined. 

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
* On the top of the storage layer, we have SQL executor, which could run separately on different hosts or the same host of the storage layer.
* There is a SQL coordinator so that it could manage catalog, index, and data type updates on k2 storage nodes. 

![Architecture](./images/K2SqlSystemArchitecture01.png)

### SQL Coordinator 

The SQL coordinator has the following responsibilities
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
*  In the future, we could add SQL parsing, planning, and optimization logic to the coordinator so that it could generate SQL plan segments 
to submit to different SQL executors to do parallel executions and keep track of the Query life cycle.
* For prototype, we could use a single instance coordinator. We could add a distributed coordinators by using raft consensus protocol. 

### SQL Executor 
The SQL executor run SQL queries locally. For the first version, it is implemented by integrating with Postgres, i.e., it is a single instance
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
    * It scans table data from K2 storage layer with column projection and predication pushdown. Paginating might be used to fetch result data. 
    * It scans table indexes for Index based scan or join.
    * It could call multiple K2 storage nodes if the SQL involves multiple tables on different K2 collections/partitions

### K2 Storage Layer 
K2 storage layer functions consist of 
* Table data are stored in K2 storage layer. The collection holds schema information.
* PG system tables, user tables, and table secondary indexes are stored as regular data in k2 storage layer.
* It provides document style APIs for the connector in SQL executor to access table data or secondary index
* SQL executor might call the transaction protocol in K2 storage layer for a SQL transaction with multiple collection/partition updates
* The collection schemas for a partition need to be updated by SQL coordinator with a different schema version id.

## SQL Layer 

The SQL layer consists of the SQL executors and the SQL coordinator(s). Let us cover them in more details.

### SQL Executor 

A more detailed view of SQL executor is shown by the following diagram.

![SQL Executor](./images/K2SqlExecutorDiagram01.png)

First of all, the SQL executor uses a bootstrap module to initialize the process Environment, for example, waiting for the coordinator 
to create system databases and tables. Then it starts a PG instance either as a child process or embedding inside. At the same time, it
starts a RPC server for coordinator to heartbeat or get its status. In the future, the RPC server could accept Query execution commands
from coordinator if we starts to support distributed query executions. The Libpq interface is provided by the PG process.

#### Postgres 

We could adopt the modified version of PG from [YugaByteDB](https://github.com/futurewei-cloud/chogori-sql/blob/master/docs/YugabyteDb.md), which has the following customization
* Modified the initdb.c to not create system databases and tables locally and relies on remote catalog manager instead.
* Wired in a Foreign Data Wrapper (DFW) to scan data from external data source instead of local memory and disk.
* Changed PG commands to call external catalog manager 
* Changed Index Scan from external data source.
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

#### PG Gate APIs 

The PG Gate APIs include the following parts. We would customize them to our need.

##### Environment and Session
The PG needs to call the K2 connector to setup runtime environment and SQL sessions.

``` c
void YBCInitPgGate(const YBCPgTypeEntity *YBCDataTypeTable, int count, YBCPgCallbacks pg_callbacks);
void YBCDestroyPgGate();

// Initialize ENV within which PGSQL calls will be executed.
YBCStatus YBCPgCreateEnv(YBCPgEnv *pg_env);
YBCStatus YBCPgDestroyEnv(YBCPgEnv pg_env);

// Initialize a session to process statements that come from the same client connection.
YBCStatus YBCPgInitSession(const YBCPgEnv pg_env, const char *database_name);

// Initialize memory context to hold all of its allocated space. Once all associated operations
//   are done, the context is destroyed.
YBCPgMemctx YBCPgCreateMemctx();
YBCStatus YBCPgDestroyMemctx(YBCPgMemctx memctx);
YBCStatus YBCPgResetMemctx(YBCPgMemctx memctx);

// Invalidate the sessions table cache.
YBCStatus YBCPgInvalidateCache();

// Clear all values and expressions that were bound to the given statement.
YBCStatus YBCPgClearBinds(YBCPgStatement handle);

// Check if initdb has been already run.
YBCStatus YBCPgIsInitDbDone(bool* initdb_done);
```
##### DDLs 

Database operations

``` c
// Connect database. Switch the connected database to the given "database_name".
YBCStatus YBCPgConnectDatabase(const char *database_name);
// Create database.
YBCStatus YBCPgNewCreateDatabase(const char *database_name,
                                 YBCPgOid database_oid,
                                 YBCPgOid source_database_oid,
                                 YBCPgOid next_oid,
                                 const bool colocated,
                                 YBCPgStatement *handle);
YBCStatus YBCPgExecCreateDatabase(YBCPgStatement handle);

// Drop database.
YBCStatus YBCPgNewDropDatabase(const char *database_name,
                               YBCPgOid database_oid,
                               YBCPgStatement *handle);
YBCStatus YBCPgExecDropDatabase(YBCPgStatement handle);

// Alter database.
YBCStatus YBCPgNewAlterDatabase(const char *database_name,
                               YBCPgOid database_oid,
                               YBCPgStatement *handle);
YBCStatus YBCPgAlterDatabaseRenameDatabase(YBCPgStatement handle, const char *newname);
YBCStatus YBCPgExecAlterDatabase(YBCPgStatement handle);

// Reserve oids.
YBCStatus YBCPgReserveOids(YBCPgOid database_oid,
                           YBCPgOid next_oid,
                           uint32_t count,
                           YBCPgOid *begin_oid,
                           YBCPgOid *end_oid);
```

Table Operations 
``` c
BCStatus YBCPgNewCreateTable(const char *database_name,
                              const char *schema_name,
                              const char *table_name,
                              YBCPgOid database_oid,
                              YBCPgOid table_oid,
                              bool is_shared_table,
                              bool if_not_exist,
                              bool add_primary_key,
                              const bool colocated,
                              YBCPgStatement *handle);

YBCStatus YBCPgCreateTableAddColumn(YBCPgStatement handle, const char *attr_name, int attr_num,
                                    const YBCPgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);
YBCStatus YBCPgExecCreateTable(YBCPgStatement handle);

YBCStatus YBCPgNewAlterTable(YBCPgOid database_oid,
                             YBCPgOid table_oid,
                             YBCPgStatement *handle);

YBCStatus YBCPgAlterTableAddColumn(YBCPgStatement handle, const char *name, int order,
                                   const YBCPgTypeEntity *attr_type, bool is_not_null);

YBCStatus YBCPgAlterTableRenameColumn(YBCPgStatement handle, const char *oldname,
                                      const char *newname);

YBCStatus YBCPgAlterTableDropColumn(YBCPgStatement handle, const char *name);

YBCStatus YBCPgAlterTableRenameTable(YBCPgStatement handle, const char *db_name,
                                     const char *newname);

YBCStatus YBCPgExecAlterTable(YBCPgStatement handle);

YBCStatus YBCPgNewDropTable(YBCPgOid database_oid,
                            YBCPgOid table_oid,
                            bool if_exist,
                            YBCPgStatement *handle);

YBCStatus YBCPgExecDropTable(YBCPgStatement handle);

YBCStatus YBCPgNewTruncateTable(YBCPgOid database_oid,
                                YBCPgOid table_oid,
                                YBCPgStatement *handle);

YBCStatus YBCPgExecTruncateTable(YBCPgStatement handle);

YBCStatus YBCPgGetTableDesc(YBCPgOid database_oid,
                            YBCPgOid table_oid,
                            YBCPgTableDesc *handle);

YBCStatus YBCPgGetColumnInfo(YBCPgTableDesc table_desc,
                             int16_t attr_number,
                             bool *is_primary,
                             bool *is_hash);

YBCStatus YBCPgGetTableProperties(YBCPgTableDesc table_desc,
                                  YBCPgTableProperties *properties);                                    
```

Index operations 
``` c
YBCStatus YBCPgNewCreateIndex(const char *database_name,
                              const char *schema_name,
                              const char *index_name,
                              YBCPgOid database_oid,
                              YBCPgOid index_oid,
                              YBCPgOid table_oid,
                              bool is_shared_index,
                              bool is_unique_index,
                              const bool skip_index_backfill,
                              bool if_not_exist,
                              YBCPgStatement *handle);

YBCStatus YBCPgCreateIndexAddColumn(YBCPgStatement handle, const char *attr_name, int attr_num,
                                    const YBCPgTypeEntity *attr_type, bool is_hash, bool is_range,
                                    bool is_desc, bool is_nulls_first);
BCStatus YBCPgExecCreateIndex(YBCPgStatement handle);

YBCStatus YBCPgNewDropIndex(YBCPgOid database_oid,
                            YBCPgOid index_oid,
                            bool if_exist,
                            YBCPgStatement *handle);

YBCStatus YBCPgExecDropIndex(YBCPgStatement handle);

YBCStatus YBCPgWaitUntilIndexPermissionsAtLeast(
    const YBCPgOid database_oid,
    const YBCPgOid table_oid,
    const YBCPgOid index_oid,
    const uint32_t target_index_permissions,
    uint32_t *actual_index_permissions);

YBCStatus YBCPgAsyncUpdateIndexPermissions(
    const YBCPgOid database_oid,
    const YBCPgOid indexed_table_oid);
```

The above DDLs are passed to the K2 connector, which calls the catalog manager in SQL coordinator under the hood.

##### DMLs 

DML statements consist of select, insert, update, delete, and truncate.

First, we need to bind a SQL statement to columns and tables.

``` c
 // This function is for specifying the selected or returned expressions.
// - SELECT target_expr1, target_expr2, ...
// - INSERT / UPDATE / DELETE ... RETURNING target_expr1, target_expr2, ...
YBCStatus YBCPgDmlAppendTarget(YBCPgStatement handle, YBCPgExpr target);
YBCStatus YBCPgDmlBindColumn(YBCPgStatement handle, int attr_num, YBCPgExpr attr_value);
YBCStatus YBCPgDmlBindColumnCondEq(YBCPgStatement handle, int attr_num, YBCPgExpr attr_value);
YBCStatus YBCPgDmlBindColumnCondBetween(YBCPgStatement handle, int attr_num, YBCPgExpr attr_value,
    YBCPgExpr attr_value_end);
YBCStatus YBCPgDmlBindColumnCondIn(YBCPgStatement handle, int attr_num, int n_attr_values,
    YBCPgExpr *attr_values);

// Binding Tables: Bind the whole table in a statement.  Do not use with BindColumn.
YBCStatus YBCPgDmlBindTable(YBCPgStatement handle);

// API for SET clause.
YBCStatus YBCPgDmlAssignColumn(YBCPgStatement handle,
                               int attr_num,
                               YBCPgExpr attr_value);

// This function is to fetch the targets in YBCPgDmlAppendTarget() from the rows that were defined
// by YBCPgDmlBindColumn().
YBCStatus YBCPgDmlFetch(YBCPgStatement handle, int32_t natts, uint64_t *values, bool *isnulls,
                        YBCPgSysColumns *syscols, bool *has_data);

// Utility method that checks stmt type and calls either exec insert, update, or delete internally.
YBCStatus YBCPgDmlExecWriteOp(YBCPgStatement handle, int32_t *rows_affected_count);

// This function returns the tuple id (ybctid) of a Postgres tuple.
YBCStatus YBCPgDmlBuildYBTupleId(YBCPgStatement handle, const YBCPgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *ybctid);
```

Insert operations 
``` c
BCStatus YBCPgNewInsert(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle);

YBCStatus YBCPgExecInsert(YBCPgStatement handle);

YBCStatus YBCPgInsertStmtSetUpsertMode(YBCPgStatement handle);

YBCStatus YBCPgInsertStmtSetWriteTime(YBCPgStatement handle, const uint64_t write_time);
```

Update operations
``` c
YBCStatus YBCPgNewUpdate(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle);

YBCStatus YBCPgExecUpdate(YBCPgStatement handle);
```

Delete operations 
``` c
YBCStatus YBCPgNewDelete(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         bool is_single_row_txn,
                         YBCPgStatement *handle);

YBCStatus YBCPgExecDelete(YBCPgStatement handle);
```

Select operations 
``` c
YBCStatus YBCPgNewSelect(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         const YBCPgPrepareParameters *prepare_params,
                         YBCPgStatement *handle);

// Set forward/backward scan direction.
YBCStatus YBCPgSetForwardScan(YBCPgStatement handle, bool is_forward_scan);

YBCStatus YBCPgExecSelect(YBCPgStatement handle, const YBCPgExecParameters *exec_params);
```

Expression operations 
``` c
 / Column references.
YBCStatus YBCPgNewColumnRef(YBCPgStatement stmt, int attr_num, const YBCPgTypeEntity *type_entity,
                            const YBCPgTypeAttrs *type_attrs, YBCPgExpr *expr_handle);

// Constant expressions.
YBCStatus YBCPgNewConstant(YBCPgStatement stmt, const YBCPgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, YBCPgExpr *expr_handle);
YBCStatus YBCPgNewConstantOp(YBCPgStatement stmt, const YBCPgTypeEntity *type_entity,
                           uint64_t datum, bool is_null, YBCPgExpr *expr_handle, bool is_gt);

// The following update functions only work for constants.
// Overwriting the constant expression with new value.
YBCStatus YBCPgUpdateConstInt2(YBCPgExpr expr, int16_t value, bool is_null);
YBCStatus YBCPgUpdateConstInt4(YBCPgExpr expr, int32_t value, bool is_null);
YBCStatus YBCPgUpdateConstInt8(YBCPgExpr expr, int64_t value, bool is_null);
YBCStatus YBCPgUpdateConstFloat4(YBCPgExpr expr, float value, bool is_null);
YBCStatus YBCPgUpdateConstFloat8(YBCPgExpr expr, double value, bool is_null);
YBCStatus YBCPgUpdateConstText(YBCPgExpr expr, const char *value, bool is_null);
YBCStatus YBCPgUpdateConstChar(YBCPgExpr expr, const char *value, int64_t bytes, bool is_null);

// Expressions with operators "=", "+", "between", "in", ...
YBCStatus YBCPgNewOperator(YBCPgStatement stmt, const char *opname,
                           const YBCPgTypeEntity *type_entity,
                           YBCPgExpr *op_handle);
YBCStatus YBCPgOperatorAppendArg(YBCPgExpr op_handle, YBCPgExpr arg);
```

Transaction operations 
``` c
YBCStatus YBCPgBeginTransaction();
YBCStatus YBCPgRestartTransaction();
YBCStatus YBCPgCommitTransaction();
YBCStatus YBCPgAbortTransaction();
YBCStatus YBCPgSetTransactionIsolationLevel(int isolation);
YBCStatus YBCPgSetTransactionReadOnly(bool read_only);
YBCStatus YBCPgSetTransactionDeferrable(bool deferrable);
YBCStatus YBCPgEnterSeparateDdlTxnMode();
YBCStatus YBCPgExitSeparateDdlTxnMode(bool success);
```
The DMLs usually involve both the Catalog manager in SQL coordinator and direct access to K2 storage nodes. 

#### K2 Connector 

The K2 connector behaves similar to the PG Gate in YugaByteDB and it is a glue layer among catalog services and storage layer. However, we need 
rewrite our own connector logic since YugaByteDB's PG Gate is heavily coupled with its catalog system, tablet service, and its own transaction control 
logic. We need to implement the PG Gate APIs using our own logic.

Whenever, the K2 connector receives API calls from PG, it dispatches the calls to DDL process or DML process depending on the call type. The 
DDL and DML process creates a session for a SQL statement to allocate memory to cache schema and other data, set up client connection to the SQL 
coordinator or the K2 storage layer or both. They also have logic to bind columns and expressions to table and then make calls to SQL coordinator 
or K2 storage layer if necessary. Some operations are done in-memory, for example, column bindings. The session will be closed and cache is validated
once a SQL statement finishes.

Its the SQL coordinator's responsibilities to decide which K2 storage node that table records are stored. When user creates a database, the catalog 
manager generates a database oid (object id) and the tables in a database are assigned with table oids, which could be used to map a collection in K2 storage layer. 

The table primary key(s) should be used to map the record partition in a K2 collection. 

### SQL Coordinator 

SQL Coordinator consists of the following components.
* (Optional) Web service to show query statuses, metrics, database states, and K2 collection information.
* RPC server for SQL executors to heartbeat and call APIs for database/table/index updates
* Catalog Manager
  * initialize, create, and save PG system databases and tables to K2 storage layer, for example, all the [system catalog](https://www.postgresql.org/docs/11/catalogs.html)
  for template1, template0, and postgres
  * manage user databases and tables such as create, insert, update, delete, and truncate.
  * provide catalog APIs to external clients such as SQL executors 
  * cache schemas locally to avoid fetching them from remote
* Index Manager is logical part of the Catalog manager. We separate it out only to emphasize its functionality.
  * create, save, update, and delete secondary indexes to K2 storage layer. 
  * Provide APIs for the Catalog RPC service to manage indexes 
* Collection manager to maintain the information of table partition to K2 storage node mapping. For example, if a SQL executor updates a table 
schema, it needs to get all the collection/partition information from the SQL coordinator so as to push schema updates to all collection/partitions 
on K2 storage nodes.
* K2 storage APIs to get active K2 nodes, store/update/delete documents on K2 storage layers for system/user databases, tables, and indexes. 

![SQL Coordinator](./images/K2SqlCoordinatorDiagram01.png)

#### Schema Entities

The entities for SQL schemas are shown in the following class diagram.
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

![SQL Schema](./images/K2SqlSchemaEntity01.png)

#### Catalog APIs 

Catalog APIs are exposed as RPC service to manage databases, tables, and indexes.


### K2 Storage

Here we focus on how to map a record to K2 storage layer and expressions for predicate pushdown. 

### How Does It Work?

Let use show how the system works by using the following scenarios.

#### Create Database and Tables

The user first creates a database and then create a table in this database as follows.
* user sends "create database" to postgres via libpq
* postgres calls the connector, which call the coordinator under the hood to create database for this user
* the catalog manager in SQL coordinator saves the database metadata to a specific catalog node on K2 storage and cache the information. 
The database is a namespace to hold a set of tables, it does not need to be stored on the K2 data node.
* user sends "create table" to postgres
* postgres calls the connector, which calls the coordinator again to create a table for this user
* the coordinator saves the table metadata to its catalog node 
* the coordinator uses the database oid and table oid to assign collection/partitions and k2 node(s)
* the coordinator returns the collection/partition information back to the connector in SQL executor
* the connector then saves the record schema to the collection/partitions
* the above create table operations should be protected by transaction so that we could rollback if anything goes wrong
* the coordinator might need to update system tables as well to reflect user table changes

![Create Database and Table Sequence Diagram](./images/K2SqlCreateDBTableSequenceDiagram01.png)

#### Select

The user runs a select query as follows
* user sends the select statement to postgres via libpq
* postgres parses the SQL and it needs to get the table schema during the parsing.
* postgres calls the connector to fetch the table schema from the coordinator
* coordinator returns the schema from its memory or gets it from its catalog storage node if not available
* postgre plans and optimize the select query
* postgre execute the query to call connector via the foreign data wrapper
* the select could also be an index scan instead of data scan depends on the query planning and optimization
* the connector calls either the data node(s) to scan data with column projection and predicate pushdown if available
* or get the index information from the catalog storage node and then do the index scan on the data storage node
* the result set is streaming back to customer
* the data/index scan could use stream APIs or data pagination depends on the implementation

![Select Statement Sequence Diagram](./images/K2SqlSelectSequenceDiagram01.png)

#### Insert 

#### Update

#### Schema Update

# Open Questions

* Do we support composite primary keys?
* Should we colocate SQL executor with K2 storage node?
* How to integrate PG transactions with K2S3 transaction protocol?
* How to partition tables in a database to different K2 collection/partitions?
* Should we partition index storage into multiple collections/partitions to scale index scans? Should we place the indexes on the same storage partition for
the same table?
* Should the SQL executor starts one PG child process or multiple ones so that one PG instance associates with one core?
* Who is responsible for collection/partitions assignment if we use range hash, i.e., the storage layer or the coordinator?