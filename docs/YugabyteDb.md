This document describes how YugbytesDb integrates with Postgres

# Introduction
Chorgori platform is built for low-latency in-memory distributed persistent OLTP databases. With the K2 storage layer in place, we need to add a 
SQL layer on top of it so that people could run SQL to interact with it. This type of SQL is fundamentally different from the traditional SQL databases
in that
* The database is distributed, not a simple sharding system of a single instance traditional database, which is hard and painful to manage. 
Instead, the system is automatically partitioned without manual efforts. 
* It supports strong consistency with distributed transactions 
* It supports GEO distributed transactions
* It scales with big data in mind

There are many of this types of so-called NewSQL projects that are inspired by the Google [Spinner paper](https://static.googleusercontent.com/media/research.google.com/en//archive/spanner-osdi2012.pdf) and 
the subsequent [F1 paper](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/41344.pdf). YugabyteDb is one of them and it was built as 
a DocDB at first with the support of Cassandra CQL language and Redis APIs from 2016. Postgres 10 was integrated in 2018 and then was upgraded to 11.2. 
The supported features can be found at https://docs.yugabyte.com/latest/api/ysql/. It could support [GraphQL](https://docs.yugabyte.com/latest/develop/graphql/) as well. 
Even our system is fundamentally different, YugaByteDB is a good reference implementation to help us for initial investigations.

# Integration

## Architecture
The integration is illustrated from high level by the following diagram.

![Architecture](./images/YugabytedbPGIntegration.png)

That is to say, postgres was customized to communicate with YugaByteDB's DocDB via a layer called PG Gate. 

## Postgres Process

First of all, a Postgres process runs together with a [yb-tserver](https://docs.yugabyte.com/latest/architecture/concepts/yb-tserver/) (tablet server) on the storage node in YugabyteDb. More specifically, 
postgres was started as a child process of yb-tserver. Please check the commit history of [pg_wrapper.cc](https://github.com/yugabyte/yugabyte-db/commits/master/src/yb/yql/pgwrapper/pg_wrapper.cc). 

The [initdb.c](https://github.com/yugabyte/yugabyte-db/commits/master/src/postgres/src/bin/initdb/initdb.c) is called to initialize Postgres installation. 
YugaByteDB is more complicated for Postgres initialization since its catalog manager is on [yb-master](https://docs.yugabyte.com/latest/architecture/concepts/yb-master/) nodes and it needs to setup system tables on catalog manager.  The reasons are as follows as implemented by this [commit](https://github.com/yugabyte/yugabyte-db/commit/ca30a3ab5252858103cf6f3f92697821e9b718df)
* Create Postgres catalog tables on yb-master during initdb, and use them for all Postgres catalog read and writes.
* the Postgres instances running on each node are now virtually stateless and no local files are created/used for either system or user tables
* This ensures metadata consistency and high-availability in case of master/node failures
* However, this has an effect on performance of DDL statements as metadata lookups can be non-local but this is mitigated by more aggressive caching -- and should not affect steady-state DML statements.

The processes on a yb-server is shown as follows.

```
# ps axvww
    PID TTY      STAT   TIME  MAJFL   TRS   DRS   RSS %MEMOMMAND
      1 ?        Ssl   42:34     96   127 6395208 2343528  7.1 /home/yugabyte/bin/yb-tserver --fs_data_dirs=/mnt/data0 --rpc_bind_addresses=yb-tserver-0.yb-tservers.test.svc.cluster.local:9100 --server_broadcast_addresses=yb-tserver-0.yb-tservers.test.svc.cluster.local:9100 --enable_ysql=true --pgsql_proxy_bind_address=10.1.41.19:5433 --use_private_ip=never --tserver_master_addrs=yb-master-0.yb-masters.test.svc.cluster.local:7100 --logtostderr
     37 ?        S      0:00      0  8575 422512 45680  0.1 /home/yugabyte/postgres/bin/postgres -D /mnt/data0/pg_data -p 5433 -h 10.1.41.19 -k  -c shared_preload_libraries=pg_stat_statements,yb_pg_metrics -c yb_pg_metrics.node_name=DEFAULT_NODE_NAME -c yb_pg_metrics.port=13000 -c config_file=/mnt/data0/pg_data/ysql_pg.conf -c hba_file=/mnt/data0/pg_data/ysql_hba.conf
     68 ?        Ssl    0:12      0  8575 496372 13768  0.0 postgres: YSQL webserver   
     70 ?        Ss     0:00      0  8575 422688 12112  0.0 postgres: checkpointer   
     71 ?        Ss     0:00      0  8575 266392 9708  0.0 postgres: stats collector   
```

The above could be viewed on a YugabyteDB cluster. To launch a mini YugaByteDB cluster, please follow the instructiond for [kubernetes](https://docs.yugabyte.com/latest/deploy/kubernetes/). Or you could
use [MicroK8s](https://ubuntu.com/tutorials/install-a-local-kubernetes-with-microk8s#1-overview) in Ubuntu to launch a local cluster using [this script](./kube/yb-test.yaml).

```
$ kubectl create namespace test
$ kubectl apply -f yb-test.yaml 
$ kubectl describe pods -n test
$ kubectl exec -it yb-tserver-0 -n test -- /home/yugabyte/bin/ysqlsh -h yb-tserver-0  --echo-queries
$ kubectl exec -it yb-master-0 -n test -- /bin/bash
```
After that, you could run [example SQLs](https://docs.yugabyte.com/latest/quick-start/explore-ysql/) or [TPC-C benchmark](https://docs.yugabyte.com/latest/benchmark/tpcc-ysql/). 

## Foreign Data Wrapper (FDW)

YugabyteDb data are external to Postgres and thus, it takes advantage of the [foreign data wrapper feature](https://wiki.postgresql.org/wiki/Foreign_data_wrappers) 
to hook in the data access logic in [src/backend/executor/ybc_fdw.c](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/backend/executor/ybc_fdw.c).

``` c
Datum ybc_fdw_handler()
{
        FdwRoutine *fdwroutine = makeNode(FdwRoutine);


        fdwroutine->GetForeignRelSize  = ybcGetForeignRelSize;
        fdwroutine->GetForeignPaths    = ybcGetForeignPaths;
        fdwroutine->GetForeignPlan     = ybcGetForeignPlan;
        fdwroutine->BeginForeignScan   = ybcBeginForeignScan;
        fdwroutine->IterateForeignScan = ybcIterateForeignScan;
        fdwroutine->ReScanForeignScan  = ybcReScanForeignScan;
        fdwroutine->EndForeignScan     = ybcEndForeignScan;

        /* TODO: These are optional but we should support them eventually. */
        /* fdwroutine->ExplainForeignScan = ybcExplainForeignScan; */
        /* fdwroutine->AnalyzeForeignTable = ybcAnalyzeForeignTable; */
        /* fdwroutine->IsForeignScanParallelSafe = ybcIsForeignScanParallelSafe; */

        PG_RETURN_POINTER(fdwroutine);
}

```
However, YugabyteDb did not use the regular FDW access path to define foreign tables since Postgres is customed to solely access its own data. 
As a result, shortcuts are used to access its catalog and data directly. For example, check the following code snippet in 
src/backend/foreign/foreign.c to access the FDW handler directly instead of reading it from catalogs, which should be the reason that YugabyteDb
is treated as native Postgres tables without additional foreign tables.

``` c
FdwRoutine * GetFdwRoutineForRelation(Relation relation, bool makecopy)
{
        FdwRoutine *fdwroutine;
        FdwRoutine *cfdwroutine;

        if (relation->rd_fdwroutine == NULL)
        {
                if (IsYBRelation(relation)) {
                        /* Get the custom YB FDW directly */
                        fdwroutine = (FdwRoutine *) ybc_fdw_handler();
                } else {
                        /* Get the info by consulting the catalogs and the FDW code */
                        fdwroutine = GetFdwRoutineByRelId(RelationGetRelid(relation));
                }

                /* Save the data for later reuse in CacheMemoryContext */
                cfdwroutine = (FdwRoutine *) MemoryContextAlloc(CacheMemoryContext, sizeof(FdwRoutine));
                memcpy(cfdwroutine, fdwroutine, sizeof(FdwRoutine));
                relation->rd_fdwroutine = cfdwroutine;


                /* Give back the locally palloc'd copy regardless of makecopy */
                return fdwroutine;
        }
    ...
}
```

Take the following YugabyteDb query execution plan as an example. The data sequential scan on the orders table used Foreign Scan wired in 
by FDW. However, the Index Scan is directly from memory. 

``` SQL
explain analyze SELECT users.id, users.name, users.email, orders.id, orders.total
          FROM orders INNER JOIN users ON orders.user_id=users.id
          LIMIT 10;
                                                           QUERY PLAN                                                           
--------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=0.00..2.14 rows=10 width=88) (actual time=1.101..5.222 rows=10 loops=1)
   ->  Nested Loop  (cost=0.00..213.89 rows=1000 width=88) (actual time=1.100..5.216 rows=10 loops=1)
         ->  Foreign Scan on orders  (cost=0.00..100.00 rows=1000 width=24) (actual time=0.670..0.683 rows=10 loops=1)
         ->  Index Scan using users_pkey on users  (cost=0.00..0.11 rows=1 width=72) (actual time=0.425..0.425 rows=1 loops=10)
               Index Cond: (id = orders.user_id)
 Planning Time: 12.095 ms
 Execution Time: 5.314 ms
```

## PG Gate 

YugabyteDB uses DocDB on top of RocksDB to store data in a document format on storage layer. The data consists of Catalog, i.e., system databases/tables and user 
databases/tables, and table data. To minimize the code change on Postgres, YugabyteDb introduced a PG gate to abstract all the interaction with 
the DocDB layer. The API from Postgres side is defined in yb/yql/pggate/ybc_pggate.h. For example,

``` c++
// This must be called exactly once to initialize the YB/PostgreSQL gateway API before any other
// functions in this API are called.
void YBCInitPgGate(const YBCPgTypeEntity *YBCDataTypeTable, int count, YBCPgCallbacks pg_callbacks);
void YBCDestroyPgGate();

//--------------------------------------------------------------------------------------------------
// Environment and Session.

// Initialize ENV within which PGSQL calls will be executed.
YBCStatus YBCPgCreateEnv(YBCPgEnv *pg_env);
YBCStatus YBCPgDestroyEnv(YBCPgEnv pg_env);

// Initialize a session to process statements that come from the same client connection.
YBCStatus YBCPgInitSession(const YBCPgEnv pg_env, const char *database_name);

// Connect database. Switch the connected database to the given "database_name".
YBCStatus YBCPgConnectDatabase(const char *database_name);

// Create and drop table "database_name.schema_name.table_name()".
// - When "schema_name" is NULL, the table "database_name.table_name" is created.
// - When "database_name" is NULL, the table "connected_database_name.table_name" is created.
YBCStatus YBCPgNewCreateTable(const char *database_name,
                              const char *schema_name,
                              const char *table_name,
                              YBCPgOid database_oid,
                              YBCPgOid table_oid,
                              bool is_shared_table,
                              bool if_not_exist,
                              bool add_primary_key,
                              const bool colocated,
                              YBCPgStatement *handle);

// Create and drop index "database_name.schema_name.index_name()".
// - When "schema_name" is NULL, the index "database_name.index_name" is created.
// - When "database_name" is NULL, the index "connected_database_name.index_name" is created.
YBCStatus YBCPgNewCreateIndex(const char *database_name,
                              const char *schema_name,
                              const char *index_name,
                              YBCPgOid database_oid,
                              YBCPgOid index_oid,
                              YBCPgOid table_oid,
                              bool is_shared_index,
                              bool is_unique_index,
                              bool if_not_exist,
                              YBCPgStatement *handle);

// This function returns the tuple id (ybctid) of a Postgres tuple.
YBCStatus YBCPgDmlBuildYBTupleId(YBCPgStatement handle, const YBCPgAttrValueDescriptor *attrs,
                                 int32_t nattrs, uint64_t *ybctid);

// SELECT 
YBCStatus YBCPgNewSelect(YBCPgOid database_oid,
                         YBCPgOid table_oid,
                         const YBCPgPrepareParameters *prepare_params,
                         YBCPgStatement *handle);

// Transaction control -----------------------------------------------------------------------------
YBCStatus YBCPgBeginTransaction();
YBCStatus YBCPgRestartTransaction();
YBCStatus YBCPgCommitTransaction();
YBCStatus YBCPgAbortTransaction();
YBCStatus YBCPgSetTransactionIsolationLevel(int isolation);

```
## Type Conversion

When acess data to and from DocDB, data types need to be converted between Postgres and DocDB. The type conversion is defined in 
[src/include/catalog/ybctype.h](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/include/catalog/ybctype.h).

``` c
extern const YBCPgTypeEntity *YBCDataTypeFromName(TypeName *typeName);
extern const YBCPgTypeEntity *YBCDataTypeFromOidMod(int attnum, Oid type_id);
bool YBCDataTypeIsValidForKey(Oid type_id);
void YBCGetTypeTable(const YBCPgTypeEntity **type_table, int *count);
```
More methods are implemented in [src/backend/catalog/ybctype.c](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/backend/catalog/ybctype.c), for example,

``` c
void YBCDatumToBool(Datum datum, bool *data, int64 *bytes) {
	*data = DatumGetBool(datum);
}
void YBCDatumToBinary(Datum datum, void **data, int64 *bytes) {
	*data = VARDATA_ANY(datum);
	*bytes = VARSIZE_ANY_EXHDR(datum);
}
```

## Catalog Access 

On top of the YugabyteDB defined methods to access system catalogs in [src/backend/access/ybcam.h](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/include/access/ybcam.h) so that they can be 
called by executors,

``` c
/*
 * Access to YB-stored system catalogs (mirroring API from genam.c)
 * We ignore the index id and always do a regular YugaByte scan (Postgres
 * would do either heap scan or index scan depending on the params).
 */
extern SysScanDesc ybc_systable_beginscan(Relation relation,
										  Oid indexId,
										  bool indexOK,
										  Snapshot snapshot,
										  int nkeys,
										  ScanKey key);
extern HeapTuple ybc_systable_getnext(SysScanDesc scanDesc);
extern void ybc_systable_endscan(SysScanDesc scan_desc);

/*
 * Access to YB-stored system catalogs (mirroring API from heapam.c)
 * We will do a YugaByte scan instead of a heap scan.
 */
extern HeapScanDesc ybc_heap_beginscan(Relation relation,
                                       Snapshot snapshot,
                                       int nkeys,
                                       ScanKey key,
									   bool temp_snap);
extern HeapTuple ybc_heap_getnext(HeapScanDesc scanDesc);
extern void ybc_heap_endscan(HeapScanDesc scanDesc);

/*
 * The ybc_idx API is used to process the following SELECT.
 *   SELECT data FROM heapRelation WHERE rowid IN
 *     ( SELECT rowid FROM indexRelation WHERE key = given_value )
 */
YbScanDesc ybcBeginScan(Relation relation,
                        Relation index,
                        bool xs_want_itup,
                        int nkeys,
                        ScanKey key);

HeapTuple ybc_getnext_heaptuple(YbScanDesc ybScan, bool is_forward_scan, bool *recheck);
IndexTuple ybc_getnext_indextuple(YbScanDesc ybScan, bool is_forward_scan, bool *recheck);

void ybcEndScan(YbScanDesc ybScan);
```

Similarily, index access is defined in [src/include/access/ybcin.h](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/include/access/ybcin.h).

``` c
extern IndexBuildResult *ybcinbuild(Relation heap, Relation index, struct IndexInfo *indexInfo);
extern void ybcinbuildempty(Relation index);
extern bool ybcininsert(Relation rel, Datum *values, bool *isnull, Datum ybctid, Relation heapRel,
						IndexUniqueCheck checkUnique, struct IndexInfo *indexInfo);
extern void ybcindelete(Relation rel, Datum *values, bool *isnull, Datum ybctid, Relation heapRel,
						struct IndexInfo *indexInfo);
extern bool ybcinvalidate(Oid opclassoid);
...
extern IndexScanDesc ybcinbeginscan(Relation rel, int nkeys, int norderbys);
extern void ybcinrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						ScanKey orderbys, int norderbys);
extern bool ybcingettuple(IndexScanDesc scan, ScanDirection dir);
extern void ybcinendscan(IndexScanDesc scan);
```

## Pushdowns
YugaByteDB supports multiple [pushdowns](https://blog.yugabyte.com/5-query-pushdowns-for-distributed-sql-and-how-they-differ-from-a-traditional-rdbms/) during scanning phase to improve performance. 
For example, Yugabyte support for aggregate pushdowns to DocDB for COUNT/MAX/MIN/SUM in this [commit](https://github.com/yugabyte/yugabyte-db/commit/e554bda510cefbe612a00883c0285fe77447039e).

The pushdown happens only if the following conditions are met:
* Outer plan is a YB foreign scan.
* No `WHERE` clause.
* No `ORDER BY`, `GROUP BY`, `DISTINCT`, or `FILTER`.
* Aggregates are one of: `COUNT`/`MIN`/`MAX`/`SUM`.
* Aggregate output type (i.e. transition type) is a supported YB key type and not a postgres internal or numeric type.
* Column type is a supported YB key type.

The pushdown logic is mainly in [src/backend/executor/nodeAgg.c](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/backend/executor/nodeAgg.c).

``` c 
/*
 * Evaluates whether plan supports pushdowns of aggregates to DocDB, and sets
 * yb_pushdown_supported accordingly in AggState.
 */
static void yb_agg_pushdown_supported(AggState *aggstate)
{
	ForeignScanState *scan_state;
	ListCell *lc_agg;
	ListCell *lc_arg;

	/* Initially set pushdown supported to false. */
	aggstate->yb_pushdown_supported = false;

	/* Phase 0 is a dummy phase, so there should be two phases. */
	if (aggstate->numphases != 2)
		return;

	/* Plain agg strategy. */
	if (aggstate->phase->aggstrategy != AGG_PLAIN)
		return;

	/* No GROUP BY. */
	if (aggstate->phase->numsets != 0)
		return;

	/* Foreign scan outer plan. */
	if (!IsA(outerPlanState(aggstate), ForeignScanState))
		return;

	scan_state = castNode(ForeignScanState, outerPlanState(aggstate));

	/* Foreign relation we are scanning is a YB table. */
	if (!IsYBRelationById(scan_state->ss.ss_currentRelation->rd_id))
		return;

	/* No WHERE quals. */
	if (scan_state->ss.ps.qual)
		return;

	foreach(lc_agg, aggstate->aggs)
	{
		AggrefExprState *aggrefstate = (AggrefExprState *) lfirst(lc_agg);
		Aggref *aggref = aggrefstate->aggref;
		char *func_name = get_func_name(aggref->aggfnoid);

		/* Only support COUNT/MIN/MAX/SUM. */
		if (strcmp(func_name, "count") != 0 &&
			strcmp(func_name, "min") != 0 &&
			strcmp(func_name, "max") != 0 &&
			strcmp(func_name, "sum") != 0)
			return;

		/* No ORDER BY. */
		if (list_length(aggref->aggorder) != 0)
			return;

		/* No DISTINCT. */
		if (list_length(aggref->aggdistinct) != 0)
			return;

		/* No FILTER. */
		if (aggref->aggfilter)
			return;

		/* No array arguments. */
		if (aggref->aggvariadic)
			return;

		/* Normal aggregate kind. */
		if (aggref->aggkind != AGGKIND_NORMAL)
			return;

		/* Does not belong to outer plan. */
		if (aggref->agglevelsup != 0)
			return;

		/* Simple split. */
		if (aggref->aggsplit != AGGSPLIT_SIMPLE)
			return;

		/* Aggtranstype is a supported YB key type and is not INTERNAL or NUMERIC. */
		if (!YBCDataTypeIsValidForKey(aggref->aggtranstype) ||
			aggref->aggtranstype == INTERNALOID ||
			aggref->aggtranstype == NUMERICOID)
			return;

		foreach(lc_arg, aggref->args)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc_arg);

			/* Only support simple column expressions until DocDB can eval PG exprs. */
			if (!IsA(tle->expr, Var))
				return;

			/*
			 * Only support types that are allowed to be YB keys as we cannot guarantee
			 * we can safely perform postgres semantic compatible DocDB aggregate evaluation
			 * otherwise.
			 */
			if (!YBCDataTypeIsValidForKey(castNode(Var, tle->expr)->vartype))
				return;
		}
	}

	/* If this is reached, YB pushdown is supported. */
	aggstate->yb_pushdown_supported = true;
}

/*
 * Populates aggregate pushdown information in the YB foreign scan state.
 */
static void yb_agg_pushdown(AggState *aggstate)
{
	ForeignScanState *scan_state = castNode(ForeignScanState, outerPlanState(aggstate));
	List *pushdown_aggs = NIL;
	int aggno;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		Aggref *aggref = aggstate->peragg[aggno].aggref;

		pushdown_aggs = lappend(pushdown_aggs, aggref);
	}
	scan_state->yb_fdw_aggs = pushdown_aggs;
}
```
This [commit](https://github.com/yugabyte/yugabyte-db/commit/05a53869165ac8b9719751234d0eb940da8e58ba) proposed expression pushdowns.

## IsYugaByteEnabled() Flag

YugbytesDb changed Postgres internal logic a lot. To make the code clear, a flag is introduced in [src/include/pg_yb_utils.h](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/include/pg_yb_utils.h) to indicate the logic path for YugaByteDB.

``` c 
/*
 * Checks whether YugaByte functionality is enabled within PostgreSQL.
 * This relies on pgapi being non-NULL, so probably should not be used
 * in postmaster (which does not need to talk to YB backend) or early
 * in backend process initialization. In those cases the
 * YBIsEnabledInPostgresEnvVar function might be more appropriate.
 */
extern bool IsYugaByteEnabled();
```
The flag is set if pggate::PgApiImpl is initialized in [/yb/pggate/ybc_pggate.cc](https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/yql/pggate/ybc_pggate.cc). 

## Caching

Postgres supports the following caches:
* System catalog cache for tuples matching a key: src/backend/utils/cache/catcache.c
* Relation descriptor cache: src/backend/utils/cache/relcache.c
* System cache: src/backend/utils/cache/syscache.c
* Query plan cache: src/backend/utils/cache/plancache.c

The caching logic is updated for YugaByteDB based on the above IsYugaByteEnabled() flag. For example, check the following 
code snippet in catcache.c.

``` c
		/*
		 * Disable negative entries for YugaByte to handle case where the entry
		 * was added by (running a command on) another node.
		 * We also don't support tuple update as of 14/12/2018.
		 */
		if (IsYugaByteEnabled())
		{
			bool allow_negative_entries = cache->id == CASTSOURCETARGET ||
			                              (cache->id == RELNAMENSP &&
			                               DatumGetObjectId(cur_skey[1].sk_argument) ==
			                               PG_CATALOG_NAMESPACE &&
			                               !YBIsPreparingTemplates());
			if (!allow_negative_entries)
			{
				return NULL;
			}
		}

		ct = CatalogCacheCreateEntry(cache, NULL, arguments,
									 hashValue, hashIndex,
									 true);
```

## Sequence 

YugaByteDB enabled sequence support in this [commit](https://github.com/yugabyte/yugabyte-db/commit/c7310c1f23755552252f7ccf6307c148cc903aa0).

The following APIs are introduced in [yb/yql/pggate/ybc_pggate.h](https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/yql/pggate/ybc_pggate.h).

``` c 
YBCStatus YBCInsertSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called);

YBCStatus YBCUpdateSequenceTupleConditionally(int64_t db_oid,
                                              int64_t seq_oid,
                                              uint64_t ysql_catalog_version,
                                              int64_t last_val,
                                              bool is_called,
                                              int64_t expected_last_val,
                                              bool expected_is_called,
                                              bool *skipped);

YBCStatus YBCUpdateSequenceTuple(int64_t db_oid,
                                 int64_t seq_oid,
                                 uint64_t ysql_catalog_version,
                                 int64_t last_val,
                                 bool is_called,
                                 bool* skipped);

YBCStatus YBCReadSequenceTuple(int64_t db_oid,
                               int64_t seq_oid,
                               uint64_t ysql_catalog_version,
                               int64_t *last_val,
                               bool *is_called);

YBCStatus YBCDeleteSequenceTuple(int64_t db_oid, int64_t seq_oid);

```
## Colocated Tables
YugabyteDB supports [colocating SQL tables](https://github.com/yugabyte/yugabyte-db/blob/master/architecture/design/ysql-colocated-tables.md) to avoid creating too many tables with small data sets. 
Colocating tables puts all of their data into a single tablet, called the colocation tablet. Here is the initial [commit](https://github.com/yugabyte/yugabyte-db/commit/6d332c9635edb474b66c5c3de6dba1afb76ef3c8).

## Transactions 

YugaByteDB changed quite a lot of logic in postgres transaction. The primary file is: [src/backend/access/transam/xact.c](https://github.com/yugabyte/yugabyte-db/blob/master/src/postgres/src/backend/access/transam/xact.c).
The commit history for this file is available [here](https://github.com/yugabyte/yugabyte-db/commits/master/src/postgres/src/backend/access/transam/xact.c)

## Parser

The parser and lexer are implemented using the well-known Unix tools [bison](https://www.gnu.org/software/bison/) and [flex](http://dinosaur.compilertools.net/).
The lexer is defined in the file scan.l and the parser grammar is defined in [src/backend/parser/gram.y](https://github.com/postgres/postgres/blob/master/src/backend/parser/gram.y).

The main changes to SQL grammar are the Introduction of 'colocated' key word for colocated tables, for exampmle,

``` SQL
CREATE DATABASE colocation_demo WITH colocated = true;
CREATE TABLE opt_out_1(a int, b int, PRIMARY KEY(a)) WITH (colocated = false);
```

YugaByteDB also added 'SPLIT INTO' clause to specify the number of tablets to be created for the table and 'SPLIT AT VALUES' 
clause to set split points to pre-split range-sharded tables.

``` SQL
CREATE TABLE tracking (id int PRIMARY KEY) SPLIT INTO 10 TABLETS;

CREATE TABLE tbl(
  a int,
  b int,
  primary key(a asc, b desc)
) SPLIT AT VALUES((100), (200), (200, 5));
```

YugaByteDB supports the following statements

```
stmt :
			| AlterDatabaseSetStmt
			| AlterDatabaseStmt
			| AlterDomainStmt
			| AlterObjectSchemaStmt
			| AlterOperatorStmt
			| AlterOpFamilyStmt
			| AlterSeqStmt
			| AlterTableStmt
			| CallStmt
			| CommentStmt
			| ConstraintsSetStmt
			| CopyStmt
			| CreateCastStmt
			| CreateDomainStmt
			| CreateOpFamilyStmt
			| CreateSchemaStmt
			| CreateUserStmt
			| CreatedbStmt
			| DeallocateStmt
			| DefineStmt
			| DeleteStmt
			| DiscardStmt
			| DropCastStmt
			| DropOpFamilyStmt
			| DropStmt
			| DropdbStmt
			| ExecuteStmt
			| ExplainStmt
			| GrantStmt
			| IndexStmt
			| InsertStmt
			| LockStmt
			| PrepareStmt
			| RemoveAggrStmt
			| RemoveOperStmt
			| RenameStmt
			| RevokeStmt
			| RuleStmt
			| SelectStmt
			| TransactionStmt
			| TruncateStmt
			| UpdateStmt
			| VariableResetStmt
			| VariableSetStmt
			| VariableShowStmt
			| ViewStmt
```
The following are Beta features

```
			| AlterExtensionContentsStmt { parser_ybc_beta_feature(@1, "extension"); }
			| AlterExtensionStmt { parser_ybc_beta_feature(@1, "extension"); }
			| AnalyzeStmt { parser_ybc_beta_feature(@1, "analyze"); }
			| CreateFunctionStmt { parser_ybc_beta_feature(@1, "function"); }
			| CreateOpClassStmt { parser_ybc_beta_feature(@1, "opclass"); }
			| CreatePolicyStmt { parser_ybc_beta_feature(@1, "roles"); }
			| DoStmt { parser_ybc_beta_feature(@1, "function"); }
			| DropOpClassStmt { parser_ybc_beta_feature(@1, "opclass"); }
			| RemoveFuncStmt { parser_ybc_beta_feature(@1, "function"); }
			| CreateTrigStmt { parser_ybc_beta_feature(@1, "trigger"); }
			| CreateExtensionStmt { parser_ybc_beta_feature(@1, "extension"); }
			| AlterDefaultPrivilegesStmt { parser_ybc_beta_feature(@1, "roles"); }
			| AlterGroupStmt { parser_ybc_beta_feature(@1, "roles"); }
			| AlterOwnerStmt { parser_ybc_beta_feature(@1, "roles"); }
			| AlterPolicyStmt { parser_ybc_beta_feature(@1, "roles"); }
			| AlterRoleSetStmt { parser_ybc_beta_feature(@1, "roles"); }
			| AlterRoleStmt { parser_ybc_beta_feature(@1, "roles"); }
			| CreateGroupStmt { parser_ybc_beta_feature(@1, "roles"); }
			| CreateRoleStmt { parser_ybc_beta_feature(@1, "roles"); }
			| DropOwnedStmt { parser_ybc_beta_feature(@1, "roles"); }
			| DropRoleStmt { parser_ybc_beta_feature(@1, "roles"); }
			| GrantRoleStmt { parser_ybc_beta_feature(@1, "roles"); }
			| ReassignOwnedStmt { parser_ybc_beta_feature(@1, "roles"); }
			| RevokeRoleStmt { parser_ybc_beta_feature(@1, "roles"); }
			| VacuumStmt { parser_ybc_beta_feature(@1, "vacuum"); }
```
There are quite some SQL grammar that are not supported. Please check the file [src/backend/parser/gram.y](https://github.com/postgres/postgres/blob/master/src/backend/parser/gram.y).
You could also view its commit history [here](https://github.com/yugabyte/yugabyte-db/commits/master/src/postgres/src/backend/parser/gram.y). 

## JIT 

JIT is provided by Postgres natively and YugabyteDB didn't change it in anyway. Here is a [readme](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/backend/jit/README) for it. PostgreSQL, by default, uses LLVM to perform JIT because it has a license compatible with
PostgreSQL, and because its IR can be generated from C using the Clang compiler. Currently expression evaluation and tuple deforming are JITed. To avoid the main PostgreSQL binary directly depending on LLVM, the LLVM dependent code is located in a shared library that is loaded on-demand. To achieve this, code intending to perform JIT (e.g. expression evaluation) calls an LLVM independent wrapper located in jit.c to do so. Lifetimes of JITed functions are managed via JITContext. To be able to generate code that can perform tasks done by "interpreted" PostgreSQL, one small file (llvmjit_types.c) references each of the types required for JITing. When to JIT is affected by cost estimation.

To better understand how the expression is handled at runtime, please see this [readme](https://github.com/futurewei-cloud/chogori-sql/blob/master/src/k2/postgres/src/backend/executor/README). In summary, there are four classes of nodes used in query execution trees: Plan nodes,their corresponding PlanState nodes, Expr nodes, and ExprState nodes. Expression trees, in contrast to Plan trees, are not mirrored into acorresponding tree of state nodes.  Instead each separately executable expression tree. Such a flat representation is usable both for fast interpreted execution and for compiling into native code.

# Control Flow

To better understand how PG integration works in ybd, we like to use the following sequence diagram to illustrate one example operation, i.e., the FetchNext operation in the data scan operation.

![Fetch Next Sequence Diagram](./images/ScanFetchNextSeqDiagram.png)

The process is as follows, some details are ommitted for brevity
* ExecForeignScan(PlanState *pstate) is called during query execution in nodeForeignscan.c, which calls the generic ExecScan() flow in execScan.c.
* execScan.c calls ExecScanFetch() under the hood, which calls the ExecForeignScan() in nodeForeignscan.c
* nodeForeignscan.c calls the DFW ybc_fdw.cc using the method ybcIterateForeignScan(), which calls YBCPgDmlFetch() in ybc_pggate.cc
* the Fetch() in pg_dml.cc is called, which tried to fetch the row from in-memory by calling getNextRow(). If not available, it calls FetchDataFromServer()
to fetch data from tserver.
* the corresponding Fetch() details are defined in pg_doc_op.cc for DocDB access, which associates the Execute() call to a pg_session
* the pg_session in pg_session.cc first checks the tserver count and then fires an AsyncCall by using a RunHelper thread pool.
* the request is handled by yb_op.cc, which made RPC calls to tserver and got response back.
* the RPC data formats are defined in [pgsql_protocol.proto](https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/common/pgsql_protocol.proto).

The DDLs are executed a bit differently since they are running directly against the catalog manager, i.e., the yb-master in YugabyteDB. Be aware that some DDLs
require communicating with the yb-tserver as well to get an idea of how many tablets so as to set up the table splits.
Here we could use the CreateTable DDL command to show how it works by the following diagram.

![Create Table Sequence Diagram](./images/CreateTableSeqDiagram.png)

* when a "create table" command is received, tablecmds.c was modified to forward the command to ybccmds.c to run the method YBCCreateTable().
* ybccmds.c calls YBCPgNewCreateTable() to create schema, CreateTableAddColumns() to add table columns, and YBCPgExecCreateTable() to actually create table on ybc_pggate.cc.
* ybc_pggate.cc calls NewCreateTable(), CreateTableAddColumn(), and ExecCreateTable() on the PG APIs on pggate.cc
* pggate.cc first creates a DDL statment, adds it to memory context, and caches it. 
* pggate.cc calls pg_ddl.cc to add columns in pg_ddl.cc to update the table schema.
* finally pggate.cc calls table_creator.cc to actually the table, which uses YBClient in client-internal.cc to send the request to yb-master via protobuf RPC.

# Resources 
* Chorgori Platform: https://github.com/futurewei-cloud/chogori-platform
* Distributed PostgreSQL on a Google Spanner Architecture – Query Layer: https://blog.yugabyte.com/distributed-postgresql-on-a-google-spanner-architecture-query-layer/
* Postgres Internals: https://www.postgresql.org/docs/11/internals.html
* Postgres Internal Tutorial Presented by Bruce Momjian: https://www.youtube.com/watch?v=JFh22atXTRQ. Slides: https://momjian.us/main/writings/pgsql/internalpics.pdf
