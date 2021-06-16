/*--------------------------------------------------------------------------------------------------
 *
 * ybcam.h
 *	  prototypes for ybc/ybcam.c
 *
 * Copyright (c) YugaByte, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 * src/include/executor/ybcam.h
 *
 *--------------------------------------------------------------------------------------------------
 */

#ifndef YBCAM_H
#define YBCAM_H

#include "postgres.h"

#include "skey.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "nodes/relation.h"
#include "utils/catcache.h"
#include "utils/resowner.h"
#include "utils/snapshot.h"

#include "pggate/pg_gate_api.h"
#include "pg_k2pg_utils.h"
#include "executor/ybcExpr.h"

/*
 * SCAN PLAN - Two structures.
 * - "struct YbScanPlanData" contains variables that are used during preparing statement.
 * - "struct K2PgScanDescData" contains variables that are used thru out the life of the statement.
 *
 * K2PG ScanPlan has two different lists.
 *   Binding list:
 *   - This list is used to receive the user-given data for key columns (WHERE clause).
 *   - "sk_attno" is the INDEX's attnum for the key columns that are used to query data.
 *     In K2PG, primary index attnum is the same as column attnum in the UserTable.
 *   - "bind_desc" is the description of the index columns (count, types, ...).
 *   - The bind lists don't need to be included in the description as they are only needed
 *     during setup.
 *   Target list:
 *   - This list identifies the user-wanted data to be fetched from the database.
 *   - "target_desc" contains the description of the target columns (count, types, ...).
 *   - The target fields are used for double-checking key values after selecting data
 *     from database. It should be removed once we no longer need to double-check the key values.
 */
typedef struct K2PgScanDescData
{
#define K2PG_MAX_SCAN_KEYS (INDEX_MAX_KEYS * 2) /* A pair of lower/upper bounds per column max */

	/* The handle for the internal YB Select statement. */
	K2PgStatement handle;
	ResourceOwner stmt_owner;
	bool is_exec_done;

	Relation index;

	int nkeys;
	ScanKey key;

	TupleDesc target_desc;
	AttrNumber target_key_attnums[K2PG_MAX_SCAN_KEYS];

	/* Oid of the table being scanned */
	Oid tableOid;

	/* Kept query-plan control to pass it to PgGate during preparation */
	K2PgPrepareParameters prepare_params;

	/*
	 * Kept execution control to pass it to PgGate.
	 * - When K2PG-index-scan layer is called by Postgres IndexScan functions, it will read the
	 *   "k2pg_exec_params" from Postgres IndexScan and kept the info in this attribute.
	 *
	 * - K2PG-index-scan in-turn will passes this attribute to PgGate to control the index-scan
	 *   execution in YB tablet server.
	 */
	K2PgExecParameters *exec_params;
} K2PgScanDescData;

typedef struct K2PgScanDescData *K2PgScanDesc;

/*
 * Access to YB-stored system catalogs (mirroring API from genam.c)
 * We ignore the index id and always do a regular YugaByte scan (Postgres
 * would do either heap scan or index scan depending on the params).
 */
extern SysScanDesc k2pg_systable_beginscan(Relation relation,
										  Oid indexId,
										  bool indexOK,
										  Snapshot snapshot,
										  int nkeys,
										  ScanKey key);
extern HeapTuple k2pg_systable_getnext(SysScanDesc scanDesc);
extern void k2pg_systable_endscan(SysScanDesc scan_desc);

/*
 * Access to K2-stored system catalogs (mirroring API from heapam.c)
 * We will do a K2 scan instead of a heap scan.
 */
extern HeapScanDesc k2pg_heap_beginscan(Relation relation,
                                       Snapshot snapshot,
                                       int nkeys,
                                       ScanKey key,
									   bool temp_snap);
extern HeapTuple k2pg_heap_getnext(HeapScanDesc scanDesc);
extern void k2pg_heap_endscan(HeapScanDesc scanDesc);

/*
 * The ybc_idx API is used to process the following SELECT.
 *   SELECT data FROM heapRelation WHERE rowid IN
 *     ( SELECT rowid FROM indexRelation WHERE key = given_value )
 */
K2PgScanDesc k2pgBeginScan(Relation relation,
                        Relation index,
                        bool xs_want_itup,
                        int nkeys,
                        ScanKey key);

HeapTuple k2pg_getnext_heaptuple(K2PgScanDesc ybScan, bool is_forward_scan, bool *recheck);
IndexTuple k2pg_getnext_indextuple(K2PgScanDesc ybScan, bool is_forward_scan, bool *recheck);

void k2pgEndScan(K2PgScanDesc ybScan);

/* Number of rows assumed for a YB table if no size estimates exist */
#define K2PG_DEFAULT_NUM_ROWS  1000

#define K2PG_SINGLE_ROW_SELECTIVITY	(1.0 / K2PG_DEFAULT_NUM_ROWS)
#define K2PG_SINGLE_KEY_SELECTIVITY	(10.0 / K2PG_DEFAULT_NUM_ROWS)
#define K2PG_HASH_SCAN_SELECTIVITY	(100.0 / K2PG_DEFAULT_NUM_ROWS)
#define K2PG_FULL_SCAN_SELECTIVITY	1.0

/*
 * For a partial index the index predicate will filter away some rows.
 * TODO: Evaluate this based on the predicate itself and table stats.
 */
#define K2PG_PARTIAL_IDX_PRED_SELECTIVITY 0.8

/*
 * Backwards scans are more expensive in DocDB.
 * TODO: the ysql_backward_prefetch_scale_factor gflag is correlated to this
 * but is too low (1/16 implying 16x slower) to be used here.
 */
#define K2PG_BACKWARDS_SCAN_COST_FACTOR 1.1

/*
 * Uncovered indexes will require extra RPCs to the main table to retrieve the
 * values for all required columns. These requests are now batched in PgGate
 * so the extra cost should be relatively low in general.
 */
#define K2PG_UNCOVERED_INDEX_COST_FACTOR 1.1

extern void k2pgCostEstimate(RelOptInfo *baserel, Selectivity selectivity,
                            bool is_backwards_scan, bool is_uncovered_idx_scan,
							Cost *startup_cost, Cost *total_cost);
extern void k2pgIndexCostEstimate(IndexPath *path, Selectivity *selectivity,
								 Cost *startup_cost, Cost *total_cost);

/*
 * Fetch a single tuple by the k2pgctid.
 */
extern HeapTuple K2PgFetchTuple(Relation relation, Datum k2pgctid);


#endif							/* YBCAM_H */
