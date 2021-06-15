/*--------------------------------------------------------------------------------------------------
 *
 * ybcModifyTable.c
 *        YB routines to stmt_handle ModifyTable nodes.
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
 * IDENTIFICATION
 *        src/backend/executor/ybcModifyTable.c
 *
 *--------------------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/ybctype.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "nodes/execnodes.h"
#include "commands/dbcommands.h"
#include "executor/tuptable.h"
#include "executor/ybcExpr.h"
#include "executor/ybcModifyTable.h"
#include "miscadmin.h"
#include "catalog/catalog.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_database.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "executor/tuptable.h"
#include "executor/ybcExpr.h"
#include "optimizer/ybcplan.h"

#include "utils/syscache.h"
#include "pggate/pg_gate_api.h"
#include "pg_k2pg_utils.h"

/*
 * Hack to ensure that the next CommandCounterIncrement() will call
 * CommandEndInvalidationMessages(). The result of this call is not
 * needed on the yb side, however the side effects are.
 */
void MarkCurrentCommandUsed() {
	(void) GetCurrentCommandId(true);
}

/*
 * Returns whether a relation's attribute is a real column in the backing
 * YugaByte table. (It implies we can both read from and write to it).
 */
bool IsRealK2PgColumn(Relation rel, int attrNum)
{
	return (attrNum > 0 && !TupleDescAttr(rel->rd_att, attrNum - 1)->attisdropped) ||
	       (rel->rd_rel->relhasoids && attrNum == ObjectIdAttributeNumber);
}

/*
 * Returns whether a relation's attribute is a YB system column.
 */
bool IsK2PgSystemColumn(int attrNum)
{
	return (attrNum == YBRowIdAttributeNumber ||
			attrNum == YBIdxBaseTupleIdAttributeNumber ||
			attrNum == YBUniqueIdxKeySuffixAttributeNumber);
}

/*
 * Returns whether relation is capable of single row execution.
 */
bool K2PgIsSingleRowTxnCapableRel(ResultRelInfo *resultRelInfo)
{
	bool has_triggers = resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->numtriggers > 0;
	bool has_indices = K2PgRelInfoHasSecondaryIndices(resultRelInfo);
	return !has_indices && !has_triggers;
}

/*
 * Get the type ID of a real or virtual attribute (column).
 * Returns InvalidOid if the attribute number is invalid.
 */
static Oid GetTypeId(int attrNum, TupleDesc tupleDesc)
{
	switch (attrNum)
	{
		case SelfItemPointerAttributeNumber:
			return TIDOID;
		case ObjectIdAttributeNumber:
			return OIDOID;
		case MinTransactionIdAttributeNumber:
			return XIDOID;
		case MinCommandIdAttributeNumber:
			return CIDOID;
		case MaxTransactionIdAttributeNumber:
			return XIDOID;
		case MaxCommandIdAttributeNumber:
			return CIDOID;
		case TableOidAttributeNumber:
			return OIDOID;
		default:
			if (attrNum > 0 && attrNum <= tupleDesc->natts)
				return TupleDescAttr(tupleDesc, attrNum - 1)->atttypid;
			else
				return InvalidOid;
	}
}

/*
 * Get primary key columns as bitmap of a table.
 */
static Bitmapset *GetTablePrimaryKey(Relation rel,
									 AttrNumber minattr,
									 bool includeYBSystemColumns)
{
	Oid            dboid         = K2PgGetDatabaseOid(rel);
	Oid            relid         = RelationGetRelid(rel);
	int            natts         = RelationGetNumberOfAttributes(rel);
	Bitmapset      *pkey         = NULL;
	K2PgTableDesc k2pg_tabledesc = NULL;

	/* Get the primary key columns 'pkey' from YugaByte. */
	HandleK2PgStatus(PgGate_GetTableDesc(dboid, relid, &k2pg_tabledesc));
	for (AttrNumber attnum = minattr; attnum <= natts; attnum++)
	{
		if ((!includeYBSystemColumns && !IsRealK2PgColumn(rel, attnum)) ||
			(!IsRealK2PgColumn(rel, attnum) && !IsK2PgSystemColumn(attnum)))
		{
			continue;
		}

		bool is_primary = false;
		bool is_hash    = false;
		HandleK2PgTableDescStatus(PgGate_GetColumnInfo(k2pg_tabledesc,
		                                           attnum,
		                                           &is_primary,
		                                           &is_hash), k2pg_tabledesc);
		if (is_primary)
		{
			pkey = bms_add_member(pkey, attnum - minattr);
		}
	}

	return pkey;
}

/*
 * Get primary key columns as bitmap of a table for real YB columns.
 */
Bitmapset *GetK2PgTablePrimaryKey(Relation rel)
{
	return GetTablePrimaryKey(rel, FirstLowInvalidHeapAttributeNumber + 1 /* minattr */,
							  false /* includeYBSystemColumns */);
}

/*
 * Get primary key columns as bitmap of a table for real and system YB columns.
 */
Bitmapset *GetFullK2PgTablePrimaryKey(Relation rel)
{
	return GetTablePrimaryKey(rel, YBSystemFirstLowInvalidAttributeNumber + 1 /* minattr */,
							  true /* includeYBSystemColumns */);
}

/*
 * Get the ybctid from a YB scan slot for UPDATE/DELETE.
 */
Datum K2PgGetPgTupleIdFromSlot(TupleTableSlot *slot)
{
	/*
	 * Look for ybctid in the tuple first if the slot contains a tuple packed with ybctid.
	 * Otherwise, look for it in the attribute list as a junk attribute.
	 */
	if (slot->tts_tuple != NULL && slot->tts_tuple->t_ybctid != 0)
	{
		return slot->tts_tuple->t_ybctid;
	}

	for (int idx = 0; idx < slot->tts_nvalid; idx++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, idx);
		if (strcmp(NameStr(att->attname), "ybctid") == 0 && !slot->tts_isnull[idx])
		{
			Assert(att->atttypid == BYTEAOID);
			return slot->tts_values[idx];
		}
	}

	return 0;
}

/*
 * Get the ybctid from a tuple.
 *
 * Note that if the relation has a DocDB RowId attribute, this will generate a new RowId value
 * meaning the ybctid will be unique. Therefore you should only use this if the relation has
 * a primary key or you're doing an insert.
 */
Datum K2PgGetPgTupleIdFromTuple(K2PgStatement pg_stmt,
							   Relation rel,
							   HeapTuple tuple,
							   TupleDesc tupleDesc) {
	Bitmapset *pkey = GetFullK2PgTablePrimaryKey(rel);
	AttrNumber minattr = YBSystemFirstLowInvalidAttributeNumber + 1;
	const int nattrs = bms_num_members(pkey);
	K2PgAttrValueDescriptor *attrs =
			(K2PgAttrValueDescriptor*)palloc(nattrs * sizeof(K2PgAttrValueDescriptor));
	uint64_t tuple_id = 0;
	K2PgAttrValueDescriptor *next_attr = attrs;
	int col = -1;
	while ((col = bms_next_member(pkey, col)) >= 0) {
		AttrNumber attnum = col + minattr;
		next_attr->attr_num = attnum;
		/*
		 * Don't need to fill in for the DocDB RowId column, however we still
		 * need to add the column to the statement to construct the ybctid.
		 */
		if (attnum != YBRowIdAttributeNumber) {
			Oid	type_id = (attnum > 0) ?
					TupleDescAttr(tupleDesc, attnum - 1)->atttypid : InvalidOid;

			next_attr->type_entity = K2PgDataTypeFromOidMod(attnum, type_id);
			next_attr->datum = heap_getattr(tuple, attnum, tupleDesc, &next_attr->is_null);
		} else {
			next_attr->datum = 0;
			next_attr->is_null = false;
			next_attr->type_entity = NULL;
		}
		++next_attr;
	}
	HandleK2PgStatus(PgGate_DmlBuildYBTupleId(pg_stmt, attrs, nattrs, &tuple_id));
	pfree(attrs);
	return (Datum)tuple_id;
}

/*
 * Bind ybctid to the statement.
 */
static void K2PgBindTupleId(K2PgStatement pg_stmt, Datum tuple_id) {
	K2PgExpr k2pg_expr = K2PgNewConstant(pg_stmt, BYTEAOID, tuple_id,
										false /* is_null */);
	HandleK2PgStatus(PgGate_DmlBindColumn(pg_stmt, YBTupleIdAttributeNumber, k2pg_expr));
}

/*
 * Check if operation changes a system table, ignore changes during
 * initialization (bootstrap mode).
 */
static bool IsSystemCatalogChange(Relation rel)
{
	return IsSystemRelation(rel) && !IsBootstrapProcessingMode();
}

/*
 * Utility method to execute a prepared write statement.
 * Will handle the case if the write changes the system catalogs meaning
 * we need to increment the catalog versions accordingly.
 */
static void K2PgExecWriteStmt(K2PgStatement k2pg_stmt, Relation rel, int *rows_affected_count)
{
	bool is_syscatalog_change = IsSystemCatalogChange(rel);
	bool modifies_row = false;
	HandleK2PgStatus(PgGate_DmlModifiesRow(k2pg_stmt, &modifies_row));

	/*
	 * If this write may invalidate catalog cache tuples (i.e. UPDATE or DELETE),
	 * or this write may insert into a cached list, we must increment the
	 * cache version so other sessions can invalidate their caches.
	 * NOTE: If this relation caches lists, an INSERT could effectively be
	 * UPDATINGing the list object.
	 */
	bool is_syscatalog_version_change = is_syscatalog_change
			&& (modifies_row || RelationHasCachedLists(rel));

	/* Let the master know if this should increment the catalog version. */
	if (is_syscatalog_version_change)
	{
		HandleK2PgStatus(PgGate_SetIsSysCatalogVersionChange(k2pg_stmt));
	}

	HandleK2PgStatus(PgGate_SetCatalogCacheVersion(k2pg_stmt, k2pg_catalog_cache_version));

	/* Execute the insert. */
	HandleK2PgStatus(PgGate_DmlExecWriteOp(k2pg_stmt, rows_affected_count));

	/*
	 * Optimization to increment the catalog version for the local cache as
	 * this backend is already aware of this change and should update its
	 * catalog caches accordingly (without needing to ask the master).
	 * Note that, since the master catalog version should have been identically
	 * incremented, it will continue to match with the local cache version if
	 * and only if no other master changes occurred in the meantime (i.e. from
	 * other backends).
	 * If changes occurred, then a cache refresh will be needed as usual.
	 */
	if (is_syscatalog_version_change)
	{
		// TODO(shane) also update the shared memory catalog version here.
		k2pg_catalog_cache_version += 1;
	}
}

/*
 * Utility method to insert a tuple into the relation's backing YugaByte table.
 */
static Oid K2PgExecuteInsertInternal(Relation rel,
                                    TupleDesc tupleDesc,
                                    HeapTuple tuple,
                                    bool is_single_row_txn)
{
	Oid            dboid    = K2PgGetDatabaseOid(rel);
	Oid            relid    = RelationGetRelid(rel);
	AttrNumber     minattr  = FirstLowInvalidHeapAttributeNumber + 1;
	int            natts    = RelationGetNumberOfAttributes(rel);
	Bitmapset      *pkey    = GetK2PgTablePrimaryKey(rel);
	K2PgStatement insert_stmt = NULL;
	bool           is_null  = false;

	/* Generate a new oid for this row if needed */
	if (rel->rd_rel->relhasoids)
	{
		if (!OidIsValid(HeapTupleGetOid(tuple)))
			HeapTupleSetOid(tuple, GetNewOid(rel));
	}

	/* Create the INSERT request and add the values from the tuple. */
	HandleK2PgStatus(PgGate_NewInsert(dboid,
	                              relid,
	                              is_single_row_txn,
	                              &insert_stmt));

	/* Get the ybctid for the tuple and bind to statement */
	tuple->t_ybctid = K2PgGetPgTupleIdFromTuple(insert_stmt, rel, tuple, tupleDesc);
	K2PgBindTupleId(insert_stmt, tuple->t_ybctid);

	for (AttrNumber attnum = minattr; attnum <= natts; attnum++)
	{
		/* Skip virtual (system) and dropped columns */
		if (!IsRealK2PgColumn(rel, attnum))
		{
			continue;
		}

		Oid   type_id = GetTypeId(attnum, tupleDesc);
		Datum datum   = heap_getattr(tuple, attnum, tupleDesc, &is_null);

		/* Check not-null constraint on primary key early */
		if (is_null && bms_is_member(attnum - minattr, pkey))
		{
			ereport(ERROR,
			        (errcode(ERRCODE_NOT_NULL_VIOLATION), errmsg(
					        "Missing/null value for primary key column")));
		}

		/* Add the column value to the insert request */
		K2PgExpr k2pg_expr = K2PgNewConstant(insert_stmt, type_id, datum, is_null);
		HandleK2PgStatus(PgGate_DmlBindColumn(insert_stmt, attnum, k2pg_expr));
	}

	/*
	 * For system tables, mark tuple for invalidation from system caches
	 * at next command boundary. Do this now so if there is an error with insert
	 * we will re-query to get the correct state from the master.
	 */
	if (IsCatalogRelation(rel))
	{
		MarkCurrentCommandUsed();
		CacheInvalidateHeapTuple(rel, tuple, NULL);
	}

	/* Execute the insert */
	K2PgExecWriteStmt(insert_stmt, rel, NULL /* rows_affected_count */);

	/* Clean up */
	insert_stmt = NULL;

	return HeapTupleGetOid(tuple);
}

/*
 * Utility method to bind const to column
 */
static void BindColumn(K2PgStatement stmt, int attr_num, Oid type_id, Datum datum, bool is_null)
{
  K2PgExpr expr = K2PgNewConstant(stmt, type_id, datum, is_null);
  HandleK2PgStatus(PgGate_DmlBindColumn(stmt, attr_num, expr));
}

/*
 * Utility method to set keys and value to index write statement
 */
static void PrepareIndexWriteStmt(K2PgStatement stmt,
                                  Relation index,
                                  Datum *values,
                                  bool *isnull,
                                  int natts,
                                  Datum ybbasectid,
                                  bool ybctid_as_value)
{
	TupleDesc tupdesc = RelationGetDescr(index);

	if (ybbasectid == 0)
	{
		ereport(ERROR,
		(errcode(ERRCODE_INTERNAL_ERROR), errmsg(
			"Missing base table ybctid in index write request")));
	}

	bool has_null_attr = false;
	for (AttrNumber attnum = 1; attnum <= natts; ++attnum)
	{
		Oid   type_id = GetTypeId(attnum, tupdesc);
		Datum value   = values[attnum - 1];
		bool  is_null = isnull[attnum - 1];
		has_null_attr = has_null_attr || is_null;
		BindColumn(stmt, attnum, type_id, value, is_null);
	}

	const bool unique_index = index->rd_index->indisunique;

	/*
	 * For unique indexes we need to set the key suffix system column:
	 * - to ybbasectid if at least one index key column is null.
	 * - to NULL otherwise (setting is_null to true is enough).
	 */
	if (unique_index)
		BindColumn(stmt,
		           YBUniqueIdxKeySuffixAttributeNumber,
		           BYTEAOID,
		           ybbasectid,
		           !has_null_attr /* is_null */);

	/*
	 * We may need to set the base ctid column:
	 * - for unique indexes only if we need it as a value (i.e. for inserts)
	 * - for non-unique indexes always (it is a key column).
	 */
	if (ybctid_as_value || !unique_index)
		BindColumn(stmt,
		           YBIdxBaseTupleIdAttributeNumber,
		           BYTEAOID,
		           ybbasectid,
		           false /* is_null */);
}

Oid K2PgExecuteInsert(Relation rel,
                     TupleDesc tupleDesc,
                     HeapTuple tuple)
{
	return K2PgExecuteInsertInternal(rel,
	                                tupleDesc,
	                                tuple,
	                                false /* is_single_row_txn */);
}

Oid K2PgExecuteNonTxnInsert(Relation rel,
						   TupleDesc tupleDesc,
						   HeapTuple tuple)
{
	return K2PgExecuteInsertInternal(rel,
	                                tupleDesc,
	                                tuple,
	                                true /* is_single_row_txn */);
}

Oid K2PgHeapInsert(TupleTableSlot *slot,
				  HeapTuple tuple,
				  EState *estate)
{
	/*
	 * get information on the (current) result relation
	 */
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
	Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;

	if (estate->es_k2pg_is_single_row_modify_txn)
	{
		/*
		 * Try to execute the statement as a single row transaction (rather
		 * than a distributed transaction) if it is safe to do so.
		 * I.e. if we are in a single-statement transaction that targets a
		 * single row (i.e. single-row-modify txn), and there are no indices
		 * or triggers on the target table.
		 */
		return K2PgExecuteNonTxnInsert(resultRelationDesc, slot->tts_tupleDescriptor, tuple);
	}
	else
	{
		return K2PgExecuteInsert(resultRelationDesc, slot->tts_tupleDescriptor, tuple);
	}
}

void K2PgExecuteInsertIndex(Relation index,
						   Datum *values,
						   bool *isnull,
						   Datum ybctid,
						   bool is_backfill)
{
	Assert(index->rd_rel->relkind == RELKIND_INDEX);
	Assert(ybctid != 0);

	Oid            dboid    = K2PgGetDatabaseOid(index);
	Oid            relid    = RelationGetRelid(index);
	K2PgStatement insert_stmt = NULL;

	/* Create the INSERT request and add the values from the tuple. */
	/*
	 * TODO(jason): rename `is_single_row_txn` to something like
	 * `non_distributed_txn` when closing issue #4906.
	 */
	HandleK2PgStatus(PgGate_NewInsert(dboid,
								  relid,
								  is_backfill /* is_single_row_txn */,
								  &insert_stmt));

	PrepareIndexWriteStmt(insert_stmt, index, values, isnull,
						  RelationGetNumberOfAttributes(index),
						  ybctid, true /* ybctid_as_value */);

	/*
	 * For non-unique indexes the primary-key component (base tuple id) already
	 * guarantees uniqueness, so no need to read and check it in DocDB.
	 */
	if (!index->rd_index->indisunique) {
		HandleK2PgStatus(PgGate_InsertStmtSetUpsertMode(insert_stmt));
	}

	/* For index backfill, set write hybrid time to a time in the past.  This
	 * is to guarantee that backfilled writes are temporally before any online
	 * writes. */
	/* TODO(jason): don't hard-code 50. */
	if (is_backfill)
		HandleK2PgStatus(PgGate_InsertStmtSetWriteTime(insert_stmt, 50));

	/* Execute the insert and clean up. */
	K2PgExecWriteStmt(insert_stmt, index, NULL /* rows_affected_count */);
}

bool K2PgExecuteDelete(Relation rel, TupleTableSlot *slot, EState *estate, ModifyTableState *mtstate)
{
	Oid            dboid          = K2PgGetDatabaseOid(rel);
	Oid            relid          = RelationGetRelid(rel);
	K2PgStatement delete_stmt    = NULL;
	bool           isSingleRow    = mtstate->k2pg_mt_is_single_row_update_or_delete;
	Datum          ybctid         = 0;

	/* Create DELETE request. */
	HandleK2PgStatus(PgGate_NewDelete(dboid,
								  relid,
								  estate->es_k2pg_is_single_row_modify_txn,
								  &delete_stmt));

	/*
	 * Look for ybctid. Raise error if ybctid is not found.
	 *
	 * If single row delete, generate ybctid from tuple values, otherwise
	 * retrieve it from the slot.
	 */
	if (isSingleRow)
	{
		HeapTuple tuple = ExecMaterializeSlot(slot);
		ybctid = K2PgGetPgTupleIdFromTuple(delete_stmt,
										  rel,
										  tuple,
										  slot->tts_tupleDescriptor);
	}
	else
	{
		ybctid = K2PgGetPgTupleIdFromSlot(slot);
	}

	if (ybctid == 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg(
					"Missing column ybctid in DELETE request to YugaByte database")));
	}

	/* Bind ybctid to identify the current row. */
	K2PgExpr ybctid_expr = K2PgNewConstant(delete_stmt, BYTEAOID, ybctid,
										   false /* is_null */);
	HandleK2PgStatus(PgGate_DmlBindColumn(delete_stmt, YBTupleIdAttributeNumber, ybctid_expr));

	/* Delete row from foreign key cache */
	HandleK2PgStatus(PgGate_DeleteFromForeignKeyReferenceCache(relid, ybctid));

	/* Execute the statement. */
	int rows_affected_count = 0;
	K2PgExecWriteStmt(delete_stmt, rel, isSingleRow ? &rows_affected_count : NULL);

	/* Cleanup. */
	delete_stmt = NULL;

	return !isSingleRow || rows_affected_count > 0;
}

void K2PgExecuteDeleteIndex(Relation index, Datum *values, bool *isnull, Datum ybctid)
{
  Assert(index->rd_rel->relkind == RELKIND_INDEX);

	Oid            dboid    = K2PgGetDatabaseOid(index);
	Oid            relid    = RelationGetRelid(index);
	K2PgStatement delete_stmt = NULL;

	/* Create the DELETE request and add the values from the tuple. */
	HandleK2PgStatus(PgGate_NewDelete(dboid,
								  relid,
								  false /* is_single_row_txn */,
								  &delete_stmt));

	PrepareIndexWriteStmt(delete_stmt, index, values, isnull,
	                      IndexRelationGetNumberOfKeyAttributes(index),
	                      ybctid, false /* ybctid_as_value */);

	/* Delete row from foreign key cache */
	HandleK2PgStatus(PgGate_DeleteFromForeignKeyReferenceCache(relid, ybctid));

	K2PgExecWriteStmt(delete_stmt, index, NULL /* rows_affected_count */);
}

bool K2PgExecuteUpdate(Relation rel,
					  TupleTableSlot *slot,
					  HeapTuple tuple,
					  EState *estate,
					  ModifyTableState *mtstate,
					  Bitmapset *updatedCols)
{
	TupleDesc      tupleDesc      = slot->tts_tupleDescriptor;
	Oid            dboid          = K2PgGetDatabaseOid(rel);
	Oid            relid          = RelationGetRelid(rel);
	K2PgStatement update_stmt    = NULL;
	bool           isSingleRow    = mtstate->k2pg_mt_is_single_row_update_or_delete;
	Datum          ybctid         = 0;

	/* Create update statement. */
	HandleK2PgStatus(PgGate_NewUpdate(dboid,
								  relid,
								  estate->es_k2pg_is_single_row_modify_txn,
								  &update_stmt));

	/*
	 * Look for ybctid. Raise error if ybctid is not found.
	 *
	 * If single row update, generate ybctid from tuple values, otherwise
	 * retrieve it from the slot.
	 */
	if (isSingleRow)
	{
		ybctid = K2PgGetPgTupleIdFromTuple(update_stmt,
										  rel,
										  tuple,
										  slot->tts_tupleDescriptor);
	}
	else
	{
		ybctid = K2PgGetPgTupleIdFromSlot(slot);
	}

	if (ybctid == 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg(
					"Missing column ybctid in UPDATE request to YugaByte database")));
	}

	/* Bind ybctid to identify the current row. */
	K2PgExpr ybctid_expr = K2PgNewConstant(update_stmt, BYTEAOID, ybctid,
										   false /* is_null */);
	HandleK2PgStatus(PgGate_DmlBindColumn(update_stmt, YBTupleIdAttributeNumber, ybctid_expr));

	/* Assign new values to the updated columns for the current row. */
	tupleDesc = RelationGetDescr(rel);
	bool whole_row = bms_is_member(InvalidAttrNumber, updatedCols);

	ModifyTable *mt_plan = (ModifyTable *) mtstate->ps.plan;
	ListCell* pushdown_lc = list_head(mt_plan->ybPushdownTlist);

	for (int idx = 0; idx < tupleDesc->natts; idx++)
	{
		FormData_pg_attribute *att_desc = TupleDescAttr(tupleDesc, idx);

		AttrNumber attnum = att_desc->attnum;
		int32_t type_id = att_desc->atttypid;
		int32_t type_mod = att_desc->atttypmod;

		/* Skip virtual (system) and dropped columns */
		if (!IsRealK2PgColumn(rel, attnum))
			continue;

		/*
		 * Skip unmodified columns if possible.
		 * Note: we only do this for the single-row case, as otherwise there
		 * might be triggers that modify the heap tuple to set (other) columns
		 * (e.g. using the SPI module functions).
		 */
		int bms_idx = attnum - K2PgGetFirstLowInvalidAttributeNumber(rel);
		if (isSingleRow && !whole_row && !bms_is_member(bms_idx, updatedCols))
			continue;

		/* Assign this attr's value, handle expression pushdown if needed. */
		if (pushdown_lc != NULL &&
		    ((TargetEntry *) lfirst(pushdown_lc))->resno == attnum)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(pushdown_lc);
			Expr *expr = copyObject(tle->expr);
			K2PgExprInstantiateParams(expr, estate->es_param_list_info);

			K2PgExpr k2pg_expr = K2PgNewEvalExprCall(update_stmt, expr, attnum, type_id, type_mod);

			HandleK2PgStatus(PgGate_DmlAssignColumn(update_stmt, attnum, k2pg_expr));

			pushdown_lc = lnext(pushdown_lc);
		}
		else
		{
			bool is_null = false;
			Datum d = heap_getattr(tuple, attnum, tupleDesc, &is_null);
			K2PgExpr k2pg_expr = K2PgNewConstant(update_stmt, type_id,
												d, is_null);

			HandleK2PgStatus(PgGate_DmlAssignColumn(update_stmt, attnum, k2pg_expr));
		}
	}

	/* Execute the statement. */
	int rows_affected_count = 0;
	K2PgExecWriteStmt(update_stmt, rel, isSingleRow ? &rows_affected_count : NULL);

	/* Cleanup. */
	update_stmt = NULL;

	/*
	 * If the relation has indexes, save the ybctid to insert the updated row into the indexes.
	 */
	if (K2PgRelHasSecondaryIndices(rel))
	{
		tuple->t_ybctid = ybctid;
	}

	return !isSingleRow || rows_affected_count > 0;
}

void K2PgDeleteSysCatalogTuple(Relation rel, HeapTuple tuple)
{
	Oid            dboid       = K2PgGetDatabaseOid(rel);
	Oid            relid       = RelationGetRelid(rel);
	K2PgStatement delete_stmt = NULL;

	if (tuple->t_ybctid == 0)
		ereport(ERROR,
		        (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg(
				        "Missing column ybctid in DELETE request to YugaByte database")));

	/* Prepare DELETE statement. */
	HandleK2PgStatus(PgGate_NewDelete(dboid,
								  relid,
								  false /* is_single_row_txn */,
								  &delete_stmt));

	/* Bind ybctid to identify the current row. */
	K2PgExpr ybctid_expr = K2PgNewConstant(delete_stmt, BYTEAOID, tuple->t_ybctid,
										   false /* is_null */);

	/* Delete row from foreign key cache */
	HandleK2PgStatus(PgGate_DeleteFromForeignKeyReferenceCache(relid, tuple->t_ybctid));

	HandleK2PgStatus(PgGate_DmlBindColumn(delete_stmt, YBTupleIdAttributeNumber, ybctid_expr));

	/*
	 * Mark tuple for invalidation from system caches at next command
	 * boundary. Do this now so if there is an error with delete we will
	 * re-query to get the correct state from the master.
	 */
	MarkCurrentCommandUsed();
	CacheInvalidateHeapTuple(rel, tuple, NULL);

	K2PgExecWriteStmt(delete_stmt, rel, NULL /* rows_affected_count */);

	/* Complete execution */
	delete_stmt = NULL;
}

void K2PgUpdateSysCatalogTuple(Relation rel, HeapTuple oldtuple, HeapTuple tuple)
{
	Oid            dboid       = K2PgGetDatabaseOid(rel);
	Oid            relid       = RelationGetRelid(rel);
	TupleDesc      tupleDesc   = RelationGetDescr(rel);
	int            natts       = RelationGetNumberOfAttributes(rel);
	K2PgStatement update_stmt = NULL;

	/* Create update statement. */
	HandleK2PgStatus(PgGate_NewUpdate(dboid,
								  relid,
								  false /* is_single_row_txn */,
								  &update_stmt));

	AttrNumber minattr = FirstLowInvalidHeapAttributeNumber + 1;
	Bitmapset  *pkey   = GetK2PgTablePrimaryKey(rel);

	/* Bind the ybctid to the statement. */
	K2PgBindTupleId(update_stmt, tuple->t_ybctid);

	/* Assign values to the non-primary-key columns to update the current row. */
	for (int idx = 0; idx < natts; idx++)
	{
		AttrNumber attnum = TupleDescAttr(tupleDesc, idx)->attnum;

		/* Skip primary-key columns */
		if (bms_is_member(attnum - minattr, pkey))
		{
			continue;
		}

		bool is_null = false;
		Datum d = heap_getattr(tuple, attnum, tupleDesc, &is_null);
		K2PgExpr k2pg_expr = K2PgNewConstant(update_stmt, TupleDescAttr(tupleDesc, idx)->atttypid,
											d, is_null);
		HandleK2PgStatus(PgGate_DmlAssignColumn(update_stmt, attnum, k2pg_expr));
	}

	/*
	 * Mark old tuple for invalidation from system caches at next command
	 * boundary, and mark the new tuple for invalidation in case we abort.
	 * In case when there is no old tuple, we will invalidate with the
	 * new tuple at next command boundary instead. Do these now so if there
	 * is an error with update we will re-query to get the correct state
	 * from the master.
	 */
	MarkCurrentCommandUsed();
	if (oldtuple)
		CacheInvalidateHeapTuple(rel, oldtuple, tuple);
	else
		CacheInvalidateHeapTuple(rel, tuple, NULL);

	/* Execute the statement and clean up */
	K2PgExecWriteStmt(update_stmt, rel, NULL /* rows_affected_count */);
	update_stmt = NULL;
}

bool
K2PgRelInfoHasSecondaryIndices(ResultRelInfo *resultRelInfo)
{
	return resultRelInfo->ri_NumIndices > 1 ||
			(resultRelInfo->ri_NumIndices == 1 &&
			 !resultRelInfo->ri_IndexRelationDescs[0]->rd_index->indisprimary);
}
