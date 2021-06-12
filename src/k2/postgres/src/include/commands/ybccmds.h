/*--------------------------------------------------------------------------------------------------
 *
 * ybccmds.h
 *	  prototypes for ybccmds.c
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
 * src/include/commands/ybccmds.h
 *
 *--------------------------------------------------------------------------------------------------
 */

#ifndef YBCCMDS_H
#define YBCCMDS_H

#include "access/htup.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"

#include "pggate/pg_gate_api.h"

/* K2 Cluster Fuctions -------------------------------------------------------------------------- */
extern void K2InitPGCluster();

extern void K2FinishInitDB();

/*  Database Functions -------------------------------------------------------------------------- */

extern void K2PgCreateDatabase(
	Oid dboid, const char *dbname, Oid src_dboid, Oid next_oid, bool colocated);

extern void K2PgDropDatabase(Oid dboid, const char *dbname);

extern void K2PgReservePgOids(Oid dboid, Oid next_oid, uint32 count, Oid *begin_oid, Oid *end_oid);

/*  Table Functions ----------------------------------------------------------------------------- */

extern void K2PgCreateTable(CreateStmt *stmt,
						   char relkind,
						   TupleDesc desc,
						   Oid relationId,
						   Oid namespaceId);

extern void K2PgDropTable(Oid relationId);

extern void K2PgTruncateTable(Relation rel);

extern void K2PgCreateIndex(const char *indexName,
						   IndexInfo *indexInfo,
						   TupleDesc indexTupleDesc,
						   int16 *coloptions,
						   Datum reloptions,
						   Oid indexId,
						   Relation rel,
						   OptSplit *split_options,
						   const bool skip_index_backfill);

extern void K2PgDropIndex(Oid relationId);

extern K2PgStatement K2PgPrepareAlterTable(AlterTableStmt* stmt, Relation rel, Oid relationId);

extern void K2PgExecAlterPgTable(K2PgStatement handle, Oid relationId);

extern void K2PgRename(RenameStmt* stmt, Oid relationId);

#endif
