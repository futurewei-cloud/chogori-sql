/*--------------------------------------------------------------------------------------------------
 *
 * ybcbootstrap.c
 *        K2PG commands for creating and altering table structures and settings
 *
 * Copyright (c) YugaByte, Inc.
 * Portions Copyright (c) 2021 Futurewei Cloud
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
 *        src/backend/commands/ybcbootstrap.c
 *
 *------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "catalog/pg_attribute.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "catalog/pg_database.h"
#include "commands/ybccmds.h"
#include "catalog/ybctype.h"

#include "catalog/catalog.h"
#include "access/htup_details.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "executor/tuptable.h"
#include "executor/ybcExpr.h"

#include "pggate/pg_gate_api.h"
#include "pg_k2pg_utils.h"

#include "parser/parser.h"

static void K2PgAddSysCatalogColumn(K2PgStatement k2pg_stmt,
								   IndexStmt *pkey_idx,
								   const char *attname,
								   int attnum,
								   Oid type_id,
								   int32 typmod,
								   bool key)
{

	ListCell      *lc;
	bool          is_key    = false;
	const K2PgTypeEntity *col_type  = K2PgDataTypeFromOidMod(attnum, type_id);

	if (pkey_idx)
	{
		foreach(lc, pkey_idx->indexParams)
		{
			IndexElem *elem = lfirst(lc);
			if (strcmp(elem->name, attname) == 0)
			{
				is_key = true;
			}
		}
	}

	/* We will call this twice, first for key columns, then for regular
	 * columns to handle any re-ordering.
	 * So only adding the if matching the is_key property.
	 */
	if (key == is_key)
	{
		HandleK2PgStatus(PgGate_CreateTableAddColumn(k2pg_stmt,
																						 attname,
																						 attnum,
																						 col_type,
																						 false /* is_hash */,
																						 is_key,
																						 false /* is_desc */,
																						 false /* is_nulls_first */));
	}
}

static void K2PgAddSysCatalogColumns(K2PgStatement k2pg_stmt,
									TupleDesc tupdesc,
									IndexStmt *pkey_idx,
									const bool key)
{
	if (tupdesc->tdhasoid)
	{
		/* Add the OID column if the table was declared with OIDs. */
		K2PgAddSysCatalogColumn(k2pg_stmt,
							   pkey_idx,
							   "oid",
							   ObjectIdAttributeNumber,
							   OIDOID,
							   0,
							   key);
	}

	/* Add the rest of the columns. */
	for (int attno = 0; attno < tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno);
		K2PgAddSysCatalogColumn(k2pg_stmt,
							   pkey_idx,
							   attr->attname.data,
							   attr->attnum,
							   attr->atttypid,
							   attr->atttypmod,
							   key);
	}
}

void K2PgCreateSysCatalogTable(const char *table_name,
                              Oid table_oid,
                              TupleDesc tupdesc,
                              bool is_shared_relation,
                              IndexStmt *pkey_idx)
{
	/* Database and schema are fixed when running inidb. */
	Assert(IsBootstrapProcessingMode());
	char           *db_name     = "template1";
	char           *schema_name = "pg_catalog";
	K2PgStatement k2pg_stmt      = NULL;

	HandleK2PgStatus(PgGate_NewCreateTable(db_name,
	                                   schema_name,
	                                   table_name,
	                                   TemplateDbOid,
	                                   table_oid,
	                                   is_shared_relation,
	                                   false, /* if_not_exists */
									   pkey_idx == NULL, /* add_primary_key */
									   true, /* colocated */
	                                   &k2pg_stmt));

	/* Add all key columns first, then the regular columns */
	if (pkey_idx != NULL)
	{
		K2PgAddSysCatalogColumns(k2pg_stmt, tupdesc, pkey_idx, /* key */ true);
	}
	K2PgAddSysCatalogColumns(k2pg_stmt, tupdesc, pkey_idx, /* key */ false);

	HandleK2PgStatus(PgGate_ExecCreateTable(k2pg_stmt));
}
