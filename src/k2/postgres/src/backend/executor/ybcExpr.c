/*--------------------------------------------------------------------------------------------------
 * ybcExpr.c
 *        Routines to construct YBC expression tree.
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
 *        src/backend/executor/ybcExpr.c
 *--------------------------------------------------------------------------------------------------
 */

#include <inttypes.h>

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "parser/parse_type.h"
#include "utils/lsyscache.h"
#include "commands/dbcommands.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/syscache.h"
#include "utils/builtins.h"

#include "pg_k2pg_utils.h"
#include "executor/ybcExpr.h"
#include "catalog/ybctype.h"

K2PgExpr YBCNewColumnRef(K2PgStatement ybc_stmt, int16_t attr_num, int attr_typid,
						  const K2PgTypeAttrs *type_attrs) {
	K2PgExpr expr = NULL;
	const K2PgTypeEntity *type_entity = YBCDataTypeFromOidMod(attr_num, attr_typid);
	HandleK2PgStatus(K2PgNewColumnRef(ybc_stmt, attr_num, type_entity, type_attrs, &expr));
	return expr;
}

K2PgExpr YBCNewConstant(K2PgStatement ybc_stmt, Oid type_id, Datum datum, bool is_null) {
	K2PgExpr expr = NULL;
	const K2PgTypeEntity *type_entity = YBCDataTypeFromOidMod(InvalidAttrNumber, type_id);
	HandleK2PgStatus(K2PgNewConstant(ybc_stmt, type_entity, datum, is_null, &expr));
	return expr;
}

K2PgExpr YBCNewEvalExprCall(K2PgStatement ybc_stmt,
                             Expr *pg_expr,
                             int32_t attno,
                             int32_t typid,
                             int32_t typmod) {
	K2PgExpr ybc_expr = NULL;
	const K2PgTypeEntity *type_ent = YBCDataTypeFromOidMod(InvalidAttrNumber, typid);
	K2PgNewOperator(ybc_stmt, "eval_expr_call", type_ent, &ybc_expr);

	Datum expr_datum = CStringGetDatum(nodeToString(pg_expr));
	K2PgExpr expr = YBCNewConstant(ybc_stmt, CSTRINGOID, expr_datum , /* IsNull */ false);
	K2PgOperatorAppendArg(ybc_expr, expr);

	/*
	 * Adding the column type id and mod to the message since we only have the YQL types in the
	 * DocDB Schema.
	 * TODO(mihnea): Eventually DocDB should know the full YSQL/PG types and we can remove this.
	 */
	K2PgExpr attno_expr = YBCNewConstant(ybc_stmt, INT4OID, (Datum) attno, /* IsNull */ false);
	K2PgOperatorAppendArg(ybc_expr, attno_expr);
	K2PgExpr typid_expr = YBCNewConstant(ybc_stmt, INT4OID, (Datum) typid, /* IsNull */ false);
	K2PgOperatorAppendArg(ybc_expr, typid_expr);
	K2PgExpr typmod_expr = YBCNewConstant(ybc_stmt, INT4OID, (Datum) typmod, /* IsNull */ false);
	K2PgOperatorAppendArg(ybc_expr, typmod_expr);

	return ybc_expr;
}
