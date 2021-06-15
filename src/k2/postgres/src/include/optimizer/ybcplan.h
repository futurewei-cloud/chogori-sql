/*--------------------------------------------------------------------------------------------------
 *
 * ybcplan.h
 *	  prototypes for ybcScan.c
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
 * src/include/executor/ybcplan.h
 *
 *--------------------------------------------------------------------------------------------------
 */

#ifndef YBCPLAN_H
#define YBCPLAN_H

#include "postgres.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"


void K2PgExprInstantiateParams(Expr* expr, ParamListInfo paramLI);

bool K2PgIsSupportedSingleRowModifyWhereExpr(Expr *expr);

bool K2PgIsSupportedSingleRowModifyAssignExpr(Expr *expr,
                                             AttrNumber target_attno,
                                             bool *needs_pushdown);

bool K2PgIsSingleRowModify(PlannedStmt *pstmt);

bool K2PgIsSingleRowUpdateOrDelete(ModifyTable *modifyTable);

bool K2PgAllPrimaryKeysProvided(Oid relid, Bitmapset *attrs);

#endif // YBCPLAN_H
