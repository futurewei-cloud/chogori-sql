/*--------------------------------------------------------------------------------------------------
 *
 * ybc_fdw.c
 *		  Foreign-data wrapper for YugabyteDB.
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
 *		  src/backend/executor/ybc_fdw.c
 *
 *--------------------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include <string.h>

#include "executor/ybc_fdw.h"

/*  TODO see which includes of this block are still needed. */
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/pg_type.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/typcache.h"
#include "utils/fmgroids.h"

/*  K2PG includes. */
#include "commands/dbcommands.h"
#include "catalog/pg_operator.h"
#include "catalog/ybctype.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pggate/pg_gate_api.h"
#include "pg_k2pg_utils.h"
#include "access/ybcam.h"
#include "executor/ybcExpr.h"

#include "utils/resowner_private.h"

#define DEFAULT_COLLATION_OID 100
#define ProcedureRelationId 1255
#define FirstBootstrapObjectId	10000

/* -------------------------------------------------------------------------- */
/*  Planner/Optimizer functions */

typedef struct K2FdwPlanState
{
	/* Bitmap of attribute (column) numbers that we need to fetch from K2. */
	Bitmapset *target_attrs;
	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown subsets.
	 */
	List	   *remote_conds;
	List	   *local_conds;
} K2FdwPlanState;

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type, or
								 * it has default collation that is not
								 * traceable to a foreign Var */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation is non-default and derives from
								 * something other than a foreign Var */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
} foreign_loc_cxt;

typedef struct FDWColumnRef {
	AttrNumber attr_num;
	int attno;
	int attr_typid;
	int atttypmod;
} FDWColumnRef;

typedef struct FDWConstValue
{
	Oid	atttypid;
	Datum value;
	bool is_null;
} FDWConstValue;

typedef struct FDWExprRefValues
{
	Oid opno;  // PG_OPERATOR OID of the operator
	List *column_refs;
	List *const_values;
	ParamListInfo paramLI; // parameters binding information for prepare statements
    bool column_ref_first;
} FDWExprRefValues;

typedef struct FDWOprCond
{
	Oid opno;  // PG_OPERATOR OID of the operator
	FDWColumnRef *ref; // column reference
	FDWConstValue *val; // column value
    bool column_ref_first;
} FDWOprCond;

typedef struct foreign_expr_cxt {
	List *opr_conds;          /* opr conditions */
} foreign_expr_cxt;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct K2FdwExecState
{
	/* The handle for the internal K2PG Select statement. */
	K2PgStatement	handle;
	ResourceOwner	stmt_owner;

	Relation index;

	List *remote_exprs;

	/* Oid of the table being scanned */
	Oid tableOid;

	/* Kept query-plan control to pass it to PgGate during preparation */
	K2PgPrepareParameters prepare_params;

	K2PgExecParameters *exec_params; /* execution control parameters for YugaByte */
	bool is_exec_done; /* Each statement should be executed exactly one time */
} K2FdwExecState;

typedef struct K2FdwScanPlanData
{
	/* The relation where to read data from */
	Relation target_relation;

	int nkeys; // number of keys
	int nNonKeys; // number of non-keys

	/* Primary and hash key columns of the referenced table/relation. */
	Bitmapset *primary_key;

	/* Set of key columns whose values will be used for scanning. */
	Bitmapset *sk_cols;

	// ParamListInfo structures are used to pass parameters into the executor for parameterized plans
	ParamListInfo paramLI;

	/* Description and attnums of the columns to bind */
	TupleDesc bind_desc;
	AttrNumber bind_key_attnums[K2PG_MAX_SCAN_KEYS];
	AttrNumber bind_nonkey_attnums[K2PG_MAX_SCAN_KEYS];
} K2FdwScanPlanData;

typedef K2FdwScanPlanData *K2FdwScanPlan;

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
static bool foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt);

static bool is_foreign_expr(PlannerInfo *root,
				RelOptInfo *baserel,
				Expr *expr);

static void parse_conditions(List *exprs, ParamListInfo paramLI, foreign_expr_cxt *expr_cxt);

static void parse_expr(Expr *node, FDWExprRefValues *ref_values);

static void parse_op_expr(OpExpr *node, FDWExprRefValues *ref_values);

static void parse_var(Var *node, FDWExprRefValues *ref_values);

static void parse_const(Const *node, FDWExprRefValues *ref_values);

static void parse_param(Param *node, FDWExprRefValues *ref_values);

static K2PgExpr build_expr(K2FdwExecState *fdw_state, FDWOprCond *opr_cond);

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else deparse_type_name might incorrectly fail to schema-qualify their names.
 * Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
bool
is_builtin(Oid objectId)
{
	return (objectId < FirstBootstrapObjectId);
}

static bool
is_foreign_expr(PlannerInfo *root,
				RelOptInfo *baserel,
				Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;

	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;

	if (!foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * If the expression has a valid collation that does not arise from a
	 * foreign var, the expression can not be sent over.
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (contain_mutable_functions((Node *) expr))
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (they are "shippable"),
 * and that all collations used in the expression derive from Vars of the
 * foreign table.  Because of the latter, the logic is pretty close to
 * assign_collations_walker() in parse_collate.c, though we can assume here
 * that the given expression is valid.  Note function mutability is not
 * currently considered here.
 */
static bool
foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	foreign_loc_cxt inner_cxt;
	Oid			collation;
	FDWCollateState state;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				/*
				 * If the Var is from the foreign table, we consider its
				 * collation (if any) safe to use.  If it is from another
				 * table, we treat its collation the same way as we would a
				 * Param's collation, i.e. it's not safe for it to have a
				 * non-default collation.
				 */
				if (var->varno == glob_cxt->foreignrel->relid &&
					var->varlevelsup == 0)
				{
					/* Var belongs to foreign table */
					collation = var->varcollid;
					state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
				}
				else
				{
					/* Var belongs to some other table */
					collation = var->varcollid;
					if (var->varcollid != InvalidOid &&
						var->varcollid != DEFAULT_COLLATION_OID)
						return false;

					if (collation == InvalidOid ||
						collation == DEFAULT_COLLATION_OID)
					{
						/*
						 * It's noncollatable, or it's safe to combine with a
						 * collatable foreign Var, so set state to NONE.
						 */
						state = FDW_COLLATE_NONE;
					}
					else
					{
						/*
						 * Do not fail right away, since the Var might appear
						 * in a collation-insensitive context.
						 */
						state = FDW_COLLATE_UNSAFE;
					}
				}
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/*
				 * If the constant has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr.
				 * It's unsafe to send to the remote unless it's used in a
				 * non-collation-sensitive context.
				 */
				collation = c->constcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				/*
				 * Collation rule is same as for Consts and non-foreign Vars.
				 */
				collation = p->paramcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ArrayRef:
			{
				ArrayRef   *ar = (ArrayRef *) node;

				/* Assignment should not be in restrictions. */
				if (ar->refassgnexpr != NULL)
					return false;

				/*
				 * Recurse to remaining subexpressions.  Since the array
				 * subscripts must yield (noncollatable) integers, they won't
				 * affect the inner_cxt state.
				 */
				if (!foreign_expr_walker((Node *) ar->refupperindexpr,
										 glob_cxt, &inner_cxt))
					return false;
				if (!foreign_expr_walker((Node *) ar->reflowerindexpr,
										 glob_cxt, &inner_cxt))
					return false;
				if (!foreign_expr_walker((Node *) ar->refexpr,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * Array subscripting should yield same collation as input,
				 * but for safety use same logic as for function nodes.
				 */
				collation = ar->refcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) fe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If function's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (fe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 fe->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = fe->funccollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
			{
				OpExpr	   *oe = (OpExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Result-collation handling is same as for functions */
				collation = oe->opcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!foreign_expr_walker((Node *) r->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * RelabelType must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a real function).
				 */
				collation = r->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;
				switch (b->boolop)
				{
					case AND_EXPR:
						break;
					case OR_EXPR:  // do not support OR and NOT for now
					case NOT_EXPR:
						return false;
						break;
					default:
						elog(ERROR, "unrecognized boolop: %d", (int) b->boolop);
						return false;
						break;
				}
				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) nt->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *a = (ArrayExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) a->elements,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * ArrayExpr must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a function).
				 */
				collation = a->array_collid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */
				collation = inner_cxt.collation;
				state = inner_cxt.state;

				/* Don't apply exprType() to the list. */
				check_type = false;
			}
			break;
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				ListCell   *lc;

				/* Not safe to pushdown when not in grouping context */
				if (!IS_UPPER_REL(glob_cxt->foreignrel))
					return false;

				/* Only non-split aggregates are pushable. */
				if (agg->aggsplit != AGGSPLIT_SIMPLE)
					return false;

				/*
				 * Recurse to input args. aggdirectargs, aggorder and
				 * aggdistinct are all present in args, so no need to check
				 * their shippability explicitly.
				 */
				foreach(lc, agg->args)
				{
					Node	   *n = (Node *) lfirst(lc);

					/* If TargetEntry, extract the expression from it */
					if (IsA(n, TargetEntry))
					{
						TargetEntry *tle = (TargetEntry *) n;

						n = (Node *) tle->expr;
					}

					if (!foreign_expr_walker(n, glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * For aggorder elements, check whether the sort operator, if
				 * specified, is shippable or not.
				 */
				if (agg->aggorder)
				{
					ListCell   *lc;

					foreach(lc, agg->aggorder)
					{
						SortGroupClause *srt = (SortGroupClause *) lfirst(lc);
						Oid			sortcoltype;
						TypeCacheEntry *typentry;
						TargetEntry *tle;

						tle = get_sortgroupref_tle(srt->tleSortGroupRef,
												   agg->args);
						sortcoltype = exprType((Node *) tle->expr);
						typentry = lookup_type_cache(sortcoltype,
													 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
						/* Check shippability of non-default sort operator. */
						if (srt->sortop != typentry->lt_opr &&
							srt->sortop != typentry->gt_opr /* &&
							!is_shippable(srt->sortop, OperatorRelationId,
										  fpinfo) */)
							return false;
					}
				}

				/* Check aggregate filter */
				if (!foreign_expr_walker((Node *) agg->aggfilter,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If aggregate's input collation is not derived from a
				 * foreign Var, it can't be sent to remote.
				 */
				if (agg->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 agg->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = agg->aggcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

	/*
	 * If result type of given expression is not built-in, it can't be sent to
	 * remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !is_builtin(exprType(node)))
		return false;

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
			case FDW_COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case FDW_COLLATE_SAFE:
				if (collation != outer_cxt->collation)
				{
					/*
					 * Non-default collation always beats default.
					 */
					if (outer_cxt->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						outer_cxt->collation = collation;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Conflict; show state as indeterminate.  We don't
						 * want to "return false" right away, since parent
						 * node might not care about collation.
						 */
						outer_cxt->state = FDW_COLLATE_UNSAFE;
					}
				}
				break;
			case FDW_COLLATE_UNSAFE:
				/* We're still conflicted ... */
				break;
		}
	}

	/* It looks OK */
	return true;
}

static void parse_conditions(List *exprs, ParamListInfo paramLI, foreign_expr_cxt *expr_cxt) {
	elog(DEBUG4, "FDW: parsing %d remote expressions", list_length(exprs));
	ListCell   *lc;
	foreach(lc, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo)) {
			expr = ((RestrictInfo *) expr)->clause;
		}
		elog(DEBUG4, "FDW: parsing expression: %s", nodeToString(expr));
		// parse a single clause
		FDWExprRefValues ref_values;
		ref_values.column_refs = NIL;
		ref_values.const_values = NIL;
		ref_values.paramLI = paramLI;
		parse_expr(expr, &ref_values);
		if (list_length(ref_values.column_refs) == 1 && list_length(ref_values.const_values) == 1) {
			FDWOprCond *opr_cond = (FDWOprCond *)palloc0(sizeof(FDWOprCond));
			opr_cond->opno = ref_values.opno;
			// found a binary condition
			ListCell   *rlc;
			foreach(rlc, ref_values.column_refs) {
				opr_cond->ref = (FDWColumnRef *)lfirst(rlc);
			}

			foreach(rlc, ref_values.const_values) {
				opr_cond->val = (FDWConstValue *)lfirst(rlc);
			}

            opr_cond->column_ref_first = ref_values.column_ref_first;

			expr_cxt->opr_conds = lappend(expr_cxt->opr_conds, opr_cond);
		}
	}
}

static void parse_expr(Expr *node, FDWExprRefValues *ref_values) {
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			parse_var((Var *) node, ref_values);
			break;
		case T_Const:
			parse_const((Const *) node, ref_values);
			break;
		case T_OpExpr:
			parse_op_expr((OpExpr *) node, ref_values);
			break;
		case T_Param:
			parse_param((Param *) node, ref_values);
			break;
		default:
			elog(DEBUG4, "FDW: unsupported expression type for expr: %s", nodeToString(node));
			break;
	}
}

static void parse_op_expr(OpExpr *node, FDWExprRefValues *ref_values) {
	if (list_length(node->args) != 2) {
		elog(DEBUG4, "FDW: we only handle binary opclause, actual args length: %d for node %s", list_length(node->args), nodeToString(node));
		return;
	} else {
		elog(DEBUG4, "FDW: handing binary opclause for node %s", nodeToString(node));
	}

	ListCell *lc;
	switch (get_oprrest(node->opno))
	{
		case F_EQSEL: //  equal =
		case F_SCALARLTSEL: // Less than <
		case F_SCALARLESEL: // Less Equal <=
		case F_SCALARGTSEL: // Greater than >
		case F_SCALARGESEL: // Greater Equal >=
			elog(DEBUG4, "FDW: parsing OpExpr: %d", get_oprrest(node->opno));

            // Creating the FDWExprRefValues loses the tree structure of the original expression
            // so we need to keep track if the column reference or the constant was first
            bool checkOrder = true;

			ref_values->opno = node->opno;
			foreach(lc, node->args)
			{
				Expr *arg = (Expr *) lfirst(lc);
				parse_expr(arg, ref_values);

                if (checkOrder && list_length(ref_values->column_refs) == 1) {
                    ref_values->column_ref_first = true;
                } else if (checkOrder) {
                    ref_values->column_ref_first = false;
                }
                checkOrder = false;
			}

			break;
		default:
			elog(DEBUG4, "FDW: unsupported OpExpr type: %d", get_oprrest(node->opno));
			break;
	}
}

static void parse_var(Var *node, FDWExprRefValues *ref_values) {
	elog(DEBUG4, "FDW: parsing Var %s", nodeToString(node));
	// the condition is at the current level
	if (node->varlevelsup == 0) {
		FDWColumnRef *col_ref = (FDWColumnRef *)palloc0(sizeof(FDWColumnRef));
		col_ref->attno = node->varno;
		col_ref->attr_num = node->varattno;
		col_ref->attr_typid = node->vartype;
		col_ref->atttypmod = node->vartypmod;
		ref_values->column_refs = lappend(ref_values->column_refs, col_ref);
	}
}

static void parse_const(Const *node, FDWExprRefValues *ref_values) {
	elog(DEBUG4, "FDW: parsing Const %s", nodeToString(node));
	FDWConstValue *val = (FDWConstValue *)palloc0(sizeof(FDWConstValue));
	val->atttypid = node->consttype;
	val->is_null = node->constisnull;

	val->value = 0;
	if (node->constisnull || node->constbyval)
		val->value = node->constvalue;
	else
		val->value = PointerGetDatum(node->constvalue);

	ref_values->const_values = lappend(ref_values->const_values, val);
}

static void parse_param(Param *node, FDWExprRefValues *ref_values) {
	elog(DEBUG4, "FDW: parsing Param %s", nodeToString(node));
	ParamExternData *prm = NULL;
	ParamExternData prmdata;
	if (ref_values->paramLI->paramFetch != NULL)
		prm = ref_values->paramLI->paramFetch(ref_values->paramLI, node->paramid,
				true, &prmdata);
	else
		prm = &ref_values->paramLI->params[node->paramid - 1];

	if (!OidIsValid(prm->ptype) ||
		prm->ptype != node->paramtype ||
		!(prm->pflags & PARAM_FLAG_CONST))
	{
		/* Planner should ensure this does not happen */
		elog(ERROR, "Invalid parameter: %s", nodeToString(node));
	}

	FDWConstValue *val = (FDWConstValue *)palloc0(sizeof(FDWConstValue));
	val->atttypid = prm->ptype;
	val->is_null = prm->isnull;
	int16		typLen = 0;
	bool		typByVal = false;
	val->value = 0;

	get_typlenbyval(node->paramtype, &typLen, &typByVal);
	if (prm->isnull || typByVal)
		val->value = prm->value;
	else
		val->value = datumCopy(prm->value, typByVal, typLen);

	ref_values->const_values = lappend(ref_values->const_values, val);
}

static void K2AddAttributeColumn(K2FdwScanPlan scan_plan, AttrNumber attnum)
{
  const int idx = K2PgAttnumToBmsIndex(scan_plan->target_relation, attnum);

  if (bms_is_member(idx, scan_plan->primary_key))
    scan_plan->sk_cols = bms_add_member(scan_plan->sk_cols, idx);
}

/*
 * Checks if an attribute is a hash or primary key column and note it in
 * the scan plan.
 */
static void K2CheckPrimaryKeyAttribute(K2FdwScanPlan      scan_plan,
										K2PgTableDesc  k2pg_table_desc,
										AttrNumber      attnum)
{
	bool is_primary = false;
	bool is_hash    = false;

	/*
	 * - Primary key indicator: IndexRelation->rd_index->indisprimary
	 * - Number of key columns: IndexRelation->rd_index->indnkeyatts
	 * - Number of all columns: IndexRelation->rd_index->indnatts
	 * - Hash, range, etc: IndexRelation->rd_indoption (Bits INDOPTION_HASH, RANGE, etc)
	 */
	HandleK2PgTableDescStatus(PgGate_GetColumnInfo(k2pg_table_desc,
											   attnum,
											   &is_primary,
											   &is_hash), k2pg_table_desc);

	int idx = K2PgAttnumToBmsIndex(scan_plan->target_relation, attnum);

	if (is_hash || is_primary)
	{
		scan_plan->primary_key = bms_add_member(scan_plan->primary_key, idx);
		scan_plan->sk_cols = bms_add_member(scan_plan->sk_cols, idx);
		scan_plan->bind_key_attnums[scan_plan->nkeys] = attnum;
		scan_plan->nkeys++;
	} else {
		scan_plan->bind_nonkey_attnums[scan_plan->nNonKeys] = attnum;
		scan_plan->nNonKeys++;
	}
}

/*
 * Get k2Sql-specific table metadata and load it into the scan_plan.
 * Currently only the hash and primary key info.
 */
static void K2LoadTableInfo(Relation relation, K2FdwScanPlan scan_plan)
{
	Oid            dboid          = K2PgGetDatabaseOid(relation);
	Oid            relid          = RelationGetRelid(relation);
	K2PgTableDesc k2pg_table_desc = NULL;

	HandleK2PgStatus(PgGate_GetTableDesc(dboid, relid, &k2pg_table_desc));

	scan_plan->nkeys = 0;
	scan_plan->nNonKeys = 0;
	// number of attributes in the relation tuple
	for (AttrNumber attnum = 1; attnum <= relation->rd_att->natts; attnum++)
	{
		K2CheckPrimaryKeyAttribute(scan_plan, k2pg_table_desc, attnum);
	}
	// we generate OIDs for rows of relation
	if (relation->rd_rel->relhasoids)
	{
		K2CheckPrimaryKeyAttribute(scan_plan, k2pg_table_desc, ObjectIdAttributeNumber);
	}
}

static Oid k2_get_atttypid(TupleDesc bind_desc, AttrNumber attnum)
{
	Oid	atttypid;

	if (attnum > 0)
	{
		/* Get the type from the description */
		atttypid = TupleDescAttr(bind_desc, attnum - 1)->atttypid;
	}
	else
	{
		/* This must be an OID column. */
		atttypid = OIDOID;
	}

  return atttypid;
}

// search for the column in the equal conditions, the performance is fine for small number of equal conditions
static List *findOprCondition(foreign_expr_cxt context, int attr_num) {
	List * result = NIL;
	ListCell *lc = NULL;
	foreach (lc, context.opr_conds) {
		FDWOprCond *first = (FDWOprCond *) lfirst(lc);
		if (first->ref->attr_num == attr_num) {
			result = lappend(result, first);
		}
	}

	return result;
}

K2PgExpr build_expr(K2FdwExecState *fdw_state, FDWOprCond *opr_cond) {
	K2PgExpr opr_expr = NULL;
	const K2PgTypeEntity *type_ent = K2PgFindTypeEntity(BYTEAOID);
	char *opr_name = NULL;

    // K2PgEpxr and FDWOprCond separate column refs and constant literals without keeping the
    // structure of the expression, so we need to switch the direction of the comparison if the
    // column reference was not first in the original expression.
	switch(get_oprrest(opr_cond->opno)) {
		case F_EQSEL: //  equal =
			opr_name = "=";
			break;
		case F_SCALARLTSEL: // Less than <
			opr_name = opr_cond->column_ref_first ? "<" : ">";
			break;
		case F_SCALARLESEL: // Less Equal <=
			opr_name = opr_cond->column_ref_first ? "<=" : ">=";
			break;
		case F_SCALARGTSEL: // Greater than >
			opr_name = opr_cond->column_ref_first ? ">" : "<";
			break;
		case F_SCALARGESEL: // Greater Euqal >=
			opr_name = opr_cond->column_ref_first ? ">=" : "<=";
			break;
		default:
			elog(DEBUG4, "FDW: unsupported OpExpr type: %d", opr_cond->opno);
			return opr_expr;
	}

    // Check for types we support for filter pushdown
    // We pushdown: basic scalar types (int, float, bool),
    // text and string types, and all PG internal types that map to K2 scalar types
	const K2PgTypeEntity *ref_type = K2PgFindTypeEntity(opr_cond->ref->attr_typid);
    switch (opr_cond->ref->attr_typid) {
        case CHAROID:
        case NAMEOID:
        case TEXTOID:
        case VARCHAROID:
        case CSTRINGOID:
            break;
        default:
            if (ref_type->k2pg_type == K2SQL_DATA_TYPE_BINARY || ref_type->k2pg_type == K2SQL_DATA_TYPE_STRING) {
                return opr_expr;
            }
            break;
    }

	PgGate_NewOperator(fdw_state->handle,  opr_name, type_ent, &opr_expr);
	K2PgTypeAttrs ref_type_attrs = { opr_cond->ref->atttypmod};
	K2PgExpr col_ref = K2PgNewColumnRef(fdw_state->handle, opr_cond->ref->attr_num, opr_cond->ref->attr_typid, &ref_type_attrs);
	PgGate_OperatorAppendArg(opr_expr, col_ref);
	K2PgExpr val = K2PgNewConstant(fdw_state->handle, opr_cond->val->atttypid, opr_cond->val->value, opr_cond->val->is_null);
	PgGate_OperatorAppendArg(opr_expr, val);
	return opr_expr;
}

static void K2BindScanKeys(Relation relation,
							K2FdwExecState *fdw_state,
							K2FdwScanPlan scan_plan) {
	if (list_length(fdw_state->remote_exprs) == 0) {
		elog(DEBUG4, "FDW: No remote exprs to bind keys for relation: %d", relation->rd_id);
		return;
	}

	foreign_expr_cxt context;
	context.opr_conds = NIL;

	parse_conditions(fdw_state->remote_exprs, scan_plan->paramLI, &context);
	elog(DEBUG4, "FDW: found %d opr_conds from %d remote exprs for relation: %d", list_length(context.opr_conds), list_length(fdw_state->remote_exprs), relation->rd_id);
	if (list_length(context.opr_conds) == 0) {
		elog(DEBUG4, "FDW: No Opr conditions are found to bind keys for relation: %d", relation->rd_id);
		return;
	}

	const K2PgTypeEntity *type_ent = K2PgFindTypeEntity(BYTEAOID);
	K2PgExpr range_conds = NULL;
	// Top level should be an "AND" node
	PgGate_NewOperator(fdw_state->handle,  "and", type_ent, &range_conds);

	/* Bind the scan keys */
	for (int i = 0; i < scan_plan->nkeys; i++)
	{
		int idx = K2PgAttnumToBmsIndex(relation, scan_plan->bind_key_attnums[i]);
		if (bms_is_member(idx, scan_plan->sk_cols))
		{
			// check if the key is in the Opr conditions
			List *opr_conds = findOprCondition(context, scan_plan->bind_key_attnums[i]);
			if (opr_conds != NIL) {
				elog(DEBUG4, "FDW: binding key with attr_num %d for relation: %d", scan_plan->bind_key_attnums[i], relation->rd_id);
				ListCell *lc = NULL;
				foreach (lc, opr_conds) {
					FDWOprCond *opr_cond = (FDWOprCond *) lfirst(lc);
					K2PgExpr arg = build_expr(fdw_state, opr_cond);
					if (arg != NULL) {
						// use primary keys as range condition
						PgGate_OperatorAppendArg(range_conds, arg);
					}
				}
			}
		}
	}

	HandleK2PgStatusWithOwner(PgGate_DmlBindRangeConds(fdw_state->handle, range_conds),
														fdw_state->handle,
														fdw_state->stmt_owner);

	K2PgExpr where_conds = NULL;
	// Top level should be an "AND" node
	PgGate_NewOperator(fdw_state->handle,  "and", type_ent, &where_conds);
	// bind non-keys
	for (int i = 0; i < scan_plan->nNonKeys; i++)
	{
		// check if the column is in the Opr conditions
		List *opr_conds = findOprCondition(context, scan_plan->bind_nonkey_attnums[i]);
		if (opr_conds != NIL) {
			elog(DEBUG4, "FDW: binding key with attr_num %d for relation: %d", scan_plan->bind_nonkey_attnums[i], relation->rd_id);
			ListCell *lc = NULL;
			foreach (lc, opr_conds) {
				FDWOprCond *opr_cond = (FDWOprCond *) lfirst(lc);
				K2PgExpr arg = build_expr(fdw_state, opr_cond);
				if (arg != NULL) {
					PgGate_OperatorAppendArg(where_conds, arg);
				}
			}
		}
	}

	HandleK2PgStatusWithOwner(PgGate_DmlBindWhereConds(fdw_state->handle, where_conds),
														fdw_state->handle,
														fdw_state->stmt_owner);
}

/*
 * k2GetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
k2GetForeignRelSize(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid)
{
	K2FdwPlanState		*fdw_plan = NULL;

	fdw_plan = (K2FdwPlanState *) palloc0(sizeof(K2FdwPlanState));

	/* Set the estimate for the total number of rows (tuples) in this table. */
	baserel->tuples = K2PG_DEFAULT_NUM_ROWS;

	/*
	 * Initialize the estimate for the number of rows returned by this query.
	 * This does not yet take into account the restriction clauses, but it will
	 * be updated later by camIndexCostEstimate once it inspects the clauses.
	 */
	baserel->rows = baserel->tuples;

	baserel->fdw_private = (void *) fdw_plan;
	fdw_plan->remote_conds = NIL;
	fdw_plan->local_conds = NIL;

	ListCell   *lc;
	elog(DEBUG4, "FDW: k2GetForeignRelSize %d base restrictinfos for relation %d", list_length(baserel->baserestrictinfo), baserel->relid);

	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
		elog(DEBUG4, "FDW: classing baserestrictinfo: %s", nodeToString(ri));
		if (is_foreign_expr(root, baserel, ri->clause))
			fdw_plan->remote_conds = lappend(fdw_plan->remote_conds, ri);
		else
			fdw_plan->local_conds = lappend(fdw_plan->local_conds, ri);
	}
	elog(DEBUG4, "FDW: classified %d remote_conds, %d local_conds", list_length(fdw_plan->remote_conds), list_length(fdw_plan->local_conds));

	/*
	 * Test any indexes of rel for applicability also.
	 */
	check_index_predicates(root, baserel);
}

/*
 * k2GetForeignPaths
 *		Create possible access paths for a scan on the foreign table, which is
 *      the full table scan plus available index paths (including the  primary key
 *      scan path if any).
 */
static void
k2GetForeignPaths(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid)
{
	Cost startup_cost;
	Cost total_cost;

	/* Estimate costs */
	camCostEstimate(baserel, K2PG_FULL_SCAN_SELECTIVITY,
	                false /* is_backwards scan */,
	                false /* is_uncovered_idx_scan */,
	                &startup_cost, &total_cost);

	/* Create a ForeignPath node and it as the scan path */
	add_path(baserel,
	         (Path *) create_foreignscan_path(root,
	                                          baserel,
	                                          NULL, /* default pathtarget */
	                                          baserel->rows,
	                                          startup_cost,
	                                          total_cost,
	                                          NIL,  /* no pathkeys */
	                                          NULL, /* no outer rel either */
	                                          NULL, /* no extra plan */
	                                          NULL  /* no options yet */ ));

	/* Add primary key and secondary index paths also */
	create_index_paths(root, baserel);
}

/*
 * k2GetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
k2GetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
				  List *scan_clauses,
				  Plan *outer_plan)
{
	K2FdwPlanState *fdw_plan_state = (K2FdwPlanState *) baserel->fdw_private;
	Index          scan_relid;
	ListCell       *lc;
	List	   *local_exprs = NIL;
	List	   *remote_exprs = NIL;

	elog(DEBUG4, "FDW: fdw_private %d remote_conds and %d local_conds for foreign relation %d",
			list_length(fdw_plan_state->remote_conds), list_length(fdw_plan_state->local_conds), foreigntableid);

	if (IS_SIMPLE_REL(baserel))
	{
		scan_relid     = baserel->relid;
		/*
		* Separate the restrictionClauses into those that can be executed remotely
		* and those that can't.  baserestrictinfo clauses that were previously
		* determined to be safe or unsafe are shown in fpinfo->remote_conds and
		* fpinfo->local_conds.  Anything else in the restrictionClauses list will
		* be a join clause, which we have to check for remote-safety.
		*/
		elog(DEBUG4, "FDW: GetForeignPlan with %d scan_clauses for simple relation %d", list_length(scan_clauses), scan_relid);
		foreach(lc, scan_clauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
			elog(DEBUG4, "FDW: classifying scan_clause: %s", nodeToString(rinfo));

			/* Ignore pseudoconstants, they are dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fdw_plan_state->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fdw_plan_state->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (is_foreign_expr(root, baserel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}
		elog(DEBUG4, "FDW: classified %d scan_clauses for relation %d: remote_exprs: %d, local_exprs: %d",
				list_length(scan_clauses), scan_relid, list_length(remote_exprs), list_length(local_exprs));
	}
	else
	{
		/*
		 * Join relation or upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;
		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fdw_plan_state->remote_conds, false);
		local_exprs = extract_actual_clauses(fdw_plan_state->local_conds, false);
	}

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Get the target columns that need to be retrieved from YugaByte */
	foreach(lc, baserel->reltarget->exprs)
	{
		Expr *expr = (Expr *) lfirst(lc);
		pull_varattnos_min_attr((Node *) expr,
		                        baserel->relid,
		                        &fdw_plan_state->target_attrs,
		                        baserel->min_attr);
	}

	foreach(lc, scan_clauses)
	{
		Expr *expr = (Expr *) lfirst(lc);
		pull_varattnos_min_attr((Node *) expr,
		                        baserel->relid,
		                        &fdw_plan_state->target_attrs,
		                        baserel->min_attr);
	}

	/* Set scan targets. */
	List *target_attrs = NULL;
	bool wholerow = false;
	for (AttrNumber attnum = baserel->min_attr; attnum <= baserel->max_attr; attnum++)
	{
		int bms_idx = attnum - baserel->min_attr + 1;
		if (wholerow || bms_is_member(bms_idx, fdw_plan_state->target_attrs))
		{
			switch (attnum)
			{
				case InvalidAttrNumber:
					/*
					 * Postgres repurposes InvalidAttrNumber to represent the "wholerow"
					 * junk attribute.
					 */
					wholerow = true;
					break;
				case SelfItemPointerAttributeNumber:
				case MinTransactionIdAttributeNumber:
				case MinCommandIdAttributeNumber:
				case MaxTransactionIdAttributeNumber:
				case MaxCommandIdAttributeNumber:
					ereport(ERROR,
					        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(
							        "System column with id %d is not supported yet",
							        attnum)));
					break;
				case TableOidAttributeNumber:
					/* Nothing to do in YugaByte: Postgres will handle this. */
					break;
				case ObjectIdAttributeNumber:
				case K2PgTupleIdAttributeNumber:
				default: /* Regular column: attrNum > 0*/
				{
					TargetEntry *target = makeNode(TargetEntry);
					target->resno = attnum;
					target_attrs = lappend(target_attrs, target);
				}
			}
		}
	}

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,  /* target list */
	                        scan_clauses,  /* ideally we should use local_exprs here, still use the whole list in case the FDW cannot process some remote exprs*/
	                        scan_relid,
	                        remote_exprs,    /* expressions K2 may evaluate */
	                        target_attrs,  /* fdw_private data for K2 */
	                        NIL,    /* custom K2 target list (none for now) */
	                        NIL,    /* custom K2 target list (none for now) */
	                        outer_plan);
}

/* ------------------------------------------------------------------------- */
/*  Scanning functions */

/*
 * k2BeginForeignScan
 *		Initiate access to the K2PG by allocating a Select handle.
 */
static void
k2BeginForeignScan(ForeignScanState *node, int eflags)
{
	EState      *estate      = node->ss.ps.state;
	Relation    relation     = node->ss.ss_currentRelation;
	ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;

	K2FdwExecState *k2pg_state = NULL;

	/* Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL. */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Allocate and initialize K2PG scan state. */
	k2pg_state = (K2FdwExecState *) palloc0(sizeof(K2FdwExecState));

	node->fdw_state = (void *) k2pg_state;
	HandleK2PgStatus(PgGate_NewSelect(K2PgGetDatabaseOid(relation),
				   RelationGetRelid(relation),
				   NULL /* prepare_params */,
				   &k2pg_state->handle));
	ResourceOwnerEnlargeYugaByteStmts(CurrentResourceOwner);
	ResourceOwnerRememberYugaByteStmt(CurrentResourceOwner, k2pg_state->handle);
	k2pg_state->stmt_owner = CurrentResourceOwner;
	k2pg_state->exec_params = &estate->k2pg_exec_params;
	k2pg_state->remote_exprs = foreignScan->fdw_exprs;
	elog(DEBUG4, "FDW: foreign_scan for relation %d, fdw_exprs: %d", relation->rd_id, list_length(foreignScan->fdw_exprs));

	k2pg_state->exec_params->rowmark = -1;
	ListCell   *l;
	foreach(l, estate->es_rowMarks) {
		ExecRowMark *erm = (ExecRowMark *) lfirst(l);
		// Do not propogate non-row-locking row marks.
		if (erm->markType != ROW_MARK_REFERENCE &&
			erm->markType != ROW_MARK_COPY)
			k2pg_state->exec_params->rowmark = erm->markType;
		break;
	}

	k2pg_state->is_exec_done = false;

	/* Set the current syscatalog version (will check that we are up to date) */
	HandleK2PgStatusWithOwner(PgGate_SetCatalogCacheVersion(k2pg_state->handle,
														k2pg_catalog_cache_version),
														k2pg_state->handle,
														k2pg_state->stmt_owner);
}

/*
 * Setup the scan targets (either columns or aggregates).
 */
static void
k2SetupScanTargets(ForeignScanState *node)
{
	EState *estate = node->ss.ps.state;
	ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;
	Relation relation = node->ss.ss_currentRelation;
	K2FdwExecState *k2pg_state = (K2FdwExecState *) node->fdw_state;
	TupleDesc tupdesc = RelationGetDescr(relation);
	ListCell *lc;

	/* Planning function above should ensure target list is set */
	List *target_attrs = foreignScan->fdw_private;

	MemoryContext oldcontext =
		MemoryContextSwitchTo(node->ss.ps.ps_ExprContext->ecxt_per_query_memory);

	/* Set scan targets. */
	if (node->k2pg_fdw_aggs == NIL)
	{
		/* Set non-aggregate column targets. */
		bool has_targets = false;
		foreach(lc, target_attrs)
		{
			TargetEntry *target = (TargetEntry *) lfirst(lc);

			/* For regular (non-system) attribute check if they were deleted */
			Oid   attr_typid  = InvalidOid;
			int32 attr_typmod = 0;
			if (target->resno > 0)
			{
				Form_pg_attribute attr;
				attr = TupleDescAttr(tupdesc, target->resno - 1);
				/* Ignore dropped attributes */
				if (attr->attisdropped)
				{
					continue;
				}
				attr_typid  = attr->atttypid;
				attr_typmod = attr->atttypmod;
			}

			K2PgTypeAttrs type_attrs = {attr_typmod};
			K2PgExpr      expr       = K2PgNewColumnRef(k2pg_state->handle,
														target->resno,
														attr_typid,
														&type_attrs);
			HandleK2PgStatusWithOwner(PgGate_DmlAppendTarget(k2pg_state->handle,
																									 expr),
															k2pg_state->handle,
															k2pg_state->stmt_owner);
			has_targets = true;
		}

		/*
		 * We can have no target columns at this point for e.g. a count(*). For now
		 * we request the first non-dropped column in that case.
		 * TODO look into handling this on YugaByte side.
		 */
		if (!has_targets)
		{
			for (int16_t i = 0; i < tupdesc->natts; i++)
			{
				/* Ignore dropped attributes */
				if (TupleDescAttr(tupdesc, i)->attisdropped)
				{
					continue;
				}

				K2PgTypeAttrs type_attrs = { TupleDescAttr(tupdesc, i)->atttypmod };
				K2PgExpr      expr       = K2PgNewColumnRef(k2pg_state->handle,
															i + 1,
															TupleDescAttr(tupdesc, i)->atttypid,
															&type_attrs);
				HandleK2PgStatusWithOwner(PgGate_DmlAppendTarget(k2pg_state->handle,
																 expr),
											k2pg_state->handle,
											k2pg_state->stmt_owner);
				break;
			}
		}
	}
	else
	{
		/* Set aggregate scan targets. */
		foreach(lc, node->k2pg_fdw_aggs)
		{
			Aggref *aggref = lfirst_node(Aggref, lc);
			char *func_name = get_func_name(aggref->aggfnoid);
			ListCell *lc_arg;
			K2PgExpr op_handle;
			const K2PgTypeEntity *type_entity;

			/* Get type entity for the operator from the aggref. */
			type_entity = K2PgFindTypeEntity(aggref->aggtranstype);

			/* Create operator. */
			HandleK2PgStatusWithOwner(PgGate_NewOperator(k2pg_state->handle,
													 func_name,
													 type_entity,
													 &op_handle),
									k2pg_state->handle,
									k2pg_state->stmt_owner);

			/* Handle arguments. */
			if (aggref->aggstar) {
				/*
				 * Add dummy argument for COUNT(*) case, turning it into COUNT(0).
				 * We don't use a column reference as we want to count rows
				 * even if all column values are NULL.
				 */
				K2PgExpr const_handle;
				PgGate_NewConstant(k2pg_state->handle,
								 type_entity,
								 0 /* datum */,
								 false /* is_null */,
								 &const_handle);
				HandleK2PgStatusWithOwner(PgGate_OperatorAppendArg(op_handle, const_handle),
										k2pg_state->handle,
										k2pg_state->stmt_owner);
			} else {
				/* Add aggregate arguments to operator. */
				foreach(lc_arg, aggref->args)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc_arg);
					if (IsA(tle->expr, Const))
					{
						Const* const_node = castNode(Const, tle->expr);
						/* Already checked by k2_agg_pushdown_supported */
						Assert(const_node->constisnull || const_node->constbyval);

						K2PgExpr const_handle;
						PgGate_NewConstant(k2pg_state->handle,
										 type_entity,
										 const_node->constvalue,
										 const_node->constisnull,
										 &const_handle);
						HandleK2PgStatusWithOwner(PgGate_OperatorAppendArg(op_handle, const_handle),
												k2pg_state->handle,
												k2pg_state->stmt_owner);
					}
					else if (IsA(tle->expr, Var))
					{
						/*
						 * Use original attribute number (varoattno) instead of projected one (varattno)
						 * as projection is disabled for tuples produced by pushed down operators.
						 */
						int attno = castNode(Var, tle->expr)->varoattno;
						Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);
						K2PgTypeAttrs type_attrs = {attr->atttypmod};

						K2PgExpr arg = K2PgNewColumnRef(k2pg_state->handle,
														attno,
														attr->atttypid,
														&type_attrs);
						HandleK2PgStatusWithOwner(PgGate_OperatorAppendArg(op_handle, arg),
												k2pg_state->handle,
												k2pg_state->stmt_owner);
					}
					else
					{
						/* Should never happen. */
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("unsupported aggregate function argument type")));
					}
				}
			}

			/* Add aggregate operator as scan target. */
			HandleK2PgStatusWithOwner(PgGate_DmlAppendTarget(k2pg_state->handle,
														 op_handle),
														 k2pg_state->handle,
														 k2pg_state->stmt_owner);
		}

		/*
		 * Setup the scan slot based on new tuple descriptor for the given targets. This is a dummy
		 * tupledesc that only includes the number of attributes. Switch to per-query memory from
		 * per-tuple memory so the slot persists across iterations.
		 */
		TupleDesc target_tupdesc = CreateTemplateTupleDesc(list_length(node->k2pg_fdw_aggs),
														   false /* hasoid */);
		ExecInitScanTupleSlot(estate, &node->ss, target_tupdesc);
	}
	MemoryContextSwitchTo(oldcontext);
}

/*
 * k2IterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
k2IterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot;
	K2FdwExecState *k2pg_state = (K2FdwExecState *) node->fdw_state;
	bool           has_data   = false;

	/* Execute the select statement one time.
	 * TODO(neil) Check whether YugaByte PgGate should combine Exec() and Fetch() into one function.
	 * - The first fetch from YugaByte PgGate requires a number of operations including allocating
	 *   operators and protobufs. These operations are done by PgGate_ExecSelect() function.
	 * - The subsequent fetches don't need to setup the query with these operations again.
	 */
	if (!k2pg_state->is_exec_done) {
		K2FdwScanPlanData scan_plan;
		memset(&scan_plan, 0, sizeof(scan_plan));

		Relation relation = node->ss.ss_currentRelation;
		scan_plan.target_relation = relation;
		scan_plan.paramLI = node->ss.ps.state->es_param_list_info;
		K2LoadTableInfo(relation, &scan_plan);
		scan_plan.bind_desc = RelationGetDescr(relation);
		K2BindScanKeys(relation, k2pg_state, &scan_plan);

		k2SetupScanTargets(node);
		HandleK2PgStatusWithOwner(PgGate_ExecSelect(k2pg_state->handle, k2pg_state->exec_params),
								k2pg_state->handle,
								k2pg_state->stmt_owner);
		k2pg_state->is_exec_done = true;
	}

	/* Clear tuple slot before starting */
	slot = node->ss.ss_ScanTupleSlot;
	ExecClearTuple(slot);

	TupleDesc       tupdesc = slot->tts_tupleDescriptor;
	Datum           *values = slot->tts_values;
	bool            *isnull = slot->tts_isnull;
	K2PgSysColumns syscols;

	/* Fetch one row. */
	HandleK2PgStatusWithOwner(PgGate_DmlFetch(k2pg_state->handle,
	                                      tupdesc->natts,
	                                      (uint64_t *) values,
	                                      isnull,
	                                      &syscols,
	                                      &has_data),
	                        k2pg_state->handle,
	                        k2pg_state->stmt_owner);

	/* If we have result(s) update the tuple slot. */
	if (has_data)
	{
		if (node->k2pg_fdw_aggs == NIL)
		{
			HeapTuple tuple = heap_form_tuple(tupdesc, values, isnull);
			if (syscols.oid != InvalidOid)
			{
				HeapTupleSetOid(tuple, syscols.oid);
			}

			slot = ExecStoreTuple(tuple, slot, InvalidBuffer, false);

			/* Setup special columns in the slot */
			slot->tts_k2pgctid = PointerGetDatum(syscols.k2pgctid);
		}
		else
		{
			/*
			 * Aggregate results stored in virtual slot (no tuple). Set the
			 * number of valid values and mark as non-empty.
			 */
			slot->tts_nvalid = tupdesc->natts;
			slot->tts_isempty = false;
		}
	}

	return slot;
}

static void
k2FreeStatementObject(K2FdwExecState* k2pg_fdw_exec_state)
{
	/* If k2pg_fdw_exec_state is NULL, we are in EXPLAIN; nothing to do */
	if (k2pg_fdw_exec_state != NULL && k2pg_fdw_exec_state->handle != NULL)
	{
		ResourceOwnerForgetYugaByteStmt(k2pg_fdw_exec_state->stmt_owner,
										k2pg_fdw_exec_state->handle);
		k2pg_fdw_exec_state->handle = NULL;
		k2pg_fdw_exec_state->stmt_owner = NULL;
		k2pg_fdw_exec_state->exec_params = NULL;
		k2pg_fdw_exec_state->is_exec_done = false;
	}
}

/*
 * fileReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
k2ReScanForeignScan(ForeignScanState *node)
{
	K2FdwExecState *k2pg_state = (K2FdwExecState *) node->fdw_state;

	/* Clear (delete) the previous select */
	k2FreeStatementObject(k2pg_state);

	/* Re-allocate and execute the select. */
	k2BeginForeignScan(node, 0 /* eflags */);
}

/*
 * k2EndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
k2EndForeignScan(ForeignScanState *node)
{
	K2FdwExecState *k2pg_state = (K2FdwExecState *) node->fdw_state;
	k2FreeStatementObject(k2pg_state);
}

/* ------------------------------------------------------------------------- */
/*  FDW declaration */

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to YugaByte callback routines.
 */
Datum
k2_fdw_handler()
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize  = k2GetForeignRelSize;
	fdwroutine->GetForeignPaths    = k2GetForeignPaths;
	fdwroutine->GetForeignPlan     = k2GetForeignPlan;
	fdwroutine->BeginForeignScan   = k2BeginForeignScan;
	fdwroutine->IterateForeignScan = k2IterateForeignScan;
	fdwroutine->ReScanForeignScan  = k2ReScanForeignScan;
	fdwroutine->EndForeignScan     = k2EndForeignScan;

	/* TODO: These are optional but we should support them eventually. */
	/* fdwroutine->ExplainForeignScan = ybcExplainForeignScan; */
	/* fdwroutine->AnalyzeForeignTable = ybcAnalyzeForeignTable; */
	/* fdwroutine->IsForeignScanParallelSafe = ybcIsForeignScanParallelSafe; */

	PG_RETURN_POINTER(fdwroutine);
}
