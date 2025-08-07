/*-------------------------------------------------------------------------
 *
 * var.c
 *	  Var node manipulation routines
 *
 * Note: for most purposes, PlaceHolderVar is considered a Var too,
 * even if its contained expression is variable-free.  Also, CurrentOfExpr
 * is treated as a Var for purposes of determining whether an expression
 * contains variables.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/var.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/prep.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"
#ifdef __OPENTENBASE_C__
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#endif

typedef struct
{
	Relids		varnos;
	int			sublevels_up;
} pull_varnos_context;

typedef struct
{
	Bitmapset  *varattnos;
	Index		varno;
} pull_varattnos_context;

typedef struct
{
	List	   *vars;
	int			sublevels_up;
} pull_vars_context;

typedef struct
{
	int			var_location;
	int			sublevels_up;
} locate_var_of_level_context;

typedef struct
{
	List	   *varlist;
	int			flags;
} pull_var_clause_context;

typedef struct
{
	List	   *varlist;
	bool		transform_grouping;
} replace_var_clause_context;

#ifdef __OPENTENBASE__
typedef struct
{
	Node	   *src;				/* source node */
	Node	   *dest;				/* destination node */
	bool		recurse_aggref;		/* mark wheather go into Aggref */
	bool		isCopy;				/* mark if copy non-leaf nodes */

	/* mark if replace the first target only or all targets */
	bool		replace_first_only;

	/*
	 * counter of how many targets have been replaced (only available on single
	 * node replace, not for list)
	 */
	uint32		replace_count;
} replace_node_clause_context;

typedef struct
{
    Index varno;       /* Var no. */
    Index varlevelsup; /* Var level up. */
    bool result;
} var_info;
#endif

typedef struct
{
	PlannerInfo *root;
	int			sublevels_up;
	bool		possible_sublink;	/* could aliases include a SubLink? */
	bool		inserted_sublink;	/* have we inserted a SubLink? */
} flatten_join_alias_vars_context;

static bool pull_varnos_walker(Node *node,
				   pull_varnos_context *context);
static bool pull_varattnos_walker(Node *node, pull_varattnos_context *context);
static bool pull_vars_walker(Node *node, pull_vars_context *context);
static bool contain_var_clause_walker(Node *node, void *context);
static bool contain_vars_of_level_walker(Node *node, int *sublevels_up);
static bool locate_var_of_level_walker(Node *node,
						   locate_var_of_level_context *context);
static bool pull_var_clause_walker(Node *node,
					   pull_var_clause_context *context);
static Node *flatten_join_alias_vars_mutator(Node *node,
								flatten_join_alias_vars_context *context);
static Relids alias_relid_set(PlannerInfo *root, Relids relids);

#ifdef __OPENTENBASE__
static bool contain_vars_upper_level_walker(Node *node, int *sublevels_up);
#endif
/*
 * pull_varnos
 *		Create a set of all the distinct varnos present in a parsetree.
 *		Only varnos that reference level-zero rtable entries are considered.
 *
 * NOTE: this is used on not-yet-planned expressions.  It may therefore find
 * bare SubLinks, and if so it needs to recurse into them to look for uplevel
 * references to the desired rtable level!	But when we find a completed
 * SubPlan, we only need to look at the parameters passed to the subplan.
 */
Relids
pull_varnos(Node *node)
{
	pull_varnos_context context;

	context.varnos = NULL;
	context.sublevels_up = 0;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	query_or_expression_tree_walker(node,
									pull_varnos_walker,
									(void *) &context,
									0);

	return context.varnos;
}

/*
 * pull_varnos_of_level
 *		Create a set of all the distinct varnos present in a parsetree.
 *		Only Vars of the specified level are considered.
 */
Relids
pull_varnos_of_level(Node *node, int levelsup)
{
	pull_varnos_context context;

	context.varnos = NULL;
	context.sublevels_up = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	query_or_expression_tree_walker(node,
									pull_varnos_walker,
									(void *) &context,
									0);

	return context.varnos;
}

static bool
pull_varnos_walker(Node *node, pull_varnos_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == context->sublevels_up)
			context->varnos = bms_add_member(context->varnos, var->varno);
		return false;
	}
	if (IsA(node, CurrentOfExpr))
	{
		CurrentOfExpr *cexpr = (CurrentOfExpr *) node;

		if (context->sublevels_up == 0)
			context->varnos = bms_add_member(context->varnos, cexpr->cvarno);
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		/*
		 * A PlaceHolderVar acts as a variable of its syntactic scope, or
		 * lower than that if it references only a subset of the rels in its
		 * syntactic scope.  It might also contain lateral references, but we
		 * should ignore such references when computing the set of varnos in
		 * an expression tree.  Also, if the PHV contains no variables within
		 * its syntactic scope, it will be forced to be evaluated exactly at
		 * the syntactic scope, so take that as the relid set.
		 */
		PlaceHolderVar *phv = (PlaceHolderVar *) node;
		pull_varnos_context subcontext;

		subcontext.varnos = NULL;
		subcontext.sublevels_up = context->sublevels_up;
		(void) pull_varnos_walker((Node *) phv->phexpr, &subcontext);
		if (phv->phlevelsup == context->sublevels_up)
		{
			subcontext.varnos = bms_int_members(subcontext.varnos,
												phv->phrels);
			if (bms_is_empty(subcontext.varnos))
				context->varnos = bms_add_members(context->varnos,
												  phv->phrels);
		}
		context->varnos = bms_join(context->varnos, subcontext.varnos);
		return false;
	}
	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		bool		result;

		context->sublevels_up++;
		result = query_tree_walker((Query *) node, pull_varnos_walker,
								   (void *) context, 0);
		context->sublevels_up--;
		return result;
	}
	return expression_tree_walker(node, pull_varnos_walker,
								  (void *) context);
}


/*
 * pull_varattnos
 *		Find all the distinct attribute numbers present in an expression tree,
 *		and add them to the initial contents of *varattnos.
 *		Only Vars of the given varno and rtable level zero are considered.
 *
 * Attribute numbers are offset by FirstLowInvalidHeapAttributeNumber so that
 * we can include system attributes (e.g., OID) in the bitmap representation.
 *
 * Currently, this does not support unplanned subqueries; that is not needed
 * for current uses.  It will handle already-planned SubPlan nodes, though,
 * looking into only the "testexpr" and the "args" list.  (The subplan cannot
 * contain any other references to Vars of the current level.)
 */
void
pull_varattnos(Node *node, Index varno, Bitmapset **varattnos)
{
	pull_varattnos_context context;

	context.varattnos = *varattnos;
	context.varno = varno;

	(void) pull_varattnos_walker(node, &context);

	*varattnos = context.varattnos;
}

static bool
pull_varattnos_walker(Node *node, pull_varattnos_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varno == context->varno && var->varlevelsup == 0)
			context->varattnos =
				bms_add_member(context->varattnos,
							   var->varattno - FirstLowInvalidHeapAttributeNumber);
		return false;
	}

	/* Should not find an unplanned subquery */
	Assert(!IsA(node, Query));

	return expression_tree_walker(node, pull_varattnos_walker,
								  (void *) context);
}

/*
 * pull_vars_of_level
 *		Create a list of all Vars (and PlaceHolderVars) referencing the
 *		specified query level in the given parsetree.
 *
 * Caution: the Vars are not copied, only linked into the list.
 */
List *
pull_vars_of_level(Node *node, int levelsup)
{
	pull_vars_context context;

	context.vars = NIL;
	context.sublevels_up = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	query_or_expression_tree_walker(node,
									pull_vars_walker,
									(void *) &context,
									0);

	return context.vars;
}

static bool
pull_vars_walker(Node *node, pull_vars_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (context->sublevels_up == -1 || var->varlevelsup == context->sublevels_up)
			context->vars = lappend(context->vars, var);
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;

		if (context->sublevels_up == -1 || phv->phlevelsup == context->sublevels_up)
			context->vars = lappend(context->vars, phv);
		/* we don't want to look into the contained expression */
		return false;
	}
	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		bool		result;

		if(context->sublevels_up != -1)
			context->sublevels_up++;
		
		result = query_tree_walker((Query *) node, pull_vars_walker,
								   (void *) context, 0);
		
		if(context->sublevels_up != -1)
			context->sublevels_up--;
		return result;
	}
	return expression_tree_walker(node, pull_vars_walker,
								  (void *) context);
}


/*
 * contain_var_clause
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  (of the current query level).
 *
 *	  Returns true if any varnode found.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
bool
contain_var_clause(Node *node)
{
	return contain_var_clause_walker(node, NULL);
}

static bool
contain_var_clause_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup == 0)
			return true;		/* abort the tree traversal and return true */
		return false;
	}
	if (IsA(node, CurrentOfExpr))
		return true;
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup == 0)
			return true;		/* abort the tree traversal and return true */
		/* else fall through to check the contained expr */
	}
	return expression_tree_walker(node, contain_var_clause_walker, context);
}


/*
 * contain_vars_of_level
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  of the specified query level.
 *
 *	  Returns true if any such Var found.
 *
 * Will recurse into sublinks.  Also, may be invoked directly on a Query.
 */
bool
contain_vars_of_level(Node *node, int levelsup)
{
	int			sublevels_up = levelsup;

	return query_or_expression_tree_walker(node,
										   contain_vars_of_level_walker,
										   (void *) &sublevels_up,
										   0);
}

static bool
contain_vars_of_level_walker(Node *node, int *sublevels_up)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup == *sublevels_up)
			return true;		/* abort tree traversal and return true */
		return false;
	}
	if (IsA(node, CurrentOfExpr))
	{
		if (*sublevels_up == 0)
			return true;
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup == *sublevels_up)
			return true;		/* abort the tree traversal and return true */
		/* else fall through to check the contained expr */
	}
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		bool		result;

		(*sublevels_up)++;
		result = query_tree_walker((Query *) node,
								   contain_vars_of_level_walker,
								   (void *) sublevels_up,
								   0);
		(*sublevels_up)--;
		return result;
	}
	return expression_tree_walker(node,
								  contain_vars_of_level_walker,
								  (void *) sublevels_up);
}


/*
 * locate_var_of_level
 *	  Find the parse location of any Var of the specified query level.
 *
 * Returns -1 if no such Var is in the querytree, or if they all have
 * unknown parse location.  (The former case is probably caller error,
 * but we don't bother to distinguish it from the latter case.)
 *
 * Will recurse into sublinks.  Also, may be invoked directly on a Query.
 *
 * Note: it might seem appropriate to merge this functionality into
 * contain_vars_of_level, but that would complicate that function's API.
 * Currently, the only uses of this function are for error reporting,
 * and so shaving cycles probably isn't very important.
 */
int
locate_var_of_level(Node *node, int levelsup)
{
	locate_var_of_level_context context;

	context.var_location = -1;	/* in case we find nothing */
	context.sublevels_up = levelsup;

	(void) query_or_expression_tree_walker(node,
										   locate_var_of_level_walker,
										   (void *) &context,
										   0);

	return context.var_location;
}

static bool
locate_var_of_level_walker(Node *node,
						   locate_var_of_level_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == context->sublevels_up &&
			var->location >= 0)
		{
			context->var_location = var->location;
			return true;		/* abort tree traversal and return true */
		}
		return false;
	}
	if (IsA(node, CurrentOfExpr))
	{
		/* since CurrentOfExpr doesn't carry location, nothing we can do */
		return false;
	}
	/* No extra code needed for PlaceHolderVar; just look in contained expr */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		bool		result;

		context->sublevels_up++;
		result = query_tree_walker((Query *) node,
								   locate_var_of_level_walker,
								   (void *) context,
								   0);
		context->sublevels_up--;
		return result;
	}
	return expression_tree_walker(node,
								  locate_var_of_level_walker,
								  (void *) context);
}


/*
 * pull_var_clause
 *	  Recursively pulls all Var nodes from an expression clause.
 *
 *	  Aggrefs are handled according to these bits in 'flags':
 *		PVC_INCLUDE_AGGREGATES		include Aggrefs in output list
 *		PVC_RECURSE_AGGREGATES		recurse into Aggref arguments
 *		neither flag				throw error if Aggref found
 *	  Vars within an Aggref's expression are included in the result only
 *	  when PVC_RECURSE_AGGREGATES is specified.
 *
 *	  WindowFuncs are handled according to these bits in 'flags':
 *		PVC_INCLUDE_WINDOWFUNCS		include WindowFuncs in output list
 *		PVC_RECURSE_WINDOWFUNCS		recurse into WindowFunc arguments
 *		neither flag				throw error if WindowFunc found
 *	  Vars within a WindowFunc's expression are included in the result only
 *	  when PVC_RECURSE_WINDOWFUNCS is specified.
 *
 *	  PlaceHolderVars are handled according to these bits in 'flags':
 *		PVC_INCLUDE_PLACEHOLDERS	include PlaceHolderVars in output list
 *		PVC_RECURSE_PLACEHOLDERS	recurse into PlaceHolderVar arguments
 *		neither flag				throw error if PlaceHolderVar found
 *	  Vars within a PHV's expression are included in the result only
 *	  when PVC_RECURSE_PLACEHOLDERS is specified.
 *
 *	  GroupingFuncs are treated mostly like Aggrefs, and so do not need
 *	  their own flag bits.
 *
 *	  CurrentOfExpr nodes are ignored in all cases.
 *
 *	  Upper-level vars (with varlevelsup > 0) should not be seen here,
 *	  likewise for upper-level Aggrefs and PlaceHolderVars.
 *
 *	  Returns list of nodes found.  Note the nodes themselves are not
 *	  copied, only referenced.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
List *
pull_var_clause(Node *node, int flags)
{
	pull_var_clause_context context;

	/* Assert that caller has not specified inconsistent flags */
	Assert((flags & (PVC_INCLUDE_AGGREGATES | PVC_RECURSE_AGGREGATES))
		   != (PVC_INCLUDE_AGGREGATES | PVC_RECURSE_AGGREGATES));
	Assert((flags & (PVC_INCLUDE_WINDOWFUNCS | PVC_RECURSE_WINDOWFUNCS))
		   != (PVC_INCLUDE_WINDOWFUNCS | PVC_RECURSE_WINDOWFUNCS));
	Assert((flags & (PVC_INCLUDE_PLACEHOLDERS | PVC_RECURSE_PLACEHOLDERS))
		   != (PVC_INCLUDE_PLACEHOLDERS | PVC_RECURSE_PLACEHOLDERS));

	context.varlist = NIL;
	context.flags = flags;

	pull_var_clause_walker(node, &context);
	return context.varlist;
}

static bool
pull_var_clause_walker(Node *node, pull_var_clause_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup != 0 &&
			!(context->flags & PVC_INCLUDE_UPPERLEVELVAR))
			elog(ERROR, "Upper-level Var found where not expected");
		context->varlist = lappend(context->varlist, node);
		return false;
	}
	else if (IsA(node, Aggref))
	{
		if (((Aggref *) node)->agglevelsup != 0)
			elog(ERROR, "Upper-level Aggref found where not expected");
		if (context->flags & PVC_INCLUDE_AGGREGATES)
		{
			context->varlist = lappend(context->varlist, node);
			/* we do NOT descend into the contained expression */
			return false;
		}
		else if (context->flags & PVC_RECURSE_AGGREGATES)
		{
			/* fall through to recurse into the aggregate's arguments */
		}
		else
			elog(ERROR, "Aggref found where not expected");
	}
	else if (IsA(node, GroupingFunc))
	{
		if (((GroupingFunc *) node)->agglevelsup != 0)
			elog(ERROR, "Upper-level GROUPING found where not expected");
		if (context->flags & PVC_INCLUDE_AGGREGATES)
		{
			context->varlist = lappend(context->varlist, node);
			/* we do NOT descend into the contained expression */
			return false;
		}
		else if (context->flags & PVC_RECURSE_AGGREGATES)
		{
			/*
			 * We do NOT descend into the contained expression, even if the
			 * caller asked for it, because we never actually evaluate it -
			 * the result is driven entirely off the associated GROUP BY
			 * clause, so we never need to extract the actual Vars here.
			 */
			return false;
		}
		else
			elog(ERROR, "GROUPING found where not expected");
	}
	else if (IsA(node, WindowFunc))
	{
		/* WindowFuncs have no levelsup field to check ... */
		if (context->flags & PVC_INCLUDE_WINDOWFUNCS)
		{
			context->varlist = lappend(context->varlist, node);
			/* we do NOT descend into the contained expressions */
			return false;
		}
		else if (context->flags & PVC_RECURSE_WINDOWFUNCS)
		{
			/* fall through to recurse into the windowfunc's arguments */
		}
		else
			elog(ERROR, "WindowFunc found where not expected");
	}
	else if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup != 0)
			elog(ERROR, "Upper-level PlaceHolderVar found where not expected");
		if (context->flags & PVC_INCLUDE_PLACEHOLDERS)
		{
			context->varlist = lappend(context->varlist, node);
			/* we do NOT descend into the contained expression */
			return false;
		}
		else if (context->flags & PVC_RECURSE_PLACEHOLDERS)
		{
			/* fall through to recurse into the placeholder's expression */
		}
		else
			elog(ERROR, "PlaceHolderVar found where not expected");
	}
	else if (IsA(node, LevelExpr) ||
			 IsA(node, CBRootExpr) ||
			 IsA(node, CBPathExpr) ||
			 IsA(node, CBIsLeafExpr) ||
			 IsA(node, CBIsCycleExpr) ||
			 IsA(node, RownumExpr))
	{
		if (context->flags & PVC_INCLUDE_CNEXPR)
		{
			context->varlist = lappend(context->varlist, node);
			/* we do NOT descend into the contained expression */
			return false;
		}
	}
	return expression_tree_walker(node, pull_var_clause_walker,
								  (void *) context);
}


/*
 * flatten_join_alias_vars
 *	  Replace Vars that reference JOIN outputs with references to the original
 *	  relation variables instead.  This allows quals involving such vars to be
 *	  pushed down.  Whole-row Vars that reference JOIN relations are expanded
 *	  into RowExpr constructs that name the individual output Vars.  This
 *	  is necessary since we will not scan the JOIN as a base relation, which
 *	  is the only way that the executor can directly handle whole-row Vars.
 *
 * This also adjusts relid sets found in some expression node types to
 * substitute the contained base rels for any join relid.
 *
 * If a JOIN contains sub-selects that have been flattened, its join alias
 * entries might now be arbitrary expressions, not just Vars.  This affects
 * this function in one important way: we might find ourselves inserting
 * SubLink expressions into subqueries, and we must make sure that their
 * Query.hasSubLinks fields get set to TRUE if so.  If there are any
 * SubLinks in the join alias lists, the outer Query should already have
 * hasSubLinks = TRUE, so this is only relevant to un-flattened subqueries.
 *
 * NOTE: this is used on not-yet-planned expressions.  We do not expect it
 * to be applied directly to the whole Query, so if we see a Query to start
 * with, we do want to increment sublevels_up (this occurs for LATERAL
 * subqueries).
 */
Node *
flatten_join_alias_vars(PlannerInfo *root, Node *node)
{
	flatten_join_alias_vars_context context;

	context.root = root;
	context.sublevels_up = 0;
	/* flag whether join aliases could possibly contain SubLinks */
	context.possible_sublink = root->parse->hasSubLinks;
	/* if hasSubLinks is already true, no need to work hard */
	context.inserted_sublink = root->parse->hasSubLinks;

	return flatten_join_alias_vars_mutator(node, &context);
}

static Node *
flatten_join_alias_vars_mutator(Node *node,
								flatten_join_alias_vars_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		RangeTblEntry *rte;
		Node	   *newvar;

		/* No change unless Var belongs to a JOIN of the target level */
		if (var->varlevelsup != context->sublevels_up)
			return node;		/* no need to copy, really */
		rte = rt_fetch(var->varno, context->root->parse->rtable);
		if (rte->rtekind != RTE_JOIN)
			return node;
		if (var->varattno == InvalidAttrNumber)
		{
			/* Must expand whole-row reference */
			RowExpr    *rowexpr;
			List	   *fields = NIL;
			List	   *colnames = NIL;
			AttrNumber	attnum;
			ListCell   *lv;
			ListCell   *ln;

			attnum = 0;
			Assert(list_length(rte->joinaliasvars) == list_length(rte->eref->colnames));
			forboth(lv, rte->joinaliasvars, ln, rte->eref->colnames)
			{
				newvar = (Node *) lfirst(lv);
				attnum++;
				/* Ignore dropped columns */
				if (newvar == NULL)
					continue;
				newvar = copyObject(newvar);

				/*
				 * If we are expanding an alias carried down from an upper
				 * query, must adjust its varlevelsup fields.
				 */
				if (context->sublevels_up != 0)
					IncrementVarSublevelsUp(newvar, context->sublevels_up, 0);
				/* Preserve original Var's location, if possible */
				if (IsA(newvar, Var))
					((Var *) newvar)->location = var->location;
				/* Recurse in case join input is itself a join */
				/* (also takes care of setting inserted_sublink if needed) */
				newvar = flatten_join_alias_vars_mutator(newvar, context);
				fields = lappend(fields, newvar);
				/* We need the names of non-dropped columns, too */
				colnames = lappend(colnames, copyObject((Node *) lfirst(ln)));
			}
			rowexpr = makeNode(RowExpr);
			rowexpr->args = fields;
			rowexpr->row_typeid = var->vartype;
			rowexpr->row_format = COERCE_IMPLICIT_CAST;
			rowexpr->colnames = colnames;
			rowexpr->location = var->location;

			return (Node *) rowexpr;
		}

		/* Expand join alias reference */
		Assert(var->varattno > 0);
		newvar = (Node *) list_nth(rte->joinaliasvars, var->varattno - 1);
		Assert(newvar != NULL);
		newvar = copyObject(newvar);

		/*
		 * If we are expanding an alias carried down from an upper query, must
		 * adjust its varlevelsup fields.
		 */
		if (context->sublevels_up != 0)
			IncrementVarSublevelsUp(newvar, context->sublevels_up, 0);

		/* Preserve original Var's location, if possible */
		if (IsA(newvar, Var))
			((Var *) newvar)->location = var->location;

		/* Recurse in case join input is itself a join */
		newvar = flatten_join_alias_vars_mutator(newvar, context);

		/* Detect if we are adding a sublink to query */
		if (context->possible_sublink && !context->inserted_sublink)
			context->inserted_sublink = checkExprHasSubLink(newvar);

		return newvar;
	}
	if (IsA(node, PlaceHolderVar))
	{
		/* Copy the PlaceHolderVar node with correct mutation of subnodes */
		PlaceHolderVar *phv;

		phv = (PlaceHolderVar *) expression_tree_mutator(node,
														 flatten_join_alias_vars_mutator,
														 (void *) context);
		/* now fix PlaceHolderVar's relid sets */
		if (phv->phlevelsup == context->sublevels_up)
		{
			phv->phrels = alias_relid_set(context->root,
										  phv->phrels);
		}
		return (Node *) phv;
	}

	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		Query	   *newnode;
		bool		save_inserted_sublink;

		context->sublevels_up++;
		save_inserted_sublink = context->inserted_sublink;
		context->inserted_sublink = ((Query *) node)->hasSubLinks;
		newnode = query_tree_mutator((Query *) node,
									 flatten_join_alias_vars_mutator,
									 (void *) context,
									 QTW_IGNORE_JOINALIASES);
		newnode->hasSubLinks |= context->inserted_sublink;
		context->inserted_sublink = save_inserted_sublink;
		context->sublevels_up--;
		return (Node *) newnode;
	}
	/* Already-planned tree not supported */
	Assert(!IsA(node, SubPlan));
	/* Shouldn't need to handle these planner auxiliary nodes here */
	Assert(!IsA(node, SpecialJoinInfo));
	Assert(!IsA(node, PlaceHolderInfo));
	Assert(!IsA(node, MinMaxAggInfo));

	return expression_tree_mutator(node, flatten_join_alias_vars_mutator,
								   (void *) context);
}

/*
 * alias_relid_set: in a set of RT indexes, replace joins by their
 * underlying base relids
 */
static Relids
alias_relid_set(PlannerInfo *root, Relids relids)
{
	Relids		result = NULL;
	int			rtindex;

	rtindex = -1;
	while ((rtindex = bms_next_member(relids, rtindex)) >= 0)
	{
		RangeTblEntry *rte = rt_fetch(rtindex, root->parse->rtable);

		if (rte->rtekind == RTE_JOIN)
			result = bms_join(result, get_relids_for_join(root, rtindex));
		else
			result = bms_add_member(result, rtindex);
	}
	return result;
}

#ifdef __OPENTENBASE__
/*
 * contain_vars_upper_level
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  (of or above the current query level).
 *
 *	  Returns true if any varnode found.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
bool
contain_vars_upper_level(Node *node, int levelsup)
{
	int			sublevels_up = levelsup;

	return query_or_expression_tree_walker(node,
										   contain_vars_upper_level_walker,
										   (void *) &sublevels_up,
										   0);
}

static bool
contain_vars_upper_level_walker(Node *node, int *sublevels_up)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup > *sublevels_up)
			return true;		/* abort tree traversal and return true */
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup > *sublevels_up)
			return true;		/* abort the tree traversal and return true */
		/* else fall through to check the contained expr */
	}
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		bool		result;
		result = query_tree_walker((Query *) node,
								   contain_vars_upper_level_walker,
								   (void *) sublevels_up,
								   0);
		return result;
	}
	return expression_tree_walker(node,
								  contain_vars_upper_level_walker,
								  (void *) sublevels_up);
}
#endif
#ifdef __OPENTENBASE_C__
static bool
replace_expression_tree_walker(Node **expr,
							   bool (*walker) (),
							   void *context, List *vars)
{
	ListCell   *temp;
	Node       *node = *expr;

	/*
	 * The walker has already visited the current node, and so we need only
	 * recurse into any sub-nodes it has.
	 *
	 * We assume that the walker is not interested in List nodes per se, so
	 * when we expect a List we just recurse directly to self without
	 * bothering to call the walker.
	 */
	if (node == NULL)
		return false;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Var:
		case T_Const:
		case T_Param:
		case T_CaseTestExpr:
		case T_SQLValueFunction:
		case T_CoerceToDomainValue:
		case T_SetToDefault:
		case T_CurrentOfExpr:
		case T_NextValueExpr:
		case T_RangeTblRef:
		case T_SortGroupClause:
		case T_LevelExpr:
		case T_CBIsLeafExpr:
		case T_CBIsCycleExpr:
			/* primitive node types with no expression subnodes */
			break;
		case T_WithCheckOption:
			return walker(&(((WithCheckOption *) node)->qual), context, vars);
		case T_Aggref:
			{
				Aggref	   *expr = (Aggref *) node;

				/* recurse directly on List */
				if (replace_expression_tree_walker((Node **) &expr->aggdirectargs,
										   walker, context, vars))
					return true;
				if (replace_expression_tree_walker((Node **) &expr->args,
										   walker, context, vars))
					return true;
				if (replace_expression_tree_walker((Node **) &expr->aggorder,
										   walker, context, vars))
					return true;
				if (replace_expression_tree_walker((Node **) &expr->aggdistinct,
										   walker, context, vars))
					return true;
				if (walker((Node **) &expr->aggfilter, context, vars))
					return true;
			}
			break;
		case T_GroupingFunc:
			{
				GroupingFunc *grouping = (GroupingFunc *) node;

				if (replace_expression_tree_walker((Node **) &grouping->args,
										   walker, context, vars))
					return true;
			}
			break;
		case T_WindowFunc:
			{
				WindowFunc *expr = (WindowFunc *) node;

				/* recurse directly on List */
				if (replace_expression_tree_walker((Node **) &expr->args,
										   walker, context, vars))
					return true;
				if (walker((Node **) &expr->aggfilter, context, vars))
					return true;
			}
			break;
		case T_ArrayRef:
			{
				ArrayRef   *aref = (ArrayRef *) node;

				/* recurse directly for upper/lower array index lists */
				if (replace_expression_tree_walker((Node **) &aref->refupperindexpr,
										   walker, context, vars))
					return true;
				if (replace_expression_tree_walker((Node **) &aref->reflowerindexpr,
										   walker, context, vars))
					return true;
				/* walker must see the refexpr and refassgnexpr, however */
				if (walker(&(aref->refexpr), context, vars))
					return true;
				if (walker(&(aref->refassgnexpr), context, vars))
					return true;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *expr = (FuncExpr *) node;

				if (replace_expression_tree_walker((Node **) &expr->args,
										   walker, context, vars))
					return true;
			}
			break;
		case T_NamedArgExpr:
			return walker(&(((NamedArgExpr *) node)->arg), context, vars);
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr	   *expr = (OpExpr *) node;

				if (replace_expression_tree_walker((Node **) &expr->args,
										   walker, context, vars))
					return true;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

				if (replace_expression_tree_walker((Node **) &expr->args,
										   walker, context, vars))
					return true;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;

				if (replace_expression_tree_walker((Node **) &expr->args,
										   walker, context, vars))
					return true;
			}
			break;
		case T_SubLink:
			{
				SubLink    *sublink = (SubLink *) node;

				if (walker(&(sublink->testexpr), context, vars))
					return true;

				/*
				 * Also invoke the walker on the sublink's Query node, so it
				 * can recurse into the sub-query if it wants to.
				 */
				return walker(&(sublink->subselect), context, vars);
			}
			break;
		case T_SubPlan:
			{
				SubPlan    *subplan = (SubPlan *) node;

				/* recurse into the testexpr, but not into the Plan */
				if (walker(&(subplan->testexpr), context, vars))
					return true;
				/* also examine args list */
				if (replace_expression_tree_walker((Node **) &subplan->args,
										   walker, context, vars))
					return true;
			}
			break;
		case T_AlternativeSubPlan:
			return walker(&(((AlternativeSubPlan *) node)->subplans), context, vars);
		case T_FieldSelect:
			return walker(&(((FieldSelect *) node)->arg), context, vars);
		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;

				if (walker(&(fstore->arg), context, vars))
					return true;
				if (walker(&(fstore->newvals), context, vars))
					return true;
			}
			break;
		case T_RelabelType:
			return walker(&(((RelabelType *) node)->arg), context, vars);
		case T_CoerceViaIO:
			return walker(&(((CoerceViaIO *) node)->arg), context, vars);
		case T_ArrayCoerceExpr:
			return walker(&(((ArrayCoerceExpr *) node)->arg), context, vars);
		case T_ConvertRowtypeExpr:
			return walker(&(((ConvertRowtypeExpr *) node)->arg), context, vars);
		case T_CollateExpr:
			return walker(&(((CollateExpr *) node)->arg), context, vars);
		case T_CaseExpr:
			{
				CaseExpr   *caseexpr = (CaseExpr *) node;

				if (walker(&(caseexpr->arg), context, vars))
					return true;
				/* we assume walker doesn't care about CaseWhens, either */
				foreach(temp, caseexpr->args)
				{
					CaseWhen   *when = lfirst_node(CaseWhen, temp);

					if (walker(&(when->expr), context, vars))
						return true;
					if (walker(&(when->result), context, vars))
						return true;
				}
				if (walker(&(caseexpr->defresult), context, vars))
					return true;
			}
			break;
		case T_ArrayExpr:
			return walker(&(((ArrayExpr *) node)->elements), context, vars);
		case T_RowExpr:
			/* Assume colnames isn't interesting */
			return walker(&(((RowExpr *) node)->args), context, vars);
		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;

				if (walker(&(rcexpr->largs), context, vars))
					return true;
				if (walker(&(rcexpr->rargs), context, vars))
					return true;
			}
			break;
		case T_CoalesceExpr:
			return walker(&(((CoalesceExpr *) node)->args), context, vars);
		case T_MinMaxExpr:
			return walker(&(((MinMaxExpr *) node)->args), context, vars);
		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;

				if (walker(&(xexpr->named_args), context, vars))
					return true;
				/* we assume walker doesn't care about arg_names */
				if (walker(&(xexpr->args), context, vars))
					return true;
			}
			break;
		case T_NullTest:
			return walker(&(((NullTest *) node)->arg), context, vars);
		case T_BooleanTest:
			return walker(&(((BooleanTest *) node)->arg), context, vars);
		case T_CoerceToDomain:
			return walker(&(((CoerceToDomain *) node)->arg), context, vars);
		case T_TargetEntry:
			return walker(&(((TargetEntry *) node)->expr), context,vars);
		case T_Query:
			/* Do nothing with a sub-Query, per discussion above */
			break;
		case T_WindowClause:
			{
				WindowClause *wc = (WindowClause *) node;

				if (walker(&(wc->partitionClause), context, vars))
					return true;
				if (walker(&(wc->orderClause), context, vars))
					return true;
				if (walker(&(wc->startOffset), context, vars))
					return true;
				if (walker(&(wc->endOffset), context, vars))
					return true;
			}
			break;
		case T_CommonTableExpr:
			{
				CommonTableExpr *cte = (CommonTableExpr *) node;

				/*
				 * Invoke the walker on the CTE's Query node, so it can
				 * recurse into the sub-query if it wants to.
				 */
				return walker(&(cte->ctequery), context, vars);
			}
			break;
		case T_List:
			foreach(temp, (List *) node)
			{
				if (walker((Node **) &lfirst(temp), context, vars))
					return true;
			}
			break;
		case T_FromExpr:
			{
				FromExpr   *from = (FromExpr *) node;

				if (walker(&(from->fromlist), context, vars))
					return true;
				if (walker(&(from->quals), context, vars))
					return true;
			}
			break;
		case T_OnConflictExpr:
			{
				OnConflictExpr *onconflict = (OnConflictExpr *) node;

				if (walker((Node **) &onconflict->arbiterElems, context, vars))
					return true;
				if (walker(&(onconflict->arbiterWhere), context, vars))
					return true;
				if (walker(&(onconflict->onConflictSet), context, vars))
					return true;
				if (walker(&(onconflict->onConflictWhere), context, vars))
					return true;
				if (walker(&(onconflict->exclRelTlist), context, vars))
					return true;
			}
			break;
		case T_ConnectByExpr:
			{
				ConnectByExpr *cb = (ConnectByExpr *) node;

				if (walker(&cb->expr, context, vars))
					return true;
				if (walker(&cb->start, context, vars))
					return true;
				if (walker((Node **) &cb->sort, context, vars))
					return true;
			}
			break;
		case T_PriorExpr:
			if (walker(&((PriorExpr *) node)->expr, context, vars))
				return true;
			break;
		case T_CBPathExpr:
			if (walker(&((CBPathExpr *) node)->expr, context, vars))
				return true;
			break;
		case T_CBRootExpr:
			if (walker(&((CBRootExpr *) node)->expr, context, vars))
				return true;
			break;
		case T_JoinExpr:
			{
				bool        left_arg_ret  = false;
				bool        right_arg_ret = false;
				bool        join_qual_ret = false;
				
				JoinExpr   *join = NULL;
				

				join = (JoinExpr *) node;
				/* check the shippability of two subqueries, we both of them can be pushed down, just push down them. */
				left_arg_ret  = walker(&(join->larg), context, vars);
				right_arg_ret = walker(&(join->rarg), context, vars);
				join_qual_ret = walker(&(join->quals), context, vars);

				if (left_arg_ret)
				{
					return true;
				}

				if (right_arg_ret)
				{
					return true;
				}

				if (join_qual_ret)
				{
					return true;
				}
				
				/*
				 * alias clause, using list are deemed uninteresting.
				 */
			}
			break;
		case T_SetOperationStmt:
			{
				SetOperationStmt *setop = (SetOperationStmt *) node;

				if (walker(&(setop->larg), context, vars))
					return true;
				if (walker(&(setop->rarg), context, vars))
					return true;

				/* groupClauses are deemed uninteresting */
			}
			break;
		case T_PlaceHolderVar:
			return walker(&(((PlaceHolderVar *) node)->phexpr), context, vars);
		case T_InferenceElem:
			return walker(&(((InferenceElem *) node)->expr), context, vars);
		case T_AppendRelInfo:
			{
				AppendRelInfo *appinfo = (AppendRelInfo *) node;

				if (replace_expression_tree_walker((Node **) &appinfo->translated_vars,
										   walker, context, vars))
					return true;
			}
			break;
		case T_PlaceHolderInfo:
			return walker(&(((PlaceHolderInfo *) node)->ph_var), context, vars);
		case T_RangeTblFunction:
			return walker(&(((RangeTblFunction *) node)->funcexpr), context, vars);
		case T_TableSampleClause:
			{
				TableSampleClause *tsc = (TableSampleClause *) node;

				if (replace_expression_tree_walker((Node **) &tsc->args,
										   walker, context, vars))
					return true;
				if (walker((Node **) &tsc->repeatable, context, vars))
					return true;
			}
			break;
		case T_TableFunc:
			{
				TableFunc  *tf = (TableFunc *) node;

				if (walker(&(tf->ns_uris), context, vars))
					return true;
				if (walker(&(tf->docexpr), context, vars))
					return true;
				if (walker(&(tf->rowexpr), context, vars))
					return true;
				if (walker(&(tf->colexprs), context, vars))
					return true;
				if (walker(&(tf->coldefexprs), context, vars))
					return true;
			}
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
	return false;
}
static bool
replace_var_clause_walker(Node **expr, replace_var_clause_context *context, List *vars)
{
	Node *node = *expr;
	
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup != 0)
			elog(ERROR, "Upper-level Var found where not expected");
		if (!list_member(vars, node))
		{
			/* replace var with const null */
			Const *con = makeConst(exprType(node),
				                   exprTypmod(node),
				                   exprCollation(node),
				                   -1,
				                   0,
				                   true,
				                   false);
			*expr = (Node *)con;
		}
		context->varlist = lappend(context->varlist, node);
		return false;
	}
	else if (IsA(node, Aggref))
	{
		if (((Aggref *) node)->agglevelsup != 0)
			elog(ERROR, "Upper-level Aggref found where not expected");
		context->varlist = lappend(context->varlist, node);
		return false;
	}
	else if (IsA(node, GroupingFunc))
	{
		GroupingFunc *gf = (GroupingFunc *) node;
		if (gf->agglevelsup != 0)
			elog(ERROR, "Upper-level GROUPING found where not expected");
		if (context->transform_grouping)
		{
			/*
			 * transformGroupingFunc only allow arguments fewer than 32
			 * which means we can make a const int32 here.
			 */
			Const *con = makeConst(INT4OID,
								   InvalidOid,
								   InvalidOid,
								   sizeof(int32),
								   (1 << list_length(gf->refs)) - 1,
								   false,
								   true);
			*expr = (Node *)con;
		}
		else
			context->varlist = lappend(context->varlist, node);
		return false;
	}
	else if (IsA(node, WindowFunc))
	{
		/* fall through to recurse into the windowfunc's arguments */
	}
	else if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup != 0)
			elog(ERROR, "Upper-level PlaceHolderVar found where not expected");
	}
	return replace_expression_tree_walker(expr, replace_var_clause_walker,
								  (void *) context, vars);
}

List *
replace_var_clause(Node **node, List *vars, bool transform_grouping)
{
	replace_var_clause_context context;

	context.varlist = NIL;
	context.transform_grouping = transform_grouping;

	replace_var_clause_walker(node, &context, vars);
	return context.varlist;
}

/*
 * mutator for replace_node_clause
 */
static Node *
replace_node_clause_mutator(Node *node, replace_node_clause_context *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(context->src, List) && IsA(context->dest, List))
	{
		ListCell *lc = NULL;
		ListCell *lc2 = NULL;

		forboth(lc, (List*)context->src, lc2, (List*)context->dest)
		{
			if (equal(node, lfirst(lc)))
			{
				return (Node*)lfirst(lc2);
			}
		}
	}
	else if (equal(node, context->src))
	{
		/* If replace_first_only is set, and we have already got one, just return */
		if (context->replace_first_only && (context->replace_count >= 1))
		{
			return node;
		}
		else
		{
			++context->replace_count;
			return context->dest;
		}
	}

	if (!context->recurse_aggref && IsA(node, Aggref))
	{
		if (context->isCopy)
		{
			return (Node*)copyObject(node);
		}
		else
		{
			return node;
		}
	}

	return expression_tree_mutator_internal(node, replace_node_clause_mutator,
								   (void *) context, context->isCopy);
}

/*
 * replace src_list with dest_list.
 */
Node *
replace_node_clause(Node *clause, Node *src_list, Node *dest_list,
					uint32 rncbehavior)
{
    replace_node_clause_context context;

    if (src_list == NULL || dest_list == NULL) {
        return clause;
    }

    context.src = (Node *)src_list;
    context.dest = (Node *)dest_list;
    context.recurse_aggref = (RNC_RECURSE_AGGREF & rncbehavior) > 0x0000;
    context.isCopy = (RNC_COPY_NON_LEAF_NODES & rncbehavior) > 0x0000;
    context.replace_first_only = (RNC_REPLACE_FIRST_ONLY & rncbehavior) > 0x0000;
    context.replace_count = 0;

    return replace_node_clause_mutator(clause, &context);
}

#endif

#ifdef __OPENTENBASE__
/*
 * Search node and compare varno.
 */
static bool
check_varno_walker(Node *node, var_info *varInfo)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;

		if (var->varno == varInfo->varno &&
			var->varlevelsup == varInfo->varlevelsup)
		{
			return false;
		}
		else
		{
			varInfo->result = false;
			return true;
		}
	}
	else if (IsA(node, SubLink))
	{
		varInfo->result = false;
		return true;
	}

	return expression_tree_walker(node, check_varno_walker, varInfo);
}

/*
 * Check this node if it only includes one varno.
 */
bool
check_varno(Node *qual, int varno, int varlevelsup)
{
	var_info varInfo = {0, 0, false};

	varInfo.varno = varno;
	varInfo.varlevelsup = varlevelsup;
	varInfo.result = true;

	(void)check_varno_walker(qual, &varInfo);

	return varInfo.result;
}

/*
 * Search same var in node, return true if found.
 */
static bool
contain_same_var_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var) && equal_var( (Var *)node, (Var *)context))
		return true;

	return expression_tree_walker(node, contain_same_var_walker, context);
}

bool
equal_var(Var *var1, Var *var2)
{
	if (NULL == var1 || NULL == var2)
		return false;

	return (var1->varno == var2->varno) && (var1->varattno == var2->varattno);
}

/* contain_vars_of_level_or_above_walker
 *   Walk through to discover Var node
 */
static bool
contain_vars_of_level_or_above_walker(Node* node, int* sublevels_up)
{
	if (node == NULL)
	{
		return false;
	}
	if (IsA(node, Var))
	{
		if (((Var*)node)->varlevelsup >= (Index)*sublevels_up)
		{
			return true;
		}
		return false;
	}
	if (IsA(node, CurrentOfExpr))
	{
		if (*sublevels_up == 0)
		{
			return true;
		}
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar*)node)->phlevelsup >= (unsigned int)*sublevels_up)
		{
			return true;
		}
	}
	if (IsA(node, Query)) {
		/* Recurse into subselects */
		bool result = false;

		result = query_tree_walker((Query*)node, (bool (*)())contain_vars_of_level_or_above_walker, 
		                        	(void*)sublevels_up, 0);
		return result;
	}
	return expression_tree_walker(node, (bool (*)())contain_vars_of_level_or_above_walker,
								(void*)sublevels_up);
}

/*
 * contain_vars_of_level_or_above
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  of the specified query level or level above.
 *
 *	  Returns true if any such Var found.
 */
bool
contain_vars_of_level_or_above(Node* node, int levelsup)
{
	int sublevels_up = levelsup;

	return query_or_expression_tree_walker(node, (bool (*)())contain_vars_of_level_or_above_walker,
											(void*)&sublevels_up, 0);
}

/*
 * contain_booling_vars_level_walker
 *	Check whether there is available bool hashable predicates.
 */
static bool
contain_booling_vars_level_walker(Node *node, int *sublevels_up)
{
	if (node == NULL)
		return false;

	switch(nodeTag(node))
	{
		case T_Var:
		{
			if (((Var *) node)->varlevelsup == *sublevels_up)
				return true;

			return false;
		}
		case T_PlaceHolderVar:
		{
			if (((PlaceHolderVar *) node)->phlevelsup == *sublevels_up)
				return true;
			
			break;
		}
		case T_BoolExpr:
		{
			if (((BoolExpr*)node)->boolop != AND_EXPR)
				return false;
			
			break;
		}
		case T_OpExpr:
		{
			OpExpr *expr = (OpExpr *) node;

			if (list_length(expr->args) == 2 &&
				op_hashjoinable(expr->opno, exprType(linitial(expr->args))))
			{
				break;
			}

			return false;
		}
		case T_RelabelType:
		case T_CoerceViaIO:
			break;
		default:
			return false;
	}

	return expression_tree_walker(node,
							contain_booling_vars_level_walker,
							(void *) sublevels_up);
}

bool
contain_booling_vars_upper_level(Node *node, int levelsup)
{
	int			sublevels_up = levelsup;

	return query_or_expression_tree_walker(node,
										   contain_booling_vars_level_walker,
										   (void *) &sublevels_up,
										   0);
}

#endif
