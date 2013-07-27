#include "postgres.h"

#include "fmgr.h"
#include "windowapi.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/syscache.h"
#include "nodes/execnodes.h"
#include "parser/parse_type.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(conditional_true_event);
extern Datum conditional_true_event(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(conditional_change_event);
extern Datum conditional_change_event(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(exponential_moving_average);
extern Datum exponential_moving_average(PG_FUNCTION_ARGS);

typedef struct cte_context {
	int	rank; // count of events for CTE / changes for CCE
} cte_context;

typedef struct ema_context {
	float	avg;
} ema_context;

// conditional_true_event (CTE)
// Rank increments on any row where predicate is true.
Datum
conditional_true_event(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	cte_context *context;
	bool		isnull;
	bool		up;

	up = DatumGetBool(WinGetFuncArgCurrent(winobj, 0, &isnull));
	context = (cte_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(cte_context));
	if (isnull)
		PG_RETURN_INT64(context->rank);
	else if (up)
		context->rank++;

	PG_RETURN_INT64(context->rank);
}

// utility routine for conditional_change_event,
// loosely modeled after the one in stock postgresql's windowfuncs.c
static bool
rank_up(FunctionCallInfo fcinfo, WindowObject winobj)
{
	bool		up = false; // should rank increase?
	int64		curpos = WinGetCurrentPosition(winobj);

	Datum curval, lastval;
	bool isnullc, isnullp, isoutp;
	
	Oid typOid;
	Type argtype;
	bool byVal;
	int typLen;

	typOid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	argtype = typeidType(typOid); // unfortunately I think we have to do this pg_type cache lookup on every call
	byVal = typeByVal(argtype);
	typLen = typeLen(argtype);
	ReleaseSysCache(argtype);

	curval = WinGetFuncArgCurrent(winobj, 0, &isnullc);
	lastval = WinGetFuncArgInPartition(winobj, 0, -1, WINDOW_SEEK_CURRENT, false, &isnullp, &isoutp);
	
	if(isoutp) // we're on the first row of the partition, CCE of which which should be 0
		up = false;
	else if (isnullc)
		up = false;
	// do current and prior tuples differ in the value of the key expression?
	else if (!datumIsEqual(curval, lastval, byVal, typLen))
		up = true;

	// We can advance the mark, but only *after* acccess to prior row
	WinSetMarkPosition(winobj, curpos);

	return up;
}

// conditional_change_event  (CCE)
// Rank increments on any row where key column value differs from that in the prior row.
// Can be emulated with CTE, but providing this function is both more run-time efficient
// and makes queries cleaner.
Datum
conditional_change_event(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	cte_context *context;
	bool		up;

	up = rank_up(fcinfo, winobj);
	context = (cte_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(cte_context));
	if (up)
		context->rank++;

	PG_RETURN_INT64(context->rank);
}

