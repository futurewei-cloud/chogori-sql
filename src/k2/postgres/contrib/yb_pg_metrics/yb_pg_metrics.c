// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

#include "postgres.h"

#include <math.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "executor/instrument.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "common/ip.h"
#include "datatype/timestamp.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/syscache.h"
#include "yb/server/pgsql_webserver_wrapper.h"

#include "pg_yb_utils.h"

#define YSQL_METRIC_PREFIX "handler_latency_yb_ysqlserver_SQLProcessor_"
#define NumBackendStatSlots (MaxBackends + NUM_AUXPROCTYPES)

PG_MODULE_MAGIC;

typedef enum statementType
{
	Select,
	Insert,
	Delete,
	Update,
	Other,
	Transaction,
	AggregatePushdown,
	kMaxStatementType
} statementType;
int num_entries = kMaxStatementType;
ybpgmEntry *ybpgm_table = NULL;
static int statement_nesting_level = 0;
char *metric_node_name = NULL;
struct WebserverWrapper *webserver = NULL;
int port = 0;
static int num_backends = 0;
static rpczEntry *rpcz = NULL;
static MemoryContext ybrpczMemoryContext = NULL;
PgBackendStatus **backendStatusArrayPointer = NULL;

void		_PG_init(void);
/*
 * Variables used for storing the previous values of used hooks.
 */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void set_metric_names(void);
static void ybpgm_startup_hook(void);
static Size ybpgm_memsize(void);
static bool isTopLevelStatement(void);
static void ybpgm_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void ybpgm_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
                              bool execute_once);
static void ybpgm_ExecutorFinish(QueryDesc *queryDesc);
static void ybpgm_ExecutorEnd(QueryDesc *queryDesc);
static void ybpgm_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                 ProcessUtilityContext context,
                                 ParamListInfo params, QueryEnvironment *queryEnv,
                                 DestReceiver *dest, char *completionTag);
static void ybpgm_Store();

/*
 * Function used for checking if the current statement is a top level statement.
 */
bool
isTopLevelStatement(void)
{
  return statement_nesting_level == 0;
}

void
set_metric_names(void)
{
  strcpy(ybpgm_table[Select].name, YSQL_METRIC_PREFIX "SelectStmt");
  strcpy(ybpgm_table[Insert].name, YSQL_METRIC_PREFIX "InsertStmt");
  strcpy(ybpgm_table[Delete].name, YSQL_METRIC_PREFIX "DeleteStmt");
  strcpy(ybpgm_table[Update].name, YSQL_METRIC_PREFIX "UpdateStmt");
  strcpy(ybpgm_table[Other].name, YSQL_METRIC_PREFIX "OtherStmts");
  strcpy(ybpgm_table[Transaction].name, YSQL_METRIC_PREFIX "Transactions");
  strcpy(ybpgm_table[AggregatePushdown].name, YSQL_METRIC_PREFIX "AggregatePushdowns");
}

/*
 * Function to calculate milliseconds elapsed from start_time to stop_time.
 */
int64
getElapsedMs(TimestampTz start_time, TimestampTz stop_time) 
{
  long secs;
  int microsecs;

  TimestampDifference(start_time, stop_time, &secs, &microsecs);

  long millisecs = (secs * 1000) + (microsecs / 1000);
  return millisecs;
}

void
pullRpczEntries(void)
{
  if (!(*backendStatusArrayPointer))
    elog(LOG, "Backend Status Array hasn't been initialized yet.");

  ybrpczMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                             "YB RPCz memory context",
                                             ALLOCSET_SMALL_SIZES);

  MemoryContext oldcontext = MemoryContextSwitchTo(ybrpczMemoryContext);
  rpcz = (rpczEntry *) palloc(sizeof(rpczEntry) * NumBackendStatSlots);

  num_backends = NumBackendStatSlots;
  volatile PgBackendStatus *beentry;

  beentry = *backendStatusArrayPointer;

  for (int i = 0; i < NumBackendStatSlots; i++)
  {
    /* To prevent locking overhead, the BackendStatusArray in postgres maintains a st_changecount
     * field for each entry. This field is incremented once before a backend starts modifying the
     * entry, and once after it is done modifying the entry. So, we check if st_changecount changes
     * while we're copying the entry or if its odd. The check for odd is needed for when a backend
     * has begun changing the entry but hasn't finished.
     */
    for (;;)
    {
      int			before_changecount;
      int			after_changecount;

      before_changecount = beentry->st_changecount;

      rpcz[i].proc_id = beentry->st_procpid;

      /* avoid filling any more fields if invalid */
      if (beentry->st_procpid <= 0) {
        break;
      }

      rpcz[i].db_oid = beentry->st_databaseid;

      rpcz[i].query = (char *) palloc(pgstat_track_activity_query_size);
      strcpy(rpcz[i].query, (char *) beentry->st_activity_raw);

      rpcz[i].application_name = (char *) palloc(NAMEDATALEN);
      strcpy(rpcz[i].application_name, (char *) beentry->st_appname);

      rpcz[i].db_name = (char *) palloc(NAMEDATALEN);
      strcpy(rpcz[i].db_name, beentry->st_databasename);

      rpcz[i].process_start_timestamp = beentry->st_proc_start_timestamp;
      rpcz[i].transaction_start_timestamp = beentry->st_xact_start_timestamp;
      rpcz[i].query_start_timestamp = beentry->st_activity_start_timestamp; 

      rpcz[i].backend_type = (char *) palloc(40);
      strcpy(rpcz[i].backend_type, pgstat_get_backend_desc(beentry->st_backendType));

      rpcz[i].backend_active = 0;
      rpcz[i].backend_status = (char *) palloc(30);
      switch (beentry->st_state) {
        case STATE_IDLE:
          strcpy(rpcz[i].backend_status, "idle");
          break;
        case STATE_RUNNING:
          rpcz[i].backend_active = 1;
          strcpy(rpcz[i].backend_status, "active");
          break;
        case STATE_IDLEINTRANSACTION:
          strcpy(rpcz[i].backend_status, "idle in transaction");
          break;
        case STATE_FASTPATH:
          rpcz[i].backend_active = 1;
          strcpy(rpcz[i].backend_status, "fastpath function call");
          break;
        case STATE_IDLEINTRANSACTION_ABORTED:
          strcpy(rpcz[i].backend_status, "idle in transaction (aborted)");
          break;
        case STATE_DISABLED:
          strcpy(rpcz[i].backend_status, "disabled");
          break;
        case STATE_UNDEFINED:
          strcpy(rpcz[i].backend_status, "");
          break;
      }

      char remote_host[NI_MAXHOST];
      char remote_port[NI_MAXSERV];
      int ret;

      remote_host[0] = '\0';
      remote_port[0] = '\0';
      ret = pg_getnameinfo_all((struct sockaddr_storage *) &beentry->st_clientaddr.addr,
                               beentry->st_clientaddr.salen,
                               remote_host, sizeof(remote_host),
                               remote_port, sizeof(remote_port),
                               NI_NUMERICHOST | NI_NUMERICSERV);
      if (ret == 0)
      {
        rpcz[i].host = (char *) palloc(NI_MAXHOST);
        rpcz[i].port = (char *) palloc(NI_MAXSERV);
        clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
        strcpy(rpcz[i].host, remote_host);
        strcpy(rpcz[i].port, remote_port);
      }
      else
      {
        rpcz[i].host = NULL;
        rpcz[i].port = NULL;
      }
      after_changecount = beentry->st_changecount;

      if (before_changecount == after_changecount &&
          (before_changecount & 1) == 0)
        break;
    }
    beentry++;
  }
  MemoryContextSwitchTo(oldcontext);
}

void
freeRpczEntries(void)
{
  MemoryContextDelete(ybrpczMemoryContext);
  ybrpczMemoryContext = NULL;
}

/*
 * Function that is executed when the YSQL webserver process is started.
 * We don't use the argument "unused", however, a postgres background worker's function
 * is required to have an argument of type Datum.
 */
void
webserver_worker_main(Datum unused)
{
  /*
   * We need to use a pointer to a pointer here because the shared memory for BackendStatusArray
   * is not allocated when we enter this function. The memory is allocated after the background
   * works are registered.
   */
  backendStatusArrayPointer = getBackendStatusArrayPointer();

  BackgroundWorkerUnblockSignals();

  webserver = CreateWebserver(ListenAddresses, port);

  RegisterMetrics(ybpgm_table, num_entries, metric_node_name);

  postgresCallbacks callbacks;
  callbacks.pullRpczEntries      = pullRpczEntries;
  callbacks.freeRpczEntries      = freeRpczEntries;
  callbacks.getTimestampTz       = GetCurrentTimestamp;
  callbacks.getTimestampTzDiffMs = getElapsedMs;
  callbacks.getTimestampTzToStr  = timestamptz_to_str;

  RegisterRpczEntries(&callbacks, &num_backends, &rpcz);

  HandleYBStatus(StartWebserver(webserver));

  WaitLatch(&MyProc->procLatch, WL_POSTMASTER_DEATH, -1, PG_WAIT_EXTENSION);

  if (rpcz != NULL && ybrpczMemoryContext != NULL)
  {
    MemoryContext oldcontext = MemoryContextSwitchTo(ybrpczMemoryContext);
    pfree(rpcz);
    MemoryContextSwitchTo(oldcontext);
  }

  proc_exit(0);
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
  if (!process_shared_preload_libraries_in_progress)
    return;

  RequestAddinShmemSpace(ybpgm_memsize());

  /*
   * Parameters that we expect to receive from the tserver process when it starts up postmaster.
   * We set the flags GUC_NO_SHOW_ALL, GUC_NO_RESET_ALL, GUC_NOT_IN_SAMPLE, GUC_DISALLOW_IN_FILE
   * so that these parameters aren’t visible, resettable, or configurable by our end user.
   */
  DefineCustomStringVariable("yb_pg_metrics.node_name", "Node name for YB metrics", NULL,
                             &metric_node_name, "", PGC_POSTMASTER,
                             GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE
                             | GUC_DISALLOW_IN_FILE,
                             NULL, NULL, NULL);

  DefineCustomIntVariable("yb_pg_metrics.port", "Port for YSQL webserver", NULL,
                          &port, 0, 0, INT_MAX, PGC_POSTMASTER,
                          GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE
                          | GUC_DISALLOW_IN_FILE,
                          NULL, NULL, NULL);

  BackgroundWorker worker;

  /* Registering the YSQL webserver as a background worker */
  MemSet(&worker, 0, sizeof(BackgroundWorker));
  strcpy(worker.bgw_name, "YSQL webserver");
  worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
  worker.bgw_start_time = BgWorkerStart_PostmasterStart;
  worker.bgw_restart_time = BGW_NEVER_RESTART;
  worker.bgw_main_arg = (Datum) 0;
  strcpy(worker.bgw_library_name, "yb_pg_metrics");
  strcpy(worker.bgw_function_name, "webserver_worker_main");
  worker.bgw_notify_pid = 0;
  RegisterBackgroundWorker(&worker);
  /*
   * Set the value of the hooks.
   */
  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = ybpgm_startup_hook;

  prev_ExecutorStart = ExecutorStart_hook;
  ExecutorStart_hook = ybpgm_ExecutorStart;

  prev_ExecutorRun = ExecutorRun_hook;
  ExecutorRun_hook = ybpgm_ExecutorRun;

  prev_ExecutorFinish = ExecutorFinish_hook;
  ExecutorFinish_hook = ybpgm_ExecutorFinish;

  prev_ExecutorEnd = ExecutorEnd_hook;
  ExecutorEnd_hook = ybpgm_ExecutorEnd;

  prev_ProcessUtility = ProcessUtility_hook;
  ProcessUtility_hook = ybpgm_ProcessUtility;
}

/*
 * Allocate or attach to shared memory.
 */
static void
ybpgm_startup_hook(void)
{
  if (prev_shmem_startup_hook)
    prev_shmem_startup_hook();

  bool found;

  ybpgm_table = ShmemInitStruct("yb_pg_metrics",
                                 num_entries * sizeof(struct ybpgmEntry),
                                 &found);
  set_metric_names();
}

static void
ybpgm_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  if (prev_ExecutorStart)
    prev_ExecutorStart(queryDesc, eflags);
  else
    standard_ExecutorStart(queryDesc, eflags);

  if (isTopLevelStatement() && queryDesc->totaltime == NULL)
  {
    MemoryContext oldcxt;

    oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
    queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_TIMER);
    MemoryContextSwitchTo(oldcxt);
  }
}

static void
ybpgm_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
                 bool execute_once)
{
  statement_nesting_level++;
  PG_TRY();
  {
    if (prev_ExecutorRun)
      prev_ExecutorRun(queryDesc, direction, count, execute_once);
    else
      standard_ExecutorRun(queryDesc, direction, count, execute_once);
    statement_nesting_level--;
  }
  PG_CATCH();
  {
    statement_nesting_level--;
    PG_RE_THROW();
  }
  PG_END_TRY();
}

static void
ybpgm_ExecutorFinish(QueryDesc *queryDesc)
{
  statement_nesting_level++;
  PG_TRY();
  {
    if (prev_ExecutorFinish)
      prev_ExecutorFinish(queryDesc);
    else
      standard_ExecutorFinish(queryDesc);
    statement_nesting_level--;
  }
  PG_CATCH();
  {
    statement_nesting_level--;
    PG_RE_THROW();
  }
  PG_END_TRY();
}

static void
ybpgm_ExecutorEnd(QueryDesc *queryDesc)
{
  statementType type;

  switch (queryDesc->operation) {
    case CMD_SELECT:
      type = Select;
      break;
    case CMD_INSERT:
      type = Insert;
      break;
    case CMD_DELETE:
      type = Delete;
      break;
    case CMD_UPDATE:
      type = Update;
      break;
    default:
      type = Other;
      break;
  }

  if (isTopLevelStatement()) {
	uint64_t time;

	InstrEndLoop(queryDesc->totaltime);
	time = (uint64_t) (queryDesc->totaltime->total * 1000000.0);

	ybpgm_Store(type, time);

	if (!queryDesc->estate->es_yb_is_single_row_modify_txn)
	  ybpgm_Store(Transaction, time);

	if (IsA(queryDesc->planstate, AggState) &&
		castNode(AggState, queryDesc->planstate)->yb_pushdown_supported)
	  ybpgm_Store(AggregatePushdown, time);
  }

  statement_nesting_level++;
  PG_TRY();
  {
    if (prev_ExecutorEnd)
      prev_ExecutorEnd(queryDesc);
    else
      standard_ExecutorEnd(queryDesc);
    statement_nesting_level--;
  }
  PG_CATCH();
  {
    statement_nesting_level--;
    PG_RE_THROW();
  }
  PG_END_TRY();
}

/*
 * Estimate shared memory space needed.
 */
static Size
ybpgm_memsize(void)
{
  Size		size;

  size = MAXALIGN(num_entries * sizeof(struct ybpgmEntry));

  return size;
}

/*
 * Hook used for tracking "Other" statements.
 */
static void
ybpgm_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                     ProcessUtilityContext context,
                     ParamListInfo params, QueryEnvironment *queryEnv,
                     DestReceiver *dest, char *completionTag)
{
  if (isTopLevelStatement() && !IsA(pstmt->utilityStmt, ExecuteStmt) &&
      !IsA(pstmt->utilityStmt, PrepareStmt) && !IsA(pstmt->utilityStmt, DeallocateStmt))
  {
    instr_time start;
    instr_time end;
    INSTR_TIME_SET_CURRENT(start);

    ++statement_nesting_level;
    PG_TRY();
    {
      if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString,
                            context, params, queryEnv,
                            dest, completionTag);
      else
        standard_ProcessUtility(pstmt, queryString,
                                context, params, queryEnv,
                                dest, completionTag);
      --statement_nesting_level;
    }
    PG_CATCH();
    {
      --statement_nesting_level;
      PG_RE_THROW();
    }
    PG_END_TRY();

    INSTR_TIME_SET_CURRENT(end);
    INSTR_TIME_SUBTRACT(end, start);
    ybpgm_Store(Other, INSTR_TIME_GET_MICROSEC(end));
  }
  else
  {
    if (prev_ProcessUtility)
      prev_ProcessUtility(pstmt, queryString,
                          context, params, queryEnv,
                          dest, completionTag);
    else
      standard_ProcessUtility(pstmt, queryString,
                              context, params, queryEnv,
                              dest, completionTag);
  }
}

static void
ybpgm_Store(statementType type, uint64_t time){
  ybpgm_table[type].calls++;
  ybpgm_table[type].total_time += time;
}
