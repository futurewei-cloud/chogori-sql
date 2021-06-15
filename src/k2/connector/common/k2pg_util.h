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

// C wrappers around some YB utilities. Suitable for inclusion into C codebases such as our modified
// version of PostgreSQL.
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {

struct varlena;

#endif

typedef struct K2PgStatusStruct* K2PgStatus;
extern K2PgStatus K2PgStatusOK();
bool K2PgStatusIsOK(K2PgStatus s);
bool K2PgStatusIsNotFound(K2PgStatus s);
bool K2PgStatusIsDuplicateKey(K2PgStatus s);
uint32_t K2PgStatusPgsqlError(K2PgStatus s);
uint16_t K2PgStatusTransactionError(K2PgStatus s);
void K2PgFreeStatus(K2PgStatus s);

size_t K2PgStatusMessageLen(K2PgStatus s);
const char* K2PgStatusMessageBegin(K2PgStatus s);
const char* K2PgStatusCodeAsCString(K2PgStatus s);
char* DupK2PgStatusMessage(K2PgStatus status, bool message_only);

bool K2PgIsRestartReadError(uint16_t txn_errcode);

void K2PgResolveHostname();

#define CHECKED_K2PGSTATUS __attribute__ ((warn_unused_result)) K2PgStatus

typedef void* (*K2PgPAllocFn)(size_t size);

typedef struct varlena* (*K2PgCStringToTextWithLenFn)(const char* c, int size);

// Global initialization of the YugaByte subsystem.
CHECKED_K2PGSTATUS K2PgInit(
    const char* argv0,
    K2PgPAllocFn palloc_fn,
    K2PgCStringToTextWithLenFn cstring_to_text_with_len_fn);

CHECKED_K2PGSTATUS K2PgInitGFlags(const char* argv0);

// From glog's log_severity.h:
// const int GLOG_INFO = 0, GLOG_WARNING = 1, GLOG_ERROR = 2, GLOG_FATAL = 3;

// Logging macros with printf-like formatting capabilities.
#define K2PG_LOG_INFO(...) \
    K2PgLogImpl(/* severity */ 0, __FILE__, __LINE__, /* stack_trace */ false, __VA_ARGS__)
#define K2PG_LOG_WARNING(...) \
    K2PgLogImpl(/* severity */ 1, __FILE__, __LINE__, /* stack_trace */ false, __VA_ARGS__)
#define K2PG_LOG_ERROR(...) \
    K2PgLogImpl(/* severity */ 2, __FILE__, __LINE__, /* stack_trace */ false, __VA_ARGS__)
#define K2PG_LOG_FATAL(...) \
    K2PgLogImpl(/* severity */ 3, __FILE__, __LINE__, /* stack_trace */ false, __VA_ARGS__)

// Versions of these warnings that do nothing in debug mode. The fatal version logs a warning
// in release mode but does not crash.
#ifndef NDEBUG
// Logging macros with printf-like formatting capabilities.
#define K2PG_DEBUG_LOG_INFO(...) K2PG_LOG_INFO(__VA_ARGS__)
#define K2PG_DEBUG_LOG_WARNING(...) K2PG_LOG_WARNING(__VA_ARGS__)
#define K2PG_DEBUG_LOG_ERROR(...) K2PG_LOG_ERROR(__VA_ARGS__)
#define K2PG_DEBUG_LOG_FATAL(...) K2PG_LOG_FATAL(__VA_ARGS__)
#else
#define K2PG_DEBUG_LOG_INFO(...)
#define K2PG_DEBUG_LOG_WARNING(...)
#define K2PG_DEBUG_LOG_ERROR(...)
#define K2PG_DEBUG_LOG_FATAL(...) K2PG_LOG_ERROR(__VA_ARGS__)
#endif

// The following functions log the given message formatted similarly to printf followed by a stack
// trace.

#define K2PG_LOG_INFO_STACK_TRACE(...) \
    K2PgLogImpl(/* severity */ 0, __FILE__, __LINE__, /* stack_trace */ true, __VA_ARGS__)
#define K2PG_LOG_WARNING_STACK_TRACE(...) \
    K2PgLogImpl(/* severity */ 1, __FILE__, __LINE__, /* stack_trace */ true, __VA_ARGS__)
#define K2PG_LOG_ERROR_STACK_TRACE(...) \
    K2PgLogImpl(/* severity */ 2, __FILE__, __LINE__, /* stack_trace */ true, __VA_ARGS__)

// 5 is the index of the format string, 6 is the index of the first printf argument to check.
void K2PgLogImpl(int severity,
                const char* file_name,
                int line_number,
                bool stack_trace,
                const char* format,
                ...) __attribute__((format(printf, 5, 6)));

const char* K2PgGetStackTrace();

#ifdef __cplusplus
} // extern "C"
#endif
