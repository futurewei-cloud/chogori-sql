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

// This module contains C definitions for all YugaByte structures that are used to exhange data
// and metadata between Postgres and YBClient libraries.
#pragma once

#include <stddef.h>
#include "common/k2pg_util.h"
#include "entities/data_type.h"

#ifdef __cplusplus

#define K2_DEFINE_HANDLE_TYPE(name, target) \
    namespace k2pg { \
    namespace gate { \
    class name; \
    } \
    } \
    typedef class k2pg::gate::name * K2##target;

#define K2SQL_DEFINE_HANDLE_TYPE(name, target) \
    namespace k2pg { \
    namespace sql { \
    class name; \
    } \
    } \
    typedef class k2pg::sql::name * K2##target;

#else
#define K2_DEFINE_HANDLE_TYPE(name, target) typedef struct name *K2##target;
#define K2SQL_DEFINE_HANDLE_TYPE(name, target) typedef struct name *K2##target;
#endif  // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus


// Handle to Env. Each Postgres process might need just one ENV, maybe more.
K2_DEFINE_HANDLE_TYPE(PgEnv, PgEnv)

// Handle to a session. Postgres should create one PgSession per client connection.
K2_DEFINE_HANDLE_TYPE(PgSession, PgSession)

// Handle to a statement.
K2_DEFINE_HANDLE_TYPE(PgStatement, PgStatement)

// Handle to an expression.
K2SQL_DEFINE_HANDLE_TYPE(PgExpr, PgExpr);

// Handle to a table description
K2_DEFINE_HANDLE_TYPE(PgTableDesc, PgTableDesc)

// Handle to a memory context.
K2_DEFINE_HANDLE_TYPE(PgMemctx, PgMemctx)

// K2 SQL data type in PG C code
typedef enum K2SqlDataType K2PgDataType;

// Datatype representation:
// Definition of a datatype is divided into two different sections.
// - K2PgTypeEntity is used to keep static information of a datatype.
// - K2PgTypeAttrs is used to keep customizable information of a datatype.
//
typedef struct PgTypeAttrs {
  // Currently, we only need typmod, but we might need more datatype information in the future.
  // For example, array dimensions might be needed.
  int32_t typmod;
} K2PgTypeAttrs;

// Datatype conversion functions.
typedef void (*K2PgDatumToData)(uint64_t datum, void *ybdata, int64_t *bytes);
typedef uint64_t (*K2PgDatumFromData)(const void *ybdata, int64_t bytes,
                                       const K2PgTypeAttrs *type_attrs);
typedef struct PgTypeEntity {
  // Postgres type OID.
  int type_oid;

  // K2 SQL type.
  K2PgDataType k2pg_type;

  // Allow to be used for primary key.
  bool allow_for_primary_key;

  // Datum in-memory fixed size.
  // - Size of in-memory representation for a type. Usually it's sizeof(a_struct).
  //   Example: BIGINT in-memory size === sizeof(int64)
  //            POINT in-memory size === sizeof(struct Point)
  // - Set to (-1) for types of variable in-memory size - VARSIZE_ANY should be used.
  int64_t datum_fixed_size;

  // Converting Postgres datum to K2 SQL expression.
  K2PgDatumToData datum_to_k2pg;

  // Converting K2 SQL values to Postgres in-memory-formatted datum.
  K2PgDatumFromData k2pg_to_datum;
} K2PgTypeEntity;

// API to read type information.
const K2PgTypeEntity *K2PgFindTypeEntity(int type_oid);
K2PgDataType K2PgGetType(const K2PgTypeEntity *type_entity);
bool K2PgAllowForPrimaryKey(const K2PgTypeEntity *type_entity);

// PostgreSQL can represent text strings up to 1 GB minus a four-byte header.
static const int64_t kMaxPostgresTextSizeBytes = 1024ll * 1024 * 1024 - 4;

// type oids defined in pg_type_d.h, which was generated by the build script
static const int32_t BOOL_TYPE_OID = 16;
static const int32_t STRING_TYPE_OID = 19;

// Postgres object identifier (OID) defined in Postgres' postgres_ext.h
typedef unsigned int K2PgOid;
#define kInvalidOid ((K2PgOid) 0)

// Structure to hold the values of hidden columns when passing tuple from K2 SQL to PG.
typedef struct PgSysColumns {
  // Postgres system columns.
  uint32_t oid;
  uint32_t tableoid;
  uint32_t xmin;
  uint32_t cmin;
  uint32_t xmax;
  uint32_t cmax;
  uint64_t ctid;

  // K2 Sql system columns.
  uint8_t *ybctid;
  uint8_t *ybbasectid;
} K2PgSysColumns;

// Structure to hold parameters for preparing query plan.
//
// Index-related parameters are used to describe different types of scan.
//   - Sequential scan: Index parameter is not used.
//     { index_oid, index_only_scan, use_secondary_index } = { kInvalidOid, false, false }
//   - IndexScan:
//     { index_oid, index_only_scan, use_secondary_index } = { IndexOid, false, true }
//   - IndexOnlyScan:
//     { index_oid, index_only_scan, use_secondary_index } = { IndexOid, true, true }
//   - PrimaryIndexScan: This is a special case as K2 SQL doesn't have a separated
//     primary-index database object from table object.
//       index_oid = TableOid
//       index_only_scan = true if ROWID is wanted. Otherwise, regular rowset is wanted.
//       use_secondary_index = false
//
// Attribute "querying_colocated_table"
//   - If 'true', SELECT from SQL system catalogs or colocated tables.
//   - Note that the system catalogs are specifically for Postgres API and not K2 SQL
//     system-tables.
typedef struct PgPrepareParameters {
  K2PgOid index_oid;
  bool index_only_scan;
  bool use_secondary_index;
  bool querying_colocated_table;
} K2PgPrepareParameters;

// Structure to hold the execution-control parameters.
typedef struct PgExecParameters {
  // TODO(neil) Move forward_scan flag here.
  // Scan parameters.
  // bool is_forward_scan;

  // LIMIT parameters for executing DML read.
  // - limit_count is the value of SELECT ... LIMIT
  // - limit_offset is value of SELECT ... OFFSET
  // - limit_use_default: Although count and offset are pushed down to K2 platform from Postgres,
  //   they are not always being used to identify the number of rows to be read from K2 platform.
  //   Full-scan is needed when further operations on the rows are not done by K2 platform.
  //
  //   Examples:
  //   o All rows must be sent to Postgres code layer
  //     for filtering before LIMIT is applied.
  //   o ORDER BY clause is not processed by K2 platform. Similarly all rows must be fetched and sent
  //     to Postgres code layer.
  uint64_t limit_count;
  uint64_t limit_offset;
  bool limit_use_default;
  // For now we only support one rowmark.
#ifdef __cplusplus
  int rowmark = -1;
  uint64_t read_time = 0;
  char *partition_key = NULL;
#else
  int rowmark;
  uint64_t read_time;
  char *partition_key;
#endif
} K2PgExecParameters;

typedef struct PgAttrValueDescriptor {
  int attr_num;
  uint64_t datum;
  bool is_null;
  const K2PgTypeEntity *type_entity;
} K2PgAttrValueDescriptor;

typedef struct PgCallbacks {
  void (*FetchUniqueConstraintName)(K2PgOid, char*, size_t);
  K2PgMemctx (*GetCurrentYbMemctx)();
} K2PgCallbacks;

typedef struct PgTableProperties {
  uint32_t num_hash_key_columns;
  bool is_colocated;
} K2PgTableProperties;

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#undef K2_DEFINE_HANDLE_TYPE
#undef K2SQL_DEFINE_HANDLE_TYPE
