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
//

#ifndef CHOGORI_SQL_PG_SYSTEM_ATTR_H
#define CHOGORI_SQL_PG_SYSTEM_ATTR_H

namespace k2 {
namespace sql {

// In addition to regular columns, YB support for postgres also have virtual columns.
// Virtual columns are just expression that is evaluated by DocDB in "doc_expr.cc".
enum class PgSystemAttrNum : int {
  // Postgres system columns.
  kSelfItemPointer      = -1, // ctid.
  kObjectId             = -2, // oid.
  kMinTransactionId     = -3, // xmin
  kMinCommandId         = -4, // cmin
  kMaxTransactionId     = -5, // xmax
  kMaxCommandId         = -6, // cmax
  kTableOid             = -7, // tableoid

  // YugaByte system columns.
  kYBTupleId            = -8, // ybctid: virtual column representing DocDB-encoded key.
                              // YB analogue of Postgres's SelfItemPointer/ctid column.

  // The following attribute numbers are stored persistently in the table schema. For this reason,
  // they are chosen to avoid potential conflict with Postgres' own sys attributes now and future.
  kYBRowId              = -100, // ybrowid: auto-generated key-column for tables without pkey.
  kYBIdxBaseTupleId     = -101, // ybidxbasectid: for indexes ybctid of the indexed table row.
  kYBUniqueIdxKeySuffix = -102, // ybuniqueidxkeysuffix: extra key column for unique indexes, used
                                // to ensure SQL semantics for null (null != null) in DocDB
                                // (where null == null). For each index row will be set to:
                                //  - the base table ctid when one or more indexed cols are null
                                //  - to null otherwise (all indexed cols are non-null).
};

}  // namespace sql
}  // namespace k2

#endif // CHOGORI_SQL_PG_SYSTEM_ATTR_H