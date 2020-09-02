// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
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
// Copyright(c) 2020 Futurewei Cloud
//
// Permission is hereby granted,
//        free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

#ifndef CHOGORI_SQL_ENTITY_IDS_H
#define CHOGORI_SQL_ENTITY_IDS_H

#include <string>
#include <set>

#include "yb/common/result.h"
#include "yb/common/type/strongly_typed_string.h"

namespace k2 {
namespace sql {

    using namespace yb;   

    using NamespaceName = std::string;
    using TableName = std::string;
    using UDTypeName = std::string;
    using RoleName = std::string;

    using NamespaceId = std::string;
    using TableId = std::string;
    using UDTypeId = std::string;

    using NamespaceIdTableNamePair = std::pair<NamespaceId, TableName>;
    
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

    static const uint32_t kPgSequencesDataTableOid = 0xFFFF;
    static const uint32_t kPgSequencesDataDatabaseOid = 0xFFFF;

    static const uint32_t kPgIndexTableOid = 2610;  // Hardcoded for pg_index. (in pg_index.h)

    extern const TableId kPgProcTableId;

    // Get YB namespace id for a Postgres database.
    NamespaceId GetPgsqlNamespaceId(uint32_t database_oid);

    // Get YB table id for a Postgres table.
    TableId GetPgsqlTableId(uint32_t database_oid, uint32_t table_oid);

    // Is the namespace/table id a Postgres database or table id?
    bool IsPgsqlId(const string& id);

    // Get Postgres database and table oids from a YB namespace/table id.
    Result<uint32_t> GetPgsqlDatabaseOid(const NamespaceId& namespace_id);
    Result<uint32_t> GetPgsqlTableOid(const TableId& table_id);
    Result<uint32_t> GetPgsqlDatabaseOidByTableId(const TableId& table_id);

    // This enum matches enum RowMarkType defined in src/include/nodes/plannodes.h.
    // The exception is ROW_MARK_ABSENT, which signifies the absence of a row mark.
    enum class RowMarkType
    {
        // Obtain exclusive tuple lock.
        ROW_MARK_EXCLUSIVE = 0,
        // Obtain no-key exclusive tuple lock.
        ROW_MARK_NOKEYEXCLUSIVE = 1,
        // Obtain shared tuple lock.
        ROW_MARK_SHARE = 2,
        // Obtain keyshare tuple lock.
        ROW_MARK_KEYSHARE = 3,
        // Not supported. Used for postgres compatibility.
        ROW_MARK_REFERENCE = 4,
        // Not supported. Used for postgres compatibility.
        ROW_MARK_COPY = 5,
        // Obtain no tuple lock (this should never sent be on the wire).  The value
        // should be high for convenient comparisons with the other row lock types.
        ROW_MARK_ABSENT = 15
    };

    bool IsValidRowMarkType(RowMarkType row_mark_type);

    bool RowMarkNeedsPessimisticLock(RowMarkType row_mark_type);

}  // namespace sql
}  // namespace k2

#endif //CHOGORI_SQL_ENTITY_IDS_H
