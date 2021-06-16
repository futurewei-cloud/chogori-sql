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
#pragma once

#include <string>
#include <set>

#include <boost/functional/hash/hash.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "common/result.h"

namespace k2pg {
namespace sql {
    using k2pg::Result;

    using TableName = std::string;
    using UDTypeName = std::string;
    using RoleName = std::string;

    using TableId = std::string;
    using UDTypeId = std::string;

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
        kYBTupleId            = -8, // k2pgctid: virtual column representing DocDB-encoded key.
                              // YB analogue of Postgres's SelfItemPointer/ctid column.

        // The following attribute numbers are stored persistently in the table schema. For this reason,
        // they are chosen to avoid potential conflict with Postgres' own sys attributes now and future.
        kYBRowId              = -100, // ybrowid: auto-generated key-column for tables without pkey.
        kYBIdxBaseTupleId     = -101, // ybidxbasectid: for indexes k2pgctid of the indexed table row.
        kYBUniqueIdxKeySuffix = -102, // ybuniqueidxkeysuffix: extra key column for unique indexes, used
                                      // to ensure SQL semantics for null (null != null) in DocDB
                                      // (where null == null). For each index row will be set to:
                                      //  - the base table ctid when one or more indexed cols are null
                                      //  - to null otherwise (all indexed cols are non-null).
    };

    // Postgres object identifier (OID).
    typedef uint32_t PgOid;
    static constexpr PgOid kPgInvalidOid = 0;
    static constexpr PgOid kPgByteArrayOid = 17;

    static const uint32_t kPgSequencesDataTableOid = 0xFFFF;
    static const uint32_t kPgSequencesDataDatabaseOid = 0xFFFF;

    static const uint32_t kPgIndexTableOid = 2610;  // Hardcoded for pg_index. (in pg_index.h)

    extern const TableId kPgProcTableId;

    // Convert a strongly typed enum to its underlying type.
    // Based on an answer to this StackOverflow question: https://goo.gl/zv2Wg3
    template <typename E>
    constexpr typename std::underlying_type<E>::type to_underlying(E e) {
        return static_cast<typename std::underlying_type<E>::type>(e);
    }

    class ObjectIdGenerator {
        public:
        ObjectIdGenerator() {}
        ~ObjectIdGenerator() {}

        std::string Next(bool binary_id = false);

        private:
        boost::uuids::random_generator oid_generator_;
    };

    // A class to identify a Postgres object by oid and the database oid it belongs to.
    class PgObjectId {
        public:
        PgObjectId(const PgOid database_oid, const PgOid object_oid) : database_oid_(database_oid), object_oid_(object_oid) {
        }

        PgObjectId(): database_oid_(kPgInvalidOid), object_oid_(kPgInvalidOid) {
        }

        explicit PgObjectId(const std::string& table_uuid) {
            auto res = GetDatabaseOidByTableUuid(table_uuid);
            if (res.ok()) {
                database_oid_ = res.get();
            }
            res = GetTableOidByTableUuid(table_uuid);
            if (res.ok()) {
                object_oid_ = res.get();
            } else {
                // Reset the previously set database_oid.
                database_oid_ = kPgInvalidOid;
            }
        }

        // Get database uuid for a Postgres database.
        std::string GetDatabaseUuid() const;
        static std::string GetDatabaseUuid(const PgOid& database_oid);

        // Get table uuid for a Postgres table.
        std::string GetTableUuid() const;
        static std::string GetTableUuid(const PgOid& database_oid, const PgOid& table_oid);

        // Get table id string from table oid
        std::string GetTableId() const;
        static std::string GetTableId(const PgOid& table_oid);

        // Is the database/table uuid a Postgres database or table uuid?
        static bool IsPgsqlId(const std::string& uuid);

        // Get Postgres database and table oids from a namespace/table uuid.
        Result<uint32_t> GetDatabaseOidByUuid(const std::string& namespace_uuid);
        static uint32_t GetTableOidByTableUuid(const std::string& table_uuid);
        static Result<uint32_t> GetDatabaseOidByTableUuid(const std::string& table_uuid);
        std::string ToString() const;

        const PgOid GetDatabaseOid() const {
            return database_oid_;
        }

        const PgOid GetObjectOid() const {
            return object_oid_;
        }

        bool IsValid() const {
            return database_oid_ != kPgInvalidOid && object_oid_ != kPgInvalidOid;
        }

        bool operator== (const PgObjectId& other) const {
            return database_oid_ == other.GetDatabaseOid() && object_oid_ == other.GetObjectOid();
        }

        friend std::size_t hash_value(const PgObjectId& id) {
            std::size_t value = 0;
            boost::hash_combine(value, id.GetDatabaseOid());
            boost::hash_combine(value, id.GetObjectOid());
            return value;
        }

        private:
        PgOid database_oid_ = kPgInvalidOid;
        PgOid object_oid_ = kPgInvalidOid;
    };

    typedef boost::hash<PgObjectId> PgObjectIdHash;

    inline std::ostream& operator<<(std::ostream& out, const PgObjectId& id) {
        return out << id.ToString();
    }

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
}  // namespace k2pg
