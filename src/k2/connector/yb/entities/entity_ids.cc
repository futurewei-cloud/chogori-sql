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

#include <boost/uuid/nil_generator.hpp>

#include "yb/entities/entity_ids.h"

#include "yb/common/strings/escaping.h"
#include "yb/common/cast.h"

using boost::uuids::uuid;

namespace k2pg {
namespace sql {

using namespace yb;   

static constexpr int kUuidVersion = 3; // Repurpose old name-based UUID v3 to embed Postgres oids.

const uint32_t kTemplate1Oid = 1;  // Hardcoded for template1. (in initdb.c)
const uint32_t kPgProcTableOid = 1255;  // Hardcoded for pg_proc. (in pg_proc.h)

// Static initialization is OK because this won't be used in another static initialization.
const TableId kPgProcTableId = GetPgsqlTableId(kTemplate1Oid, kPgProcTableOid);

//-------------------------------------------------------------------------------------------------

namespace {

// Layout of Postgres database and table 4-byte oids in a YugaByte 16-byte table UUID:
//
// +-----------------------------------------------------------------------------------------------+
// |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |  9  |  10 |  11 |  12 |  13 |  14 |  15 |
// +-----------------------------------------------------------------------------------------------+
// |        database       |           | vsn |     | var |     |           |        table          |
// |          oid          |           |     |     |     |     |           |         oid           |
// +-----------------------------------------------------------------------------------------------+

void UuidSetDatabaseId(const uint32_t database_oid, uuid* id) {
  id->data[0] = database_oid >> 24 & 0xFF;
  id->data[1] = database_oid >> 16 & 0xFF;
  id->data[2] = database_oid >> 8  & 0xFF;
  id->data[3] = database_oid & 0xFF;
}

void UuidSetTableIds(const uint32_t table_oid, uuid* id) {
  id->data[12] = table_oid >> 24 & 0xFF;
  id->data[13] = table_oid >> 16 & 0xFF;
  id->data[14] = table_oid >> 8  & 0xFF;
  id->data[15] = table_oid & 0xFF;
}

std::string UuidToString(uuid* id) {
  // Set variant that is stored in octet 7, which is index 8, since indexes count backwards.
  // Variant must be 0b10xxxxxx for RFC 4122 UUID variant 1.
  id->data[8] &= 0xBF;
  id->data[8] |= 0x80;

  // Set version that is stored in octet 9 which is index 6, since indexes count backwards.
  id->data[6] &= 0x0F;
  id->data[6] |= (kUuidVersion << 4);

  return b2a_hex(util::to_char_ptr(id->data), sizeof(id->data));
}

} // namespace

NamespaceId GetPgsqlNamespaceId(const uint32_t database_oid) {
  uuid id = boost::uuids::nil_uuid();
  UuidSetDatabaseId(database_oid, &id);
  return UuidToString(&id);
}

TableId GetPgsqlTableId(const uint32_t database_oid, const uint32_t table_oid) {
  uuid id = boost::uuids::nil_uuid();
  UuidSetDatabaseId(database_oid, &id);
  UuidSetTableIds(table_oid, &id);
  return UuidToString(&id);
}

bool IsPgsqlId(const string& id) {
  if (id.size() != 32) return false; // Ignore non-UUID id like "sys.catalog.uuid"
  try {
    size_t pos = 0;
#ifndef NDEBUG
    const int variant = std::stoi(id.substr(8 * 2, 2), &pos, 16);
    DCHECK((pos == 2) && (variant & 0xC0) == 0x80) << "Invalid Postgres id " << id;
#endif

    const int version = std::stoi(id.substr(6 * 2, 2), &pos, 16);
    if ((pos == 2) && (version & 0xF0) >> 4 == kUuidVersion) return true;

  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }

  return false;
}

Result<uint32_t> GetPgsqlDatabaseOid(const NamespaceId& namespace_id) {
  DCHECK(IsPgsqlId(namespace_id));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(namespace_id.substr(0, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }

  return STATUS(InvalidArgument, "Invalid PostgreSQL namespace id", namespace_id);
}

Result<uint32_t> GetPgsqlTableOid(const TableId& table_id) {
  DCHECK(IsPgsqlId(table_id));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(table_id.substr(12 * 2, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }

  return STATUS(InvalidArgument, "Invalid PostgreSQL table id", table_id);
}

Result<uint32_t> GetPgsqlDatabaseOidByTableId(const TableId& table_id) {
  DCHECK(IsPgsqlId(table_id));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(table_id.substr(0, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
      // TODO: log the actual exceptions
  }

  return STATUS(InvalidArgument, "Invalid PostgreSQL table id", table_id);
}

    bool IsValidRowMarkType(RowMarkType row_mark_type) {
        switch (row_mark_type) {
            case RowMarkType::ROW_MARK_EXCLUSIVE: FALLTHROUGH_INTENDED;
            case RowMarkType::ROW_MARK_NOKEYEXCLUSIVE: FALLTHROUGH_INTENDED;
            case RowMarkType::ROW_MARK_SHARE: FALLTHROUGH_INTENDED;
            case RowMarkType::ROW_MARK_KEYSHARE:
            return true;
            break;
            default:
            return false;
            break;
        }
    }

    bool RowMarkNeedsPessimisticLock(RowMarkType row_mark_type) {
        /*
        * Currently, using pessimistic locking for all supported row marks except the key share lock.
        * This is because key share locks are used for foreign keys and we don't want pessimistic
        * locking there.
        */
        return IsValidRowMarkType(row_mark_type) &&
            row_mark_type != RowMarkType::ROW_MARK_KEYSHARE;
    }

}  // namespace sql
}  // namespace k2pg
