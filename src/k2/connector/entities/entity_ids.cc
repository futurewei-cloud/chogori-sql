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

#include "entities/entity_ids.h"

#include <boost/uuid/nil_generator.hpp>

#include <fmt/format.h>

namespace k2pg {
namespace sql {
using boost::uuids::uuid;
using yb::Status;

static constexpr int kUuidVersion = 3; // Repurpose old name-based UUID v3 to embed Postgres oids.

const uint32_t kTemplate1Oid = 1;  // Hardcoded for template1. (in initdb.c)
const uint32_t kPgProcTableOid = 1255;  // Hardcoded for pg_proc. (in pg_proc.h)

// Static initialization is OK because this won't be used in another static initialization.
const TableId kPgProcTableId = PgObjectId::GetTableUuid(kTemplate1Oid, kPgProcTableOid);

//-------------------------------------------------------------------------------------------------

namespace {
// TODO: get rid of YugaByte 16 byid UUID, and use PG dboid and tabld oid directly.
// Layout of Postgres database and table 4-byte oids in a YugaByte 16-byte table UUID:
//
// +-----------------------------------------------------------------------------------------------+
// |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |  9  |  10 |  11 |  12 |  13 |  14 |  15 |
// +-----------------------------------------------------------------------------------------------+
// |        database       |           | vsn |     | var |     |           |        table          |
// |          oid          |           |     |     |     |     |           |         oid           |
// +-----------------------------------------------------------------------------------------------+
static char hex_chars[] = "0123456789abcdef";

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

std::string b2a_hex(const char* input, int len) {
  std::string result;
  result.resize(len << 1);
  const unsigned char* b = reinterpret_cast<const unsigned char*>(input);
  for (int i = 0; i < len; i++) {
    result[i * 2 + 0] = hex_chars[b[i] >> 4];
    result[i * 2 + 1] = hex_chars[b[i] & 0xf];
  }
  return result;
}

std::string UuidToString(uuid* id) {
  // Set variant that is stored in octet 7, which is index 8, since indexes count backwards.
  // Variant must be 0b10xxxxxx for RFC 4122 UUID variant 1.
  id->data[8] &= 0xBF;
  id->data[8] |= 0x80;

  // Set version that is stored in octet 9 which is index 6, since indexes count backwards.
  id->data[6] &= 0x0F;
  id->data[6] |= (kUuidVersion << 4);
  return b2a_hex(yb::util::to_char_ptr(id->data), sizeof(id->data));
}

} // namespace

std::string ObjectIdGenerator::Next(const bool binary_id) {
  boost::uuids::uuid oid = oid_generator_();
  return binary_id ? string(yb::util::to_char_ptr(oid.data), sizeof(oid.data))
                   : b2a_hex(yb::util::to_char_ptr(oid.data), sizeof(oid.data));
}

std::string PgObjectId::ToString() const {
  return fmt::format("{{}, {}}", database_oid_, object_oid_);
}

std::string PgObjectId::GetDatabaseUuid() const {
  return GetDatabaseUuid(database_oid_);
}

std::string PgObjectId::GetDatabaseUuid(const PgOid& database_oid) {
  uuid id = boost::uuids::nil_uuid();
  UuidSetDatabaseId(database_oid, &id);
  return UuidToString(&id);
}

std::string PgObjectId::GetTableUuid(const PgOid& database_oid, const PgOid& table_oid) {
  uuid id = boost::uuids::nil_uuid();
  UuidSetDatabaseId(database_oid, &id);
  UuidSetTableIds(table_oid, &id);
  return UuidToString(&id);
}

std::string PgObjectId::GetTableUuid() const {
  return GetTableUuid(database_oid_, object_oid_);
}

std::string PgObjectId::GetTableId() const {
  return GetTableId(object_oid_);
}

std::string PgObjectId::GetTableId(const PgOid& table_oid) {
// table id is a uuid without database oid information
  return GetTableUuid(kPgInvalidOid, table_oid);
}

bool PgObjectId::IsPgsqlId(const string& uuid) {
  if (uuid.size() != 32) return false; // Ignore non-UUID string like "sys.catalog.uuid"
  try {
    size_t pos = 0;
#ifndef NDEBUG
    const int variant = std::stoi(uuid.substr(8 * 2, 2), &pos, 16);
    DCHECK((pos == 2) && (variant & 0xC0) == 0x80) << "Invalid Postgres uuid " << uuid;
#endif

    const int version = std::stoi(uuid.substr(6 * 2, 2), &pos, 16);
    if ((pos == 2) && (version & 0xF0) >> 4 == kUuidVersion) return true;

  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }

  return false;
}

Result<uint32_t> PgObjectId::GetDatabaseOidByUuid(const std::string& database_uuid) {
  DCHECK(IsPgsqlId(database_uuid));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(database_uuid.substr(0, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }
  return kPgInvalidOid;
  return STATUS(InvalidArgument, "Invalid PostgreSQL database uuid", database_uuid);
}

uint32_t PgObjectId::GetTableOidByTableUuid(const std::string& table_uuid) {
  DCHECK(IsPgsqlId(table_uuid));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(table_uuid.substr(12 * 2, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
    // TODO: log the actual exceptions
  }
  return kPgInvalidOid;
}

Result<uint32_t> PgObjectId::GetDatabaseOidByTableUuid(const std::string& table_uuid) {
  DCHECK(IsPgsqlId(table_uuid));
  try {
    size_t pos = 0;
    const uint32_t oid = stoul(table_uuid.substr(0, sizeof(uint32_t) * 2), &pos, 16);
    if (pos == sizeof(uint32_t) * 2) {
      return oid;
    }
  } catch(const std::invalid_argument&) {
  } catch(const std::out_of_range&) {
      // TODO: log the actual exceptions
  }

  return STATUS(InvalidArgument, "Invalid PostgreSQL table uuid", table_uuid);
}

    bool IsValidRowMarkType(RowMarkType row_mark_type) {
        switch (row_mark_type) {
            case RowMarkType::ROW_MARK_EXCLUSIVE: [[fallthrough]];
            case RowMarkType::ROW_MARK_NOKEYEXCLUSIVE: [[fallthrough]];
            case RowMarkType::ROW_MARK_SHARE: [[fallthrough]];
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
