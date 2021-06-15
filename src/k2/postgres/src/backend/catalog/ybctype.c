/*--------------------------------------------------------------------------------------------------
 *
 * ybctype.c
 *        Commands for creating and altering table structures and settings
 *
 * Copyright (c) YugaByte, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 * IDENTIFICATION
 *        src/backend/catalog/ybctype.c
 *
 * TODO(all) At the mininum we must support the following datatype efficiently as they are used
 * for system tables.
 *   bool
 *   char
 *   text
 *   int2
 *   int4
 *   int8
 *   float4
 *   float8
 *   timestamptz
 *   bytea
 *   oid
 *   xid
 *   cid
 *   tid
 *   name (same as text?)
 *   aclitem
 *   pg_node_tree
 *   pg_lsn
 *   pg_ndistinct
 *   pg_dependencies
 *
 *   OID aliases:
 *
 *   regproc
 *   regprocedure
 *   regoper
 *   regoperator
 *   regclass
 *   regtype
 *   regconfig
 *   regdictionary
 *
 *   Vectors/arrays:
 *
 *   int2vector (list of 16-bit integers)
 *   oidvector (list of 32-bit unsigned integers)
 *   anyarray (list of 32-bit integers - signed or unsigned)
 *--------------------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "catalog/ybctype.h"
#include "mb/pg_wchar.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/cash.h"
#include "utils/date.h"
#include "utils/geo_decls.h"
#include "utils/inet.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

#include "pggate/pg_gate_api.h"

#include "pg_k2pg_utils.h"

Datum K2SqlToDatum(const uint8 *data, int64 bytes, const K2PgTypeAttrs *type_attrs);
static const K2PgTypeEntity K2SqlFixedLenByValTypeEntity;
static const K2PgTypeEntity K2SqlNullTermByRefTypeEntity;
static const K2PgTypeEntity K2SqlVarLenByRefTypeEntity;
void DatumToK2Sql(Datum datum, uint8 **data, int64 *bytes);

/***************************************************************************************************
 * Find YugaByte storage type for each PostgreSQL datatype.
 * NOTE: Because YugaByte network buffer can be deleted after it is processed, Postgres layer must
 *       allocate a buffer to keep the data in its slot.
 **************************************************************************************************/
const K2PgTypeEntity *
K2PgDataTypeFromOidMod(int attnum, Oid type_id)
{
	/* Find type for system column */
	if (attnum < InvalidAttrNumber) {
		switch (attnum) {
			case SelfItemPointerAttributeNumber: /* ctid */
				type_id = TIDOID;
				break;
			case ObjectIdAttributeNumber: /* oid */
			case TableOidAttributeNumber: /* tableoid */
				type_id = OIDOID;
				break;
			case MinCommandIdAttributeNumber: /* cmin */
			case MaxCommandIdAttributeNumber: /* cmax */
				type_id = CIDOID;
				break;
			case MinTransactionIdAttributeNumber: /* xmin */
			case MaxTransactionIdAttributeNumber: /* xmax */
				type_id = XIDOID;
				break;
			case YBTupleIdAttributeNumber:            /* ybctid */
			case YBIdxBaseTupleIdAttributeNumber:     /* ybidxbasectid */
			case YBUniqueIdxKeySuffixAttributeNumber: /* ybuniqueidxkeysuffix */
				type_id = BYTEAOID;
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("System column not yet supported in YugaByte: %d", attnum)));
				break;
		}
	}

	/* Find the type mapping entry */
	const K2PgTypeEntity *type_entity = K2PgFindTypeEntity(type_id);
	K2PgDataType k2pg_type = K2PgGetType(type_entity);

	/* For non-primitive types, we need to look up the definition */
	if (k2pg_type == K2SQL_DATA_TYPE_UNKNOWN_DATA) {
		HeapTuple type = typeidType(type_id);
		Form_pg_type tp = (Form_pg_type) GETSTRUCT(type);
		Oid basetp_oid = tp->typbasetype;
		ReleaseSysCache(type);

		switch (tp->typtype) {
			case TYPTYPE_BASE:
				if (tp->typbyval) {
					/* fixed-length, pass-by-value base type */
					return &K2SqlFixedLenByValTypeEntity;
				} else {
					switch (tp->typlen) {
						case -2:
							/* null-terminated, pass-by-reference base type */
							return &K2SqlNullTermByRefTypeEntity;
							break;
						case -1:
							/* variable-length, pass-by-reference base type */
							return &K2SqlVarLenByRefTypeEntity;
							break;
						default:;
							/* fixed-length, pass-by-reference base type */
							K2PgTypeEntity *fixed_ref_type_entity = (K2PgTypeEntity *)palloc(
									sizeof(K2PgTypeEntity));
							fixed_ref_type_entity->type_oid = InvalidOid;
							fixed_ref_type_entity->k2pg_type = K2SQL_DATA_TYPE_BINARY;
							fixed_ref_type_entity->allow_for_primary_key = false;
							fixed_ref_type_entity->datum_fixed_size = tp->typlen;
							fixed_ref_type_entity->datum_to_k2pg = (K2PgDatumToData)DatumToK2Sql;
							fixed_ref_type_entity->k2pg_to_datum =
								(K2PgDatumFromData)K2SqlToDatum;
							return fixed_ref_type_entity;
							break;
					}
				}
				break;
			case TYPTYPE_COMPOSITE:
				basetp_oid = RECORDOID;
				break;
			case TYPTYPE_DOMAIN:
				break;
			case TYPTYPE_ENUM:
				/*
				 * TODO(jason): use the following line instead once user-defined enums can be
				 * primary keys:
				 *   basetp_oid = ANYENUMOID;
				 */
				return &K2SqlFixedLenByValTypeEntity;
				break;
			case TYPTYPE_RANGE:
				basetp_oid = ANYRANGEOID;
				break;
			default:
				K2PG_REPORT_TYPE_NOT_SUPPORTED(type_id);
				break;
		}
		return K2PgDataTypeFromOidMod(InvalidAttrNumber, basetp_oid);
	}

	/* Report error if type is not supported */
	if (k2pg_type == K2SQL_DATA_TYPE_NOT_SUPPORTED) {
		K2PG_REPORT_TYPE_NOT_SUPPORTED(type_id);
	}

	/* Return the type-mapping entry */
	return type_entity;
}

bool
K2PgDataTypeIsValidForKey(Oid type_id)
{
	const K2PgTypeEntity *type_entity = K2PgDataTypeFromOidMod(InvalidAttrNumber, type_id);
	return K2PgAllowForPrimaryKey(type_entity);
}

const K2PgTypeEntity *
K2PgDataTypeFromName(TypeName *typeName)
{
	Oid   type_id = 0;
	int32 typmod  = 0;

	typenameTypeIdAndMod(NULL /* parseState */ , typeName, &type_id, &typmod);
	return K2PgDataTypeFromOidMod(InvalidAttrNumber, type_id);
}

/***************************************************************************************************
 * Conversion Functions.
 **************************************************************************************************/
/*
 * BOOL conversion.
 * Fixed size: Ignore the "bytes" data size.
 */
void K2SqlDatumToBool(Datum datum, bool *data, int64 *bytes) {
	*data = DatumGetBool(datum);
}

Datum K2SqlBoolToDatum(const bool *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return BoolGetDatum(*data);
}

/*
 * BINARY conversion.
 */
void K2SqlDatumToBinary(Datum datum, void **data, int64 *bytes) {
	*data = VARDATA_ANY(datum);
	*bytes = VARSIZE_ANY_EXHDR(datum);
}

Datum K2SqlBinaryToDatum(const void *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	/* PostgreSQL can represent text strings up to 1 GB minus a four-byte header. */
	if (bytes > kMaxPostgresTextSizeBytes || bytes < 0) {
		ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
						errmsg("Invalid data size")));
	}
	return PointerGetDatum(cstring_to_text_with_len(data, bytes));
}

/*
 * TEXT type conversion.
 */
void K2SqlDatumToText(Datum datum, char **data, int64 *bytes) {
	*data = VARDATA_ANY(datum);
	*bytes = VARSIZE_ANY_EXHDR(datum);
}

Datum K2SqlTextToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	/* While reading back TEXT from storage, we don't need to check for data length. */
	return PointerGetDatum(cstring_to_text_with_len(data, bytes));
}

/*
 * CHAR type conversion.
 */
void K2SqlDatumToChar(Datum datum, char *data, int64 *bytes) {
	*data = DatumGetChar(datum);
}

Datum K2SqlCharToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return CharGetDatum(*data);
}

/*
 * CHAR-based type conversion.
 */
void K2SqlDatumToBPChar(Datum datum, char **data, int64 *bytes) {
	int size;
	*data = TextDatumGetCString(datum);

	/*
	 * Right trim all spaces on the right. For CHAR(n) - BPCHAR - datatype, Postgres treats space
	 * characters at tail-end the same as '\0' characters.
	 *   "abc  " == "abc"
	 * Left spaces don't have this special behaviors.
	 *   "  abc" != "abc"
	 */
	size = strlen(*data);
	while (size > 0 && isspace((*data)[size - 1])) {
		size--;
	}
	*bytes = size;
}

Datum K2SqlBPCharToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	/* PostgreSQL can represent text strings up to 1 GB minus a four-byte header. */
	if (bytes > kMaxPostgresTextSizeBytes || bytes < 0) {
		ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
						errmsg("Invalid data size")));
	}

	/* Convert YugaByte cstring to Postgres internal representation */
	FunctionCallInfoData fargs;
	FunctionCallInfo fcinfo = &fargs;
	PG_GETARG_DATUM(0) = CStringGetDatum(data);
	PG_GETARG_DATUM(2) = Int32GetDatum(type_attrs->typmod);
	return bpcharin(fcinfo);
}

void K2SqlDatumToVarchar(Datum datum, char **data, int64 *bytes) {
	*data = TextDatumGetCString(datum);
	*bytes = strlen(*data);
}

Datum K2SqlVarcharToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	/* PostgreSQL can represent text strings up to 1 GB minus a four-byte header. */
	if (bytes > kMaxPostgresTextSizeBytes || bytes < 0) {
		ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
						errmsg("Invalid data size")));
	}

	/* Convert YugaByte cstring to Postgres internal representation */
	FunctionCallInfoData fargs;
	FunctionCallInfo fcinfo = &fargs;
	PG_GETARG_DATUM(0) = CStringGetDatum(data);
	PG_GETARG_DATUM(2) = Int32GetDatum(type_attrs->typmod);
	return varcharin(fcinfo);
}

/*
 * NAME conversion.
 */
void K2SqlDatumToName(Datum datum, char **data, int64 *bytes) {
	*data = DatumGetCString(datum);
	*bytes = strlen(*data);
}

Datum K2SqlNameToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	/* PostgreSQL can represent text strings up to 1 GB minus a four-byte header. */
	if (bytes > kMaxPostgresTextSizeBytes || bytes < 0) {
		ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
						errmsg("Invalid data size")));
	}

	/* Truncate oversize input */
	if (bytes >= NAMEDATALEN)
		bytes = pg_mbcliplen(data, bytes, NAMEDATALEN - 1);

	/* We use palloc0 here to ensure result is zero-padded */
	Name result = (Name)palloc0(NAMEDATALEN);
	memcpy(NameStr(*result), data, bytes);
	return NameGetDatum(result);
}

/*
 * PSEUDO-type cstring conversion.
 * Not a type that is used by users.
 */
void K2SqlDatumToCStr(Datum datum, char **data, int64 *bytes) {
	*data = DatumGetCString(datum);
	*bytes = strlen(*data);
}

Datum K2SqlCStrToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	/* PostgreSQL can represent text strings up to 1 GB minus a four-byte header. */
	if (bytes > kMaxPostgresTextSizeBytes || bytes < 0) {
		ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
						errmsg("Invalid data size")));
	}

	/* Convert YugaByte cstring to Postgres internal representation */
	FunctionCallInfoData fargs;
	FunctionCallInfo fcinfo = &fargs;
	PG_GETARG_DATUM(0) = CStringGetDatum(data);
	return cstring_in(fcinfo);
}

/*
 * INTEGERs conversion.
 * Fixed size: Ignore the "bytes" data size.
 */
void K2SqlDatumToInt16(Datum datum, int16 *data, int64 *bytes) {
	*data = DatumGetInt16(datum);
}

Datum K2SqlInt16ToDatum(const int16 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Int16GetDatum(*data);
}

void K2SqlDatumToInt32(Datum datum, int32 *data, int64 *bytes) {
	*data = DatumGetInt32(datum);
}

Datum K2SqlInt32ToDatum(const int32 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Int32GetDatum(*data);
}

void K2SqlDatumToInt64(Datum datum, int64 *data, int64 *bytes) {
	*data = DatumGetInt64(datum);
}

Datum K2SqlInt64ToDatum(const int64 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Int64GetDatum(*data);
}

void K2SqlDatumToUInt64(Datum datum, uint64 *data, uint64 *bytes) {
        *data = DatumGetUInt64(datum);
}

Datum K2SqlUInt64ToDatum(const uint64 *data, uint64 bytes, const K2PgTypeAttrs *type_attrs) {
        return UInt64GetDatum(*data);
}

void K2SqlDatumToOid(Datum datum, Oid *data, int64 *bytes) {
	*data = DatumGetObjectId(datum);
}

Datum K2SqlOidToDatum(const Oid *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return ObjectIdGetDatum(*data);
}

void K2SqlDatumToCommandId(Datum datum, CommandId *data, int64 *bytes) {
	*data = DatumGetCommandId(datum);
}

Datum K2SqlCommandIdToDatum(const CommandId *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return CommandIdGetDatum(*data);
}

void K2SqlDatumToTransactionId(Datum datum, TransactionId *data, int64 *bytes) {
	*data = DatumGetTransactionId(datum);
}

Datum K2SqlTransactionIdToDatum(const TransactionId *data, int64 bytes,
							  const K2PgTypeAttrs *type_attrs) {
	return TransactionIdGetDatum(*data);
}

/*
 * FLOATs conversion.
 * Fixed size: Ignore the "bytes" data size.
 */
void K2SqlDatumToFloat4(Datum datum, float *data, int64 *bytes) {
	*data = DatumGetFloat4(datum);
}

Datum K2SqlFloat4ToDatum(const float *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Float4GetDatum(*data);
}

void K2SqlDatumToFloat8(Datum datum, double *data, int64 *bytes) {
	*data = DatumGetFloat8(datum);
}

Datum K2SqlFloat8ToDatum(const double *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Float8GetDatum(*data);
}

/*
 * DECIMAL / NUMERIC conversion.
 * We're using plaintext c-string as an intermediate step between PG and YB numerics.
 */
void K2SqlDatumToDecimalText(Datum datum, char *plaintext[], int64 *bytes) {
	Numeric num = DatumGetNumeric(datum);
	*plaintext = numeric_normalize(num);
	// NaN support will be added in ENG-4645
	if (strncmp(*plaintext, "NaN", 3) == 0) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("DECIMAL does not support NaN yet")));
	}
}

Datum K2SqlDecimalTextToDatum(const char plaintext[], int64 bytes, const K2PgTypeAttrs *type_attrs) {
	FunctionCallInfoData fargs;
	FunctionCallInfo fcinfo = &fargs;
	PG_GETARG_DATUM(0) = CStringGetDatum(plaintext);
	PG_GETARG_DATUM(2) = Int32GetDatum(type_attrs->typmod);
	return numeric_in(fcinfo);
}

/*
 * MONEY conversion.
 * We're using int64 as a representation, just like Postgres does.
 */
void K2SqlDatumToMoneyInt64(Datum datum, int64 *data, int64 *bytes) {
	*data = DatumGetCash(datum);
}

Datum K2SqlMoneyInt64ToDatum(const int64 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return CashGetDatum(*data);
}

/*
 * UUID Datatype.
 */
void K2SqlDatumToUuid(Datum datum, unsigned char **data, int64 *bytes) {
	// Postgres store uuid as hex string.
	*data = (DatumGetUUIDP(datum))->data;
	*bytes = UUID_LEN;
}

Datum K2SqlUuidToDatum(const unsigned char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	// We have to make a copy for data because the "data" pointer belongs to YugaByte cache memory
	// which can be cleared at any time.
	pg_uuid_t *uuid;
	if (bytes != UUID_LEN) {
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("Unexpected size for UUID (%ld)", bytes)));
	}

	uuid = (pg_uuid_t *)palloc(sizeof(pg_uuid_t));
	memcpy(uuid->data, data, UUID_LEN);
	return UUIDPGetDatum(uuid);
}

/*
 * DATE conversions.
 * PG represents DATE as signed int32 number of days since 2000-01-01, we store it as-is
 */

void K2SqlDatumToDate(Datum datum, int32 *data, int64 *bytes) {
	*data = DatumGetDateADT(datum);
}

Datum K2SqlDateToDatum(const int32 *data, int64 bytes, const K2PgTypeAttrs* type_attrs) {
	return DateADTGetDatum(*data);
}

/*
 * TIME conversions.
 * PG represents TIME as microseconds in int64, we store it as-is
 */

void K2SqlDatumToTime(Datum datum, int64 *data, int64 *bytes) {
	*data = DatumGetTimeADT(datum);
}

Datum K2SqlTimeToDatum(const int64 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return TimeADTGetDatum(*data);
}

/*
 * INTERVAL conversions.
 * PG represents INTERVAL as 128 bit structure, store it as binary
 */
void K2SqlDatumToInterval(Datum datum, void **data, int64 *bytes) {
	*data = DatumGetIntervalP(datum);
	*bytes = sizeof(Interval);
}

Datum K2SqlIntervalToDatum(const void *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	const size_t sz = sizeof(Interval);
	if (bytes != sz) {
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("Unexpected size for Interval (%ld)", bytes)));
	}
	Interval* result = palloc(sz);
	memcpy(result, data, sz);
	return IntervalPGetDatum(result);
}

/*
 * Workaround: These conversion functions can be used as a quick workaround to support a type.
 * - Used for Datum that contains address or pointer of actual data structure.
 *     Datum = pointer to { 1 or 4 bytes for data-size | data }
 * - Save Datum exactly as-is in YugaByte storage when writing.
 * - Read YugaByte storage and copy as-is to Postgres's in-memory datum when reading.
 *
 * IMPORTANT NOTE: This doesn't work for data values that are cached in-place instead of in a
 * separate space to which the datum is pointing to. For example, it doesn't work for numeric
 * values such as int64_t.
 *   int64_value = (int64)(datum)
 *   Datum = (cached_in_place_datatype)(data)
 */

void DatumToK2Sql(Datum datum, uint8 **data, int64 *bytes) {
	if (*bytes < 0) {
		*bytes = VARSIZE_ANY(datum);
	}
	*data = (uint8 *)datum;
}

Datum K2SqlToDatum(const uint8 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	uint8 *result = palloc(bytes);
	memcpy(result, data, bytes);
	return PointerGetDatum(result);
}

/*
 * Other conversions.
 */

/***************************************************************************************************
 * Conversion Table
 * Contain function pointers for conversion between PostgreSQL Datum to YugaByte data.
 *
 * TODO(Alex)
 * - Change NOT_SUPPORTED to proper datatype.
 * - Turn ON or OFF certain type for KEY (true or false) when testing its support.
 **************************************************************************************************/
static const K2PgTypeEntity K2SqlTypeEntityTable[] = {
	{ BOOLOID, K2SQL_DATA_TYPE_BOOL, true, sizeof(bool),
		(K2PgDatumToData)K2SqlDatumToBool,
		(K2PgDatumFromData)K2SqlBoolToDatum },

	{ BYTEAOID, K2SQL_DATA_TYPE_BINARY, true, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ CHAROID, K2SQL_DATA_TYPE_INT8, true, -1,
		(K2PgDatumToData)K2SqlDatumToChar,
		(K2PgDatumFromData)K2SqlCharToDatum },

	{ NAMEOID, K2SQL_DATA_TYPE_STRING, true, -1,
		(K2PgDatumToData)K2SqlDatumToName,
		(K2PgDatumFromData)K2SqlNameToDatum },

	{ INT8OID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToInt64,
		(K2PgDatumFromData)K2SqlInt64ToDatum },

	{ INT2OID, K2SQL_DATA_TYPE_INT16, true, sizeof(int16),
		(K2PgDatumToData)K2SqlDatumToInt16,
		(K2PgDatumFromData)K2SqlInt16ToDatum },

	{ INT2VECTOROID, K2SQL_DATA_TYPE_BINARY, true, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT4OID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)K2SqlDatumToInt32,
		(K2PgDatumFromData)K2SqlInt32ToDatum },

	{ REGPROCOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ TEXTOID, K2SQL_DATA_TYPE_STRING, true, -1,
		(K2PgDatumToData)K2SqlDatumToText,
		(K2PgDatumFromData)K2SqlTextToDatum },

	{ OIDOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ TIDOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(ItemPointerData),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ XIDOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(TransactionId),
		(K2PgDatumToData)K2SqlDatumToTransactionId,
		(K2PgDatumFromData)K2SqlTransactionIdToDatum },

	{ CIDOID, K2SQL_DATA_TYPE_UINT32, false, sizeof(CommandId),
		(K2PgDatumToData)K2SqlDatumToCommandId,
		(K2PgDatumFromData)K2SqlCommandIdToDatum },

	{ OIDVECTOROID, K2SQL_DATA_TYPE_BINARY, true, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ JSONOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ XMLOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ XMLARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ PGNODETREEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ PGNDISTINCTOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ PGDEPENDENCIESOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ PGDDLCOMMANDOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToInt64,
		(K2PgDatumFromData)K2SqlInt64ToDatum },

	{ SMGROID, K2SQL_DATA_TYPE_INT16, true, sizeof(int16),
		(K2PgDatumToData)K2SqlDatumToInt16,
		(K2PgDatumFromData)K2SqlInt16ToDatum },

	{ POINTOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(Point),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ LSEGOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(LSEG),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ PATHOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ BOXOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(BOX),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ POLYGONOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ LINEOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(LINE),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ LINEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ FLOAT4OID, K2SQL_DATA_TYPE_FLOAT, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToFloat4,
		(K2PgDatumFromData)K2SqlFloat4ToDatum },

	{ FLOAT8OID, K2SQL_DATA_TYPE_DOUBLE, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToFloat8,
		(K2PgDatumFromData)K2SqlFloat8ToDatum },

	/* Deprecated datatype in postgres since 6.3 release */
	{ ABSTIMEOID, K2SQL_DATA_TYPE_NOT_SUPPORTED, true, sizeof(int32),
		(K2PgDatumToData)K2SqlDatumToInt32,
		(K2PgDatumFromData)K2SqlInt32ToDatum },

	/* Deprecated datatype in postgres since 6.3 release */
	{ RELTIMEOID, K2SQL_DATA_TYPE_NOT_SUPPORTED, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	/* Deprecated datatype in postgres since 6.3 release */
	{ TINTERVALOID, K2SQL_DATA_TYPE_NOT_SUPPORTED, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	/* Deprecated datatype in postgres since 6.3 release */
	{ UNKNOWNOID, K2SQL_DATA_TYPE_NOT_SUPPORTED, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ CIRCLEOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(CIRCLE),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ CIRCLEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	/* We're using int64 to represent monetary type, just like Postgres does. */
	{ CASHOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToMoneyInt64,
		(K2PgDatumFromData)K2SqlMoneyInt64ToDatum },

	{ MONEYARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ MACADDROID, K2SQL_DATA_TYPE_BINARY, false, sizeof(macaddr),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ INETOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ CIDROID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ MACADDR8OID, K2SQL_DATA_TYPE_BINARY, false, sizeof(macaddr8),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ BOOLARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ BYTEAARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ CHARARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ NAMEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT2ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT2VECTORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT4ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGPROCARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TEXTARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ OIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ XIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ CIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ OIDVECTORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ BPCHARARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ VARCHARARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT8ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ POINTARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ LSEGARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ PATHARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ BOXARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ FLOAT4ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ FLOAT8ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ ABSTIMEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ RELTIMEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TINTERVALARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ POLYGONARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ ACLITEMOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(AclItem),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ ACLITEMARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ MACADDRARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ MACADDR8ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INETARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ CIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ CSTRINGARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ BPCHAROID, K2SQL_DATA_TYPE_STRING, true, -1,
		(K2PgDatumToData)K2SqlDatumToBPChar,
		(K2PgDatumFromData)K2SqlBPCharToDatum },

	{ VARCHAROID, K2SQL_DATA_TYPE_STRING, true, -1,
		(K2PgDatumToData)K2SqlDatumToVarchar,
		(K2PgDatumFromData)K2SqlVarcharToDatum },

	{ DATEOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)K2SqlDatumToDate,
		(K2PgDatumFromData)K2SqlDateToDatum },

	{ TIMEOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToTime,
		(K2PgDatumFromData)K2SqlTimeToDatum },

	{ TIMESTAMPOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToInt64,
		(K2PgDatumFromData)K2SqlInt64ToDatum },

	{ TIMESTAMPARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ DATEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TIMEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TIMESTAMPTZOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToInt64,
		(K2PgDatumFromData)K2SqlInt64ToDatum },

	{ TIMESTAMPTZARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INTERVALOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(Interval),
		(K2PgDatumToData)K2SqlDatumToInterval,
		(K2PgDatumFromData)K2SqlIntervalToDatum },

	{ INTERVALARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ NUMERICARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TIMETZOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(TimeTzADT),
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ TIMETZARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ BITOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ BITARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ VARBITOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ VARBITARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ NUMERICOID, K2SQL_DATA_TYPE_DECIMAL, true, -1,
		(K2PgDatumToData)K2SqlDatumToDecimalText,
		(K2PgDatumFromData)K2SqlDecimalTextToDatum },

	{ REFCURSOROID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ REGPROCEDUREOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ REGOPEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ REGOPERATOROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ REGCLASSOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ REGTYPEOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ REGROLEOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ REGNAMESPACEOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ REGPROCEDUREARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGOPERARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGOPERATORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGCLASSARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGTYPEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGROLEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGNAMESPACEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ UUIDOID, K2SQL_DATA_TYPE_BINARY, true, -1,
		(K2PgDatumToData)K2SqlDatumToUuid,
		(K2PgDatumFromData)K2SqlUuidToDatum },

	{ UUIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ LSNOID, K2SQL_DATA_TYPE_UINT64, true, sizeof(uint64),
		(K2PgDatumToData)K2SqlDatumToUInt64,
		(K2PgDatumFromData)K2SqlUInt64ToDatum },

	{ PG_LSNARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TSVECTOROID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ GTSVECTOROID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ TSQUERYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ REGCONFIGOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ REGDICTIONARYOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ TSVECTORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ GTSVECTORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TSQUERYARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGCONFIGARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ REGDICTIONARYARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ JSONBOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ JSONBARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TXID_SNAPSHOTOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TXID_SNAPSHOTARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT4RANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT4RANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ NUMRANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ NUMRANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TSRANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TSRANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TSTZRANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ TSTZRANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ DATERANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ DATERANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT8RANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ INT8RANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ RECORDOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	{ RECORDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },

	/* Length(cstring) == -2 to be consistent with Postgres's 'typlen' attribute */
	{ CSTRINGOID, K2SQL_DATA_TYPE_STRING, true, -2,
		(K2PgDatumToData)K2SqlDatumToCStr,
		(K2PgDatumFromData)K2SqlCStrToDatum },

	{ ANYOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)K2SqlDatumToInt32,
		(K2PgDatumFromData)K2SqlInt32ToDatum },

	{ ANYARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum },

	{ VOIDOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToInt64,
		(K2PgDatumFromData)K2SqlInt64ToDatum },

	{ TRIGGEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ EVTTRIGGEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ LANGUAGE_HANDLEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ INTERNALOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToInt64,
		(K2PgDatumFromData)K2SqlInt64ToDatum },

	{ OPAQUEOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)K2SqlDatumToInt32,
		(K2PgDatumFromData)K2SqlInt32ToDatum },

	{ ANYELEMENTOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)K2SqlDatumToInt32,
		(K2PgDatumFromData)K2SqlInt32ToDatum },

	{ ANYNONARRAYOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)K2SqlDatumToInt32,
		(K2PgDatumFromData)K2SqlInt32ToDatum },

	{ ANYENUMOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)K2SqlDatumToInt32,
		(K2PgDatumFromData)K2SqlInt32ToDatum },

	{ FDW_HANDLEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ INDEX_AM_HANDLEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ TSM_HANDLEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)K2SqlDatumToOid,
		(K2PgDatumFromData)K2SqlOidToDatum },

	{ ANYRANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)DatumToK2Sql,
		(K2PgDatumFromData)K2SqlToDatum },
};

/* Special type entity used for fixed-length, pass-by-value user-defined types.
 * TODO(jason): When user-defined types as primary keys are supported, change the below `false` to
 * `true`.
 */
static const K2PgTypeEntity K2SqlFixedLenByValTypeEntity =
	{ InvalidOid, K2SQL_DATA_TYPE_INT64, false, sizeof(int64),
		(K2PgDatumToData)K2SqlDatumToInt64,
		(K2PgDatumFromData)K2SqlInt64ToDatum };
/* Special type entity used for null-terminated, pass-by-reference user-defined types.
 * TODO(jason): When user-defined types as primary keys are supported, change the below `false` to
 * `true`.
 */
static const K2PgTypeEntity K2SqlNullTermByRefTypeEntity =
	{ InvalidOid, K2SQL_DATA_TYPE_BINARY, false, -2,
		(K2PgDatumToData)K2SqlDatumToCStr,
		(K2PgDatumFromData)K2SqlCStrToDatum };
/* Special type entity used for variable-length, pass-by-reference user-defined types.
 * TODO(jason): When user-defined types as primary keys are supported, change the below `false` to
 * `true`.
 */
static const K2PgTypeEntity K2SqlVarLenByRefTypeEntity =
	{ InvalidOid, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)K2SqlDatumToBinary,
		(K2PgDatumFromData)K2SqlBinaryToDatum };

void K2PgGetTypeTable(const K2PgTypeEntity **type_table, int *count) {
	*type_table = K2SqlTypeEntityTable;
	*count = sizeof(K2SqlTypeEntityTable)/sizeof(K2PgTypeEntity);
}
