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

Datum YBCDocdbToDatum(const uint8 *data, int64 bytes, const K2PgTypeAttrs *type_attrs);
static const K2PgTypeEntity YBCFixedLenByValTypeEntity;
static const K2PgTypeEntity YBCNullTermByRefTypeEntity;
static const K2PgTypeEntity YBCVarLenByRefTypeEntity;
void YBCDatumToDocdb(Datum datum, uint8 **data, int64 *bytes);

/***************************************************************************************************
 * Find YugaByte storage type for each PostgreSQL datatype.
 * NOTE: Because YugaByte network buffer can be deleted after it is processed, Postgres layer must
 *       allocate a buffer to keep the data in its slot.
 **************************************************************************************************/
const K2PgTypeEntity *
YBCDataTypeFromOidMod(int attnum, Oid type_id)
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
					return &YBCFixedLenByValTypeEntity;
				} else {
					switch (tp->typlen) {
						case -2:
							/* null-terminated, pass-by-reference base type */
							return &YBCNullTermByRefTypeEntity;
							break;
						case -1:
							/* variable-length, pass-by-reference base type */
							return &YBCVarLenByRefTypeEntity;
							break;
						default:;
							/* fixed-length, pass-by-reference base type */
							K2PgTypeEntity *fixed_ref_type_entity = (K2PgTypeEntity *)palloc(
									sizeof(K2PgTypeEntity));
							fixed_ref_type_entity->type_oid = InvalidOid;
							fixed_ref_type_entity->k2pg_type = K2SQL_DATA_TYPE_BINARY;
							fixed_ref_type_entity->allow_for_primary_key = false;
							fixed_ref_type_entity->datum_fixed_size = tp->typlen;
							fixed_ref_type_entity->datum_to_k2pg = (K2PgDatumToData)YBCDatumToDocdb;
							fixed_ref_type_entity->k2pg_to_datum =
								(K2PgDatumFromData)YBCDocdbToDatum;
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
				return &YBCFixedLenByValTypeEntity;
				break;
			case TYPTYPE_RANGE:
				basetp_oid = ANYRANGEOID;
				break;
			default:
				K2PG_REPORT_TYPE_NOT_SUPPORTED(type_id);
				break;
		}
		return YBCDataTypeFromOidMod(InvalidAttrNumber, basetp_oid);
	}

	/* Report error if type is not supported */
	if (k2pg_type == K2SQL_DATA_TYPE_NOT_SUPPORTED) {
		K2PG_REPORT_TYPE_NOT_SUPPORTED(type_id);
	}

	/* Return the type-mapping entry */
	return type_entity;
}

bool
YBCDataTypeIsValidForKey(Oid type_id)
{
	const K2PgTypeEntity *type_entity = YBCDataTypeFromOidMod(InvalidAttrNumber, type_id);
	return K2PgAllowForPrimaryKey(type_entity);
}

const K2PgTypeEntity *
YBCDataTypeFromName(TypeName *typeName)
{
	Oid   type_id = 0;
	int32 typmod  = 0;

	typenameTypeIdAndMod(NULL /* parseState */ , typeName, &type_id, &typmod);
	return YBCDataTypeFromOidMod(InvalidAttrNumber, type_id);
}

/***************************************************************************************************
 * Conversion Functions.
 **************************************************************************************************/
/*
 * BOOL conversion.
 * Fixed size: Ignore the "bytes" data size.
 */
void YBCDatumToBool(Datum datum, bool *data, int64 *bytes) {
	*data = DatumGetBool(datum);
}

Datum YBCBoolToDatum(const bool *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return BoolGetDatum(*data);
}

/*
 * BINARY conversion.
 */
void YBCDatumToBinary(Datum datum, void **data, int64 *bytes) {
	*data = VARDATA_ANY(datum);
	*bytes = VARSIZE_ANY_EXHDR(datum);
}

Datum YBCBinaryToDatum(const void *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
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
void YBCDatumToText(Datum datum, char **data, int64 *bytes) {
	*data = VARDATA_ANY(datum);
	*bytes = VARSIZE_ANY_EXHDR(datum);
}

Datum YBCTextToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	/* While reading back TEXT from storage, we don't need to check for data length. */
	return PointerGetDatum(cstring_to_text_with_len(data, bytes));
}

/*
 * CHAR type conversion.
 */
void YBCDatumToChar(Datum datum, char *data, int64 *bytes) {
	*data = DatumGetChar(datum);
}

Datum YBCCharToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return CharGetDatum(*data);
}

/*
 * CHAR-based type conversion.
 */
void YBCDatumToBPChar(Datum datum, char **data, int64 *bytes) {
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

Datum YBCBPCharToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
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

void YBCDatumToVarchar(Datum datum, char **data, int64 *bytes) {
	*data = TextDatumGetCString(datum);
	*bytes = strlen(*data);
}

Datum YBCVarcharToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
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
void YBCDatumToName(Datum datum, char **data, int64 *bytes) {
	*data = DatumGetCString(datum);
	*bytes = strlen(*data);
}

Datum YBCNameToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
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
void YBCDatumToCStr(Datum datum, char **data, int64 *bytes) {
	*data = DatumGetCString(datum);
	*bytes = strlen(*data);
}

Datum YBCCStrToDatum(const char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
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
void YBCDatumToInt16(Datum datum, int16 *data, int64 *bytes) {
	*data = DatumGetInt16(datum);
}

Datum YBCInt16ToDatum(const int16 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Int16GetDatum(*data);
}

void YBCDatumToInt32(Datum datum, int32 *data, int64 *bytes) {
	*data = DatumGetInt32(datum);
}

Datum YBCInt32ToDatum(const int32 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Int32GetDatum(*data);
}

void YBCDatumToInt64(Datum datum, int64 *data, int64 *bytes) {
	*data = DatumGetInt64(datum);
}

Datum YBCInt64ToDatum(const int64 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Int64GetDatum(*data);
}

void YBCDatumToUInt64(Datum datum, uint64 *data, uint64 *bytes) {
        *data = DatumGetUInt64(datum);
}

Datum YBCUInt64ToDatum(const uint64 *data, uint64 bytes, const K2PgTypeAttrs *type_attrs) {
        return UInt64GetDatum(*data);
}

void YBCDatumToOid(Datum datum, Oid *data, int64 *bytes) {
	*data = DatumGetObjectId(datum);
}

Datum YBCOidToDatum(const Oid *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return ObjectIdGetDatum(*data);
}

void YBCDatumToCommandId(Datum datum, CommandId *data, int64 *bytes) {
	*data = DatumGetCommandId(datum);
}

Datum YBCCommandIdToDatum(const CommandId *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return CommandIdGetDatum(*data);
}

void YBCDatumToTransactionId(Datum datum, TransactionId *data, int64 *bytes) {
	*data = DatumGetTransactionId(datum);
}

Datum YBCTransactionIdToDatum(const TransactionId *data, int64 bytes,
							  const K2PgTypeAttrs *type_attrs) {
	return TransactionIdGetDatum(*data);
}

/*
 * FLOATs conversion.
 * Fixed size: Ignore the "bytes" data size.
 */
void YBCDatumToFloat4(Datum datum, float *data, int64 *bytes) {
	*data = DatumGetFloat4(datum);
}

Datum YBCFloat4ToDatum(const float *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Float4GetDatum(*data);
}

void YBCDatumToFloat8(Datum datum, double *data, int64 *bytes) {
	*data = DatumGetFloat8(datum);
}

Datum YBCFloat8ToDatum(const double *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return Float8GetDatum(*data);
}

/*
 * DECIMAL / NUMERIC conversion.
 * We're using plaintext c-string as an intermediate step between PG and YB numerics.
 */
void YBCDatumToDecimalText(Datum datum, char *plaintext[], int64 *bytes) {
	Numeric num = DatumGetNumeric(datum);
	*plaintext = numeric_normalize(num);
	// NaN support will be added in ENG-4645
	if (strncmp(*plaintext, "NaN", 3) == 0) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("DECIMAL does not support NaN yet")));
	}
}

Datum YBCDecimalTextToDatum(const char plaintext[], int64 bytes, const K2PgTypeAttrs *type_attrs) {
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
void YBCDatumToMoneyInt64(Datum datum, int64 *data, int64 *bytes) {
	*data = DatumGetCash(datum);
}

Datum YBCMoneyInt64ToDatum(const int64 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return CashGetDatum(*data);
}

/*
 * UUID Datatype.
 */
void YBCDatumToUuid(Datum datum, unsigned char **data, int64 *bytes) {
	// Postgres store uuid as hex string.
	*data = (DatumGetUUIDP(datum))->data;
	*bytes = UUID_LEN;
}

Datum YBCUuidToDatum(const unsigned char *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
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

void YBCDatumToDate(Datum datum, int32 *data, int64 *bytes) {
	*data = DatumGetDateADT(datum);
}

Datum YBCDateToDatum(const int32 *data, int64 bytes, const K2PgTypeAttrs* type_attrs) {
	return DateADTGetDatum(*data);
}

/*
 * TIME conversions.
 * PG represents TIME as microseconds in int64, we store it as-is
 */

void YBCDatumToTime(Datum datum, int64 *data, int64 *bytes) {
	*data = DatumGetTimeADT(datum);
}

Datum YBCTimeToDatum(const int64 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
	return TimeADTGetDatum(*data);
}

/*
 * INTERVAL conversions.
 * PG represents INTERVAL as 128 bit structure, store it as binary
 */
void YBCDatumToInterval(Datum datum, void **data, int64 *bytes) {
	*data = DatumGetIntervalP(datum);
	*bytes = sizeof(Interval);
}

Datum YBCIntervalToDatum(const void *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
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

void YBCDatumToDocdb(Datum datum, uint8 **data, int64 *bytes) {
	if (*bytes < 0) {
		*bytes = VARSIZE_ANY(datum);
	}
	*data = (uint8 *)datum;
}

Datum YBCDocdbToDatum(const uint8 *data, int64 bytes, const K2PgTypeAttrs *type_attrs) {
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
static const K2PgTypeEntity YBCTypeEntityTable[] = {
	{ BOOLOID, K2SQL_DATA_TYPE_BOOL, true, sizeof(bool),
		(K2PgDatumToData)YBCDatumToBool,
		(K2PgDatumFromData)YBCBoolToDatum },

	{ BYTEAOID, K2SQL_DATA_TYPE_BINARY, true, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ CHAROID, K2SQL_DATA_TYPE_INT8, true, -1,
		(K2PgDatumToData)YBCDatumToChar,
		(K2PgDatumFromData)YBCCharToDatum },

	{ NAMEOID, K2SQL_DATA_TYPE_STRING, true, -1,
		(K2PgDatumToData)YBCDatumToName,
		(K2PgDatumFromData)YBCNameToDatum },

	{ INT8OID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToInt64,
		(K2PgDatumFromData)YBCInt64ToDatum },

	{ INT2OID, K2SQL_DATA_TYPE_INT16, true, sizeof(int16),
		(K2PgDatumToData)YBCDatumToInt16,
		(K2PgDatumFromData)YBCInt16ToDatum },

	{ INT2VECTOROID, K2SQL_DATA_TYPE_BINARY, true, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT4OID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)YBCDatumToInt32,
		(K2PgDatumFromData)YBCInt32ToDatum },

	{ REGPROCOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ TEXTOID, K2SQL_DATA_TYPE_STRING, true, -1,
		(K2PgDatumToData)YBCDatumToText,
		(K2PgDatumFromData)YBCTextToDatum },

	{ OIDOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ TIDOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(ItemPointerData),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ XIDOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(TransactionId),
		(K2PgDatumToData)YBCDatumToTransactionId,
		(K2PgDatumFromData)YBCTransactionIdToDatum },

	{ CIDOID, K2SQL_DATA_TYPE_UINT32, false, sizeof(CommandId),
		(K2PgDatumToData)YBCDatumToCommandId,
		(K2PgDatumFromData)YBCCommandIdToDatum },

	{ OIDVECTOROID, K2SQL_DATA_TYPE_BINARY, true, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ JSONOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ XMLOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ XMLARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ PGNODETREEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ PGNDISTINCTOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ PGDEPENDENCIESOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ PGDDLCOMMANDOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToInt64,
		(K2PgDatumFromData)YBCInt64ToDatum },

	{ SMGROID, K2SQL_DATA_TYPE_INT16, true, sizeof(int16),
		(K2PgDatumToData)YBCDatumToInt16,
		(K2PgDatumFromData)YBCInt16ToDatum },

	{ POINTOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(Point),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ LSEGOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(LSEG),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ PATHOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ BOXOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(BOX),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ POLYGONOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ LINEOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(LINE),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ LINEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ FLOAT4OID, K2SQL_DATA_TYPE_FLOAT, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToFloat4,
		(K2PgDatumFromData)YBCFloat4ToDatum },

	{ FLOAT8OID, K2SQL_DATA_TYPE_DOUBLE, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToFloat8,
		(K2PgDatumFromData)YBCFloat8ToDatum },

	/* Deprecated datatype in postgres since 6.3 release */
	{ ABSTIMEOID, K2SQL_DATA_TYPE_NOT_SUPPORTED, true, sizeof(int32),
		(K2PgDatumToData)YBCDatumToInt32,
		(K2PgDatumFromData)YBCInt32ToDatum },

	/* Deprecated datatype in postgres since 6.3 release */
	{ RELTIMEOID, K2SQL_DATA_TYPE_NOT_SUPPORTED, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	/* Deprecated datatype in postgres since 6.3 release */
	{ TINTERVALOID, K2SQL_DATA_TYPE_NOT_SUPPORTED, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	/* Deprecated datatype in postgres since 6.3 release */
	{ UNKNOWNOID, K2SQL_DATA_TYPE_NOT_SUPPORTED, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ CIRCLEOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(CIRCLE),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ CIRCLEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	/* We're using int64 to represent monetary type, just like Postgres does. */
	{ CASHOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToMoneyInt64,
		(K2PgDatumFromData)YBCMoneyInt64ToDatum },

	{ MONEYARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ MACADDROID, K2SQL_DATA_TYPE_BINARY, false, sizeof(macaddr),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ INETOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ CIDROID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ MACADDR8OID, K2SQL_DATA_TYPE_BINARY, false, sizeof(macaddr8),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ BOOLARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ BYTEAARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ CHARARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ NAMEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT2ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT2VECTORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT4ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGPROCARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TEXTARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ OIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ XIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ CIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ OIDVECTORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ BPCHARARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ VARCHARARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT8ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ POINTARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ LSEGARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ PATHARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ BOXARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ FLOAT4ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ FLOAT8ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ ABSTIMEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ RELTIMEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TINTERVALARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ POLYGONARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ ACLITEMOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(AclItem),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ ACLITEMARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ MACADDRARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ MACADDR8ARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INETARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ CIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ CSTRINGARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ BPCHAROID, K2SQL_DATA_TYPE_STRING, true, -1,
		(K2PgDatumToData)YBCDatumToBPChar,
		(K2PgDatumFromData)YBCBPCharToDatum },

	{ VARCHAROID, K2SQL_DATA_TYPE_STRING, true, -1,
		(K2PgDatumToData)YBCDatumToVarchar,
		(K2PgDatumFromData)YBCVarcharToDatum },

	{ DATEOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)YBCDatumToDate,
		(K2PgDatumFromData)YBCDateToDatum },

	{ TIMEOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToTime,
		(K2PgDatumFromData)YBCTimeToDatum },

	{ TIMESTAMPOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToInt64,
		(K2PgDatumFromData)YBCInt64ToDatum },

	{ TIMESTAMPARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ DATEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TIMEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TIMESTAMPTZOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToInt64,
		(K2PgDatumFromData)YBCInt64ToDatum },

	{ TIMESTAMPTZARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INTERVALOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(Interval),
		(K2PgDatumToData)YBCDatumToInterval,
		(K2PgDatumFromData)YBCIntervalToDatum },

	{ INTERVALARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ NUMERICARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TIMETZOID, K2SQL_DATA_TYPE_BINARY, false, sizeof(TimeTzADT),
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ TIMETZARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ BITOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ BITARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ VARBITOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ VARBITARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ NUMERICOID, K2SQL_DATA_TYPE_DECIMAL, true, -1,
		(K2PgDatumToData)YBCDatumToDecimalText,
		(K2PgDatumFromData)YBCDecimalTextToDatum },

	{ REFCURSOROID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ REGPROCEDUREOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ REGOPEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ REGOPERATOROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ REGCLASSOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ REGTYPEOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ REGROLEOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ REGNAMESPACEOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ REGPROCEDUREARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGOPERARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGOPERATORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGCLASSARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGTYPEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGROLEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGNAMESPACEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ UUIDOID, K2SQL_DATA_TYPE_BINARY, true, -1,
		(K2PgDatumToData)YBCDatumToUuid,
		(K2PgDatumFromData)YBCUuidToDatum },

	{ UUIDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ LSNOID, K2SQL_DATA_TYPE_UINT64, true, sizeof(uint64),
		(K2PgDatumToData)YBCDatumToUInt64,
		(K2PgDatumFromData)YBCUInt64ToDatum },

	{ PG_LSNARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TSVECTOROID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ GTSVECTOROID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ TSQUERYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ REGCONFIGOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ REGDICTIONARYOID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ TSVECTORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ GTSVECTORARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TSQUERYARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGCONFIGARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ REGDICTIONARYARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ JSONBOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ JSONBARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TXID_SNAPSHOTOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TXID_SNAPSHOTARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT4RANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT4RANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ NUMRANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ NUMRANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TSRANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TSRANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TSTZRANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ TSTZRANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ DATERANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ DATERANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT8RANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ INT8RANGEARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ RECORDOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	{ RECORDARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },

	/* Length(cstring) == -2 to be consistent with Postgres's 'typlen' attribute */
	{ CSTRINGOID, K2SQL_DATA_TYPE_STRING, true, -2,
		(K2PgDatumToData)YBCDatumToCStr,
		(K2PgDatumFromData)YBCCStrToDatum },

	{ ANYOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)YBCDatumToInt32,
		(K2PgDatumFromData)YBCInt32ToDatum },

	{ ANYARRAYOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum },

	{ VOIDOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToInt64,
		(K2PgDatumFromData)YBCInt64ToDatum },

	{ TRIGGEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ EVTTRIGGEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ LANGUAGE_HANDLEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ INTERNALOID, K2SQL_DATA_TYPE_INT64, true, sizeof(int64),
		(K2PgDatumToData)YBCDatumToInt64,
		(K2PgDatumFromData)YBCInt64ToDatum },

	{ OPAQUEOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)YBCDatumToInt32,
		(K2PgDatumFromData)YBCInt32ToDatum },

	{ ANYELEMENTOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)YBCDatumToInt32,
		(K2PgDatumFromData)YBCInt32ToDatum },

	{ ANYNONARRAYOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)YBCDatumToInt32,
		(K2PgDatumFromData)YBCInt32ToDatum },

	{ ANYENUMOID, K2SQL_DATA_TYPE_INT32, true, sizeof(int32),
		(K2PgDatumToData)YBCDatumToInt32,
		(K2PgDatumFromData)YBCInt32ToDatum },

	{ FDW_HANDLEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ INDEX_AM_HANDLEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ TSM_HANDLEROID, K2SQL_DATA_TYPE_UINT32, true, sizeof(Oid),
		(K2PgDatumToData)YBCDatumToOid,
		(K2PgDatumFromData)YBCOidToDatum },

	{ ANYRANGEOID, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToDocdb,
		(K2PgDatumFromData)YBCDocdbToDatum },
};

/* Special type entity used for fixed-length, pass-by-value user-defined types.
 * TODO(jason): When user-defined types as primary keys are supported, change the below `false` to
 * `true`.
 */
static const K2PgTypeEntity YBCFixedLenByValTypeEntity =
	{ InvalidOid, K2SQL_DATA_TYPE_INT64, false, sizeof(int64),
		(K2PgDatumToData)YBCDatumToInt64,
		(K2PgDatumFromData)YBCInt64ToDatum };
/* Special type entity used for null-terminated, pass-by-reference user-defined types.
 * TODO(jason): When user-defined types as primary keys are supported, change the below `false` to
 * `true`.
 */
static const K2PgTypeEntity YBCNullTermByRefTypeEntity =
	{ InvalidOid, K2SQL_DATA_TYPE_BINARY, false, -2,
		(K2PgDatumToData)YBCDatumToCStr,
		(K2PgDatumFromData)YBCCStrToDatum };
/* Special type entity used for variable-length, pass-by-reference user-defined types.
 * TODO(jason): When user-defined types as primary keys are supported, change the below `false` to
 * `true`.
 */
static const K2PgTypeEntity YBCVarLenByRefTypeEntity =
	{ InvalidOid, K2SQL_DATA_TYPE_BINARY, false, -1,
		(K2PgDatumToData)YBCDatumToBinary,
		(K2PgDatumFromData)YBCBinaryToDatum };

void YBCGetTypeTable(const K2PgTypeEntity **type_table, int *count) {
	*type_table = YBCTypeEntityTable;
	*count = sizeof(YBCTypeEntityTable)/sizeof(K2PgTypeEntity);
}
