/*-------------------------------------------------------------------------
 *
 * pg_yb_common.c
 *	  Common utilities for YugaByte/PostgreSQL integration that are reused
 *	  between PostgreSQL server code and other PostgreSQL programs such as
 *    initdb.
 *
 * Copyright (c) YugaByte, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * IDENTIFICATION
 *	  src/common/pg_yb_common.cc
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postgres_fe.h"

#include "common/pg_yb_common.h"

#include "utils/elog.h"

bool
YBCIsEnvVarTrue(const char* env_var_name)
{
	return YBCIsEnvVarTrueWithDefault(env_var_name, /* default_value */ false);
}

bool
YBCIsEnvVarTrueWithDefault(const char* env_var_name, bool default_value)
{
	const char* env_var_value = getenv(env_var_name);
	if (!env_var_value ||
		strlen(env_var_value) == 0 ||
		strcmp(env_var_value, "auto") == 0)
	{
		return default_value;
	}
	return strcmp(env_var_value, "1") == 0 || strcmp(env_var_value, "true") == 0;
}

bool
YBIsEnabledInPostgresEnvVar()
{
	static int cached_value = -1;
	if (cached_value == -1)
	{
		cached_value = YBCIsEnvVarTrue("YB_ENABLED_IN_POSTGRES");
	}
	return cached_value;
}

bool
YBShouldAllowRunningAsAnyUser()
{
	if (YBIsEnabledInPostgresEnvVar())
    {
		return true;
	}
	static int cached_value = -1;
	if (cached_value == -1)
    {
		cached_value = YBCIsEnvVarTrue("YB_PG_ALLOW_RUNNING_AS_ANY_USER");
	}
	return cached_value;
}

bool YBIsInitDbModeEnvVarSet()
{

	static int cached_value = -1;
	if (cached_value == -1)
    {
		cached_value = YBCIsEnvVarTrue("YB_PG_INITDB_MODE");
	}
	return cached_value;
}

void YBSetInitDbModeEnvVar()
{
	int setenv_retval = setenv("YB_PG_INITDB_MODE", "1", /* overwrite */ true);
	if (setenv_retval != 0)
	{
		perror("Could not set environment variable YB_PG_INITDB_MODE");
		exit(EXIT_FAILURE);
	}
}

bool
YBIsUsingYBParser()
{
	static int cached_value = -1;
	if (cached_value == -1) {
		cached_value = !YBIsInitDbModeEnvVarSet() && YBIsEnabledInPostgresEnvVar();
	}
	return cached_value;
}

int
YBUnsupportedFeatureSignalLevel()
{
	static int cached_value = -1;
	if (cached_value == -1) {
		// TODO(dmitry): Remove 'YB_SUPPRESS_UNSUPPORTED_ERROR'
		cached_value = YBCIsEnvVarTrue("YB_SUPPRESS_UNSUPPORTED_ERROR") ||
									 YBCIsEnvVarTrue("FLAGS_ysql_suppress_unsupported_error") ? WARNING : ERROR;
	}
	return cached_value;
}

bool
YBIsNonTxnCopyEnabled()
{
	static int cached_value = -1;
	if (cached_value == -1)
	{
		cached_value = YBCIsEnvVarTrue("FLAGS_ysql_non_txn_copy");
	}
	return cached_value;
}
