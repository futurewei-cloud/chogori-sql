/* ----------
 * pg_yb_utils.h
 *
 * Common utilities for YugaByte/PostgreSQL integration that are reused between
 * PostgreSQL server code and other PostgreSQL programs such as initdb.
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
 * src/common/pg_yb_common.h
 * ----------
 */

#ifndef PG_YB_COMMON_H
#define PG_YB_COMMON_H

#define YB_INITDB_ALREADY_DONE_EXIT_CODE 125

/**
 * Checks if the given environment variable is set to a "true" value (e.g. "1").
 */
extern bool YBCIsEnvVarTrue(const char* env_var_name);

/**
 * Checks if the given environment variable is set to a "true" value (e.g. "1"),
 * but with the given default value in case the environment variable is not
 * defined, or is set to an empty string or the string "auto".
 */
extern bool YBCIsEnvVarTrueWithDefault(
    const char* env_var_name,
    bool default_value);

/**
 * Checks if the YB_ENABLED_IN_POSTGRES is set. This is different from
 * IsYugaByteEnabled(), because the IsYugaByteEnabled() also checks that we are
 * in the "normal processing mode" and we have a YB client session.
 */
extern bool YBIsEnabledInPostgresEnvVar();

/**
 * Returns true to allow running PostgreSQL server and initdb as any user. This
 * is needed by some Docker/Kubernetes environments.
 */
extern bool YBShouldAllowRunningAsAnyUser();

/**
 * Check if the environment variable indicating that this is a child process
 * of initdb is set.
 */
extern bool YBIsInitDbModeEnvVarSet();

/**
 * Set the environment variable that will tell initdb's child process
 * that they are running as part of initdb.
 */
extern void YBSetInitDbModeEnvVar();


/**
 * Checks if environment variables indicating that YB's unsupported features must
 * be restricted are set
 */
extern bool YBIsUsingYBParser();

/**
 * Returns ERROR or WARNING level depends on environment variable
 */
extern int YBUnsupportedFeatureSignalLevel();

/**
 * Returns whether non-transactional COPY gflag is enabled.
 */
extern bool YBIsNonTxnCopyEnabled();

#endif /* PG_YB_COMMON_H */
