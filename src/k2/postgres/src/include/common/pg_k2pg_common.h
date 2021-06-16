/* ----------
 * pg_k2pg_utils.h
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
 * src/common/pg_k2pg_common.h
 * ----------
 */

#ifndef PG_K2PG_COMMON_H
#define PG_K2PG_COMMON_H

#define K2PG_INITDB_ALREADY_DONE_EXIT_CODE 125

/**
 * Checks if the given environment variable is set to a "true" value (e.g. "1").
 */
extern bool K2PgIsEnvVarTrue(const char* env_var_name);

/**
 * Checks if the given environment variable is set to a "true" value (e.g. "1"),
 * but with the given default value in case the environment variable is not
 * defined, or is set to an empty string or the string "auto".
 */
extern bool K2PgIsEnvVarTrueWithDefault(
    const char* env_var_name,
    bool default_value);

/**
 * Checks if the K2PG_ENABLED_IN_POSTGRES is set. This is different from
 * IsK2PgEnabled(), because the IsK2PgEnabled() also checks that we are
 * in the "normal processing mode" and we have a K2PG client session.
 */
extern bool K2PgIsEnabledInPostgresEnvVar();

/**
 * Returns true to allow running PostgreSQL server and initdb as any user. This
 * is needed by some Docker/Kubernetes environments.
 */
extern bool K2PgShouldAllowRunningAsAnyUser();

/**
 * Check if the environment variable indicating that this is a child process
 * of initdb is set.
 */
extern bool K2PgIsInitDbModeEnvVarSet();

/**
 * Set the environment variable that will tell initdb's child process
 * that they are running as part of initdb.
 */
extern void K2PgSetInitDbModeEnvVar();


/**
 * Checks if environment variables indicating that K2PG's unsupported features must
 * be restricted are set
 */
extern bool IsUsingK2PGParser();

/**
 * Returns ERROR or WARNING level depends on environment variable
 */
extern int K2PgUnsupportedFeatureSignalLevel();

/**
 * Returns whether non-transactional COPY gflag is enabled.
 */
extern bool K2PgIsNonTxnCopyEnabled();

#endif /* PG_K2PG_COMMON_H */
