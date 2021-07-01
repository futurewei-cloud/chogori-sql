/*--------------------------------------------------------------------------------------------------
 *
 * ybc_fdw.h
 *	  prototypes for ybc_fdw.h
 *
 * Copyright (c) YugaByte, Inc.
 * Portions Copyright (c) 2021 Futurewei Cloud
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
 * src/include/executor/ybc_fdw.h
 *
 *--------------------------------------------------------------------------------------------------
 */

#ifndef YBC_FDW_H
#define YBC_FDW_H

#include "postgres.h"

extern Datum k2_fdw_handler();

#endif							/* YBC_FDW_H */
