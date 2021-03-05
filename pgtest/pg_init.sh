#!/bin/bash
export LD_LIBRARY_PATH=/build/build/src/k2/connector/common/:/build/build/src/k2/connector/entities/
#export YB_ENABLED_IN_POSTGRES=1
export YB_PG_TRANSACTIONS_ENABLED=1
export YB_PG_ALLOW_RUNNING_AS_ANY_USER=1

mkdir -p pgroot/data
/build/src/k2/postgres/bin/initdb -D pgroot/data
