#!/bin/bash
export LD_LIBRARY_PATH=/build/build/src/k2/connector/common/:/build/build/src/k2/connector/entities/:/build/src/k2/postgres/lib
export YB_ENABLED_IN_POSTGRES=1
export YB_PG_TRANSACTIONS_ENABLED=1
export YB_PG_ALLOW_RUNNING_AS_ANY_USER=1

export K2_CONFIG_FILE=/build/pgtest/k2config_pgrun.json

export GLOG_logtostderr=1 # log all to stderr
export GLOG_v=5 #
export GLOG_log_dir=/tmp

cp pg_hba.conf pgroot/data/
chown root pgroot/data/pg_hba.conf
chmod 0700 pgroot/data/pg_hba.conf

/build/src/k2/postgres/bin/postgres -D pgroot/data -p5433 -h "*"
