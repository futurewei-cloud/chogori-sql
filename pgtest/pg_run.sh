#!/bin/bash
export SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
export LD_LIBRARY_PATH=$SCRIPT_DIR/../build/src/k2/connector/common/:$SCRIPT_DIR/../build/src/k2/connector/entities/:$SCRIPT_DIR/../src/k2/postgres/lib
export K2PG_ENABLED_IN_POSTGRES=1
export K2PG_TRANSACTIONS_ENABLED=1
export K2PG_ALLOW_RUNNING_AS_ANY_USER=1
export PG_NO_RESTART_ALL_CHILDREN_ON_CRASH=1

export K2_CONFIG_FILE=$SCRIPT_DIR/k2config_pgrun.json

export GLOG_logtostderr=1 # log all to stderr
export GLOG_v=5 #
export GLOG_log_dir=/tmp

cp pg_hba.conf pgroot/data/
chown root pgroot/data/pg_hba.conf
chmod 0700 pgroot/data/pg_hba.conf

$SCRIPT_DIR/../src/k2/postgres/bin/postgres -D pgroot/data -p5433 -h "*"
