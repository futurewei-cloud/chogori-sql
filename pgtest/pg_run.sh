#!/bin/bash
export LD_LIBRARY_PATH=/build/build/src/k2/connector/yb/common/:/build/build/src/k2/connector/yb/entities/:/build/src/k2/postgres/lib
export YB_ENABLED_IN_POSTGRES=1
export YB_PG_TRANSACTIONS_ENABLED=1
export YB_PG_ALLOW_RUNNING_AS_ANY_USER=1

#export K2_RDMA_DEVICE=mlx5_1
#export K2_HUGE_PAGES=TRUE
export K2_CPO_ADDRESS=tcp+k2rpc://0.0.0.0:9000
export K2_TSO_ADDRESS=tcp+k2rpc://0.0.0.0:13000
export K2_PG_CORES=10
#export K2_PG_CORES="1 2 4 10"

export K2_PG_MEM=2G
export K2_CPO_TIMEOUT=100ms
export K2_CPO_BACKOFF=100ms
export K2_MSG_CHECKSUM=TRUE
export K2_CONFIG_FILE=/build/pgtest/k2config.json
export K2_LOG_LEVEL="INFO k2::pggate=INFO k2::pg_catalog=INFO k2::tsoclient=INFO k2::cpo_client=INFO k2::transport=INFO"

export GLOG_logtostderr=1 # log all to stderr
export GLOG_v=5 #
export GLOG_log_dir=/tmp

cp pg_hba.conf pgroot/data/
chown root pgroot/data/pg_hba.conf
chmod 0700 pgroot/data/pg_hba.conf

/build/src/k2/postgres/bin/postgres -D pgroot/data -p5433 -h "*"