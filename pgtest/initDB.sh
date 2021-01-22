#!/bin/bash
export LD_LIBRARY_PATH=/build/build/src/k2/connector/yb/common/:/build/build/src/k2/connector/yb/entities/
export YB_ENABLED_IN_POSTGRES=1
export YB_PG_TRANSACTIONS_ENABLED=1
export YB_PG_ALLOW_RUNNING_AS_ANY_USER=1
#export GLOG_logtostderr=1

#export K2_RDMA_DEVICE=mlx5_1
#export K2_HUGE_PAGES=TRUE
export K2_CPO_ADDRESS=tcp+k2rpc://0.0.0.0:9000
export K2_TSO_ADDRESS=tcp+k2rpc://0.0.0.0:13000
export K2_PG_CORES=1
#export K2_PG_CORES="1 2 4 10"
export K2_PG_MEM=1G
export K2_CPO_TIMEOUT=100ms
export K2_CPO_BACKOFF=100ms
export K2_MSG_CHECKSUM=TRUE
export K2_CONFIG_FILE=/build/pgtest/k2config.json
export K2_LOG_LEVEL="INFO log::pg=DEBUG k2::pggate=DEBUG k2::tsoclient=INFO k2::cpo_client=INFO k2::transport=INFO"

export CPODIR=/tmp/___cpo_dir
export EPS="tcp+k2rpc://0.0.0.0:10000"
export PERSISTENCE=tcp+k2rpc://0.0.0.0:12001
export GLOG_logtostderr=1 # log all to stderr
export GLOG_v=5 #
export GLOG_log_dir=/tmp

rm -rf pgroot/data
mkdir -p pgroot/data
/build/src/k2/postgres/bin/initdb --locale=C -D pgroot/data -d 
