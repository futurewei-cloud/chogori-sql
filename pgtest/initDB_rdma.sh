#!/bin/bash
echo ">>>>>>>>>>>>>>>>>>>>"
echo "start container with"
echo docker run --privileged --network=host -v "/dev/:/dev" -v "/sys/:/sys/" -v ${PWD}:/build -it --rm -e RDMAV_HUGEPAGES_SAFE=1  k2-bvu-10001.usrd.futurewei.com/k2sql_builder:latest
echo ">>>>>>>>>>>>>>>>>>>>"
export REMOTE_IP=192.168.33.2
export PROTO="auto-rrdma+k2rpc"
echo "{
    \"create_collections\": {
        \"PG_DEFAULT_CLUSTER\": {
            \"range_ends\": [\"\"],
            \"endpoints\": [\"${PROTO}://${REMOTE_IP}:10000\"]
        },
        \"template1\": {
            \"range_ends\": [\"\"],
            \"endpoints\": [\"${PROTO}://${REMOTE_IP}:10001\"]
        },
        \"template0\": {
            \"range_ends\": [\"\"],
            \"endpoints\": [\"${PROTO}://${REMOTE_IP}:10002\"]
        },
        \"postgres\": {
            \"range_ends\": [\"\"],
            \"endpoints\": [\"${PROTO}://${REMOTE_IP}:10003\"]
        }
    }
}
" > k2config_rdma.json

export LD_LIBRARY_PATH=/build/build/src/k2/connector/yb/common/:/build/build/src/k2/connector/yb/entities/
export YB_ENABLED_IN_POSTGRES=1
export YB_PG_TRANSACTIONS_ENABLED=1
export YB_PG_ALLOW_RUNNING_AS_ANY_USER=1
#export GLOG_logtostderr=1

export K2_RDMA_DEVICE=mlx5_1
export K2_HUGE_PAGES=TRUE
export K2_CPO_ADDRESS=${PROTO}://${REMOTE_IP}:9000
export K2_TSO_ADDRESS=${PROTO}://${REMOTE_IP}:13000
export K2_PG_CORES=1
#export K2_PG_CORES="1 2 4 10"

export K2_PG_MEM=1G
export K2_CPO_TIMEOUT=100ms
export K2_CPO_BACKOFF=100ms
export K2_MSG_CHECKSUM=TRUE
export K2_CONFIG_FILE=/build/pgtest/k2config_rdma.json
export K2_LOG_LEVEL="INFO k2::pggate=INFO k2::pg_catalog=INFO k2::tsoclient=INFO k2::cpo_client=INFO k2::transport=INFO"

export GLOG_logtostderr=1 # log all to stderr
export GLOG_v=5 #
export GLOG_log_dir=/tmp

rm -rf pgroot/data
mkdir -p pgroot/data

/build/src/k2/postgres/bin/initdb --locale=C -D pgroot/data
