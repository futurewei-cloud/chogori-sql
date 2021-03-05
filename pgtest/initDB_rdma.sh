#!/bin/bash
echo ">>>>>>>>>>>>>>>>>>>>"
echo "start container with"
echo docker run --privileged --network=host -v "/dev/:/dev" -v "/sys/:/sys/" -v ${PWD}:/build -it --rm -e RDMAV_HUGEPAGES_SAFE=1 -e RDMAV_FORK_SAFE=1 -e IBV_FORK_SAFE=1 k2-bvu-10001.usrd.futurewei.com/k2sql_builder:latest
echo ">>>>>>>>>>>>>>>>>>>>"
export REMOTE_IP=192.168.1.6
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
    },
    \"force_sync_finalize\": true,
    \"thread_pool_size\": 2,

    \"prometheus_port\": -1,
    \"prometheus_push_interval_ms\": 10000,
    \"prometheus_push_address\": \"127.0.0.1:9091\",

    \"cpo\": \"${PROTO}://192.168.1.8:7000\",
    \"tso_endpoint\": \"${PROTO}://192.168.1.8:8000\",
    \"partition_request_timeout\": \"900ms\",
    \"cpo_request_timeout\": \"900ms\",
    \"cpo_request_backoff\": \"300ms\",

    \"smp\": 1,
    \"memory\": \"1G\",
    \"hugepages\": true,
    \"rdma\": \"mlx5_0\",
    \"thread-affinity\": false,
    \"reactor-backend\": \"epoll\",
    \"poll-mode\": true,
    \"enable_tx_checksum\": false,
    \"log_level\": \"INFO k2::pggate=INFO k2::pg_catalog=INFO k2::tsoclient=INFO k2::cpo_client=INFO k2::transport=INFO\"

}
" > k2config_rdma.json

export LD_LIBRARY_PATH=/build/build/src/k2/connector/common/:/build/build/src/k2/connector/entities/
export YB_ENABLED_IN_POSTGRES=1
export YB_PG_TRANSACTIONS_ENABLED=1
export YB_PG_ALLOW_RUNNING_AS_ANY_USER=1

export K2_CONFIG_FILE=/build/pgtest/k2config_rdma.json

export GLOG_logtostderr=1 # log all to stderr
export GLOG_v=5 #
export GLOG_log_dir=/tmp

rm -rf pgroot/data
mkdir -p pgroot/data

/build/src/k2/postgres/bin/initdb --locale=C -D pgroot/data
