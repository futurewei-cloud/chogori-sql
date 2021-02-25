#!/bin/bash
export K2_CPO_ADDRESS=tcp+k2rpc://0.0.0.0:9000
export K2_TSO_ADDRESS=tcp+k2rpc://0.0.0.0:13000

export CPODIR=/tmp/___cpo_dir
export EPS="tcp+k2rpc://0.0.0.0:10000 tcp+k2rpc://0.0.0.0:10001 tcp+k2rpc://0.0.0.0:10002 tcp+k2rpc://0.0.0.0:10003"
export PERSISTENCE=tcp+k2rpc://0.0.0.0:12001

rm -rf ${CPODIR}
export PATH=${PATH}:/usr/local/bin

# start CPO
cpo_main -c1 --tcp_endpoints ${K2_CPO_ADDRESS} --data_dir ${CPODIR} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63000 --heartbeat_deadline=60s &
cpo_child_pid=$!

# start nodepool
nodepool -c4 --tcp_endpoints ${EPS} --enable_tx_checksum true --k23si_persistence_endpoint ${PERSISTENCE} --reactor-backend epoll --prometheus_port 63001 --k23si_cpo_endpoint ${K2_CPO_ADDRESS} --tso_endpoint ${K2_TSO_ADDRESS} -m24G &
nodepool_child_pid=$!

# start persistence
persistence -c1 --tcp_endpoints ${PERSISTENCE} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63002 &
persistence_child_pid=$!

# start tso
tso -c2 --tcp_endpoints ${K2_TSO_ADDRESS} 13001 --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63003 &
tso_child_pid=$!
