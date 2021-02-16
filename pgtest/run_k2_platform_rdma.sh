#!/bin/bash
echo ">>>>>>>>>>>>>>>>>>>>"
echo "start container with"
echo docker run --privileged --network=host -v "/dev/:/dev" -v "/sys/:/sys/" -v ${PWD}:/build -it --rm -e RDMAV_HUGEPAGES_SAFE=1  k2-bvu-10001.usrd.futurewei.com/k2sql_builder:latest
echo ">>>>>>>>>>>>>>>>>>>>"
export MY_IP=192.168.33.2
export PROTO="auto-rrdma+k2rpc"
export RDMA=("--hugepages" "--rdma mlx5_1")

export K2_CPO_ADDRESS=${PROTO}://${MY_IP}:9000
export K2_TSO_ADDRESS=${PROTO}://${MY_IP}:13000

export CPODIR=/tmp/___cpo_dir
export EPS="${PROTO}://${MY_IP}:10000 ${PROTO}://${MY_IP}:10001 ${PROTO}://${MY_IP}:10002 ${PROTO}://${MY_IP}:10003"
export PERSISTENCE=${PROTO}://${MY_IP}:12001

rm -rf ${CPODIR}
export PATH=${PATH}:/usr/local/bin

# start CPO
cpo_main -c1 --cpuset=0 --tcp_endpoints ${K2_CPO_ADDRESS} --data_dir ${CPODIR} --enable_tx_checksum true --prometheus_port 63000 --heartbeat_deadline=30s ${RDMA[@]} -m1G &
cpo_child_pid=$!

# start nodepool
nodepool -c4 --cpuset=1-4 --tcp_endpoints ${EPS} --enable_tx_checksum true --k23si_persistence_endpoint ${PERSISTENCE} --prometheus_port 63001 --k23si_cpo_endpoint ${K2_CPO_ADDRESS} --tso_endpoint ${K2_TSO_ADDRESS} -m1G ${RDMA[@]} &
nodepool_child_pid=$!

# start persistence
persistence -c1 --cpuset=5 --tcp_endpoints ${PERSISTENCE} --enable_tx_checksum true --prometheus_port 63002 ${RDMA[@]} &
persistence_child_pid=$!

# start tso
tso -c2 --cpuset=6-7 --tcp_endpoints ${K2_TSO_ADDRESS} ${PROTO}://${MY_IP}:13001 --enable_tx_checksum true --prometheus_port 63003 ${RDMA[@]} &
tso_child_pid=$!

function finish {
  # cleanup code
  rm -rf ${CPODIR}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${nodepool_child_pid}
  echo "Waiting for nodepool child pid: ${nodepool_child_pid}"
  wait ${nodepool_child_pid}

  kill ${persistence_child_pid}
  echo "Waiting for persistence child pid: ${persistence_child_pid}"
  wait ${persistence_child_pid}

  kill ${tso_child_pid}
  echo "Waiting for tso child pid: ${tso_child_pid}"
  wait ${tso_child_pid}
}
trap finish EXIT

sleep infinity
