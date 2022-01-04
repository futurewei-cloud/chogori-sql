#!/bin/bash
echo ">>>>>>>>>>>>>>>>>>>>"
echo "start container with"
echo docker run --privileged --network=host -v "/dev/:/dev" -v "/sys/:/sys/" -v ${PWD}:/build -it --rm -e RDMAV_HUGEPAGES_SAFE=1  k2-bvu-10001.usrd.futurewei.com/k2sql_builder:latest
echo ">>>>>>>>>>>>>>>>>>>>"
MY_IP=192.168.33.2
PROTO="auto-rrdma+k2rpc"
RDMA=("--hugepages" "--rdma mlx5_1")

K2_CPO_ADDRESS=${PROTO}://${MY_IP}:9000
K2_TSO_ADDRESS=${PROTO}://${MY_IP}:13000

CPODIR=/tmp/___cpo_dir
EPS="${PROTO}://${MY_IP}:10000 ${PROTO}://${MY_IP}:10001 ${PROTO}://${MY_IP}:10002 ${PROTO}://${MY_IP}:10003"
PERSISTENCE=${PROTO}://${MY_IP}:12001

rm -rf ${CPODIR}
PATH=${PATH}:/usr/local/bin

# start CPO
cpo_main -c1 --cpuset=0 --tcp_endpoints ${K2_CPO_ADDRESS} --data_dir ${CPODIR} --prometheus_port 63000 --txn_heartbeat_deadline=1s ${RDMA[@]} -m1G --assignment_timeout=10s --nodepool_endpoints ${EPS} --tso_endpoints ${K2_TSO_ADDRESS} --persistence_endpoints ${PERSISTENCE} --heartbeat_interval=1s&
cpo_child_pid=$!

# start nodepool
nodepool -c4 --cpuset=1-4 --tcp_endpoints ${EPS} --k23si_persistence_endpoint ${PERSISTENCE} --prometheus_port 63001 --k23si_cpo_endpoint ${K2_CPO_ADDRESS} -m1G ${RDMA[@]} &
nodepool_child_pid=$!

# start persistence
persistence -c1 --cpuset=5 --tcp_endpoints ${PERSISTENCE} --prometheus_port 63002 ${RDMA[@]} &
persistence_child_pid=$!

# start tso
tso -c1 --cpuset=6 --tcp_endpoints ${K2_TSO_ADDRESS} --prometheus_port 63003 ${RDMA[@]} &
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
