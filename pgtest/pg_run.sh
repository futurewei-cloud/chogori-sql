#!/bin/bash
#useradd -m -G users -s /bin/bash pg
#su - pg

export LD_LIBRARY_PATH=/build/build/src/k2/connector/yb/common/:/build/build/src/k2/connector/yb/entities/
export YB_PG_ALLOW_RUNNING_AS_ANY_USER=1
export YB_ENABLED_IN_POSTGRES=1
export YB_PG_TRANSACTIONS_ENABLED=1

#export K2_RDMA_DEVICE=mlx5_1
#export K2_HUGE_PAGES=TRUE
export K2_CPO_ADDRESS=tcp+k2rpc://0.0.0.0:9000
export K2_TSO_ADDRESS=tcp+k2rpc://0.0.0.0:13000
export K2_PG_CORES=1
#export K2_PG_CORES="1 2 4 10"
export K2_PG_MEM=200M
export K2_CPO_TIMEOUT=100ms
export K2_CPO_BACKOFF=100ms
export K2_MSG_CHECKSUM=TRUE

export CPODIR=/tmp/___cpo_dir
export EPS="tcp+k2rpc://0.0.0.0:10000"
export PERSISTENCE=tcp+k2rpc://0.0.0.0:12001

rm -rf ${CPODIR}
export PATH=${PATH}:/usr/local/bin

# start CPO
cpo_main -c1 --tcp_endpoints ${K2_CPO_ADDRESS} 9001 --data_dir ${CPODIR} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63000 &
cpo_child_pid=$!

# start nodepool
nodepool -c1 --tcp_endpoints ${EPS} --enable_tx_checksum true --k23si_persistence_endpoint ${PERSISTENCE} --reactor-backend epoll --prometheus_port 63001 --k23si_cpo_endpoint ${K2_CPO_ADDRESS} --tso_endpoint ${K2_TSO_ADDRESS} &
nodepool_child_pid=$!

# start persistence
persistence -c1 --tcp_endpoints ${PERSISTENCE} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63002 &
persistence_child_pid=$!

# start tso
tso -c2 --tcp_endpoints ${K2_TSO_ADDRESS} 13001 --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63003 &
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

sleep 2




/build/src/k2/postgres/bin/postgres -D pgroot/data -d5 -p5433 -h "*"
