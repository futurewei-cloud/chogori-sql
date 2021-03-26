#!/bin/bash

topname=$(dirname "$0")
cd ${topname}/../..

set -e

OUTFILE=${1:-/dev/null}

function finish {
    killall postgres
    sleep 1
    killall nodepool
    killall tso
    killall persistence
    killall cpo_main
}
trap finish EXIT

cd pgtest/
echo "Starting initDB..."
./initDB.sh 2> $OUTFILE > $OUTFILE
./pg_run.sh 2> $OUTFILE > $OUTFILE &
sleep 1
cd ../test/integration/
./integrate.py -v
