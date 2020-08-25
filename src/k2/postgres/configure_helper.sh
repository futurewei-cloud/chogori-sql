#!/bin/bash
set -e

echo "1=$1"
echo "2=$2"
echo "3=$3"
echo "4=$4"
echo "5=$5"
echo "pwd=$PWD"

# extra libraries "-lxxx -lyyy"
export LIBS=$2
# extra lib paths "-Lxxx -Lyyy"
export LDFLAGS=$3
# extra include paths "-Ixxx -Iyyy"
export CPPFLAGS=$4
${1}/configure --prefix=$5
