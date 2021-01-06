#!/bin/bash
set -e

echo "script=$1"
echo "LIBS=$2"
echo "LDFLAGS=$3"
echo "CPPFLAGS=$4"
echo "prefix=$5"
echo "pwd=$PWD"

# extra libraries "-lxxx -lyyy"
export LIBS=$2
# extra lib paths "-Lxxx -Lyyy"
export LDFLAGS=$3
# extra include paths "-Ixxx -Iyyy"
export CPPFLAGS=$4
export CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer"


${1}/configure --enable-cassert --enable-debug --prefix=$5
