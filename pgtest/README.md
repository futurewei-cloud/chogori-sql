# setup a yugabyte/vanilla PG test
## yugabyte
- start yugabyte
>docker container run --rm --init -p 5433:5433 -it yugabytedb/yugabyte
>yugabyted start

- stop yugabyte with
>yugabyte stop

## tester
- compile/run inside of builder container
> docker container run --rm --init -it --net=host -v ${PWD}:/build:delegated k2-bvu-10001.usrd.futurewei.com/k2sql_builder:latest

- compile with
>g++ -O3 -std=c++17 -I../src/k2/postgres/include/ -L../src/k2/postgres/lib/ pg_test.cpp Logging.cpp -o pg_test -lpq

- run client with helper script
> ./pg_test.sh

- run server with helper script
> ./pg_init.sh && ./pg_run.sh
