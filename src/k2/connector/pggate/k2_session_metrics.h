/*
MIT License

Copyright(c) 2021 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include "k2_metrics_api.h"
#include "k2_log.h"

/*
histogram examples:
1. latency from 1us to 10s: st=1, factor=1.2, count=90
2. latency from 1us to 1min: st=1, factor=1.3, count=70
3. latency from 1us to 10min: st=1, factor=1.3, count=78
4. latency from 1us to 100min: st=1, factor=1.3, count=87
*/
namespace k2pg::session {
using namespace metrics;

inline std::unique_ptr<Histogram> write_op_latency;
inline std::unique_ptr<Histogram> read_op_latency;
inline std::unique_ptr<Histogram> scan_op_latency;
inline std::unique_ptr<Histogram> txn_latency;
inline std::unique_ptr<Histogram> txn_begin_latency;
inline std::unique_ptr<Histogram> txn_end_latency;
inline std::unique_ptr<Histogram> txn_ops;
inline std::unique_ptr<Histogram> txn_read_ops;
inline std::unique_ptr<Histogram> txn_write_ops;
inline std::unique_ptr<Histogram> txn_scan_ops;
inline std::unique_ptr<Histogram> in_flight_ops;
inline std::unique_ptr<Histogram> in_flight_txns;
inline std::unique_ptr<Counter> txn_commit_count;
inline std::unique_ptr<Counter> txn_abort_count;
inline std::unique_ptr<Histogram> thread_pool_task_duration;
inline std::unique_ptr<Histogram> thread_pool_qwait;
inline std::unique_ptr<Histogram> gate_get_schema_latency;
inline std::unique_ptr<Histogram> gate_create_schema_latency;
inline std::unique_ptr<Histogram> gate_create_collection_latency;
inline std::unique_ptr<Histogram> gate_drop_collection_latency;
inline std::unique_ptr<Histogram> gate_create_scanread_latency;

inline void start() {
    K2LOG_I(log::k2Client, "creating session metrics");
    write_op_latency.reset(new Histogram("write_op_latency", "latency of write ops in usec", 1, 1.2, 90, {}));
    read_op_latency.reset(new Histogram("read_op_latency", "latency of read ops in usec", 1, 1.2, 90, {}));
    scan_op_latency.reset(new Histogram("scan_op_latency", "latency of scan ops in usec", 1, 1.3, 78, {}));
    txn_latency.reset(new Histogram("txn_latency", "latency of txns in usec", 1, 1.3, 87, {}));
    txn_begin_latency.reset(new Histogram("txn_begin_latency", "latency of txn begin in usec", 1, 1.2, 90, {}));
    txn_end_latency.reset(new Histogram("txn_end_latency", "latency of txn end in usec", 1, 1.2, 90, {}));

    txn_ops.reset(new Histogram("txn_ops", "count of total ops in txn", 1, 1.3, 50, {}));
    txn_read_ops.reset(new Histogram("txn_read_ops", "count of read ops in txn", 1, 1.3, 50, {}));
    txn_write_ops.reset(new Histogram("txn_write_ops", "count of write ops in txn", 1, 1.3, 50, {}));
    txn_scan_ops.reset(new Histogram("txn_scan_ops", "count of scan ops in txn", 1, 1.3, 50, {}));
    in_flight_ops.reset(new Histogram("in_flight_ops", "total ops in flight", 1, 1.3, 50, {}));
    in_flight_txns.reset(new Histogram("in_flight_txns", "total txns in flight", 1, 1.3, 50, {}));

    thread_pool_task_duration.reset(new Histogram("thread_pool_task_duration", "latency of tasks executed in threadpool", 1, 1.3, 87, {}));
    thread_pool_qwait.reset(new Histogram("thread_pool_qwait", "Queue wait time for k2 thread pool", 1, 1.3, 87, {}));
    gate_get_schema_latency.reset(new Histogram("gate_get_schema_latency", "latency of schema get in usec", 1, 1.3, 78, {}));
    gate_create_schema_latency.reset(new Histogram("gate_create_schema_latency", "latency of schema create in usec", 1, 1.3, 78, {}));
    gate_create_collection_latency.reset(new Histogram("gate_create_collection_latency", "latency of collection create in usec", 1, 1.3, 78, {}));
    gate_drop_collection_latency.reset(new Histogram("gate_drop_collection_latency", "latency of collection drop in usec", 1, 1.3, 78, {}));
    gate_create_scanread_latency.reset(new Histogram("gate_create_scanread_latency", "latency of scan read create in usec", 1, 1.3, 78, {}));

}

}
