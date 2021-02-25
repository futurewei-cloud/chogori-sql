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

// latency histograms
inline std::unique_ptr<Histogram> write_op_latency;
inline std::unique_ptr<Histogram> read_op_latency;
inline std::unique_ptr<Histogram> scan_op_latency;
inline std::unique_ptr<Histogram> txn_latency;
inline std::unique_ptr<Histogram> txn_begin_latency;
inline std::unique_ptr<Histogram> txn_end_latency;

// in each txn, report the total ops we saw
inline std::unique_ptr<Histogram> txn_ops;
inline std::unique_ptr<Histogram> txn_read_ops;
inline std::unique_ptr<Histogram> txn_write_ops;
inline std::unique_ptr<Histogram> txn_scan_ops;

inline std::unique_ptr<Gauge> in_flight_ops;
inline std::unique_ptr<Gauge> in_flight_txns;

inline std::unique_ptr<Counter> txn_commit_count;
inline std::unique_ptr<Counter> txn_abort_count;

inline std::unique_ptr<Histogram> thread_pool_task_duration;

inline void start() {
    K2LOG_D(log::pg, "creating session metrics");
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

    in_flight_ops.reset(new Gauge("in_flight_ops", "total ops in flight", {}));
    in_flight_txns.reset(new Gauge("in_flight_txns", "total txns in flight", {}));

    txn_commit_count.reset(new Counter("txn_commit_count", "total txns committed", {}));
    txn_abort_count.reset(new Counter("total_aborted_txns", "total txns aborted", {}));

    thread_pool_task_duration.reset(new Histogram("thread_pool_task_duration", "latency of tasks executed in threadpool", 1, 1.3, 87, {}));
}

inline void stop() {
    K2LOG_D(log::pg, "destroying session metrics");
    write_op_latency.reset(nullptr);
    read_op_latency.reset(nullptr);
    scan_op_latency.reset(nullptr);
    txn_latency.reset(nullptr);
    txn_begin_latency.reset(nullptr);
    txn_end_latency.reset(nullptr);

    txn_ops.reset(nullptr);
    txn_read_ops.reset(nullptr);
    txn_write_ops.reset(nullptr);
    txn_scan_ops.reset(nullptr);

    in_flight_ops.reset(nullptr);
    in_flight_txns.reset(nullptr);

    txn_commit_count.reset(nullptr);
    txn_abort_count.reset(nullptr);

    thread_pool_task_duration.reset(nullptr);
}
}
