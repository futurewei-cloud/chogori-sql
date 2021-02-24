#pragma once

#include "k2_metrics_api.h"

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
inline Histogram *write_op_latency;
inline Histogram *read_op_latency;
inline Histogram *scan_op_latency;
inline Histogram *txn_latency;
inline Histogram *txn_begin_latency;
inline Histogram *txn_end_latency;

// in each txn, report the total ops we saw
inline Histogram *txn_ops;
inline Histogram *txn_read_ops;
inline Histogram *txn_write_ops;
inline Histogram *txn_scan_ops;

inline Gauge *in_flight_ops;
inline Gauge *in_flight_txns;

inline Counter *txn_commit_count;
inline Counter *txn_abort_count;

inline Histogram *thread_pool_task_duration;

inline void start() {
    write_op_latency = new Histogram("write_op_latency", "latency of write ops in usec", 1, 1.2, 90, {});
    read_op_latency = new Histogram("read_op_latency", "latency of read ops in usec", 1, 1.2, 90, {});
    scan_op_latency = new Histogram("scan_op_latency", "latency of scan ops in usec", 1, 1.3, 78, {});
    txn_latency = new Histogram("txn_latency", "latency of txns in usec", 1, 1.3, 87, {});
    txn_begin_latency = new Histogram("txn_begin_latency", "latency of txn begin in usec", 1, 1.2, 90, {});
    txn_end_latency = new Histogram("txn_end_latency", "latency of txn end in usec", 1, 1.2, 90, {});

    txn_ops = new Histogram("txn_ops", "count of total ops in txn", 1, 1.3, 50, {});
    txn_read_ops = new Histogram("txn_read_ops", "count of read ops in txn", 1, 1.3, 50, {});
    txn_write_ops = new Histogram("txn_write_ops", "count of write ops in txn", 1, 1.3, 50, {});
    txn_scan_ops = new Histogram("txn_scan_ops", "count of scan ops in txn", 1, 1.3, 50, {});

    in_flight_ops = new Gauge("in_flight_ops", "total ops in flight", {});
    in_flight_txns = new Gauge("in_flight_txns", "total txns in flight", {});

    txn_commit_count = new Counter("txn_commit_count", "total txns committed", {});
    txn_abort_count = new Counter("total_aborted_txns", "total txns aborted", {});

    thread_pool_task_duration = new Histogram("thread_pool_task_duration", "latency of tasks executed in threadpool", 1, 1.3, 87, {});
}

inline void stop() {
    delete write_op_latency;
    delete read_op_latency;
    delete scan_op_latency;
    delete txn_latency;
    delete txn_begin_latency;
    delete txn_end_latency;

    delete txn_ops;
    delete txn_read_ops;
    delete txn_write_ops;
    delete txn_scan_ops;

    delete in_flight_ops;
    delete in_flight_txns;

    delete txn_commit_count;
    delete txn_abort_count;

    delete thread_pool_task_duration;
}
}
