/*
MIT License

Copyright(c) 2020 Futurewei Cloud

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
#include "k2_txn.h"

#include "k2_queue_defs.h"
#include "k2_session_metrics.h"

namespace k2pg {
namespace gate {
using namespace k2;

K23SITxn::K23SITxn(k2::dto::K23SI_MTR mtr, k2::TimePoint startTime):_mtr(std::move(mtr)), _startTime(startTime){
    K2LOG_D(log::k2Client, "starting txn {} at time: {}", _mtr, _startTime);
    _inFlightTxns++;
    session::in_flight_txns->observe(_inFlightTxns);
}

K23SITxn::~K23SITxn() {
    K2LOG_D(log::k2Client, "dtor for txn {} started at {}", _mtr, _startTime);
}

CBFuture<EndResult> K23SITxn::endTxn(bool shouldCommit) {
    EndTxnRequest qr{.mtr=_mtr, .shouldCommit = shouldCommit, .prom={}};
    _inFlightOps ++;
    session::in_flight_ops->observe(_inFlightOps);
    auto result = CBFuture<EndResult>(qr.prom.get_future(), [this, st=_startTime, endRequestTime=Clock::now()] {
        auto now = Clock::now();
        K2LOG_D(log::k2Client, "ended txn {} started at {}", _mtr, _startTime);
        _inFlightOps --;
        _inFlightTxns--;

        session::txn_latency->observe(now - st);
        session::txn_end_latency->observe(now - endRequestTime);
        _reportEndMetrics(now);
    });

    K2LOG_D(log::k2Client, "endtxn: {}", qr.mtr);
    pushQ(endTxQ, std::move(qr));
    return result;
}


CBFuture<k2::QueryResult> K23SITxn::scanRead(std::shared_ptr<k2::Query> query) {
    _scanOps++;
    ScanReadRequest sr {.mtr = _mtr, .query=query, .prom={}};

    _inFlightOps++;
    session::in_flight_ops->observe(_inFlightOps);
    auto result = CBFuture<QueryResult>(sr.prom.get_future(), [this, st = Clock::now()] {
        _inFlightOps--;
        session::scan_op_latency->observe(Clock::now() - st);
    });

    K2LOG_D(log::k2Client, "scanread: mtr={}, query={}", sr.mtr, (*query));
    pushQ(scanReadTxQ, std::move(sr));
    return result;
}

CBFuture<ReadResult<dto::SKVRecord>> K23SITxn::read(dto::SKVRecord&& rec) {
    _readOps++;
    ReadRequest qr {.mtr = _mtr, .record=std::move(rec), .key=k2::dto::Key(), .collectionName="", .prom={}};

    _inFlightOps++;
    session::in_flight_ops->observe(_inFlightOps);
    auto result = CBFuture<ReadResult<dto::SKVRecord>>(qr.prom.get_future(), [this, st = Clock::now()] {
        _inFlightOps--;
        session::read_op_latency->observe(Clock::now() - st);
    });

    K2LOG_D(log::k2Client, "read: mtr={}, coll={}, schema-name={}, schema-version={}, key-pk={}, key-rk={}",
            qr.mtr,
            qr.record.collectionName,
            qr.record.schema->name,
            qr.record.schema->version,
            qr.record.getPartitionKey(),
            qr.record.getRangeKey());
    pushQ(readTxQ, std::move(qr));
    return result;
}

CBFuture<k2::ReadResult<k2::SKVRecord>> K23SITxn::read(k2::dto::Key key, std::string collectionName) {
    _readOps++;
    ReadRequest qr {.mtr = _mtr, .record=k2::dto::SKVRecord(), .key=std::move(key),
                    .collectionName=std::move(collectionName), .prom={}};

    _inFlightOps++;
    session::in_flight_ops->observe(_inFlightOps);
    auto result = CBFuture<ReadResult<dto::SKVRecord>>(qr.prom.get_future(), [this, st = Clock::now()] {
        _inFlightOps--;
        session::read_op_latency->observe(Clock::now() - st);
    });

    K2LOG_D(log::k2Client, "read: mtr={}, coll={}, key-pk={}, key-rk={}",
                qr.mtr,
                qr.collectionName,
                qr.key.partitionKey,
                qr.key.rangeKey);
    pushQ(readTxQ, std::move(qr));
    return result;
}

CBFuture<WriteResult> K23SITxn::write(dto::SKVRecord&& rec, bool erase, k2::dto::ExistencePrecondition precondition) {
    _writeOps++;
    WriteRequest qr{.mtr = _mtr, .erase=erase, .precondition=precondition, .record=std::move(rec), .prom={}};

    _inFlightOps++;
    session::in_flight_ops->observe(_inFlightOps);
    auto result = CBFuture<WriteResult>(qr.prom.get_future(), [this, st = Clock::now()] {
        _inFlightOps--;
        session::write_op_latency->observe(Clock::now() - st);
    });

    K2LOG_D(log::k2Client,
        "write: mtr={}, erase={}, precondition={}, coll={}, schema-name={}, schema-version={}, key-pk={}, key-rk={}",
        qr.mtr,
        qr.erase,
        qr.precondition,
        qr.record.collectionName,
        qr.record.schema->name,
        qr.record.schema->version,
        qr.record.getPartitionKey(),
        qr.record.getRangeKey());

    pushQ(writeTxQ, std::move(qr));
    return result;
}

CBFuture<PartialUpdateResult> K23SITxn::partialUpdate(dto::SKVRecord&& rec,
                                                         std::vector<uint32_t> fieldsForUpdate,
                                                         std::string partitionKey) {
    _writeOps++;
    k2::dto::Key key{};
    if (!partitionKey.empty()) {
        key.schemaName = rec.schema->name;
        key.partitionKey = partitionKey;
        key.rangeKey = "";
    }

    UpdateRequest qr{.mtr = _mtr, .record=std::move(rec), .fieldsForUpdate=std::move(fieldsForUpdate),
                     .key=std::move(key), .prom={}};

    _inFlightOps++;
    session::in_flight_ops->observe(_inFlightOps);
    auto result = CBFuture<PartialUpdateResult>(qr.prom.get_future(), [this, st = Clock::now()] {
        _inFlightOps--;
        session::write_op_latency->observe(Clock::now() - st);
    });

    K2LOG_D(log::k2Client,
        "partial write: mtr={}, record={}, kkey-pk={}, kkey-rk={}, fieldsForUpdate={}",
        qr.mtr,
        qr.record,
        qr.key.partitionKey,
        qr.key.rangeKey,
        qr.fieldsForUpdate);
    pushQ(updateTxQ, std::move(qr));
    return result;
}

const k2::dto::K23SI_MTR& K23SITxn::mtr() const {
    return _mtr;
}

void K23SITxn::_reportEndMetrics(k2::TimePoint now) {
    session::txn_latency->observe(now - _startTime);
    session::txn_ops->observe(_readOps + _writeOps + _scanOps);
    session::txn_read_ops->observe(_readOps);
    session::txn_write_ops->observe(_writeOps);
    session::txn_scan_ops->observe(_scanOps);
};

} // ns gate
} // ns k2pg
