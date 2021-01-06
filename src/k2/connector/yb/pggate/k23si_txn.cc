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
#include "k23si_txn.h"
#include "k23si_queue_defs.h"

namespace k2pg {
namespace gate {
using namespace k2;


K23SITxn::K23SITxn(k2::dto::K23SI_MTR mtr):_mtr(std::move(mtr)){}

std::future<EndResult> K23SITxn::endTxn(bool shouldCommit) {
    EndTxnRequest qr{.mtr=_mtr, .shouldCommit = shouldCommit, .prom={}};

    auto result = qr.prom.get_future();
    K2DEBUG("endtxn: " << qr.mtr);
    pushQ(endTxQ, std::move(qr));
    return result;
}


std::future<k2::QueryResult> K23SITxn::scanRead(std::shared_ptr<k2::Query> query) {
    ScanReadRequest sr {.mtr = _mtr, .query=query, .prom={}};

    auto result = sr.prom.get_future();
    K2DEBUG("scanread: "
            << ", query-start-pk=" << escape(query->startScanRecord.getPartitionKey())
            << ", query-start-rk=" << escape(query->startScanRecord.getRangeKey())
            << ", query-end-pk=" << escape(query->endScanRecord.getPartitionKey())
            << ", query-end-rk=" << escape(query->endScanRecord.getRangeKey()))
    pushQ(scanReadTxQ, std::move(sr));
    return result;
}

std::future<ReadResult<dto::SKVRecord>> K23SITxn::read(dto::SKVRecord&& rec) {
    ReadRequest qr {.mtr = _mtr, .record=std::move(rec), .key=k2::dto::Key(), .collectionName="", .prom={}};

    auto result = qr.prom.get_future();
    K2DEBUG("read: mtr=" << qr.mtr
                         << ", coll=" << escape(qr.record.collectionName)
                         << ", schema-name=" << escape(qr.record.schema->name)
                         << ", schema-version=" << qr.record.schema->version
                         << ", key-pk=" << escape(qr.record.getPartitionKey())
                         << ", key-rk=" << escape(qr.record.getRangeKey()));
    pushQ(readTxQ, std::move(qr));
    return result;
}

std::future<k2::ReadResult<k2::SKVRecord>> K23SITxn::read(k2::dto::Key key, std::string collectionName) {
    ReadRequest qr {.mtr = _mtr, .record=k2::dto::SKVRecord(), .key=std::move(key),
                    .collectionName=std::move(collectionName), .prom={}};

    auto result = qr.prom.get_future();

    K2DEBUG("read: mtr=" << qr.mtr
                         << ", coll=" << escape(qr.collectionName)
                         << ", key-pk=" << escape(qr.key.partitionKey)
                         << ", key-rk=" << escape(qr.key.rangeKey));
    pushQ(readTxQ, std::move(qr));
    return result;
}

std::future<WriteResult> K23SITxn::write(dto::SKVRecord&& rec, bool erase, bool rejectIfExists) {
    WriteRequest qr{.mtr = _mtr, .erase=erase, .rejectIfExists=rejectIfExists, .record=std::move(rec), .prom={}};

    auto result = qr.prom.get_future();

    K2DEBUG("write: mtr=" << qr.mtr
                          << ", erase=" << qr.erase << ", reject=" << qr.rejectIfExists
                          << ", coll=" << escape(qr.record.collectionName)
                          << ", schema-name=" << escape(qr.record.schema->name)
                          << ", schema-version=" << qr.record.schema->version
                          << ", key-pk=" << escape(qr.record.getPartitionKey())
                          << ", key-rk=" << escape(qr.record.getRangeKey()));
    pushQ(writeTxQ, std::move(qr));
    return result;
}

std::future<PartialUpdateResult> K23SITxn::partialUpdate(dto::SKVRecord&& rec,
                                                         std::vector<uint32_t> fieldsForUpdate,
                                                         std::string partitionKey) {
    k2::dto::Key key{};
    if (!partitionKey.empty()) {
        key.schemaName = rec.schema->name;
        key.partitionKey = partitionKey;
        key.rangeKey = "";
    }

    UpdateRequest qr{.mtr = _mtr, .record=std::move(rec), .fieldsForUpdate=std::move(fieldsForUpdate),
                     .key=std::move(key), .prom={}};

    auto result = qr.prom.get_future();

    K2DEBUG("partial write: mtr=" << qr.mtr
                          << ", coll=" << escape(qr.record.collectionName)
                          << ", schema-name=" << escape(qr.record.schema->name)
                          << ", schema-version=" << qr.record.schema->version
                          << ", key-pk=" << escape(qr.record.getPartitionKey())
                          << ", key-rk=" << escape(qr.record.getRangeKey())
                          << ", kkey-pk=" << escape(qr.key.partitionKey)
                          << ", kkey-rk=" << escape(qr.key.rangeKey));
    pushQ(updateTxQ, std::move(qr));
    return result;
}

const k2::dto::K23SI_MTR& K23SITxn::mtr() const {
    return _mtr;
}

} // ns gate
} // ns k2pg
