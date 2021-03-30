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
#include "k2_gate.h"

#include "k2_config.h"
#include "k2_queue_defs.h"
#include "k2_session_metrics.h"

namespace k2pg {
namespace gate {
using namespace k2;

K23SIGate::K23SIGate() {
    Config conf;
    _syncFinalize = conf()["force_sync_finalize"];
}

CBFuture<K23SITxn> K23SIGate::beginTxn(const K2TxnOptions& txnOpts) {
    auto start = Clock::now();
    BeginTxnRequest qr{.opts=txnOpts, .prom={}, .startTime=start};
    qr.opts.syncFinalize = _syncFinalize;

    auto result = CBFuture<K23SITxn>(qr.prom.get_future(), [start] {
        session::txn_begin_latency->observe(Clock::now() - start);
    });
    K2LOG_D(log::k2Client, "starting txn: enqueue");
    pushQ(beginTxQ, std::move(qr));
    return result;
}

CBFuture<k2::GetSchemaResult> K23SIGate::getSchema(const k2::String& collectionName, const k2::String& schemaName, uint64_t schemaVersion) {
    SchemaGetRequest qr{.collectionName = collectionName, .schemaName = schemaName, .schemaVersion = schemaVersion, .prom={}};

    auto result = CBFuture<GetSchemaResult>(qr.prom.get_future(), [st=Clock::now()] {
        session::gate_get_schema_latency->observe(Clock::now() - st);
    });
    K2LOG_D(log::k2Client, "get schema: collname={}, schema={}, version={}", collectionName, schemaName, schemaVersion);
    pushQ(schemaGetTxQ, std::move(qr));
    return result;
}

CBFuture<k2::CreateSchemaResult> K23SIGate::createSchema(const k2::String& collectionName, k2::dto::Schema& schema) {
    SchemaCreateRequest qr{.collectionName = collectionName, .schema = schema, .prom = {}};

    auto result = CBFuture<CreateSchemaResult>(qr.prom.get_future(), [st = Clock::now()] {
        session::gate_create_schema_latency->observe(Clock::now() - st);
    });
    K2LOG_D(log::k2Client, "create schema: collname={}, schema={}, raw={}", collectionName, schema.name, schema);
    pushQ(schemaCreateTxQ, std::move(qr));
    return result;
}

CBFuture<k2::Status> K23SIGate::createCollection(k2::dto::CollectionCreateRequest&& ccr)
{
    CollectionCreateRequest req{.ccr = std::move(ccr), .prom = {}};

    auto result = CBFuture<Status>(req.prom.get_future(), [st = Clock::now()] {
        session::gate_create_collection_latency->observe(Clock::now() - st);
    });

    K2LOG_D(log::k2Client, "create collection: cname={}", ccr.metadata.name);
    pushQ(collectionCreateTxQ, std::move(req));
    return result;
}

CBFuture<CreateScanReadResult> K23SIGate::createScanRead(const k2::String& collectionName,
                                                            const k2::String& schemaName) {
    ScanReadCreateRequest cr{.collectionName = collectionName, .schemaName = schemaName, .prom = {}};

    auto result = CBFuture<CreateScanReadResult>(cr.prom.get_future(), [st = Clock::now()] {
        session::gate_create_scanread_latency->observe(Clock::now() - st);
    });
    K2LOG_D(log::k2Client, "create scanread: coll={}, schema={}", collectionName, schemaName);
    pushQ(scanReadCreateTxQ, std::move(cr));
    return result;
}

} // ns gate
} // ns k2pg
