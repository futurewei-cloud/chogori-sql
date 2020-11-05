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
#include "k23si_gate.h"
#include <k2/dto/Collection.h>
#include "k23si_queue_defs.h"

namespace k2pg {
namespace gate {
using namespace k2;

K23SIGate::K23SIGate() {
}

std::future<K23SITxn> K23SIGate::beginTxn(const K2TxnOptions& txnOpts) {
    BeginTxnRequest qr{.opts=txnOpts, .prom={}};

    auto result = qr.prom.get_future();
    pushQ(beginTxQ, std::move(qr));
    return result;
}

std::future<k2::GetSchemaResult> K23SIGate::getSchema(const k2::String& collectionName, const k2::String& schemaName, uint64_t schemaVersion) {
    SchemaGetRequest qr{.collectionName = collectionName, .schemaName = schemaName, .schemaVersion = schemaVersion, .prom={}};

    auto result = qr.prom.get_future();
    pushQ(schemaGetTxQ, std::move(qr));
    return result;
}

std::future<k2::CreateSchemaResult> K23SIGate::createSchema(const k2::String& collectionName, k2::dto::Schema schema) {
    SchemaCreateRequest qr{.collectionName = collectionName, .schema = schema, .prom = {}};

    auto result = qr.prom.get_future();
    pushQ(schemaCreateTxQ, std::move(qr));
    return result;
}

std::future<k2::CreateQueryResult> K23SIGate::createScanRead(const k2::String& collectionName, 
                                                             const k2::String& schemaName) {
    ScanReadCreateRequest cr{.collectionName = collectionName, .schemaName = schemaName, .prom = {}};

    auto result = cr.prom.get_future();
    pushQ(scanReadCreateTxQ, std::move(cr));
    return result;
}

} // ns gate
} // ns k2pg
