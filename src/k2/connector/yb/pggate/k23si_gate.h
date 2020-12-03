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

// This file contains the k2 client library shim. It allows the PG process to perform K2 transactions using
// an interface similar to the k2 native client library, and reusing the same DTO data structures. For details
// on semantics of the interface, please see the k2-native library here:
// https://github.com/futurewei-cloud/chogori-platform/blob/master/src/k2/module/k23si/client/k23si_client.h
//

#pragma once
#include <k2/module/k23si/client/k23si_client.h>
#include "k23si_txn.h"
#include "k23si_queue_defs.h"
#include <future>

namespace k2pg {
namespace gate {

// This class is the client library for interacting with K2. Most importantly, it allows the user to start
// a new K2 transaction
class K23SIGate {
public:
    // Ctor: creates a new library instance. The library just proxies calls over request queues to the
    // seastar counterpart. All configuration relevant to the k2 client is performed in the seastar client (k23si_app.h)
    K23SIGate();
    static constexpr auto ANY_VERSION = k2::K23SIClient::ANY_VERSION;

    // Starts a new transaction with the given options.
    // the result future is eventually satisfied with a valid transaction handle, or with an exception if the library
    // is unable to start a transaction
    std::future<K23SITxn>
    beginTxn(const k2::K2TxnOptions& txnOpts);
    std::future<k2::GetSchemaResult> getSchema(const k2::String& collectionName, const k2::String& schemaName, uint64_t schemaVersion);
    std::future<k2::CreateSchemaResult> createSchema(const k2::String& collectionName, k2::dto::Schema schema);
    std::future<CreateScanReadResult> createScanRead(const k2::String& collectionName,
                                                     const k2::String& schemaName);
};  // class K23SIGate

}  // namespace gate
}  // namespace k2pg
