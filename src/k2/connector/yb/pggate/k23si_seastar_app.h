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

#pragma once

#include <k2/appbase/AppEssentials.h>
#include <k2/common/Log.h>
#include <k2/module/k23si/client/k23si_client.h>

namespace k2pg {
namespace gate {
namespace log {
inline thread_local k2::logging::Logger k2ss("k2::pg_seastar");
}
class PGK2Client {
public:
    PGK2Client();
    ~PGK2Client() {}
    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();
private:
    std::unique_ptr<k2::K23SIClient> _client = nullptr;
    std::unordered_map<k2::dto::K23SI_MTR, k2::K2TxnHandle> _txns;
    // SQL does inserts and writes sequentially, but we want to run them concurrently when possible,
    // so PGK2Client will ack the writes immediately to the upper layer and keep track of active writes
    // in this data structure. If SQL does a read, scan, commit, or partial update, we will wait for
    // all active writes. We keep a single future and chain off of it in order to maintain ordering
    std::unordered_map<k2::dto::K23SI_MTR, seastar::future<>> _activeWrites;

    seastar::future<> _poller = seastar::make_ready_future();
    seastar::future<> _pollForWork();

    seastar::future<> _pollBeginQ();
    seastar::future<> _pollEndQ();
    seastar::future<> _pollSchemaGetQ();
    seastar::future<> _pollSchemaCreateQ();
    seastar::future<> _pollReadQ();
    seastar::future<> _pollCreateScanReadQ();
    seastar::future<> _pollScanReadQ();
    seastar::future<> _pollWriteQ();
    seastar::future<> _pollUpdateQ();
    seastar::future<> _pollCreateCollectionQ();

    bool _stop = false;
    bool _concurrentWrites = false;
};

}  // namespace gate
}  // namespace k2pg
