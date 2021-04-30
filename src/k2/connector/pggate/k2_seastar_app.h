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
#include "k2_includes.h"

namespace k2 {
    class K2TxnHandle;
    class K23SIClient;
namespace dto {
    class K23SI_MTR;
}
}
namespace k2pg {
namespace gate {
namespace log {
inline thread_local k2::logging::Logger k2ss("k2::pg_seastar");
}
class PGK2Client {
public:
    PGK2Client();
    ~PGK2Client();
    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();
private:
    k2::K23SIClient* _client;
    std::unordered_map<k2::dto::K23SI_MTR, k2::K2TxnHandle>* _txns;

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
    seastar::future<> _pollDropCollectionQ();

    bool _stop = false;
};

}  // namespace gate
}  // namespace k2pg
