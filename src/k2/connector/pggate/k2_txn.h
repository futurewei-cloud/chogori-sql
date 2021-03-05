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
#include "k2_includes.h"
#include "k2_log.h"
#include "k2_future.h"

namespace k2pg {
namespace gate {

// These transaction handles are produced by the K23SIGate class. The user should use this
// handle to perform operation which should be part of the transaction
// all APIs are semantically the same as defined in
// https://github.com/futurewei-cloud/chogori-platform/blob/master/src/k2/module/k23si/client/k23si_client.h
class K23SITxn {
public:
    // Ctor: creates a new transaction with the given mtr.
    K23SITxn(k2::dto::K23SI_MTR mtr, k2::TimePoint startTime);

    ~K23SITxn();

    // Scans records from K2.
    // The result future is eventually satisfied with the resulting SKVRecords of the scan.
    // Uncaught exceptions may also be propagated and show up as exceptional futures here.
    CBFuture<k2::QueryResult> scanRead(std::shared_ptr<k2::Query> query);

    // Reads a record from K2.
    // The result future is eventually satisfied with the result of the read.
    // Uncaught exceptions may also be propagated and show up as exceptional futures here.
    CBFuture<k2::ReadResult<k2::SKVRecord>> read(k2::dto::SKVRecord&& rec);
    CBFuture<k2::ReadResult<k2::SKVRecord>> read(k2::dto::Key key, std::string collectionName);

    // Writes a record (full) into K2. The erase flag is used if this write should delete
    // the record from K2.
    // The result future is eventually satisfied with the result of the write
    // Uncaught exceptions may also be propagated and show up as exceptional futures here.
    CBFuture<k2::WriteResult> write(k2::dto::SKVRecord&& rec, bool erase=false, bool rejectIfExists=false);

    // Writes a partial update (e.g. SQL UPDATE) into K2.
    // fieldsToUpdate are the indexes of the fields to change, key may be empty in which case the key is
    // generated from the SKVRecord or filled in with a previously cached value
    // The result future is eventually satisfied with the result of the update
    // Uncaught exceptions may also be propagated and show up as exceptional futures here.
    CBFuture<k2::PartialUpdateResult> partialUpdate(k2::dto::SKVRecord&& rec,
                                                       std::vector<uint32_t> fieldsForUpdate,
                                                       std::string key="");

    // Ends the transaction. The transaction can be either committed or aborted.
    // The result future is eventually satisfied with the result of the end operation
    // Uncaught exceptions may also be propagated and show up as exceptional futures here.
    CBFuture<k2::EndResult> endTxn(bool shouldCommit);

    // Returns the MTR for this transaction. This is unique for each transaction and
    // can be useful to keep track of transactions or to log
    // The MTR will be unique in spacetime
    const k2::dto::K23SI_MTR& mtr() const;

private: // fields
    k2::dto::K23SI_MTR _mtr; // mtr for this transaction
    void _reportEndMetrics(k2::TimePoint now);

    // the time at which SQL asked to start this txn
    k2::TimePoint _startTime;

    uint32_t _readOps{0};
    uint32_t _writeOps{0};
    uint32_t _scanOps{0};
    uint32_t _inFlightOps{0};
    static inline uint32_t _inFlightTxns{0};
 };  // class K23SITxn

} // ns gate
} // ns k2pg
