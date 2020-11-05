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

#ifndef CHOGORI_GATE_PG_TXN_HANDLER_H
#define CHOGORI_GATE_PG_TXN_HANDLER_H

#include <atomic>

#include "yb/common/result.h"
#include "yb/pggate/k23si_txn.h"
#include "yb/pggate/k2_adapter.h"

namespace k2pg {
namespace gate {

// These should match XACT_READ_UNCOMMITED, XACT_READ_COMMITED, XACT_REPEATABLE_READ,
// XACT_SERIALIZABLE from xact.h.
enum class PgIsolationLevel {
  READ_UNCOMMITED = 0,
  READ_COMMITED = 1,
  REPEATABLE_READ = 2,
  SERIALIZABLE = 3,
};

class PgTxnHandler {
  public:
  PgTxnHandler(std::shared_ptr<K2Adapter> adapter);

  virtual ~PgTxnHandler();

  CHECKED_STATUS BeginTransaction();

  CHECKED_STATUS RestartTransaction();

  CHECKED_STATUS CommitTransaction();

  CHECKED_STATUS AbortTransaction();

  CHECKED_STATUS SetIsolationLevel(int isolation);

  CHECKED_STATUS SetReadOnly(bool read_only);

  CHECKED_STATUS SetDeferrable(bool deferrable);

  CHECKED_STATUS EnterSeparateDdlTxnMode();

  CHECKED_STATUS ExitSeparateDdlTxnMode(bool success);

  std::shared_ptr<K23SITxn> getTxnHandler() {
    return txn_;
  }

  std::shared_ptr<K23SITxn> GetNewTransactionIfNecessary(bool read_only);

  private:  

  void ResetTransaction();

  void StartNewTransaction();

  std::shared_ptr<K23SITxn> txn_ = nullptr;

  bool txn_in_progress_ = false;

  // Postgres transaction characteristics.
  PgIsolationLevel isolation_level_ = PgIsolationLevel::REPEATABLE_READ;

  bool read_only_ = false;

  bool deferrable_ = false;

  std::atomic<bool> can_restart_{true};

  std::shared_ptr<K2Adapter> adapter_;
};

}  // namespace gate
}  // namespace k2pg

#endif //CHOGORI_GATE_PG_TXN_HANDLER_H    