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

#include "pggate/pg_txn_handler.h"

namespace k2pg {
namespace gate {

using yb::Status;

PgTxnHandler::PgTxnHandler(std::shared_ptr<K2Adapter> adapter) : adapter_(adapter) {
}

PgTxnHandler::~PgTxnHandler() {
  // Abort the transaction before the transaction handler gets destroyed.
  if (txn_ != nullptr) {
    auto status = adapter_->SyncAbortTransaction(txn_);
    if (!status.ok()) {
      K2LOG_E(log::pg, "In progress transaction abortion failed due to: {}", status.code());
    }
  }
  ResetTransaction();
}

Status PgTxnHandler::BeginTransaction() {
  K2LOG_D(log::pg, "BeginTransaction: txn_in_progress_={}", txn_in_progress_);
  if (txn_in_progress_) {
    return STATUS(IllegalState, "Transaction is already in progress");
  }
  ResetTransaction();
  txn_in_progress_ = true;
  return StartNewTransaction();
}

Status PgTxnHandler::RestartTransaction() {
  // TODO: how do we decide whether a transaction is restart required?

  if (txn_ != nullptr) {
    auto result = adapter_->SyncAbortTransaction(txn_);
    if (!result.ok()) {
      return result;
    }
  }
  ResetTransaction();
  txn_in_progress_ = true;
  return StartNewTransaction();
}

Status PgTxnHandler::CommitTransaction() {
  if (!txn_in_progress_) {
    K2LOG_D(log::pg, "No transaction in progress, nothing to commit.");
    return Status::OK();
  }

  if (txn_ != nullptr && read_only_) {
    K2LOG_D(log::pg, "This was a read-only transaction, nothing to commit.");
    // currently for K2-3SI transaction, we actually just abort the transaction if it is read only
    return AbortTransaction();
  }

  K2LOG_D(log::pg, "Committing transaction.");
  // Use synchronous call for now until PG supports additional state check after this call
  auto result = adapter_->SyncCommitTransaction(txn_);
  if (!result.ok()) {
    K2LOG_W(log::pg, "Transaction commit failed due to: {}", result.code());
    return result;
  }
  ResetTransaction();
  K2LOG_D(log::pg, "Transaction commit succeeded");
  return Status::OK();
}

Status PgTxnHandler::AbortTransaction() {
  if (!txn_in_progress_) {
    return Status::OK();
  }
  if (txn_ != nullptr && read_only_) {
    // This was a read-only transaction, nothing to commit.
    ResetTransaction();
    return Status::OK();
  }
  // Use synchronous call for now until PG supports additional state check after this call
  auto result = adapter_->SyncAbortTransaction(txn_);
  // always abandon current transaction and reset regardless abort success or not.
  ResetTransaction();  
  if (!result.ok()) {
    K2LOG_W(log::pg, "Transaction abort failed due to: {}", result.code());
    return result;
  }
  return Status::OK();
}

Status PgTxnHandler::SetIsolationLevel(int level) {
  isolation_level_ = static_cast<PgIsolationLevel>(level);
  return Status::OK();
}

Status PgTxnHandler::SetReadOnly(bool read_only) {
  read_only_ = read_only;
  return Status::OK();
}

Status PgTxnHandler::SetDeferrable(bool deferrable) {
  deferrable_ = deferrable;
  return Status::OK();
}

Status PgTxnHandler::EnterSeparateDdlTxnMode() {
  // TODO: do we support this mode and how ?
  return Status::OK();
}

Status PgTxnHandler::ExitSeparateDdlTxnMode(bool success) {
  // TODO: do we support this mode and how ?
  return Status::OK();
}

Status PgTxnHandler::StartNewTransactionIfNotYet(bool read_only) {
  // start transaction if not yet started.
  if (txn_ == nullptr) {
    read_only_ = read_only;
    return BeginTransaction();
  } 

  DCHECK(txn_in_progress_);
  return Status::OK();
}

void PgTxnHandler::ResetTransaction() {
  read_only_ = false;
  txn_in_progress_ = false;
  txn_ = nullptr;
  can_restart_.store(true, std::memory_order_release);
}

// internal BeginTransaction helper 
Status PgTxnHandler::StartNewTransaction() {
  std::shared_ptr<K23SITxn> txn_tmp = nullptr;
  Status result = adapter_->SyncBeginTransaction(txn_tmp);
  if (!result.ok())
  {
    return result;
  } 
  
  DCHECK(txn_tmp != nullptr);
  txn_ = txn_tmp;
  return result;
}

}  // namespace gate
}  // namespace k2pg
