// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
// Copyright(c) 2020 Futurewei Cloud
//
// Permission is hereby granted,
//        free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
//        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//        DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

#include "yb/pggate/pg_txn_handler.h"

namespace k2pg {
namespace gate {

using yb::Status;

PgTxnHandler::PgTxnHandler(K2Adapter *adapter) : adapter_(adapter) {
}

PgTxnHandler::~PgTxnHandler() {
  // Abort the transaction before the transaction handler gets destroyed.
  if (txn_ != nullptr) {
    txn_->endTxn(false);
  }
  ResetTransaction();
}

Status PgTxnHandler::BeginTransaction() {
  VLOG(2) << "BeginTransaction: txn_in_progress_=" << txn_in_progress_;
  if (txn_in_progress_) {
    return STATUS(IllegalState, "Transaction is already in progress");
  }
  ResetTransaction();
  txn_in_progress_ = true;
  StartNewTransaction();
  return Status::OK();
}

Status PgTxnHandler::RestartTransaction() {
  // TODO: how do we decide whether a transaction is restart required?

   if (txn_ != nullptr) {
    txn_->endTxn(false);
  }
  ResetTransaction();
  txn_in_progress_ = true;
  StartNewTransaction();
  DCHECK(can_restart_.load(std::memory_order_acquire));

  return Status::OK();
}

Status PgTxnHandler::CommitTransaction() {
  if (!txn_in_progress_) {
    VLOG(2) << "No transaction in progress, nothing to commit.";
    return Status::OK();
  }

  if (txn_ != nullptr && read_only_) {
    VLOG(2) << "This was a read-only transaction, nothing to commit.";
    ResetTransaction();
    return Status::OK();
  }

  VLOG(2) << "Committing transaction.";
  // TODO: add more logic for transaction result handling
  std::future<k2::EndResult> result = txn_->endTxn(true);
  result.get();
  // TODO:: log commit status
  VLOG(2) << "Transaction commit status: ";
  ResetTransaction();
  return Status::OK();
}

Status PgTxnHandler::AbortTransaction() {
  if (!txn_in_progress_) {
    return Status::OK();
  }
  if (txn_ != nullptr) {
    // This was a read-only transaction, nothing to commit.
    ResetTransaction();
    return Status::OK();
  }
  // TODO: how do we report errors if the transaction has already committed?
  txn_->endTxn(false);
  ResetTransaction();
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

std::shared_ptr<K23SITxn> PgTxnHandler::GetNewTransactionIfNecessary(bool read_only) {
    if (txn_ == nullptr) {
        BeginTransaction();
        return txn_;
    } else {
        // SKV does not support read only transaction yet, we always use the K23SI transaction
        return txn_;
    }
}

void PgTxnHandler::ResetTransaction() {
  txn_in_progress_ = false;
  txn_ = nullptr;
  can_restart_.store(true, std::memory_order_release);    
}

void PgTxnHandler::StartNewTransaction() {
    txn_ = adapter_->beginTransaction();
}

}  // namespace gate
}  // namespace k2pg    