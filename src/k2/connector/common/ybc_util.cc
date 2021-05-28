// Copyright (c) YugaByte, Inc.
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

#include "ybc_util.h"

#include <stdarg.h>
#include <fstream>
#include <gflags/gflags.h>

#include <fmt/format.h>

#include "status.h"
#include "pgsql_error.h"
#include "transaction_error.h"
#include "ybc-internal.h"
#include "env.h"
#include "scope_exit.h"
#include "flag_tags.h"

using std::string;
DEFINE_string(process_info_dir, "", \
                 "Directory where all postgres process will writes their PIDs and executable name");


YBCStatus YBCStatusOK() {
  return nullptr;
}

namespace yb {

namespace {

void ChangeWorkingDir(const char* dir) {
  int chdir_result = chdir(dir);
  if (chdir_result != 0) {
    throw std::runtime_error(fmt::format("Failed to change working directory to {}, error was {} {}!", dir, errno, std::strerror(errno)));
  }
}

void WriteCurrentProcessInfo(const string& destination_dir) {
  string executable_path;
  if (Env::Default()->GetExecutablePath(&executable_path).ok()) {
    const auto destination_file = fmt::format("{}/{}" , destination_dir, getpid()); //Format("$0/$1", destination_dir, getpid());
    std::ofstream out(destination_file, std::ios_base::out);
    out << executable_path;
    if (out) {
      return;
    }
  }
  throw std::runtime_error(fmt::format("Unable to write process info to {}  dir: error {} {}", destination_dir, errno, std::strerror(errno)));
}

Status InitGFlags(const char* argv0) {

  const char* executable_path = argv0;
  std::string executable_path_str;
  if (executable_path == nullptr) {
    RETURN_NOT_OK(Env::Default()->GetExecutablePath(&executable_path_str));
    executable_path = executable_path_str.c_str();
  }
  if (executable_path == nullptr) {
    throw std::runtime_error("Unable to get path to executable");
  }

  // Change current working directory from postgres data dir (as set by postmaster)
  char pg_working_dir[PATH_MAX];
  if (getcwd(pg_working_dir, sizeof(pg_working_dir)) == nullptr) {
    throw std::runtime_error("pg_working_dir is not set");
  }

  const char* yb_working_dir = getenv("YB_WORKING_DIR");
  if (yb_working_dir) {
    ChangeWorkingDir(yb_working_dir);
  }
  auto se = ScopeExit([&pg_working_dir] {
    // Restore PG data dir as current directory.
    ChangeWorkingDir(pg_working_dir);
  });

  // Also allow overriding flags on the command line using the appropriate environment variables.
  std::vector<google::CommandLineFlagInfo> flag_infos;
  google::GetAllFlags(&flag_infos);
  for (auto& flag_info : flag_infos) {
    string env_var_name = "FLAGS_" + flag_info.name;
    const char* env_var_value = getenv(env_var_name.c_str());
    if (env_var_value) {
      google::SetCommandLineOption(flag_info.name.c_str(), env_var_value);
    }
  }

  // bypass CPU flag checking

  // Use InitGoogleLoggingSafeBasic() instead of InitGoogleLoggingSafe() to avoid calling
  // google::InstallFailureSignalHandler(). This will prevent interference with PostgreSQL's
  // own signal handling.

  //  yb::InitGoogleLoggingSafeBasic(executable_path);
  // use above google api directly to avoid bringing in too many dependencies
  // TODO: rework the above line
  google::InitGoogleLogging(executable_path);

  return Status::OK();
}

} // anonymous namespace

extern "C" {

YBCStatus YBCStatus_OK = nullptr;

// Wraps Status object created by YBCStatus.
// Uses trick with AddRef::kFalse and DetachStruct, to avoid incrementing and decrementing
// ref counter.
class StatusWrapper {
 public:
  explicit StatusWrapper(YBCStatus s) : status_(s, AddRef::kFalse) {}

  ~StatusWrapper() {
    status_.DetachStruct();
  }

  Status* operator->() {
    return &status_;
  }

  Status& operator*() {
    return status_;
  }

 private:
  Status status_;
};

bool YBCStatusIsOK(YBCStatus s) {
  return StatusWrapper(s)->IsOk();
}

bool YBCStatusIsNotFound(YBCStatus s) {
  return StatusWrapper(s)->IsNotFound();
}

bool YBCStatusIsDuplicateKey(YBCStatus s) {
  return StatusWrapper(s)->IsAlreadyPresent();
}

uint32_t YBCStatusPgsqlError(YBCStatus s) {
  StatusWrapper wrapper(s);
  const uint8_t* pg_err_ptr = wrapper->ErrorData(PgsqlErrorTag::kCategory);
  // If we have PgsqlError explicitly set, we decode it
  YBPgErrorCode result = pg_err_ptr != nullptr ? PgsqlErrorTag::Decode(pg_err_ptr)
                                               : YBPgErrorCode::YB_PG_INTERNAL_ERROR;

  // If the error is the default generic YB_PG_INTERNAL_ERROR (as we also set in AsyncRpc::Failed)
  // then we try to deduce it from a transaction error.
  if (result == YBPgErrorCode::YB_PG_INTERNAL_ERROR) {
    const uint8_t* txn_err_ptr = wrapper->ErrorData(TransactionErrorTag::kCategory);
    if (txn_err_ptr != nullptr) {
      switch (TransactionErrorTag::Decode(txn_err_ptr)) {
        case TransactionErrorCode::kAborted: FALLTHROUGH_INTENDED;
        case TransactionErrorCode::kReadRestartRequired: FALLTHROUGH_INTENDED;
        case TransactionErrorCode::kConflict:
          result = YBPgErrorCode::YB_PG_T_R_SERIALIZATION_FAILURE;
          break;
        case TransactionErrorCode::kSnapshotTooOld:
          result = YBPgErrorCode::YB_PG_SNAPSHOT_TOO_OLD;
          break;
        case TransactionErrorCode::kNone: FALLTHROUGH_INTENDED;
        default:
          result = YBPgErrorCode::YB_PG_INTERNAL_ERROR;
      }
    }
  }
  return static_cast<uint32_t>(result);
}

uint16_t YBCStatusTransactionError(YBCStatus s) {
  const TransactionError txn_err(*StatusWrapper(s));
  return static_cast<uint16_t>(txn_err.value());
}

void YBCFreeStatus(YBCStatus s) {
  FreeYBCStatus(s);
}

size_t YBCStatusMessageLen(YBCStatus s) {
  return StatusWrapper(s)->message().size();
}

const char* YBCStatusMessageBegin(YBCStatus s) {
  return StatusWrapper(s)->message().cdata();
}

const char* YBCStatusCodeAsCString(YBCStatus s) {
  return StatusWrapper(s)->CodeAsCString();
}

char* DupYBStatusMessage(YBCStatus status, bool message_only) {
  const char* const code_as_cstring = YBCStatusCodeAsCString(status);
  const size_t code_strlen = strlen(code_as_cstring);
  const size_t status_len = YBCStatusMessageLen(status);
  size_t sz = code_strlen + status_len + 3;
  if (message_only) {
    sz -= 2 + code_strlen;
  }
  char* const msg_buf = reinterpret_cast<char*>(YBCPAlloc(sz));
  char* pos = msg_buf;
  if (!message_only) {
    memcpy(msg_buf, code_as_cstring, code_strlen);
    pos += code_strlen;
    *pos++ = ':';
    *pos++ = ' ';
  }
  memcpy(pos, YBCStatusMessageBegin(status), status_len);
  pos[status_len] = 0;
  return msg_buf;
}

bool YBCIsRestartReadError(uint16_t txn_errcode) {
  return txn_errcode == static_cast<uint16_t>(TransactionErrorCode::kReadRestartRequired);
}

YBCStatus YBCInitGFlags(const char* argv0) {
  return ToYBCStatus(yb::InitGFlags(argv0));
}

 YBCStatus YBCInit(const char* argv0,
                  YBCPAllocFn palloc_fn,
                  YBCCStringToTextWithLenFn cstring_to_text_with_len_fn) {
  YBCSetPAllocFn(palloc_fn);
  if (cstring_to_text_with_len_fn) {
    YBCSetCStringToTextWithLenFn(cstring_to_text_with_len_fn);
  }
  auto status = yb::InitGFlags(argv0);
  if (status.ok() && !FLAGS_process_info_dir.empty()) {
    WriteCurrentProcessInfo(FLAGS_process_info_dir);
  }
  return ToYBCStatus(status);
}

void YBCLogImpl(
    google::LogSeverity severity,
    const char* file,
    int line,
    bool with_stack_trace,
    const char* format,
    ...) {
  va_list argptr;
  va_start(argptr, format); \
  string buf;
  StringAppendV(&buf, format, argptr);
  va_end(argptr);
  google::LogMessage log_msg(file, line, severity);
  log_msg.stream() << buf;
}

const char* YBCGetStackTrace() {
    return YBCPAllocStdString("Not Implemented");
}

} // extern "C"

} // namespace yb
