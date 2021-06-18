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

#include "k2pg_util.h"

#include <stdarg.h>
#include <fstream>
#include <string>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#include <sys/sysctl.h>
#else
#include <linux/falloc.h>
#include <sys/sysinfo.h>
#endif  // defined(__APPLE__)

#include <gflags/gflags.h>

#include <fmt/format.h>

#include "status.h"
#include "pgsql_error.h"
#include "transaction_error.h"
#include "k2pg-internal.h"
#include "scope_exit.h"
#include "flag_tags.h"
#include "common-utils.h"

DEFINE_string(process_info_dir, "", \
                 "Directory where all postgres process will writes their PIDs and executable name");


K2PgStatus K2PgStatusOK() {
  return nullptr;
}

namespace k2pg {

namespace {

Status GetExecutablePath(std::string* path) {
  uint32_t size = 64;
  uint32_t len = 0;
  while (true) {
    std::unique_ptr<char[]> buf(new char[size]);
#if defined(__linux__)
    int rc = readlink("/proc/self/exe", buf.get(), size);
    if (rc == -1) {
      return STATUS(IOError, "Unable to determine own executable path", "", Errno(errno));
    } else if (rc >= size) {
      // The buffer wasn't large enough
      size *= 2;
      continue;
    }
    len = rc;
#elif defined(__APPLE__)
    if (_NSGetExecutablePath(buf.get(), &size) != 0) {
      // The buffer wasn't large enough; 'size' has been updated.
      continue;
    }
    len = strlen(buf.get());
#else
#error Unsupported platform
#endif

    path->assign(buf.get(), len);
    break;
  }
  return Status::OK();
}

void ChangeWorkingDir(const char* dir) {
  int chdir_result = chdir(dir);
  if (chdir_result != 0) {
    throw std::runtime_error(fmt::format("Failed to change working directory to {}, error was {} {}!", dir, errno, std::strerror(errno)));
  }
}

void WriteCurrentProcessInfo(const std::string& destination_dir) {
  std::string executable_path;
if (GetExecutablePath(&executable_path).ok()) {
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
    RETURN_NOT_OK(GetExecutablePath(&executable_path_str));
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

  const char* k2pg_working_dir = getenv("K2PG_WORKING_DIR");
  if (k2pg_working_dir) {
    ChangeWorkingDir(k2pg_working_dir);
  }
  auto se = ScopeExit([&pg_working_dir] {
    // Restore PG data dir as current directory.
    ChangeWorkingDir(pg_working_dir);
  });

  // Also allow overriding flags on the command line using the appropriate environment variables.
  std::vector<google::CommandLineFlagInfo> flag_infos;
  google::GetAllFlags(&flag_infos);
  for (auto& flag_info : flag_infos) {
    std::string env_var_name = "FLAGS_" + flag_info.name;
    const char* env_var_value = getenv(env_var_name.c_str());
    if (env_var_value) {
      google::SetCommandLineOption(flag_info.name.c_str(), env_var_value);
    }
  }

  // bypass CPU flag checking since k2 does not rely on cpu types
  google::InitGoogleLogging(executable_path);

  return Status::OK();
}

} // anonymous namespace

extern "C" {

K2PgStatus K2PgStatus_OK = nullptr;

// Wraps Status object created by K2PgStatus.
class StatusWrapper {
 public:
  explicit StatusWrapper(K2PgStatus s) : status_(s, false) {}

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

bool K2PgStatusIsOK(K2PgStatus s) {
  return StatusWrapper(s)->IsOk();
}

bool K2PgStatusIsNotFound(K2PgStatus s) {
  return StatusWrapper(s)->IsNotFound();
}

bool K2PgStatusIsDuplicateKey(K2PgStatus s) {
  return StatusWrapper(s)->IsAlreadyPresent();
}

uint32_t K2PgStatusPgsqlError(K2PgStatus s) {
  StatusWrapper wrapper(s);
  const uint8_t* pg_err_ptr = wrapper->ErrorData(PgsqlErrorTag::kCategory);
  // If we have PgsqlError explicitly set, we decode it
  K2PgErrorCode result = pg_err_ptr != nullptr ? PgsqlErrorTag::Decode(pg_err_ptr)
                                               : K2PgErrorCode::K2PG_INTERNAL_ERROR;

  // If the error is the default generic K2PG_INTERNAL_ERROR (as we also set in AsyncRpc::Failed)
  // then we try to deduce it from a transaction error.
  if (result == K2PgErrorCode::K2PG_INTERNAL_ERROR) {
    const uint8_t* txn_err_ptr = wrapper->ErrorData(TransactionErrorTag::kCategory);
    if (txn_err_ptr != nullptr) {
      switch (TransactionErrorTag::Decode(txn_err_ptr)) {
        case TransactionErrorCode::kAborted: [[fallthrough]];
        case TransactionErrorCode::kReadRestartRequired: [[fallthrough]];
        case TransactionErrorCode::kConflict:
          result = K2PgErrorCode::K2PG_T_R_SERIALIZATION_FAILURE;
          break;
        case TransactionErrorCode::kSnapshotTooOld:
          result = K2PgErrorCode::K2PG_SNAPSHOT_TOO_OLD;
          break;
        case TransactionErrorCode::kNone: [[fallthrough]];
        default:
          result = K2PgErrorCode::K2PG_INTERNAL_ERROR;
      }
    }
  }
  return static_cast<uint32_t>(result);
}

uint16_t K2PgStatusTransactionError(K2PgStatus s) {
  const TransactionError txn_err(*StatusWrapper(s));
  return static_cast<uint16_t>(txn_err.value());
}

void K2PgFreeStatus(K2PgStatus s) {
  FreeK2PgStatus(s);
}

size_t K2PgStatusMessageLen(K2PgStatus s) {
  return StatusWrapper(s)->message().size();
}

const char* K2PgStatusMessageBegin(K2PgStatus s) {
  return StatusWrapper(s)->message().cdata();
}

const char* K2PgStatusCodeAsCString(K2PgStatus s) {
  return StatusWrapper(s)->CodeAsCString();
}

char* DupK2PgStatusMessage(K2PgStatus status, bool message_only) {
  const char* const code_as_cstring = K2PgStatusCodeAsCString(status);
  const size_t code_strlen = strlen(code_as_cstring);
  const size_t status_len = K2PgStatusMessageLen(status);
  size_t sz = code_strlen + status_len + 3;
  if (message_only) {
    sz -= 2 + code_strlen;
  }
  char* const msg_buf = reinterpret_cast<char*>(K2PgPAlloc(sz));
  char* pos = msg_buf;
  if (!message_only) {
    memcpy(msg_buf, code_as_cstring, code_strlen);
    pos += code_strlen;
    *pos++ = ':';
    *pos++ = ' ';
  }
  memcpy(pos, K2PgStatusMessageBegin(status), status_len);
  pos[status_len] = 0;
  return msg_buf;
}

bool K2PgIsRestartReadError(uint16_t txn_errcode) {
  return txn_errcode == static_cast<uint16_t>(TransactionErrorCode::kReadRestartRequired);
}

K2PgStatus K2PgInitGFlags(const char* argv0) {
  return ToK2PgStatus(k2pg::InitGFlags(argv0));
}

K2PgStatus K2PgInit(const char* argv0,
                  K2PgPAllocFn palloc_fn,
                  K2PgCStringToTextWithLenFn cstring_to_text_with_len_fn) {
  K2PgSetPAllocFn(palloc_fn);
  if (cstring_to_text_with_len_fn) {
    K2PgSetCStringToTextWithLenFn(cstring_to_text_with_len_fn);
  }
  auto status = k2pg::InitGFlags(argv0);
  if (status.ok() && !FLAGS_process_info_dir.empty()) {
    WriteCurrentProcessInfo(FLAGS_process_info_dir);
  }
  return ToK2PgStatus(status);
}

void K2PgLogImpl(
    google::LogSeverity severity,
    const char* file,
    int line,
    bool with_stack_trace,
    const char* format,
    ...) {
  va_list argptr;
  va_start(argptr, format); \
  std::string buf;
  StringAppend(&buf, format, argptr);
  va_end(argptr);
  google::LogMessage log_msg(file, line, severity);
  log_msg.stream() << buf;
}

const char* K2PgGetStackTrace() {
    return K2PgPAllocStdString("Not Implemented");
}

} // extern "C"

} // namespace k2pg
