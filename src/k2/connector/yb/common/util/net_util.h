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
#ifndef YB_UTIL_NET_NET_UTIL_H
#define YB_UTIL_NET_NET_UTIL_H

#include <string>
#include <vector>
#include <memory>

#include "yb/common/result.h"
#include "yb/common/strings/strip.h"
#include "yb/common/status.h"

namespace yb {

// A container for a host:port pair.
class HostPort {
 public:
  HostPort();
  HostPort(std::string host, uint16_t port);

  // Parse a "host:port" pair into this object.
  // If there is no port specified in the string, then 'default_port' is used.
  CHECKED_STATUS ParseString(const std::string& str, uint16_t default_port);

  static Result<HostPort> FromString(const std::string& str, uint16_t default_port) {
    HostPort result;
    RETURN_NOT_OK(result.ParseString(str, default_port));
    return result;
  }

  std::string ToString() const;

  const std::string& host() const { return host_; }
  void set_host(const std::string& host) { host_ = host; }

  uint16_t port() const { return port_; }
  void set_port(uint16_t port) { port_ = port; }

  // Parse a comma separated list of "host:port" pairs into a vector
  // HostPort objects. If no port is specified for an entry in the
  // comma separated list, 'default_port' is used for that entry's
  // pair.
  static CHECKED_STATUS ParseStrings(
      const std::string& comma_sep_addrs,
      uint16_t default_port,
      std::vector<HostPort>* res,
      const char* separator = ",");

  static Result<std::vector<HostPort>> ParseStrings(
      const std::string& comma_sep_addrs, uint16_t default_port,
      const char* separator = ",") {
    std::vector<HostPort> result;
    RETURN_NOT_OK(ParseStrings(comma_sep_addrs, default_port, &result, separator));
    return result;
  }

  bool operator==(const HostPort& other) const {
    return host() == other.host() && port() == other.port();
  }

  friend bool operator<(const HostPort& lhs, const HostPort& rhs) {
    auto cmp_hosts = lhs.host_.compare(rhs.host_);
    return cmp_hosts < 0 || (cmp_hosts == 0 && lhs.port_ < rhs.port_);
  }

 private:
  std::string host_;
  uint16_t port_;
};

inline std::ostream& operator<<(std::ostream& out, const HostPort& value) {
  return out << value.ToString();
}

// Accepts entries like: [::1], 127.0.0.1, [::1]:7100, 0.0.0.0:7100,
// f.q.d.n:7100
Status HostPort::ParseString(const string &str_in, uint16_t default_port) {
  uint32_t port;
  string host;

  string str(str_in);
  StripWhiteSpace(&str);
  size_t pos = str.rfind(':');
  if (str[0] == '[' && str[str.length() - 1] == ']' && str.length() > 2) {
    // The whole thing is an IPv6 address.
    host = str.substr(1, str.length() - 2);
    port = default_port;
  } else if (pos == string::npos) {
    // No port was specified, the whole thing must be a host.
    host = str;
    port = default_port;
  } else if (pos > 1 && pos + 1 < str.length() &&
             SimpleAtoi(str.substr(pos + 1), &port)) {

    if (port > numeric_limits<uint16_t>::max()) {
      return STATUS(InvalidArgument, "Invalid port", str);
    }

    // Got a host:port
    host = str.substr(0, pos);
    if (host[0] == '[' && host[host.length() - 1] == ']' && host.length() > 2) {
      // Remove brackets if we have an IPv6 address
      host = host.substr(1, host.length() - 2);
    }
  } else {
    return STATUS(InvalidArgument,
                  Format(
                      "Invalid port: expected port after ':' "
                      "at position $0 in $1",
                      pos, str));
  }

  host_ = host;
  port_ = port;
  return Status::OK();
}

std::string HostPortToString(const std::string& host, int port) {
  DCHECK_GE(port, 0);
  DCHECK_LE(port, 65535);
  if (host.find(':') != string::npos) {
    return Format("[$0]:$1", host, port);
  } else {
    return Format("$0:$1", host, port);
  }
}

string HostPort::ToString() const { return HostPortToString(host_, port_); }

} // namespace yb

#endif  // YB_UTIL_NET_NET_UTIL_H
