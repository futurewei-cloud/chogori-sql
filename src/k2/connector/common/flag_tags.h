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
// Portions Copyright (c) 2021 Futurewei Cloud
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
// Flag Tags provide a way to attach arbitrary textual tags to gflags in
// a global registry. K2Pg uses the following flag tags:
//
// - "stable":
//         These flags are considered user-facing APIs. Therefore, the
//         semantics of the flag should not be changed except between major
//         versions. Similarly, they must not be removed except between major
//         versions.
//
// - "evolving":
//         These flags are considered user-facing APIs, but are not yet
//         locked down. For example, they may pertain to a newly introduced
//         feature that is still being actively developed. These may be changed
//         between minor versions, but should be suitably release-noted.
//
//         This is the default assumed stability level, but can be tagged
//         if you'd like to make it explicit.
//
// - "experimental":
//         These flags are considered user-facing APIs, but are related to
//         an experimental feature, or otherwise likely to change or be
//         removed at any point. Users should not expect any compatibility
//         of these flags.
//
//         TODO: we should add a new flag like -unlock_experimental_flags (NOLINT)
//         which would be required if the user wants to use any of these,
//         similar to the JVM's -XX:+UnlockExperimentalVMOptions.
//
// - "hidden":
//         These flags are for internal use only (e.g. testing) and should
//         not be included in user-facing documentation.
//
// - "advanced":
//         These flags are for advanced users or debugging purposes. While
//         they aren't likely to be actively harmful (see "unsafe" below),
//         they're also likely to be used only rarely and should be relegated
//         to more detailed sections of documentation.
//
// - "unsafe":
//         These flags are for internal use only (e.g. testing), and changing
//         them away from the defaults may result in arbitrarily bad things
//         happening. These flags are automatically excluded from user-facing
//         documentation even if they are not also marked 'hidden'.
//
//         TODO: we should add a flag -unlock_unsafe_flags which would be required (NOLINT)
//         to use any of these flags.
//
// - "runtime":
//         These flags can be safely changed at runtime via an RPC to the
//         server. Changing a flag at runtime that does not have this tag is allowed
//         only if the user specifies a "force_unsafe_change" flag in the RPC.
//
//         NOTE: because gflags are simple global variables, it's important to
//         think very carefully before tagging a flag with 'runtime'. In particular,
//         if a string-type flag is marked 'runtime', you should never access it
//         using the raw 'FLAGS_foo_bar' name. Instead, you must use the
//         google::GetCommandLineFlagInfo(...) API to make a copy of the flag value
//         under a lock. Otherwise, the 'std::string' instance could be mutated
//         underneath the reader causing a crash.
//
//         For primitive-type flags, we assume that reading a variable is atomic.
//         That is to say that a reader will either see the old value or the new
//         one, but not some invalid value. However, for the runtime change to
//         have any effect, you must be sure to use the FLAGS_foo_bar variable directly
//         rather than initializing some instance variable during program startup.
//
// A given flag may have zero or more tags associated with it. The system does
// not make any attempt to check integrity of the tags - for example, it allows
// you to mark a flag as both stable and unstable, even though this makes no
// real sense. Nevertheless, you should strive to meet the following requirements:
//
// - A flag should have exactly no more than one of stable/evolving/experimental
//   indicating its stability. 'evolving' is considered the default.
// - A flag should have no more than one of advanced/hidden indicating visibility
//   in documentation. If neither is specified, the flag will be in the main
//   section of the documentation.
// - It is likely that most 'experimental' flags will also be 'advanced' or 'hidden',
//   and that 'stable' flags are not likely to be 'hidden' or 'unsafe'.
//
// To add a tag to a flag, use the TAG_FLAG macro. For example:
//
//  DEFINE_bool(sometimes_crash, false, "This flag makes K2PG crash a lot");
//  TAG_FLAG(sometimes_crash, unsafe);
//  TAG_FLAG(sometimes_crash, runtime);
//
// To fetch the list of tags associated with a flag, use 'GetFlagTags'.
#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include <boost/preprocessor/cat.hpp>

namespace k2pg {

struct FlagTags {
  enum {
    stable,
    evolving,
    experimental,
    hidden,
    advanced,
    unsafe,
    runtime
  };
};

// Tag the flag 'flag_name' with the given tag 'tag'.
//
// This verifies that 'flag_name' is a valid gflag, which must be defined
// or declared above the use of the TAG_FLAG macro.
//
// This also validates that 'tag' is a valid flag as defined in the FlagTags
// enum above.
#define TAG_FLAG(flag_name, tag) \
  COMPILE_ASSERT(sizeof(FLAGS_##flag_name), flag_does_not_exist); \
  COMPILE_ASSERT(sizeof(::k2pg::FlagTags::tag), invalid_tag);   \
  namespace {                                                     \
    ::k2pg::flag_tags_internal::FlagTagger t_##flag_name##_##tag( \
        AS_STRING(flag_name), AS_STRING(tag));                    \
  }

// Fetch the list of flags associated with the given flag.
//
// If the flag is invalid or has no tags, sets 'tags' to be empty.
void GetFlagTags(const std::string& flag_name,
                 std::unordered_set<std::string>* tags);

// ------------------------------------------------------------
// Internal implementation details
// ------------------------------------------------------------
namespace flag_tags_internal {

class FlagTagger {
 public:
  FlagTagger(const char* name, const char* tag);
  ~FlagTagger();

  private:
  FlagTagger(const FlagTagger&) = delete;
  void operator=(const FlagTagger&) = delete;
};

} // namespace flag_tags_internal

} // namespace k2pg

#define DEFINE_test_flag(type, name, default_value, description) \
    BOOST_PP_CAT(DEFINE_, type)(TEST_##name, default_value, description " (For testing only!)"); \
    TAG_FLAG(TEST_##name, unsafe); \
    TAG_FLAG(TEST_##name, hidden);
