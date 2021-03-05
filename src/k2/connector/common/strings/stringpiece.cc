// Copyright 2004 and onwards Google Inc.
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
//

#include "stringpiece.h"

#include <algorithm>
#include <climits>
#include <glog/logging.h>
#include <string.h>
#include <string>

#include "common/hash/hash.h"
#include "common/log/logging-inl.h"
#include "common/util/stl_util.h"
#include "common/util/memutil.h"

using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
using std::string;

namespace std {
  size_t hash<GStringPiece>::operator()(GStringPiece s) const {
    return HashTo32(s.data(), s.size());
  }
} // namespace std

std::ostream& operator<<(std::ostream& o, GStringPiece piece) {
  o.write(piece.data(), piece.size());
  return o;
}

GStringPiece::GStringPiece(GStringPiece x, int pos)
    : ptr_(x.ptr_ + pos), length_(x.length_ - pos) {
  DCHECK_LE(0, pos);
  DCHECK_LE(pos, x.length_);
}

GStringPiece::GStringPiece(GStringPiece x, int pos, int len)
    : ptr_(x.ptr_ + pos), length_(min(len, x.length_ - pos)) {
  DCHECK_LE(0, pos);
  DCHECK_LE(pos, x.length_);
  DCHECK_GE(len, 0);
}

void GStringPiece::CopyToString(string* target) const {
  STLAssignToString(target, ptr_, length_);
}

void GStringPiece::AppendToString(string* target) const {
  STLAppendToString(target, ptr_, length_);
}

int GStringPiece::copy(char* buf, size_type n, size_type pos) const {
  int ret = min(length_ - pos, n);
  memcpy(buf, ptr_ + pos, ret);
  return ret;
}

bool GStringPiece::contains(GStringPiece s) const {
  return find(s, 0) != npos;
}

int GStringPiece::find(GStringPiece s, size_type pos) const {
  if (length_ <= 0 || pos > static_cast<size_type>(length_)) {
    if (length_ == 0 && pos == 0 && s.length_ == 0) return 0;
    return npos;
  }
  const char *result = memmatch(ptr_ + pos, length_ - pos,
                                s.ptr_, s.length_);
  return result ? result - ptr_ : npos;
}

int GStringPiece::find(char c, size_type pos) const {
  if (length_ <= 0 || pos >= static_cast<size_type>(length_)) {
    return npos;
  }
  const char* result = static_cast<const char*>(
      memchr(ptr_ + pos, c, length_ - pos));
  return result != nullptr ? result - ptr_ : npos;
}

int GStringPiece::rfind(GStringPiece s, size_type pos) const {
  if (length_ < s.length_) return npos;
  const size_t ulen = length_;
  if (s.length_ == 0) return min(ulen, pos);

  const char* last = ptr_ + min(ulen - s.length_, pos) + s.length_;
  const char* result = std::find_end(ptr_, last, s.ptr_, s.ptr_ + s.length_);
  return result != last ? result - ptr_ : npos;
}

// Search range is [0..pos] inclusive.  If pos == npos, search everything.
int GStringPiece::rfind(char c, size_type pos) const {
  // Note: memrchr() is not available on Windows.
  if (length_ <= 0) return npos;
  for (int i = min(pos, static_cast<size_type>(length_ - 1));
       i >= 0; --i) {
    if (ptr_[i] == c) {
      return i;
    }
  }
  return npos;
}

// For each character in characters_wanted, sets the index corresponding
// to the ASCII code of that character to 1 in table.  This is used by
// the find_.*_of methods below to tell whether or not a character is in
// the lookup table in constant time.
// The argument `table' must be an array that is large enough to hold all
// the possible values of an unsigned char.  Thus it should be be declared
// as follows:
//   bool table[UCHAR_MAX + 1]
static inline void BuildLookupTable(GStringPiece characters_wanted,
                                    bool* table) {
  const int length = characters_wanted.length();
  const char* const data = characters_wanted.data();
  for (int i = 0; i < length; ++i) {
    table[static_cast<unsigned char>(data[i])] = true;
  }
}

int GStringPiece::find_first_of(GStringPiece s, size_type pos) const {
  if (length_ <= 0 || s.length_ <= 0) {
    return npos;
  }
  // Avoid the cost of BuildLookupTable() for a single-character search.
  if (s.length_ == 1) return find_first_of(s.ptr_[0], pos);

  bool lookup[UCHAR_MAX + 1] = { false };
  BuildLookupTable(s, lookup);
  for (int i = pos; i < length_; ++i) {
    if (lookup[static_cast<unsigned char>(ptr_[i])]) {
      return i;
    }
  }
  return npos;
}

int GStringPiece::find_first_not_of(GStringPiece s, size_type pos) const {
  if (length_ <= 0) return npos;
  if (s.length_ <= 0) return 0;
  // Avoid the cost of BuildLookupTable() for a single-character search.
  if (s.length_ == 1) return find_first_not_of(s.ptr_[0], pos);

  bool lookup[UCHAR_MAX + 1] = { false };
  BuildLookupTable(s, lookup);
  for (int i = pos; i < length_; ++i) {
    if (!lookup[static_cast<unsigned char>(ptr_[i])]) {
      return i;
    }
  }
  return npos;
}

int GStringPiece::find_first_not_of(char c, size_type pos) const {
  if (length_ <= 0) return npos;

  for (; pos < static_cast<size_type>(length_); ++pos) {
    if (ptr_[pos] != c) {
      return pos;
    }
  }
  return npos;
}

int GStringPiece::find_last_of(GStringPiece s, size_type pos) const {
  if (length_ <= 0 || s.length_ <= 0) return npos;
  // Avoid the cost of BuildLookupTable() for a single-character search.
  if (s.length_ == 1) return find_last_of(s.ptr_[0], pos);

  bool lookup[UCHAR_MAX + 1] = { false };
  BuildLookupTable(s, lookup);
  for (int i = min(pos, static_cast<size_type>(length_ - 1));
       i >= 0; --i) {
    if (lookup[static_cast<unsigned char>(ptr_[i])]) {
      return i;
    }
  }
  return npos;
}

int GStringPiece::find_last_not_of(GStringPiece s, size_type pos) const {
  if (length_ <= 0) return npos;

  int i = min(pos, static_cast<size_type>(length_ - 1));
  if (s.length_ <= 0) return i;

  // Avoid the cost of BuildLookupTable() for a single-character search.
  if (s.length_ == 1) return find_last_not_of(s.ptr_[0], pos);

  bool lookup[UCHAR_MAX + 1] = { false };
  BuildLookupTable(s, lookup);
  for (; i >= 0; --i) {
    if (!lookup[static_cast<unsigned char>(ptr_[i])]) {
      return i;
    }
  }
  return npos;
}

int GStringPiece::find_last_not_of(char c, size_type pos) const {
  if (length_ <= 0) return npos;

  for (int i = min(pos, static_cast<size_type>(length_ - 1));
       i >= 0; --i) {
    if (ptr_[i] != c) {
      return i;
    }
  }
  return npos;
}

GStringPiece GStringPiece::substr(size_type pos, size_type n) const {
  if (pos > length_) pos = length_;
  if (n > length_ - pos) n = length_ - pos;
  return GStringPiece(ptr_ + pos, n);
}

const GStringPiece::size_type GStringPiece::npos = size_type(-1);
