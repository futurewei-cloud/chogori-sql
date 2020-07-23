// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
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
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.
//
// Slices can be built around faststrings and GStringPieces using constructors
// with implicit casts. Both GStringPieces and faststrings depend on a great
// deal of gutil code, so these constructors are conditionalized on
// YB_HEADERS_USE_RICH_SLICE. Likewise, YB_HEADERS_USE_RICH_SLICE controls
// whether to use gutil-based memeq/memcmp substitutes; if it is unset, Slice
// will fall back to standard memcmp.

#ifndef YB_UTIL_SLICE_H_
#define YB_UTIL_SLICE_H_

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <map>
#include <string>

#include "fastmem.h"
#include "stringpiece.h"
#include "faststring.h"
#include "cast.h"


namespace yb {

class Status;
struct SliceParts;

class Slice {
 public:
  // Create an empty slice.
  Slice() : begin_(util::to_uchar_ptr("")), end_(begin_) { }

  // Create a slice that refers to d[0,n-1].
  Slice(const uint8_t* d, size_t n) : begin_(d), end_(d + n) {}
  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : Slice(util::to_uchar_ptr(d), n) {}

  // Create a slice that refers to [begin, end).
  Slice(const uint8_t* begin, const uint8_t* end) : begin_(begin), end_(end) {
    CHECK_LE(begin, end);
  }

  template<size_t N>
  explicit Slice(const std::array<char, N>& arr) : Slice(arr.data(), N) {}

  template<size_t N>
  explicit Slice(const std::array<unsigned char, N>& arr) : Slice(arr.data(), N) {}

  Slice(const char* begin, const char* end)
      : Slice(util::to_uchar_ptr(begin), util::to_uchar_ptr(end)) {}

  // Create a slice that refers to the contents of "s"
  template <class CharTraits, class Allocator>
  Slice(const std::basic_string<char, CharTraits, Allocator>& s) // NOLINT(runtime/explicit)
      : Slice(util::to_uchar_ptr(s.data()), s.size()) {}

  // Create a slice that refers to s[0,strlen(s)-1]
  Slice(const char* s) // NOLINT(runtime/explicit)
      : Slice(util::to_uchar_ptr(s), strlen(s)) {}

  // Create a slice that refers to the contents of the faststring.
  // Note that further appends to the faststring may invalidate this slice.
  Slice(const faststring &s) // NOLINT(runtime/explicit)
      : Slice(s.data(), s.size()) {}

  Slice(const GStringPiece& s) // NOLINT(runtime/explicit)
      : Slice(util::to_uchar_ptr(s.data()), s.size()) {}

  // Create a single slice from SliceParts using buf as storage.
  // buf must exist as long as the returned Slice exists.
  Slice(const SliceParts& parts, std::string* buf);

  const char* cdata() const { return util::to_char_ptr(begin_); }

  // Return a pointer to the beginning of the referenced data
  const uint8_t* data() const { return begin_; }

  // Return a mutable pointer to the beginning of the referenced data.
  uint8_t *mutable_data() { return const_cast<uint8_t*>(begin_); }

  const uint8_t* end() const { return end_; }

  const char* cend() const { return util::to_char_ptr(end_); }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return end_ - begin_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return begin_ == end_; }

  template <class... Args>
  bool GreaterOrEqual(const Slice& arg0, Args&&... args) const {
    return !Less(arg0, std::forward<Args>(args)...);
  }

  template <class... Args>
  bool Less(const Slice& arg0, Args&&... args) const {
    return DoLess(arg0, std::forward<Args>(args)...);
  }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  uint8_t operator[](size_t n) const {
    DCHECK_LT(n, size());
    return begin_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {
    begin_ = util::to_uchar_ptr("");
    end_ = begin_;
  }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    DCHECK_LE(n, size());
    begin_ += n;
  }

  Slice Prefix(size_t n) const {
    DCHECK_LE(n, size());
    return Slice(begin_, n);
  }

  // Drop the last "n" bytes from this slice.
  void remove_suffix(size_t n) {
    DCHECK_LE(n, size());
    end_ -= n;
  }

  Slice Suffix(size_t n) const {
    DCHECK_LE(n, size());
    return Slice(end_ - n, end_);
  }

  void CopyTo(void* buffer) const {
    memcpy(buffer, begin_, size());
  }

  // Truncate the slice to "n" bytes
  void truncate(size_t n) {
    DCHECK_LE(n, size());
    end_ = begin_ + n;
  }

  char consume_byte() {
    DCHECK_GT(end_, begin_);
    return *begin_++;
  }

  bool TryConsumeByte(char c) {
    if (empty() || *begin_ != c) {
      return false;
    }
    ++begin_;
    return true;
  }

  char FirstByteOr(char def) {
    return !empty() ? *begin_ : def;
  }

  MUST_USE_RESULT Status consume_byte(char c);

  // Checks that this slice has size() = 'expected_size' and returns
  // STATUS(Corruption, ) otherwise.
  Status check_size(size_t expected_size) const;

  // Compare two slices and returns the offset of the first byte where they differ.
  size_t difference_offset(const Slice& b) const;

  void CopyToBuffer(std::string* buffer) const;

  // Return a string that contains the copy of the referenced data.
  std::string ToBuffer() const;

  std::string ToString() const __attribute__ ((deprecated)) { return ToBuffer(); }
  std::string ToString(bool hex) const __attribute__ ((deprecated));

  std::string ToDebugString(size_t max_len = 0) const;
  std::string ToDebugHexString() const;

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;
  int compare_prefix(const Slice& b) const;

  size_t hash() const noexcept;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    return starts_with(x.data(), x.size());
  }

  bool starts_with(const uint8_t* data, size_t size) const {
    return (this->size() >= size) && MemEqual(begin_, data, size);
  }

  bool starts_with(const char* data, size_t size) const {
    return (this->size() >= size) && MemEqual(begin_, data, size);
  }

  bool starts_with(char c) const {
    return (size() >= 1) && (*begin_ == c);
  }

  bool ends_with(const Slice& x) const {
    size_t xsize = x.size();
    return (size() >= xsize) && MemEqual(end_ - xsize, x.begin_, xsize);
  }

  bool ends_with(char c) const {
    return (size() >= 1) && *(end_ - 1) == c;
  }

  // Comparator struct, useful for ordered collections (like STL maps).
  struct Comparator {
    bool operator()(const Slice& a, const Slice& b) const {
      return a.compare(b) < 0;
    }
  };

  struct Hash {
    size_t operator()(const Slice& a) const noexcept {
      return a.hash();
    }
  };

  // Relocates this slice's data into 'd' provided this isn't already the
  // case. It is assumed that 'd' is large enough to fit the data.
  void relocate(uint8_t* d) {
    if (begin_ != d) {
      size_t size = end_ - begin_;
      memcpy(d, begin_, size);
      begin_ = d;
      end_ = d + size;
    }
  }

  size_t DynamicMemoryUsage() const { return 0; }

 private:
  friend bool operator==(const Slice& x, const Slice& y);

  bool DoLess() const {
    return !empty();
  }

  template <class... Args>
  bool DoLess(const Slice& arg0, Args&&... args) const {
    auto arg0_size = arg0.size();
    if (size() < arg0_size) {
      return compare(arg0) < 0;
    }

    int cmp = Slice(begin_, arg0_size).compare(arg0);
    if (cmp != 0) {
      return cmp < 0;
    }

    return Slice(begin_ + arg0_size, end_).DoLess(std::forward<Args>(args)...);
  }

  static bool MemEqual(const void* a, const void* b, size_t n) {
    return strings::memeq(a, b, n);
  }

  static int MemCompare(const void* a, const void* b, size_t n) {
    return strings::fastmemcmp_inlined(a, b, n);
  }

  const uint8_t* begin_;
  const uint8_t* end_;

  // Intentionally copyable
};

struct SliceParts {
  SliceParts(const Slice* _parts, int _num_parts) :
      parts(_parts), num_parts(_num_parts) { }
  SliceParts() : parts(nullptr), num_parts(0) {}

  template<size_t N>
  SliceParts(const std::array<Slice, N>& input) // NOLINT
      : parts(input.data()), num_parts(N) {
  }

  std::string ToDebugHexString() const {
    std::string result;
    for (int i = 0; i != num_parts; ++i) {
      result += parts[i].ToDebugHexString();
    }
    return result;
  }

  const Slice* parts;
  int num_parts;
};

inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (Slice::MemEqual(x.data(), y.data(), x.size())));
}

inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

inline std::ostream& operator<<(std::ostream& o, const Slice& s) {
  return o << s.ToDebugString(16); // should be enough for anyone...
}

inline int Slice::compare(const Slice& b) const {
  auto my_size = size();
  auto b_size = b.size();
  const size_t min_len = std::min(my_size, b_size);
  int r = MemCompare(begin_, b.begin_, min_len);
  if (r == 0) {
    if (my_size < b_size) { return -1; }
    if (my_size > b_size) { return 1; }
  }
  return r;
}

inline int Slice::compare_prefix(const Slice& b) const {
  return Slice(begin_, std::min(size(), b.size())).compare(b);
}

inline size_t Slice::hash() const noexcept {
  constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  size_t result = kFnvOffset;
  const uint8_t* e = end();
  for (const uint8_t* i = begin_; i != e; ++i) {
    result = (result * kFnvPrime) ^ *i;
  }
  return result;
}

inline size_t Slice::difference_offset(const Slice& b) const {
  size_t off = 0;
  const size_t len = std::min(size(), b.size());
  for (; off < len; off++) {
    if (begin_[off] != b.begin_[off]) break;
  }
  return off;
}

// STL map whose keys are Slices.
//
// See sample usage in slice-test.cc.
template <typename T>
struct SliceMap {
  typedef std::map<Slice, T, Slice::Comparator> type;
};

// Can be used with source implicitly convertible to Slice, for example std::string.
inline void CopyToBuffer(const Slice& source, std::string* dest) {
  // operator= or assign(const std::string&) will shrink the capacity on at least CentOS gcc
  // build, so we have to use assign(const char*, size_t) to preserve buffer capacity and avoid
  // unnecessary memory reallocations.
  source.CopyToBuffer(dest);
}

}  // namespace yb

namespace rocksdb {

typedef yb::Slice Slice;
typedef yb::SliceParts SliceParts;

}  // namespace rocksdb

#endif // YB_UTIL_SLICE_H_
