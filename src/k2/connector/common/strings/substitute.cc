// Copyright 2008 Google Inc.  All rights reserved.
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

#include "substitute.h"

#include <glog/logging.h>
#include "common/log/logging-inl.h"
#include "common/macros.h"
#include "ascii_ctype.h"
#include "common/gscoped_ptr.h"
#include "common/util/stl_util.h"

namespace strings {

using internal::SubstituteArg;

const SubstituteArg SubstituteArg::NoArg;

// Returns the number of args in arg_array which were passed explicitly
// to Substitute().
static int CountSubstituteArgs(const SubstituteArg* const* args_array) {
  int count = 0;
  while (args_array[count] != &SubstituteArg::NoArg) {
    ++count;
  }
  return count;
}

namespace internal {
// ----------------------------------------------------------------------
// copied over from escape.cc
// CEscapeString()
//    Copies 'src' to 'dest', escaping dangerous characters using
//    C-style escape sequences. This is very useful for preparing query
//    flags. 'src' and 'dest' should not overlap. The 'Hex' version uses
//    hexadecimal rather than octal sequences. The 'Utf8Safe' version doesn't
//    touch UTF-8 bytes.
//    Returns the number of bytes written to 'dest' (not including the \0)
//    or -1 if there was insufficient space.
//
//    Currently only \n, \r, \t, ", ', \ and !ascii_isprint() chars are escaped.
// ----------------------------------------------------------------------
    int CEscapeInternal(const char* src, int src_len, char* dest,
                        int dest_len, bool use_hex, bool utf8_safe) {
        const char* src_end = src + src_len;
        int used = 0;
        bool last_hex_escape = false;  // true if last output char was \xNN

        for (; src < src_end; src++) {
            if (dest_len - used < 2)   // Need space for two letter escape
                return -1;

            bool is_hex_escape = false;
            switch (*src) {
                case '\n': dest[used++] = '\\'; dest[used++] = 'n';  break;
                case '\r': dest[used++] = '\\'; dest[used++] = 'r';  break;
                case '\t': dest[used++] = '\\'; dest[used++] = 't';  break;
                case '\"': dest[used++] = '\\'; dest[used++] = '\"'; break;
                case '\'': dest[used++] = '\\'; dest[used++] = '\''; break;
                case '\\': dest[used++] = '\\'; dest[used++] = '\\'; break;
                default:
                    // Note that if we emit \xNN and the src character after that is a hex
                    // digit then that digit must be escaped too to prevent it being
                    // interpreted as part of the character code by C.
                    // XXX: converted (*src) to unsigned char to workaround the error -Werror=type-limits
                    if ((!utf8_safe || static_cast<unsigned char>(*src) < 0x80) &&
                        (!ascii_isprint(*src) ||
                         (last_hex_escape && ascii_isxdigit(*src)))) {
                        if (dest_len - used < 4)  // need space for 4 letter escape
                            return -1;
                        sprintf(dest + used, (use_hex ? "\\x%02x" : "\\%03o"), *src);
                        is_hex_escape = use_hex;
                        used += 4;
                    } else {
                        dest[used++] = *src;
                        break;
                    }
            }
            last_hex_escape = is_hex_escape;
        }

        if (dest_len - used < 1)   // make sure that there is room for \0
            return -1;

        dest[used] = '\0';   // doesn't count towards return value though
        return used;
    }

    int CEscapeString(const char* src, int src_len, char* dest, int dest_len) {
        return CEscapeInternal(src, src_len, dest, dest_len, false, false);
    }

    string CEscape(const GStringPiece& src) {
        const int dest_length = src.size() * 4 + 1;  // Maximum possible expansion
        gscoped_array<char> dest(new char[dest_length]);
        const int len = CEscapeInternal(src.data(), src.size(),
                                        dest.get(), dest_length, false, false);
        DCHECK_GE(len, 0);
        return string(dest.get(), len);
    }

    int SubstitutedSize(GStringPiece format,
                    const SubstituteArg* const* args_array) {
  int size = 0;
  for (int i = 0; i < format.size(); i++) {
    if (format[i] == '$') {
      if (i+1 >= format.size()) {
        LOG(DFATAL) << "Invalid strings::Substitute() format string: \""
                    << CEscape(format) << "\".";
        return 0;
      } else if (ascii_isdigit(format[i+1])) {
        int index = format[i+1] - '0';
        if (args_array[index]->size() == -1) {
          LOG(DFATAL)
            << "strings::Substitute format string invalid: asked for \"$"
            << index << "\", but only " << CountSubstituteArgs(args_array)
            << " args were given.  Full format string was: \""
            << CEscape(format) << "\".";
          return 0;
        }
        size += args_array[index]->size();
        ++i;  // Skip next char.
      } else if (format[i+1] == '$') {
        ++size;
        ++i;  // Skip next char.
      } else {
        LOG(DFATAL) << "Invalid strings::Substitute() format string: \""
                    << CEscape(format) << "\".";
        return 0;
      }
    } else {
      ++size;
    }
  }
  return size;
}

char* SubstituteToBuffer(GStringPiece format,
                         const SubstituteArg* const* args_array,
                         char* target) {
  CHECK_NOTNULL(target);
  for (int i = 0; i < format.size(); i++) {
    if (format[i] == '$') {
      if (ascii_isdigit(format[i+1])) {
        const SubstituteArg* src = args_array[format[i+1] - '0'];
        memcpy(target, src->data(), src->size());
        target += src->size();
        ++i;  // Skip next char.
      } else if (format[i+1] == '$') {
        *target++ = '$';
        ++i;  // Skip next char.
      }
    } else {
      *target++ = format[i];
    }
  }
  return target;
}

} // namespace internal

void SubstituteAndAppend(
    string* output, GStringPiece format,
    const SubstituteArg& arg0, const SubstituteArg& arg1,
    const SubstituteArg& arg2, const SubstituteArg& arg3,
    const SubstituteArg& arg4, const SubstituteArg& arg5,
    const SubstituteArg& arg6, const SubstituteArg& arg7,
    const SubstituteArg& arg8, const SubstituteArg& arg9) {
  const SubstituteArg* const args_array[] = {
    &arg0, &arg1, &arg2, &arg3, &arg4, &arg5, &arg6, &arg7, &arg8, &arg9, nullptr
  };

  // Determine total size needed.
  int size = SubstitutedSize(format, args_array);
  if (size == 0) return;

  // Build the string.
  int original_size = output->size();
  STLStringResizeUninitialized(output, original_size + size);
  char* target = string_as_array(output) + original_size;

  target = SubstituteToBuffer(format, args_array, target);
  DCHECK_EQ(target - output->data(), output->size());
}

SubstituteArg::SubstituteArg(const void* value) {
  COMPILE_ASSERT(sizeof(scratch_) >= sizeof(value) * 2 + 2,
                 fix_sizeof_scratch_);
  if (value == nullptr) {
    text_ = "NULL";
    size_ = strlen(text_);
  } else {
    char* ptr = scratch_ + sizeof(scratch_);
    uintptr_t num = reinterpret_cast<uintptr_t>(value);
    static const char kHexDigits[] = "0123456789abcdef";
    do {
      *--ptr = kHexDigits[num & 0xf];
      num >>= 4;
    } while (num != 0);
    *--ptr = 'x';
    *--ptr = '0';
    text_ = ptr;
    size_ = scratch_ + sizeof(scratch_) - ptr;
  }
}

}  // namespace strings
