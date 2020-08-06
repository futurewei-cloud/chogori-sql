//
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
//
#ifndef BASE_ATOMIC_REFCOUNT_H_
#define BASE_ATOMIC_REFCOUNT_H_
// Copyright 2008 Google Inc.
// All rights reserved.

// Atomic increment and decrement for reference counting.
// For atomic operations on statistics counters and sequence numbers,
// see atomic_stats_counter.h and atomic_sequence_num.h respectively.

// Some clients use atomic operations for reference counting.
// you use one of them:
//         util/refcount/reference_counted.h
//         util/gtl/refcounted_ptr.h
//         util/gtl/shared_ptr.h
// Alternatively, use a Mutex to maintain your reference count.
// If you really must build your own reference counts with atomic operations,
// use the following routines in the way suggested by this example:
//    AtomicWord ref_count_;  // remember to initialize this to 0
//    ...
//    void Ref() {
//      base::RefCountInc(&this->ref_count_);
//    }
//    void Unref() {
//      if (!base::RefCountDec(&this->ref_count_)) {
//        delete this;
//      }
//    }
// Using these routines (rather than the ones in atomicops.h) will provide the
// correct semantics; in particular, the memory ordering needed to make
// reference counting work will be guaranteed.
// You need not declare the reference count word "volatile".  After
// initialization you should use the word only via the routines below; the
// "volatile" in the signatures below is for backwards compatibility.
//
// The implementation includes annotations to avoid some false alarms
// when using Helgrind (the data race detector).
//
// If you need to do something very different from this, use a Mutex.

#include <glog/logging.h>

#include "yb/common/concurrent/atomicops.h"
#include "yb/common/type/integral_types.h"
#include "yb/common/log/logging-inl.h"
#include "yb/common/dynamic_annotations.h"

namespace base {

// These calls are available for both Atomic32, and AtomicWord types,
// and also for base::subtle::Atomic64 if available on the platform.

// Normally, clients are expected to use RefCountInc/RefCountDec.
// In rare cases, it may be necessary to adjust the reference count by
// more than 1, in which case they may use RefCountIncN/RefCountDecN.

// Increment a reference count by "increment", which must exceed 0.
inline void RefCountIncN(volatile Atomic32 *ptr, Atomic32 increment) {
  DCHECK_GT(increment, 0);
  base::subtle::NoBarrier_AtomicIncrement(ptr, increment);
}

// Decrement a reference count by "decrement", which must exceed 0,
// and return whether the result is non-zero.
// Insert barriers to ensure that state written before the reference count
// became zero will be visible to a thread that has just made the count zero.
inline bool RefCountDecN(volatile Atomic32 *ptr, Atomic32 decrement) {
  DCHECK_GT(decrement, 0);
  ANNOTATE_HAPPENS_BEFORE(ptr);
  bool res = base::subtle::Barrier_AtomicIncrement(ptr, -decrement) != 0;
  if (!res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}

// Increment a reference count by 1.
inline void RefCountInc(volatile Atomic32 *ptr) {
  base::RefCountIncN(ptr, 1);
}

// Decrement a reference count by 1 and return whether the result is non-zero.
// Insert barriers to ensure that state written before the reference count
// became zero will be visible to a thread that has just made the count zero.
inline bool RefCountDec(volatile Atomic32 *ptr) {
  return base::RefCountDecN(ptr, 1);
}

// Return whether the reference count is one.
// If the reference count is used in the conventional way, a
// refrerence count of 1 implies that the current thread owns the
// reference and no other thread shares it.
// This call performs the test for a referenece count of one, and
// performs the memory barrier needed for the owning thread
// to act on the object, knowing that it has exclusive access to the
// object.
inline bool RefCountIsOne(const volatile Atomic32 *ptr) {
  bool res = base::subtle::Acquire_Load(ptr) == 1;
  if (res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}

// Return whether the reference count is zero.  With conventional object
// referencing counting, the object will be destroyed, so the reference count
// should never be zero.  Hence this is generally used for a debug check.
inline bool RefCountIsZero(const volatile Atomic32 *ptr) {
  bool res = (subtle::Acquire_Load(ptr) == 0);
  if (res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}

#if BASE_HAS_ATOMIC64
// Implementations for Atomic64, if available.
inline void RefCountIncN(volatile base::subtle::Atomic64 *ptr,
                         base::subtle::Atomic64 increment) {
  DCHECK_GT(increment, 0);
  base::subtle::NoBarrier_AtomicIncrement(ptr, increment);
}
inline bool RefCountDecN(volatile base::subtle::Atomic64 *ptr,
                         base::subtle::Atomic64 decrement) {
  DCHECK_GT(decrement, 0);
  ANNOTATE_HAPPENS_BEFORE(ptr);
  bool res = base::subtle::Barrier_AtomicIncrement(ptr, -decrement) != 0;
  if (!res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}
inline void RefCountInc(volatile base::subtle::Atomic64 *ptr) {
  base::RefCountIncN(ptr, 1);
}
inline bool RefCountDec(volatile base::subtle::Atomic64 *ptr) {
  return base::RefCountDecN(ptr, 1);
}
inline bool RefCountIsOne(const volatile base::subtle::Atomic64 *ptr) {
  bool res = base::subtle::Acquire_Load(ptr) == 1;
  if (res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}
inline bool RefCountIsZero(const volatile base::subtle::Atomic64 *ptr) {
  bool res = (base::subtle::Acquire_Load(ptr) == 0);
  if (res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}
#endif

#ifdef AtomicWordCastType
// Implementations for AtomicWord, if it's a different type from the above.
inline void RefCountIncN(volatile AtomicWord *ptr, AtomicWord increment) {
  base::RefCountIncN(
      reinterpret_cast<volatile AtomicWordCastType *>(ptr), increment);
}
inline bool RefCountDecN(volatile AtomicWord *ptr, AtomicWord decrement) {
  ANNOTATE_HAPPENS_BEFORE(ptr);
  bool res = base::RefCountDecN(
      reinterpret_cast<volatile AtomicWordCastType *>(ptr), decrement);
  if (!res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}
inline void RefCountInc(volatile AtomicWord *ptr) {
  base::RefCountIncN(ptr, 1);
}
inline bool RefCountDec(volatile AtomicWord *ptr) {
  return base::RefCountDecN(ptr, 1);
}
inline bool RefCountIsOne(const volatile AtomicWord *ptr) {
  bool res = base::subtle::Acquire_Load(
      reinterpret_cast<const volatile AtomicWordCastType *>(ptr)) == 1;
  if (res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}
inline bool RefCountIsZero(const volatile AtomicWord *ptr) {
  bool res = base::subtle::Acquire_Load(
      reinterpret_cast<const volatile AtomicWordCastType *>(ptr)) == 0;
  if (res) {
    ANNOTATE_HAPPENS_AFTER(ptr);
  }
  return res;
}
#endif

} // namespace base

#endif  // BASE_ATOMIC_REFCOUNT_H_
