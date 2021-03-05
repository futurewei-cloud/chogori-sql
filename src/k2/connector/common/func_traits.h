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
// renamed from common/type/type_traits.h to func_traits.h

#ifndef YB_UTIL_FUNC_TRAITS_H
#define YB_UTIL_FUNC_TRAITS_H

#include <boost/preprocessor/cat.hpp>
#include <boost/tti/has_type.hpp>

namespace yb {

#define HAS_FREE_FUNCTION(function) \
  template <class T> \
  struct BOOST_PP_CAT(HasFreeFunction_, function) { \
    typedef int Yes; \
    typedef struct { Yes array[2]; } No; \
    typedef typename std::remove_cv<typename std::remove_reference<T>::type>::type CleanedT; \
    \
    template <class U> \
    static auto Test(const U* u) -> decltype(function(*u), Yes(0)) {} \
    static No Test(...) {} \
    \
    static constexpr bool value = \
        sizeof(Test(static_cast<const CleanedT*>(nullptr))) == sizeof(Yes); \
  };

#define HAS_MEMBER_FUNCTION(function) \
    template<class T> \
    struct BOOST_PP_CAT(HasMemberFunction_, function) { \
      typedef int Yes; \
      typedef struct { Yes array[2]; } No; \
      typedef typename std::remove_reference<T>::type StrippedT; \
      template<class U> static Yes Test(typename std::remove_reference< \
          decltype(static_cast<U*>(nullptr)->function())>::type*); \
      template<class U> static No Test(...); \
      static const bool value = sizeof(Yes) == sizeof(Test<StrippedT>(nullptr)); \
    };

// We suppose that if class has nested const_iterator then it is collection.
BOOST_TTI_HAS_TYPE(const_iterator);

template <class T>
class IsCollection : public has_type_const_iterator<
    typename std::remove_cv<typename std::remove_reference<T>::type>::type> {
};

// This class is used to determine whether T is similar to pointer.
// We suppose that if class provides * and -> operators so it is pointer.
template<class T>
class IsPointerLikeHelper {
 private:
  typedef int Yes;
  typedef struct { Yes array[2]; } No;

  template <typename C> static Yes HasDeref(decltype(&C::operator*));
  template <typename C> static No HasDeref(...);

  template <typename C> static Yes HasArrow(decltype(&C::operator->));
  template <typename C> static No HasArrow(...);
 public:
  typedef boost::mpl::bool_<sizeof(HasDeref<T>(nullptr)) == sizeof(Yes) &&
                            sizeof(HasArrow<T>(nullptr)) == sizeof(Yes)> type;
};

template<class T>
class IsPointerLikeImpl : public IsPointerLikeHelper<T>::type {};

template<class T>
class IsPointerLikeImpl<T*> : public boost::mpl::true_ {};

// For correct routing we should strip reference and const, volatile specifiers.
template<class T>
class IsPointerLike : public IsPointerLikeImpl<
    typename std::remove_cv<typename std::remove_reference<T>::type>::type> {
};

}  // namespace yb

#endif  // YB_UTIL_FUNC_TRAITS_H
