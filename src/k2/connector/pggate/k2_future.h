#pragma once
#include <future>

namespace k2pg {

// Wrapper for std futures which allows a callback to be invoked right after the
// future's blocking get() call returns, but before the user sees the result
// Useful for example for reporting metrics on request durations
template <typename T>
class CBFuture : std::future<T>  {
public:
    CBFuture(): _callback([]{}) {}

    template <typename Func>
    CBFuture(std::future<T> stdfut, Func&& callback):
        _stdfut(std::move(stdfut)), _callback(std::forward<Func>(callback)){}

    T get() {
        try {
            T result(_stdfut.get());
            _callback();
            return result;
        }
        catch(...) {
            _callback();
            std::rethrow_exception(std::current_exception());
        }
    }

    bool valid() const noexcept {
        return _stdfut.valid();
    }

private:
    std::future<T> _stdfut;
    std::function<void(void)> _callback;
};

// Wrapper for std futures which allows a TransformFunc callback to be invoked 
// right after the future's blocking get() call returns, but before the user sees the result.
// Useful for example for exception handling, return type conversion, reporting metrics on request durations, etc
template <typename TransformFunc,  typename... T>
class CBFuture2 {
    using Fut_T = std::tuple<T...>;
    using InvokeResult_T = std::invoke_result_t<TransformFunc, Fut_T, std::exception_ptr>;

public:

    CBFuture2(std::future<Fut_T> stdfut, TransformFunc&& callback):
         _stdfut(std::move(stdfut)),
         _callback(std::forward<TransformFunc>(callback)) {}
    InvokeResult_T get() {
        try {
            return _callback(_stdfut.get(), nullptr);
        } catch (...) {
            return _callback(Fut_T{}, std::current_exception());
        }
    }
    bool valid() const noexcept {
        return _stdfut.valid();
    }
   private:
    std::future<Fut_T> _stdfut;
    std::function<InvokeResult_T(Fut_T, std::exception_ptr)> _callback;
};
}
