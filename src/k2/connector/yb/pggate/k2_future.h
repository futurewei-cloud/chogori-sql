#pragma once
#include <future>

namespace k2pg {
template <typename T>

// Wrapper for std futures which allows a callback to be invoked right after the
// future's blocking get() call returns, but before the user sees the result
// Useful for example for reporting metrics on request durations
class CBFuture : std::future<T> {
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
}
