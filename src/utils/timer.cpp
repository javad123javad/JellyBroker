#include "timer.h"
#include <boost/system/error_code.hpp>

Timer::Timer(boost::asio::io_context& io)
    : timer_(io) {
}

Timer::~Timer() {
    cancel();
}

void Timer::start(std::chrono::milliseconds duration, std::function<void()> callback) {
    cancel();
    duration_ = duration;
    callback_ = std::move(callback);
    active_ = true;
    timer_.expires_after(duration_);
    timer_.async_wait([this](const boost::system::error_code& ec) { on_tick(ec); });
}

void Timer::cancel() {
    active_ = false;
    boost::system::error_code ec;
    timer_.cancel(ec);
}

void Timer::reset() {
    if (active_ && duration_.count() > 0) {
        boost::system::error_code ec;
        timer_.cancel(ec);
        timer_.expires_after(duration_);
        timer_.async_wait([this](const boost::system::error_code& ec) { on_tick(ec); });
    }
}

bool Timer::expired() const {
    return !active_;
}

void Timer::on_tick(const boost::system::error_code& ec) {
    if (ec == boost::asio::error::operation_aborted || !active_) return;
    active_ = false;
    if (callback_) {
        callback_();
    }
}
