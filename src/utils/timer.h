#pragma once
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/io_context.hpp>
#include <chrono>
#include <functional>
#include <memory>

class Timer {
public:
    explicit Timer(boost::asio::io_context& io);
    ~Timer();

    void start(std::chrono::milliseconds duration, std::function<void()> callback);
    void cancel();
    void reset();

    bool expired() const;

private:
    boost::asio::steady_timer timer_;
    std::function<void()> callback_;
    std::chrono::milliseconds duration_{0};
    bool active_ = false;

    void on_tick(const boost::system::error_code& ec);
};
