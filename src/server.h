#pragma once
#include "session.h"
#include "core/context.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <atomic>
#include <memory>

class Server {
public:
    Server(boost::asio::io_context& io,
           BrokerContext& ctx,
           const std::string& host,
           uint16_t port,
           int max_connections);

    void start();
    void stop();

    int active_connections() const { return active_connections_.load(); }

private:
    void do_accept();
    void on_accept(const boost::system::error_code& ec,
                   std::shared_ptr<boost::asio::ip::tcp::socket> socket);

    boost::asio::io_context& io_;
    boost::asio::ip::tcp::acceptor acceptor_;
    BrokerContext& ctx_;
    std::atomic<int> active_connections_{0};
    int max_connections_;
};
