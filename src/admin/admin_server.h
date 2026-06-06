#pragma once
#include "core/context.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <memory>
#include <string>

class AdminServer {
public:
    AdminServer(boost::asio::io_context& io,
                BrokerContext& ctx,
                const std::string& host,
                uint16_t port,
                boost::asio::ssl::context* ssl_ctx = nullptr);

    void start();
    void stop();

private:
    struct Connection {
        std::shared_ptr<boost::asio::ip::tcp::socket> socket;
        std::shared_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>> ssl;
    };

    void do_accept();
    void do_tls_handshake(std::shared_ptr<Connection> conn);
    void start_read(std::shared_ptr<Connection> conn);
    void handle_line(std::shared_ptr<Connection> conn, const std::string& line);
    void close_conn(const std::shared_ptr<Connection>& conn);

    std::string handle_command(const std::string& line);
    std::string cmd_clients();
    std::string cmd_stats();
    std::string cmd_client(const std::string& client_id);

    boost::asio::io_context& io_;
    boost::asio::ip::tcp::acceptor acceptor_;
    BrokerContext& ctx_;
    boost::asio::ssl::context* ssl_ctx_;
    bool running_ = false;
};
