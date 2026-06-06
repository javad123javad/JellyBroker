#include "server.h"
#include "logger.h"

Server::Server(boost::asio::io_context& io,
               BrokerContext& ctx,
               const std::string& host,
               uint16_t port,
               int max_connections)
    : io_(io)
    , acceptor_(io)
    , ctx_(ctx)
    , max_connections_(max_connections) {

    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::make_address(host), port);

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();

    Logger::instance().info("Listening on {}:{}", host, port);
}

void Server::start() {
    do_accept();
}

void Server::stop() {
    boost::system::error_code ec;
    acceptor_.close(ec);
}

void Server::do_accept() {
    auto socket = std::make_shared<boost::asio::ip::tcp::socket>(io_);

    acceptor_.async_accept(*socket,
        [this, socket](const boost::system::error_code& ec) {
            on_accept(ec, socket);
        });
}

void Server::on_accept(const boost::system::error_code& ec,
                       std::shared_ptr<boost::asio::ip::tcp::socket> socket) {
    if (ec) {
        Logger::instance().debug("Accept error: {}", ec.message());
        do_accept();
        return;
    }

    if (active_connections_.load() >= max_connections_) {
        Logger::instance().warn("Max connections reached, rejecting");
        boost::system::error_code close_ec;
        socket->close(close_ec);
        do_accept();
        return;
    }

    auto session = std::make_shared<Session>(
        std::move(*socket), ctx_);

    active_connections_.fetch_add(1);
    session->start();

    Logger::instance().info("New connection accepted (active: {})", active_connections_.load());

    do_accept();
}
