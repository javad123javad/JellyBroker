#include "admin_server.h"
#include "config.h"
#include "logger.h"
#include "session.h"
#include <nlohmann/json.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>
#include <algorithm>
#include <sstream>

AdminServer::AdminServer(boost::asio::io_context& io,
                         BrokerContext& ctx,
                         const std::string& host,
                         uint16_t port,
                         boost::asio::ssl::context* ssl_ctx)
    : io_(io)
    , acceptor_(io)
    , ctx_(ctx)
    , ssl_ctx_(ssl_ctx) {

    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::make_address(host), port);

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();

    Logger::instance().info("Admin endpoint listening on {}:{} (TLS: {})",
                            host, port, ssl_ctx_ ? "yes" : "no");
}

void AdminServer::start() {
    running_ = true;
    do_accept();
}

void AdminServer::stop() {
    running_ = false;
    boost::system::error_code ec;
    acceptor_.close(ec);
}

void AdminServer::close_conn(const std::shared_ptr<Connection>& conn) {
    boost::system::error_code ec;
    if (conn->ssl) {
        conn->ssl->shutdown(ec);
    }
    conn->socket->close(ec);
}

void AdminServer::do_accept() {
    if (!running_) return;

    auto conn = std::make_shared<Connection>();
    conn->socket = std::make_shared<boost::asio::ip::tcp::socket>(io_);

    if (ssl_ctx_) {
        conn->ssl = std::make_shared<boost::asio::ssl::stream<
            boost::asio::ip::tcp::socket&>>(*conn->socket, *ssl_ctx_);
    }

    acceptor_.async_accept(*conn->socket,
        [this, conn](const boost::system::error_code& ec) {
            if (!running_) return;
            if (ec) {
                Logger::instance().debug("Admin accept error: {}", ec.message());
                do_accept();
                return;
            }

            if (conn->ssl) {
                do_tls_handshake(conn);
            } else {
                start_read(conn);
            }
            do_accept();
        });
}

void AdminServer::do_tls_handshake(std::shared_ptr<Connection> conn) {
    conn->ssl->async_handshake(
        boost::asio::ssl::stream_base::server,
        [this, conn](const boost::system::error_code& ec) {
            if (ec) {
                Logger::instance().debug("Admin TLS handshake error: {}", ec.message());
                close_conn(conn);
                return;
            }
            start_read(conn);
        });
}

void AdminServer::start_read(std::shared_ptr<Connection> conn) {
    auto read_buf = std::make_shared<boost::asio::streambuf>();

    auto handler = [this, conn, read_buf](const boost::system::error_code& ec, size_t) {
        if (ec) {
            close_conn(conn);
            return;
        }

        std::string line;
        std::istream is(read_buf.get());
        std::getline(is, line);

        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        handle_line(conn, line);
    };

    if (conn->ssl) {
        boost::asio::async_read_until(*conn->ssl, *read_buf, '\n', std::move(handler));
    } else {
        boost::asio::async_read_until(*conn->socket, *read_buf, '\n', std::move(handler));
    }
}

void AdminServer::handle_line(std::shared_ptr<Connection> conn, const std::string& line) {
    std::string response;
    if (!line.empty()) {
        response = handle_command(line);
    }
    response += '\n';

    auto resp_buf = std::make_shared<std::string>(std::move(response));

    auto handler = [conn, resp_buf](const boost::system::error_code&, size_t) {
        boost::system::error_code ec;
        if (conn->ssl) {
            conn->ssl->shutdown(ec);
        }
        conn->socket->close(ec);
    };

    if (conn->ssl) {
        boost::asio::async_write(*conn->ssl, boost::asio::buffer(*resp_buf), std::move(handler));
    } else {
        boost::asio::async_write(*conn->socket, boost::asio::buffer(*resp_buf), std::move(handler));
    }
}

std::string AdminServer::handle_command(const std::string& line) {
    std::string upper = line;
    for (auto& c : upper) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

    if (upper == "CLIENTS") {
        return cmd_clients();
    }
    if (upper == "STATS") {
        return cmd_stats();
    }
    if (upper.compare(0, 7, "CLIENT ") == 0 && upper.size() > 7) {
        auto id = line.substr(7);
        return cmd_client(id);
    }

    nlohmann::json err;
    err["error"] = "Unknown command. Try: CLIENTS, STATS, CLIENT <id>";
    return err.dump();
}

std::string AdminServer::cmd_clients() {
    nlohmann::json arr = nlohmann::json::array();

    if (!ctx_.server_state) return arr.dump();

    std::lock_guard<std::mutex> lock(ctx_.server_state->mutex);
    for (const auto& [cid, wptr] : ctx_.server_state->active_clients) {
        auto session = wptr.lock();
        if (!session) continue;

        nlohmann::json entry;
        entry["client_id"] = session->client_id();
        entry["remote_ip"] = session->remote_ip();
        entry["username"] = session->username();
        entry["clean_session"] = session->clean_session();
        entry["keepalive"] = session->keepalive();
        entry["connected"] = session->connected();
        arr.push_back(std::move(entry));
    }
    return arr.dump(2);
}

std::string AdminServer::cmd_stats() {
    nlohmann::json obj;

    obj["active_connections"] = 0;
    if (ctx_.server_state) {
        std::lock_guard<std::mutex> lock(ctx_.server_state->mutex);
        obj["active_connections"] = static_cast<int>(ctx_.server_state->active_clients.size());
    }

    if (ctx_.counters) {
        obj["received"] = ctx_.counters->received.load();
        obj["published"] = ctx_.counters->published.load();
        obj["dropped"] = ctx_.counters->dropped.load();
        obj["subscribed"] = ctx_.counters->subscribed.load();
    }

    return obj.dump(2);
}

std::string AdminServer::cmd_client(const std::string& client_id) {
    nlohmann::json obj;

    if (!ctx_.server_state) {
        obj["error"] = "Server state unavailable";
        return obj.dump();
    }

    std::shared_ptr<Session> session;
    {
        std::lock_guard<std::mutex> lock(ctx_.server_state->mutex);
        auto it = ctx_.server_state->active_clients.find(client_id);
        if (it == ctx_.server_state->active_clients.end()) {
            obj["error"] = "Client not found: " + client_id;
            return obj.dump();
        }
        session = it->second.lock();
        if (!session) {
            obj["error"] = "Client disconnected: " + client_id;
            return obj.dump();
        }
    }

    obj["client_id"] = session->client_id();
    obj["remote_ip"] = session->remote_ip();
    obj["username"] = session->username();
    obj["clean_session"] = session->clean_session();
    obj["keepalive"] = session->keepalive();
    obj["connected"] = session->connected();

    if (ctx_.sub_manager) {
        auto subs = ctx_.sub_manager->get_client_subs(client_id);
        nlohmann::json subs_arr = nlohmann::json::array();
        for (const auto& sub : subs) {
            nlohmann::json s;
            s["topic_filter"] = sub.topic_filter;
            s["qos"] = sub.qos;
            subs_arr.push_back(std::move(s));
        }
        obj["subscriptions"] = std::move(subs_arr);
    }

    return obj.dump(2);
}
