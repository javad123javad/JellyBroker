#pragma once
#include "packet/parser.h"
#include "packet/builder.h"
#include "packet/packets.h"
#include "topic/topic_tree.h"
#include "subscription.h"
#include "auth/authenticator.h"
#include "core/context.h"
#include "utils/timer.h"
#include "utils/buffer.h"
#include "logger.h"
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/ssl.hpp>
#include <deque>
#include <memory>
#include <string>

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(boost::asio::ip::tcp::socket socket,
            BrokerContext& ctx,
            const std::string& remote_ip);

    ~Session();

    void start();
    void stop();

    void deliver(const Buffer& packet);

    const std::string& client_id() const { return client_id_; }
    bool connected() const { return connected_; }
    const std::string& remote_ip() const { return remote_ip_; }
    const std::string& username() const { return username_; }
    bool clean_session() const { return clean_session_; }
    uint16_t keepalive() const { return keepalive_; }

private:
    void do_ssl_handshake();
    void do_read();
    void on_read(const boost::system::error_code& ec, size_t bytes_transferred);
    void do_write();

    void handle_packet(Buffer& packet);
    void handle_connect(Buffer& packet);
    void handle_publish(Buffer& packet, const FixedHeader& hdr);
    void handle_subscribe(Buffer& packet);
    void handle_unsubscribe(Buffer& packet);
    void handle_pingreq();
    void handle_disconnect();
    void handle_puback(Buffer& packet);
    void handle_pubrec(Buffer& packet);
    void handle_pubrel(Buffer& packet);
    void handle_pubcomp(Buffer& packet);

    void close();
    void start_keepalive();
    void on_keepalive_timeout();
    bool check_auth_rate_limit();

    boost::asio::ip::tcp::socket socket_;
    std::unique_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>> ssl_stream_;
    boost::asio::strand<boost::asio::any_io_executor> strand_;

    PacketParser parser_;
    Buffer read_buf_;
    std::deque<Buffer> write_queue_;
    bool writing_ = false;

    std::string client_id_;
    std::string username_;
    std::string remote_ip_;
    bool clean_session_ = true;
    bool connected_ = false;
    uint16_t keepalive_ = 0;
    std::unique_ptr<Timer> keepalive_timer_;

    BrokerContext& ctx_;
};
