#include "session.h"
#include "config.h"
#include <boost/asio/write.hpp>
#include <chrono>

Session::Session(boost::asio::ip::tcp::socket socket,
                 BrokerContext& ctx,
                 const std::string& remote_ip)
    : socket_(std::move(socket))
    , strand_(boost::asio::make_strand(socket_.get_executor()))
    , remote_ip_(remote_ip)
    , ctx_(ctx) {

    if (ctx_.ssl_ctx) {
        ssl_stream_ = std::make_unique<boost::asio::ssl::stream<
            boost::asio::ip::tcp::socket&>>(socket_, *ctx_.ssl_ctx);
    }
}

Session::~Session() {
    stop();
}

void Session::start() {
    auto self = shared_from_this();
    boost::asio::dispatch(strand_, [this, self]() {
        if (ssl_stream_) {
            do_ssl_handshake();
        } else {
            do_read();
        }
    });
}

void Session::do_ssl_handshake() {
    auto self = shared_from_this();
    ssl_stream_->async_handshake(
        boost::asio::ssl::stream_base::server,
        boost::asio::bind_executor(strand_,
            [this, self](const boost::system::error_code& ec) {
                if (ec) {
                    Logger::instance().warn("SSL handshake failed from {}: {}", remote_ip_, ec.message());
                    close();
                    return;
                }
                Logger::instance().info("SSL handshake completed from {}", remote_ip_);
                do_read();
            }));
}

void Session::stop() {
    connected_ = false;
    if (keepalive_timer_) {
        keepalive_timer_->cancel();
    }
    boost::system::error_code ec;
    if (ssl_stream_) {
        ssl_stream_->shutdown(ec);
    }
    socket_.close(ec);
}

void Session::deliver(const Buffer& packet) {
    auto self = shared_from_this();
    boost::asio::dispatch(strand_, [this, self, packet]() {
        if (!connected_) return;
        auto max_q = Config::instance().max_write_queue_size();
        if (static_cast<int>(write_queue_.size()) >= max_q) {
            Logger::instance().warn("Write queue overflow for {} ({}), closing",
                                    client_id_, remote_ip_);
            close();
            return;
        }
        bool was_empty = write_queue_.empty();
        write_queue_.push_back(packet);
        if (was_empty) {
            do_write();
        }
    });
}

void Session::do_read() {
    auto self = shared_from_this();
    read_buf_.clear();
    read_buf_.ensure_writable(4096);

    auto handler = boost::asio::bind_executor(strand_,
        [this, self](const boost::system::error_code& ec, size_t bytes) {
            if (!ec) {
                read_buf_.advance_write(bytes);
            }
            on_read(ec, bytes);
        });

    if (ssl_stream_) {
        ssl_stream_->async_read_some(
            boost::asio::buffer(read_buf_.write_head(), read_buf_.write_capacity()),
            handler);
    } else {
        socket_.async_read_some(
            boost::asio::buffer(read_buf_.write_head(), read_buf_.write_capacity()),
            handler);
    }
}

void Session::on_read(const boost::system::error_code& ec, size_t bytes_transferred) {
    if (ec) {
        Logger::instance().debug("Session read error from {}: {}", remote_ip_, ec.message());
        close();
        return;
    }

    parser_.feed(read_buf_.data(), bytes_transferred);

    Buffer packet;
    while (parser_.try_extract(packet)) {
        if (!packet.readable()) continue;
        try {
            handle_packet(packet);
        } catch (const std::exception& e) {
            Logger::instance().warn("Session handler error from {}: {}", remote_ip_, e.what());
            close();
            return;
        }
    }

    if (parser_.has_error()) {
        Logger::instance().warn("Parse error from {}: {}", remote_ip_, parser_.error_message());
        close();
        return;
    }

    if (connected_ && keepalive_timer_) {
        keepalive_timer_->reset();
    }

    if (connected_) {
        do_read();
    }
}

void Session::do_write() {
    if (!connected_ || write_queue_.empty()) {
        writing_ = false;
        return;
    }

    writing_ = true;
    auto self = shared_from_this();
    auto& packet = write_queue_.front();

    auto handler = boost::asio::bind_executor(strand_,
        [this, self](const boost::system::error_code& ec, size_t) {
            if (ec) {
                Logger::instance().debug("Session write error from {}: {}", remote_ip_, ec.message());
                close();
                return;
            }
            write_queue_.pop_front();
            do_write();
        });

    if (ssl_stream_) {
        boost::asio::async_write(
            *ssl_stream_,
            boost::asio::buffer(packet.data(), packet.size()),
            handler);
    } else {
        boost::asio::async_write(
            socket_,
            boost::asio::buffer(packet.data(), packet.size()),
            handler);
    }
}

void Session::handle_packet(Buffer& packet) {
    if (packet.empty()) return;
    if (packet.readable() < 1) return;

    uint8_t byte1 = packet.read_byte();
    auto type = static_cast<PacketType>((byte1 >> 4) & 0x0F);

    FixedHeader hdr;
    hdr.type = type;
    hdr.dup = (byte1 & 0x08) != 0;
    hdr.qos = (byte1 >> 1) & 0x03;
    hdr.retain = (byte1 & 0x01) != 0;
    hdr.remaining_length = packet.read_remaining_length();

    switch (type) {
        case PacketType::CONNECT:
            handle_connect(packet);
            break;
        case PacketType::PUBLISH:
            handle_publish(packet, hdr);
            break;
        case PacketType::PUBACK:
            handle_puback(packet);
            break;
        case PacketType::PUBREC:
            handle_pubrec(packet);
            break;
        case PacketType::PUBREL:
            handle_pubrel(packet);
            break;
        case PacketType::PUBCOMP:
            handle_pubcomp(packet);
            break;
        case PacketType::SUBSCRIBE:
            handle_subscribe(packet);
            break;
        case PacketType::UNSUBSCRIBE:
            handle_unsubscribe(packet);
            break;
        case PacketType::PINGREQ:
            handle_pingreq();
            break;
        case PacketType::DISCONNECT:
            handle_disconnect();
            break;
        default:
            Logger::instance().warn("Unhandled packet type from {}: {}", remote_ip_, static_cast<int>(type));
            break;
    }
}

bool Session::check_auth_rate_limit() {
    if (!ctx_.server_state) return true;

    std::lock_guard<std::mutex> lock(ctx_.server_state->mutex);
    int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    auto ban_it = ctx_.server_state->auth_ban_until.find(remote_ip_);
    if (ban_it != ctx_.server_state->auth_ban_until.end()) {
        if (now < ban_it->second) {
            Logger::instance().warn("Auth rate limit: {} is banned", remote_ip_);
            return false;
        }
        ctx_.server_state->auth_ban_until.erase(ban_it);
        ctx_.server_state->auth_failures.erase(remote_ip_);
    }

    return true;
}

void Session::handle_connect(Buffer& packet) {
    if (connected_) {
        close();
        return;
    }

    ConnectPacket conn;
    try {
        conn = ConnectPacket::parse(packet, 0);
    } catch (const std::exception& e) {
        Logger::instance().warn("Invalid CONNECT packet from {}: {}", remote_ip_, e.what());
        close();
        return;
    }

    if (conn.protocol_name != MQTT_PROTOCOL_NAME || conn.protocol_level != MQTT_PROTOCOL_LEVEL) {
        Logger::instance().warn("Unsupported protocol: {}/{}", conn.protocol_name, conn.protocol_level);
        auto nak = PacketBuilder::build_connack(false, ConnackCode::REFUSED_PROTOCOL);
        deliver(nak);
        close();
        return;
    }

    if (conn.client_id.empty() && !conn.clean_session) {
        auto nak = PacketBuilder::build_connack(false, ConnackCode::REFUSED_ID_REJECTED);
        deliver(nak);
        close();
        return;
    }

    if (!check_auth_rate_limit()) {
        auto nak = PacketBuilder::build_connack(false, ConnackCode::REFUSED_SERVER_UNAVAIL);
        deliver(nak);
        close();
        return;
    }

    if (!conn.username.empty() || !conn.password.empty()) {
        auto auth_result = ctx_.auth->authenticate(conn.client_id, conn.username, conn.password);
        if (!auth_result.success) {
            Logger::instance().warn("Auth failed for client: {} from {}", conn.client_id, remote_ip_);

            if (ctx_.server_state) {
                std::lock_guard<std::mutex> lock(ctx_.server_state->mutex);
                int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                int fails = ++ctx_.server_state->auth_failures[remote_ip_];

                auto max_attempts = Config::instance().max_auth_attempts();
                if (fails >= max_attempts) {
                    auto ban_secs = Config::instance().auth_ban_seconds();
                    ctx_.server_state->auth_ban_until[remote_ip_] = now + ban_secs;
                    Logger::instance().warn("Auth banned {} for {}s after {} failures",
                                            remote_ip_, ban_secs, fails);
                }
            }

            auto nak = PacketBuilder::build_connack(false, auth_result.reason);
            deliver(nak);
            close();
            return;
        }
    }

    if (ctx_.server_state) {
        std::lock_guard<std::mutex> lock(ctx_.server_state->mutex);
        ctx_.server_state->auth_failures.erase(remote_ip_);
        ctx_.server_state->auth_ban_until.erase(remote_ip_);

        auto it = ctx_.server_state->active_clients.find(conn.client_id);
        if (it != ctx_.server_state->active_clients.end()) {
            auto old_session = it->second.lock();
            if (old_session && old_session.get() != this) {
                Logger::instance().info("Client {} reconnecting, closing old session", conn.client_id);
                old_session->close();
            }
        }
        ctx_.server_state->active_clients[conn.client_id] = shared_from_this();
    }

    client_id_ = conn.client_id;
    username_ = conn.username;
    clean_session_ = conn.clean_session;

    auto max_kal = Config::instance().max_keepalive();
    keepalive_ = conn.keepalive;
    if (keepalive_ == 0) {
        keepalive_ = static_cast<uint16_t>(max_kal);
    } else if (keepalive_ > max_kal) {
        keepalive_ = static_cast<uint16_t>(max_kal);
    }

    connected_ = true;

    bool session_present = false;
    if (!clean_session_) {
        auto existing_subs = ctx_.sub_manager->get_client_subs(client_id_);
        if (!existing_subs.empty()) {
            session_present = true;
            for (const auto& sub : existing_subs) {
                SubscriberEntry entry;
                entry.session = shared_from_this();
                entry.qos = sub.qos;
                ctx_.topic_tree->subscribe(sub.topic_filter, entry);
            }
        }
    } else {
        ctx_.sub_manager->remove_client(client_id_);
    }

    auto ack = PacketBuilder::build_connack(session_present, ConnackCode::ACCEPTED);
    deliver(ack);

    start_keepalive();

    Logger::instance().info("Client connected: {} (clean={}) from {}", client_id_, clean_session_, remote_ip_);
}

void Session::handle_publish(Buffer& packet, const FixedHeader& hdr) {
    if (!connected_) return;

    PublishPacket pub;
    try {
        pub = PublishPacket::parse(packet, hdr);
    } catch (const std::exception& e) {
        Logger::instance().warn("Invalid PUBLISH from {}: {}", remote_ip_, e.what());
        close();
        return;
    }

    auto access = ctx_.auth->check_acl(username_, pub.topic, Access::WRITE);
    if (access == Access::NONE) {
        Logger::instance().warn("ACL denied PUBLISH for {} on {}", client_id_, pub.topic);
        if (ctx_.counters) ctx_.counters->dropped++;
        return;
    }

    if (!topic::is_valid_topic(pub.topic)) {
        Logger::instance().warn("Invalid topic from {}: {}", client_id_, pub.topic);
        if (ctx_.counters) ctx_.counters->dropped++;
        return;
    }

    if (ctx_.counters) ctx_.counters->received++;

    if (pub.retain) {
        RetainedMessage rm;
        if (!pub.payload.empty()) {
            rm.payload = Buffer(pub.payload.data(), pub.payload.size());
            rm.qos = pub.qos;
        }
        if (!ctx_.topic_tree->set_retained(pub.topic, rm)) {
            Logger::instance().warn("Retained message limit reached, dropping for topic: {}", pub.topic);
        }
    }

    switch (pub.qos) {
        case 0:
            ctx_.delivery->publish_from_client(*ctx_.topic_tree, pub);
            if (ctx_.counters) ctx_.counters->published++;
            break;
        case 1: {
            auto puback = PacketBuilder::build_puback(pub.packet_id);
            deliver(puback);
            ctx_.delivery->publish_from_client(*ctx_.topic_tree, pub);
            if (ctx_.counters) ctx_.counters->published++;
            break;
        }
        case 2: {
            auto pubrec = PacketBuilder::build_pubrec(pub.packet_id);
            deliver(pubrec);
            ctx_.delivery->store_incoming_qos2(client_id_, std::move(pub));
            // published incremented on PUBREL completion
            break;
        }
    }
}

void Session::handle_subscribe(Buffer& packet) {
    if (!connected_) return;

    SubscribePacket sub;
    try {
        sub = SubscribePacket::parse(packet, 0);
    } catch (const std::exception& e) {
        Logger::instance().warn("Invalid SUBSCRIBE from {}: {}", remote_ip_, e.what());
        close();
        return;
    }

    std::vector<SubackCode> return_codes;

    for (const auto& filter : sub.filters) {
        if (!topic::is_valid_filter(filter.topic_filter)) {
            return_codes.push_back(SubackCode::FAILURE);
            continue;
        }

        auto access = ctx_.auth->check_acl(username_, filter.topic_filter, Access::READ);
        if (access == Access::NONE) {
            Logger::instance().warn("ACL denied SUBSCRIBE for {} on {}", client_id_, filter.topic_filter);
            return_codes.push_back(SubackCode::FAILURE);
            continue;
        }

        if (!clean_session_) {
            auto existing = ctx_.sub_manager->get_client_subs(client_id_);
            auto max_subs = Config::instance().max_subscriptions_per_client();
            if (static_cast<int>(existing.size()) >= max_subs) {
                Logger::instance().warn("Max subscriptions reached for {} ({})", client_id_, max_subs);
                return_codes.push_back(SubackCode::FAILURE);
                continue;
            }
        }

        SubscriberEntry entry;
        entry.session = shared_from_this();
        entry.qos = filter.requested_qos;
        ctx_.topic_tree->subscribe(filter.topic_filter, entry);

        if (!clean_session_) {
            ctx_.sub_manager->add(client_id_, filter.topic_filter, filter.requested_qos);
        }

        return_codes.push_back(static_cast<SubackCode>(filter.requested_qos));
        if (ctx_.counters) ctx_.counters->subscribed++;

        auto retained = ctx_.topic_tree->get_all_retained();
        for (const auto& [topic, msg] : retained) {
            if (topic::matches(topic, filter.topic_filter)) {
                auto pub_pkt = PacketBuilder::build_publish(
                    topic, msg.payload.data(), msg.payload.size(),
                    msg.qos, true, false, 0);
                deliver(pub_pkt);
            }
        }
    }

    auto suback = PacketBuilder::build_suback(sub.packet_id, return_codes);
    deliver(suback);
}

void Session::handle_unsubscribe(Buffer& packet) {
    if (!connected_) return;

    UnsubscribePacket unsub;
    try {
        unsub = UnsubscribePacket::parse(packet, 0);
    } catch (const std::exception& e) {
        Logger::instance().warn("Invalid UNSUBSCRIBE from {}: {}", remote_ip_, e.what());
        close();
        return;
    }

    for (const auto& filter : unsub.topic_filters) {
        ctx_.topic_tree->unsubscribe(filter, shared_from_this());
        if (!clean_session_) {
            ctx_.sub_manager->remove(client_id_, filter);
        }
    }

    auto unsuback = PacketBuilder::build_unsuback(unsub.packet_id);
    deliver(unsuback);
}

void Session::handle_pingreq() {
    if (!connected_) return;
    auto resp = PacketBuilder::build_pingresp();
    deliver(resp);
}

void Session::handle_disconnect() {
    Logger::instance().info("Client disconnected: {} from {}", client_id_, remote_ip_);
    connected_ = false;
    close();
}

void Session::handle_puback(Buffer& packet) {
    if (!connected_) return;
    try {
        auto puback = PubackPacket::parse(packet);
        ctx_.delivery->handle_puback(client_id_, puback.packet_id);
    } catch (const std::exception& e) {
        Logger::instance().warn("Invalid PUBACK from {}: {}", remote_ip_, e.what());
        close();
    }
}

void Session::handle_pubrec(Buffer& packet) {
    if (!connected_) return;
    try {
        auto pubrec = PubrecPacket::parse(packet);
        ctx_.delivery->handle_pubrec(client_id_, pubrec.packet_id);
    } catch (const std::exception& e) {
        Logger::instance().warn("Invalid PUBREC from {}: {}", remote_ip_, e.what());
        close();
    }
}

void Session::handle_pubrel(Buffer& packet) {
    if (!connected_) return;
    try {
        auto pubrel = PubrelPacket::parse(packet);
        if (ctx_.delivery->complete_incoming_qos2(client_id_, pubrel.packet_id, *ctx_.topic_tree)) {
            if (ctx_.counters) ctx_.counters->published++;
            auto pubcomp = PacketBuilder::build_pubcomp(pubrel.packet_id);
            deliver(pubcomp);
        } else {
            ctx_.delivery->handle_pubrel(client_id_, pubrel.packet_id);
        }
    } catch (const std::exception& e) {
        Logger::instance().warn("Invalid PUBREL from {}: {}", remote_ip_, e.what());
        close();
    }
}

void Session::handle_pubcomp(Buffer& packet) {
    if (!connected_) return;
    try {
        auto pubcomp = PubcompPacket::parse(packet);
        ctx_.delivery->handle_pubcomp(client_id_, pubcomp.packet_id);
    } catch (const std::exception& e) {
        Logger::instance().warn("Invalid PUBCOMP from {}: {}", remote_ip_, e.what());
        close();
    }
}

void Session::close() {
    if (!connected_) return;

    connected_ = false;

    if (ctx_.server_state) {
        std::lock_guard<std::mutex> lock(ctx_.server_state->mutex);

        auto ip_it = ctx_.server_state->connections_per_ip.find(remote_ip_);
        if (ip_it != ctx_.server_state->connections_per_ip.end()) {
            if (--ip_it->second <= 0) {
                ctx_.server_state->connections_per_ip.erase(ip_it);
            }
        }

        if (!client_id_.empty()) {
            auto it = ctx_.server_state->active_clients.find(client_id_);
            if (it != ctx_.server_state->active_clients.end()) {
                auto existing = it->second.lock();
                if (existing.get() == this) {
                    ctx_.server_state->active_clients.erase(it);
                }
            }
        }
    }

    if (!client_id_.empty()) {
        ctx_.delivery->clear_client_incoming(client_id_);
    }

    if (!clean_session_ && !client_id_.empty()) {
        // Persistent session: keep subscriptions
    } else if (!client_id_.empty()) {
        auto subs = ctx_.sub_manager->get_client_subs(client_id_);
        for (const auto& sub : subs) {
            ctx_.topic_tree->unsubscribe(sub.topic_filter, shared_from_this());
        }
        ctx_.sub_manager->remove_client(client_id_);
    }

    boost::system::error_code ec;
    if (ssl_stream_) {
        ssl_stream_->shutdown(ec);
    }
    socket_.close(ec);
}

void Session::start_keepalive() {
    if (keepalive_ == 0) return;

    keepalive_timer_ = std::make_unique<Timer>(
        static_cast<boost::asio::io_context&>(socket_.get_executor().context()));

    auto multiplier = Config::instance().keepalive_multiplier();
    auto duration = std::chrono::seconds(static_cast<int>(keepalive_ * multiplier));

    auto self = shared_from_this();
    keepalive_timer_->start(duration, [this, self]() {
        on_keepalive_timeout();
    });
}

void Session::on_keepalive_timeout() {
    Logger::instance().warn("Keepalive timeout for client: {} from {}", client_id_, remote_ip_);
    close();
}
