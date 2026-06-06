#include "session.h"
#include "config.h"
#include <boost/asio/write.hpp>
#include <chrono>

Session::Session(boost::asio::ip::tcp::socket socket,
                 BrokerContext& ctx)
    : socket_(std::move(socket))
    , strand_(boost::asio::make_strand(socket_.get_executor()))
    , ctx_(ctx) {
}

Session::~Session() {
    stop();
}

void Session::start() {
    auto self = shared_from_this();
    boost::asio::dispatch(strand_, [this, self]() {
        do_read();
    });
}

void Session::stop() {
    connected_ = false;
    if (keepalive_timer_) {
        keepalive_timer_->cancel();
    }
    boost::system::error_code ec;
    socket_.close(ec);
}

void Session::deliver(const Buffer& packet) {
    auto self = shared_from_this();
    boost::asio::dispatch(strand_, [this, self, packet]() {
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

    socket_.async_read_some(
        boost::asio::buffer(read_buf_.write_head(), read_buf_.write_capacity()),
        boost::asio::bind_executor(strand_,
            [this, self](const boost::system::error_code& ec, size_t bytes) {
                if (!ec) {
                    read_buf_.advance_write(bytes);
                }
                on_read(ec, bytes);
            }));
}

void Session::on_read(const boost::system::error_code& ec, size_t bytes_transferred) {
    if (ec) {
        Logger::instance().debug("Session read error: {}", ec.message());
        close();
        return;
    }

    parser_.feed(read_buf_.data(), bytes_transferred);

    Buffer packet;
    while (parser_.try_extract(packet)) {
        handle_packet(packet);
    }

    if (parser_.has_error()) {
        Logger::instance().warn("Parse error: {}", parser_.error_message());
        close();
        return;
    }

    if (connected_ && keepalive_timer_) {
        keepalive_timer_->reset();
    }

    do_read();
}

void Session::do_write() {
    if (write_queue_.empty()) {
        writing_ = false;
        return;
    }

    writing_ = true;
    auto self = shared_from_this();
    auto& packet = write_queue_.front();

    boost::asio::async_write(
        socket_,
        boost::asio::buffer(packet.data(), packet.size()),
        boost::asio::bind_executor(strand_,
            [this, self](const boost::system::error_code& ec, size_t) {
                if (ec) {
                    Logger::instance().debug("Session write error: {}", ec.message());
                    close();
                    return;
                }
                write_queue_.pop_front();
                do_write();
            }));
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
            Logger::instance().warn("Unhandled packet type: {}", static_cast<int>(type));
            break;
    }
}

void Session::handle_connect(Buffer& packet) {
    if (connected_) {
        close();
        return;
    }

    auto conn = ConnectPacket::parse(packet, 0);

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

    if (!conn.username.empty() || !conn.password.empty()) {
        auto auth_result = ctx_.auth->authenticate(conn.client_id, conn.username, conn.password);
        if (!auth_result.success) {
            Logger::instance().warn("Auth failed for client: {}", conn.client_id);
            auto nak = PacketBuilder::build_connack(false, auth_result.reason);
            deliver(nak);
            close();
            return;
        }
    }

    client_id_ = conn.client_id;
    username_ = conn.username;
    clean_session_ = conn.clean_session;
    keepalive_ = conn.keepalive;
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

    Logger::instance().info("Client connected: {} (clean={})", client_id_, clean_session_);
}

void Session::handle_publish(Buffer& packet, const FixedHeader& hdr) {
    if (!connected_) return;

    auto pub = PublishPacket::parse(packet, hdr);

    if (!username_.empty()) {
        auto access = ctx_.auth->check_acl(username_, pub.topic, Access::WRITE);
        if (access == Access::NONE) {
            Logger::instance().warn("ACL denied PUBLISH for {} on {}", client_id_, pub.topic);
            return;
        }
    }

    if (!topic::is_valid_topic(pub.topic)) {
        Logger::instance().warn("Invalid topic from {}: {}", client_id_, pub.topic);
        return;
    }

    if (pub.retain) {
        if (pub.payload.empty()) {
            RetainedMessage rm;
            ctx_.topic_tree->set_retained(pub.topic, rm);
        } else {
            RetainedMessage rm;
            rm.payload = Buffer(pub.payload.data(), pub.payload.size());
            rm.qos = pub.qos;
            ctx_.topic_tree->set_retained(pub.topic, rm);
        }
    }

    switch (pub.qos) {
        case 0:
            ctx_.delivery->publish_from_client(*ctx_.topic_tree, pub);
            break;
        case 1: {
            auto puback = PacketBuilder::build_puback(pub.packet_id);
            deliver(puback);
            ctx_.delivery->publish_from_client(*ctx_.topic_tree, pub);
            break;
        }
        case 2: {
            auto pubrec = PacketBuilder::build_pubrec(pub.packet_id);
            deliver(pubrec);
            ctx_.delivery->publish_from_client(*ctx_.topic_tree, pub);
            break;
        }
    }
}

void Session::handle_subscribe(Buffer& packet) {
    if (!connected_) return;

    auto sub = SubscribePacket::parse(packet, 0);
    std::vector<SubackCode> return_codes;

    for (const auto& filter : sub.filters) {
        if (!topic::is_valid_filter(filter.topic_filter)) {
            return_codes.push_back(SubackCode::FAILURE);
            continue;
        }

        if (!username_.empty()) {
            auto access = ctx_.auth->check_acl(username_, filter.topic_filter, Access::READ);
            if (access == Access::NONE) {
                Logger::instance().warn("ACL denied SUBSCRIBE for {} on {}", client_id_, filter.topic_filter);
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

    auto unsub = UnsubscribePacket::parse(packet, 0);

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
    Logger::instance().info("Client disconnected: {}", client_id_);
    connected_ = false;
    close();
}

void Session::handle_puback(Buffer& packet) {
    if (!connected_) return;
    auto puback = PubackPacket::parse(packet);
    ctx_.delivery->handle_puback(client_id_, puback.packet_id);
}

void Session::handle_pubrec(Buffer& packet) {
    if (!connected_) return;
    auto pubrec = PubrecPacket::parse(packet);
    ctx_.delivery->handle_pubrec(client_id_, pubrec.packet_id);
}

void Session::handle_pubrel(Buffer& packet) {
    if (!connected_) return;
    auto pubrel = PubrelPacket::parse(packet);
    ctx_.delivery->handle_pubrel(client_id_, pubrel.packet_id);
    auto pubcomp = PacketBuilder::build_pubcomp(pubrel.packet_id);
    deliver(pubcomp);
}

void Session::handle_pubcomp(Buffer& packet) {
    if (!connected_) return;
    auto pubcomp = PubcompPacket::parse(packet);
    ctx_.delivery->handle_pubcomp(client_id_, pubcomp.packet_id);
}

void Session::close() {
    if (!clean_session_ && !client_id_.empty()) {
        // Persistent session: keep subscriptions
    } else if (!client_id_.empty()) {
        auto subs = ctx_.sub_manager->get_client_subs(client_id_);
        for (const auto& sub : subs) {
            ctx_.topic_tree->unsubscribe(sub.topic_filter, shared_from_this());
        }
        ctx_.sub_manager->remove_client(client_id_);
    }

    connected_ = false;
    boost::system::error_code ec;
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
    Logger::instance().warn("Keepalive timeout for client: {}", client_id_);
    close();
}
