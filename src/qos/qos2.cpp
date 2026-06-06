#include "qos2.h"
#include "session.h"
#include "config.h"
#include "logger.h"
#include <chrono>

Qos2Handler::Qos2Handler(boost::asio::io_context& io)
    : io_(io) {
}

uint16_t Qos2Handler::allocate_packet_id(ClientOutgoing& client) {
    uint16_t start = client.next_packet_id;
    while (client.in_use.count(client.next_packet_id) > 0) {
        client.next_packet_id++;
        if (client.next_packet_id == 0) client.next_packet_id = 1;
        if (client.next_packet_id == start) {
            return 0;
        }
    }
    uint16_t pid = client.next_packet_id++;
    if (client.next_packet_id == 0) client.next_packet_id = 1;
    client.in_use.insert(pid);
    return pid;
}

void Qos2Handler::deliver(TopicTree& tree, const PublishPacket& pub) {
    auto subscribers = tree.lookup(pub.topic);

    for (auto& entry : subscribers) {
        auto session = entry.session.lock();
        if (!session) continue;

        uint8_t granted_qos = std::min(entry.qos, pub.qos);
        if (granted_qos < 2) {
            auto packet = PacketBuilder::build_publish(
                pub.topic, pub.payload.data(), pub.payload.size(),
                granted_qos, false, false, 0);
            session->deliver(packet);
            continue;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto& client = outgoing_[session->client_id()];

        uint16_t pid = allocate_packet_id(client);
        if (pid == 0) {
            Logger::instance().warn("QoS2 packet ID space exhausted for {}", session->client_id());
            continue;
        }

        auto publish_pkt = PacketBuilder::build_publish(
            pub.topic, pub.payload.data(), pub.payload.size(),
            2, false, false, pid);

        auto& msg = client.messages[pid];
        msg.session = session;
        msg.topic = pub.topic;
        msg.payload = Buffer(pub.payload.data(), pub.payload.size());
        msg.state = OutgoingState::PUBLISH_SENT;
        msg.retry_count = 0;
        msg.retry_timer = std::make_unique<Timer>(io_);

        session->deliver(publish_pkt);

        auto interval = std::chrono::seconds(Config::instance().retry_interval_seconds());
        msg.retry_timer->start(interval, [this, client_id = session->client_id(), pid]() {
            retry_message(client_id, pid);
        });
    }
}

void Qos2Handler::handle_pubrec(const std::string& client_id, uint16_t packet_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = outgoing_.find(client_id);
    if (client_it == outgoing_.end()) return;

    auto msg_it = client_it->second.messages.find(packet_id);
    if (msg_it == client_it->second.messages.end()) return;

    auto& msg = msg_it->second;
    msg.state = OutgoingState::PUBREC_RECEIVED;
    msg.retry_count = 0;

    auto pubrel = PacketBuilder::build_pubrel(packet_id);
    if (msg.session && msg.session->connected()) {
        msg.session->deliver(pubrel);
    }
}

void Qos2Handler::handle_pubrel(const std::string& client_id, uint16_t packet_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = outgoing_.find(client_id);
    if (client_it == outgoing_.end()) return;

    auto msg_it = client_it->second.messages.find(packet_id);
    if (msg_it == client_it->second.messages.end()) return;

    msg_it->second.retry_timer->cancel();
    client_it->second.in_use.erase(packet_id);
    client_it->second.messages.erase(msg_it);

    if (client_it->second.messages.empty()) {
        outgoing_.erase(client_it);
    }
}

void Qos2Handler::handle_pubcomp(const std::string& client_id, uint16_t packet_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = outgoing_.find(client_id);
    if (client_it == outgoing_.end()) return;

    auto msg_it = client_it->second.messages.find(packet_id);
    if (msg_it == client_it->second.messages.end()) return;

    msg_it->second.retry_timer->cancel();
    client_it->second.in_use.erase(packet_id);
    client_it->second.messages.erase(msg_it);

    if (client_it->second.messages.empty()) {
        outgoing_.erase(client_it);
    }
}

void Qos2Handler::retry_message(const std::string& client_id, uint16_t packet_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = outgoing_.find(client_id);
    if (client_it == outgoing_.end()) return;

    auto msg_it = client_it->second.messages.find(packet_id);
    if (msg_it == client_it->second.messages.end()) return;

    auto& msg = msg_it->second;
    auto max_retries = Config::instance().max_retry_count();
    if (msg.retry_count >= max_retries) {
        Logger::instance().warn("QoS 2 retry exhausted for {} packet {}", client_id, packet_id);
        client_it->second.in_use.erase(packet_id);
        client_it->second.messages.erase(msg_it);
        if (client_it->second.messages.empty()) {
            outgoing_.erase(client_it);
        }
        return;
    }

    ++msg.retry_count;
    if (msg.session && msg.session->connected()) {
        switch (msg.state) {
            case OutgoingState::PUBLISH_SENT: {
                auto pkt = PacketBuilder::build_publish(
                    msg.topic, msg.payload.data(), msg.payload.size(),
                    2, false, true, packet_id);
                msg.session->deliver(pkt);
                break;
            }
            case OutgoingState::PUBREC_RECEIVED: {
                auto pkt = PacketBuilder::build_pubrel(packet_id);
                msg.session->deliver(pkt);
                break;
            }
        }
    }

    auto interval = std::chrono::seconds(Config::instance().retry_interval_seconds());
    msg.retry_timer->start(interval, [this, client_id, packet_id]() {
        retry_message(client_id, packet_id);
    });
}
