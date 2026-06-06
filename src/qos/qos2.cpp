#include "qos2.h"
#include "session.h"
#include "logger.h"
#include <chrono>

Qos2Handler::Qos2Handler(boost::asio::io_context& io)
    : io_(io) {
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
        uint16_t pid = client.next_packet_id++;

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

        msg.retry_timer->start(
            std::chrono::seconds(5),
            [this, client_id = session->client_id(), pid]() {
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
    // Subscriber sent PUBREL, we send PUBCOMP
    // This handles the case where broker receives PUBREL
    // The session handler will call this and build PUBCOMP

    // Find the outgoing message and finalize
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = outgoing_.find(client_id);
    if (client_it == outgoing_.end()) return;

    auto msg_it = client_it->second.messages.find(packet_id);
    if (msg_it == client_it->second.messages.end()) return;

    msg_it->second.retry_timer->cancel();
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
    if (msg.retry_count >= 3) {
        Logger::instance().warn("QoS 2 retry exhausted for client {} packet {}", client_id, packet_id);
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

    msg.retry_timer->start(
        std::chrono::seconds(5),
        [this, client_id, packet_id]() {
            retry_message(client_id, packet_id);
        });
}
