#include "qos1.h"
#include "session.h"
#include "logger.h"
#include <chrono>

Qos1Handler::Qos1Handler(boost::asio::io_context& io)
    : io_(io) {
}

void Qos1Handler::deliver(TopicTree& tree, const PublishPacket& pub) {
    auto subscribers = tree.lookup(pub.topic);
    auto packet = PacketBuilder::build_publish(
        pub.topic, pub.payload.data(), pub.payload.size(),
        1, pub.retain, false, 0);

    for (auto& entry : subscribers) {
        auto session = entry.session.lock();
        if (!session) continue;

        std::lock_guard<std::mutex> lock(mutex_);
        auto& client = pending_[session->client_id()];
        uint16_t pid = client.next_packet_id++;

        auto pub_with_pid = PacketBuilder::build_publish(
            pub.topic, pub.payload.data(), pub.payload.size(),
            1, pub.retain, false, pid);

        auto& pm = client.messages[pid];
        pm.session = session;
        pm.publish_packet = std::move(pub_with_pid);
        pm.retry_count = 0;
        pm.retry_timer = std::make_unique<Timer>(io_);

        session->deliver(pm.publish_packet);

        pm.retry_timer->start(
            std::chrono::seconds(5),
            [this, client_id = session->client_id(), pid]() {
                retry_message(client_id, pid);
            });
    }
}

void Qos1Handler::handle_puback(const std::string& client_id, uint16_t packet_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = pending_.find(client_id);
    if (client_it == pending_.end()) return;

    auto& messages = client_it->second.messages;
    auto msg_it = messages.find(packet_id);
    if (msg_it == messages.end()) return;

    msg_it->second.retry_timer->cancel();
    messages.erase(msg_it);

    if (messages.empty()) {
        pending_.erase(client_it);
    }
}

uint16_t Qos1Handler::next_packet_id(const std::string& client_id) {
    auto it = pending_.find(client_id);
    if (it == pending_.end()) return 1;
    return it->second.next_packet_id++;
}

void Qos1Handler::retry_message(const std::string& client_id, uint16_t packet_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = pending_.find(client_id);
    if (client_it == pending_.end()) return;

    auto msg_it = client_it->second.messages.find(packet_id);
    if (msg_it == client_it->second.messages.end()) return;

    auto& pm = msg_it->second;
    if (pm.retry_count >= 3) {
        Logger::instance().warn("QoS 1 message retry exhausted for client {} packet {}", client_id, packet_id);
        client_it->second.messages.erase(msg_it);
        if (client_it->second.messages.empty()) {
            pending_.erase(client_it);
        }
        return;
    }

    ++pm.retry_count;
    if (pm.session && pm.session->connected()) {
        pm.session->deliver(pm.publish_packet);
    }

    pm.retry_timer->start(
        std::chrono::seconds(5),
        [this, client_id, packet_id]() {
            retry_message(client_id, packet_id);
        });
}
