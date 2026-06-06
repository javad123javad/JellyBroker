#include "qos1.h"
#include "session.h"
#include "config.h"
#include "logger.h"
#include <chrono>

Qos1Handler::Qos1Handler(boost::asio::io_context& io)
    : io_(io) {
}

uint16_t Qos1Handler::allocate_packet_id(ClientPending& client) {
    uint16_t start = client.next_packet_id;
    while (client.in_use.count(client.next_packet_id) > 0) {
        client.next_packet_id++;
        if (client.next_packet_id == 0) client.next_packet_id = 1;
        if (client.next_packet_id == start) {
            // All IDs exhausted
            return 0;
        }
    }
    uint16_t pid = client.next_packet_id++;
    if (client.next_packet_id == 0) client.next_packet_id = 1;
    client.in_use.insert(pid);
    return pid;
}

void Qos1Handler::deliver(TopicTree& tree, const PublishPacket& pub) {
    auto subscribers = tree.lookup(pub.topic);

    for (auto& entry : subscribers) {
        auto session = entry.session.lock();
        if (!session) continue;

        std::lock_guard<std::mutex> lock(mutex_);
        auto& client = pending_[session->client_id()];

        uint16_t pid = allocate_packet_id(client);
        if (pid == 0) {
            Logger::instance().warn("QoS1 packet ID space exhausted for {}", session->client_id());
            continue;
        }

        auto pub_with_pid = PacketBuilder::build_publish(
            pub.topic, pub.payload.data(), pub.payload.size(),
            1, pub.retain, false, pid);

        auto& pm = client.messages[pid];
        pm.session = session;
        pm.publish_packet = std::move(pub_with_pid);
        pm.retry_count = 0;
        pm.retry_timer = std::make_unique<Timer>(io_);

        session->deliver(pm.publish_packet);

        auto interval = std::chrono::seconds(Config::instance().retry_interval_seconds());
        pm.retry_timer->start(interval, [this, client_id = session->client_id(), pid]() {
            retry_message(client_id, pid);
        });
    }
}

void Qos1Handler::handle_puback(const std::string& client_id, uint16_t packet_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = pending_.find(client_id);
    if (client_it == pending_.end()) return;

    auto& client = client_it->second;
    auto msg_it = client.messages.find(packet_id);
    if (msg_it == client.messages.end()) return;

    msg_it->second.retry_timer->cancel();
    client.in_use.erase(packet_id);
    client.messages.erase(msg_it);

    if (client.messages.empty()) {
        pending_.erase(client_it);
    }
}

void Qos1Handler::retry_message(const std::string& client_id, uint16_t packet_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto client_it = pending_.find(client_id);
    if (client_it == pending_.end()) return;

    auto& client = client_it->second;
    auto msg_it = client.messages.find(packet_id);
    if (msg_it == client.messages.end()) return;

    auto& pm = msg_it->second;
    auto max_retries = Config::instance().max_retry_count();
    if (pm.retry_count >= max_retries) {
        Logger::instance().warn("QoS 1 retry exhausted for {} packet {}", client_id, packet_id);
        client.in_use.erase(packet_id);
        client.messages.erase(msg_it);
        if (client.messages.empty()) {
            pending_.erase(client_it);
        }
        return;
    }

    ++pm.retry_count;
    if (pm.session && pm.session->connected()) {
        pm.session->deliver(pm.publish_packet);
    }

    auto interval = std::chrono::seconds(Config::instance().retry_interval_seconds());
    pm.retry_timer->start(interval, [this, client_id, packet_id]() {
        retry_message(client_id, packet_id);
    });
}
