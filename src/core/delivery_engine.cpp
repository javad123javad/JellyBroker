#include "core/delivery_engine.h"
#include "logger.h"

DeliveryEngine::DeliveryEngine(boost::asio::io_context& io)
    : qos0_()
    , qos1_(io)
    , qos2_(io) {
}

void DeliveryEngine::publish_from_client(TopicTree& tree, const PublishPacket& pub) {
    switch (pub.qos) {
        case 0:
            qos0_.deliver(tree, pub);
            break;
        case 1:
            qos1_.deliver(tree, pub);
            break;
        case 2:
            qos2_.deliver(tree, pub);
            break;
    }
}

void DeliveryEngine::handle_puback(const std::string& client_id, uint16_t packet_id) {
    qos1_.handle_puback(client_id, packet_id);
}

void DeliveryEngine::handle_pubrec(const std::string& client_id, uint16_t packet_id) {
    qos2_.handle_pubrec(client_id, packet_id);
}

void DeliveryEngine::handle_pubrel(const std::string& client_id, uint16_t packet_id) {
    qos2_.handle_pubrel(client_id, packet_id);
}

void DeliveryEngine::handle_pubcomp(const std::string& client_id, uint16_t packet_id) {
    qos2_.handle_pubcomp(client_id, packet_id);
}

void DeliveryEngine::store_incoming_qos2(const std::string& client_id, PublishPacket pub) {
    IncomingMessage msg;
    msg.topic = pub.topic;
    msg.payload = Buffer(pub.payload.data(), pub.payload.size());
    msg.qos = pub.qos;

    std::lock_guard<std::mutex> lock(incoming_mutex_);
    incoming_qos2_[client_id][pub.packet_id] = std::move(msg);
}

bool DeliveryEngine::complete_incoming_qos2(const std::string& client_id, uint16_t packet_id, TopicTree& tree) {
    IncomingMessage msg;

    {
        std::lock_guard<std::mutex> lock(incoming_mutex_);
        auto client_it = incoming_qos2_.find(client_id);
        if (client_it == incoming_qos2_.end()) return false;

        auto msg_it = client_it->second.find(packet_id);
        if (msg_it == client_it->second.end()) return false;

        msg = std::move(msg_it->second);
        client_it->second.erase(msg_it);
        if (client_it->second.empty()) {
            incoming_qos2_.erase(client_it);
        }
    }

    PublishPacket pub;
    pub.topic = msg.topic;
    pub.payload = Buffer(msg.payload.data(), msg.payload.size());
    pub.qos = msg.qos;
    pub.packet_id = 0;

    qos2_.deliver(tree, pub);

    return true;
}

void DeliveryEngine::clear_client_incoming(const std::string& client_id) {
    std::lock_guard<std::mutex> lock(incoming_mutex_);
    incoming_qos2_.erase(client_id);
}
