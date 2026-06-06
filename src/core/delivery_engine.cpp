#include "core/delivery_engine.h"

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
