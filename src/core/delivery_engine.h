#pragma once
#include "topic/topic_tree.h"
#include "packet/packets.h"
#include "qos/qos0.h"
#include "qos/qos1.h"
#include "qos/qos2.h"
#include <boost/asio/io_context.hpp>
#include <string>

class DeliveryEngine {
public:
    explicit DeliveryEngine(boost::asio::io_context& io);

    void publish_from_client(TopicTree& tree, const PublishPacket& pub);

    void handle_puback(const std::string& client_id, uint16_t packet_id);
    void handle_pubrec(const std::string& client_id, uint16_t packet_id);
    void handle_pubrel(const std::string& client_id, uint16_t packet_id);
    void handle_pubcomp(const std::string& client_id, uint16_t packet_id);

private:
    Qos0Handler qos0_;
    Qos1Handler qos1_;
    Qos2Handler qos2_;
};
