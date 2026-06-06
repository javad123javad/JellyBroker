#pragma once
#include "topic/topic_tree.h"
#include "packet/packets.h"
#include "utils/timer.h"
#include <boost/asio/io_context.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

class Session;

class Qos2Handler {
public:
    explicit Qos2Handler(boost::asio::io_context& io);

    void deliver(TopicTree& tree, const PublishPacket& pub);
    void handle_pubrec(const std::string& client_id, uint16_t packet_id);
    void handle_pubrel(const std::string& client_id, uint16_t packet_id);
    void handle_pubcomp(const std::string& client_id, uint16_t packet_id);

private:
    enum class OutgoingState { PUBLISH_SENT, PUBREC_RECEIVED };

    struct OutgoingMessage {
        std::shared_ptr<Session> session;
        std::string topic;
        Buffer payload;
        OutgoingState state = OutgoingState::PUBLISH_SENT;
        std::unique_ptr<Timer> retry_timer;
        int retry_count = 0;
    };

    struct ClientOutgoing {
        std::unordered_map<uint16_t, OutgoingMessage> messages;
        uint16_t next_packet_id = 1;
        std::unordered_set<uint16_t> in_use;
    };

    boost::asio::io_context& io_;
    std::unordered_map<std::string, ClientOutgoing> outgoing_;
    std::mutex mutex_;

    uint16_t allocate_packet_id(ClientOutgoing& client);
    void retry_message(const std::string& client_id, uint16_t packet_id);
};
