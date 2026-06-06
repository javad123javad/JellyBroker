#pragma once
#include "topic/topic_tree.h"
#include "packet/packets.h"
#include "utils/timer.h"
#include <boost/asio/io_context.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

class Session;

class Qos1Handler {
public:
    explicit Qos1Handler(boost::asio::io_context& io);

    void deliver(TopicTree& tree, const PublishPacket& pub);
    void handle_puback(const std::string& client_id, uint16_t packet_id);

private:
    struct PendingMessage {
        std::shared_ptr<Session> session;
        Buffer publish_packet;
        std::unique_ptr<Timer> retry_timer;
        int retry_count = 0;
    };

    struct ClientPending {
        std::unordered_map<uint16_t, PendingMessage> messages;
        uint16_t next_packet_id = 1;
    };

    boost::asio::io_context& io_;
    std::unordered_map<std::string, ClientPending> pending_;
    std::mutex mutex_;

    uint16_t next_packet_id(const std::string& client_id);
    void retry_message(const std::string& client_id, uint16_t packet_id);
};
