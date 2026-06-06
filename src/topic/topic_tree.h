#pragma once
#include "filter.h"
#include "packet/types.h"
#include "utils/buffer.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>

class Session;

struct SubscriberEntry {
    std::weak_ptr<Session> session;
    uint8_t qos = 0;
};

struct RetainedMessage {
    Buffer payload;
    uint8_t qos = 0;
};

class TopicTree {
public:
    TopicTree() = default;
    ~TopicTree() = default;

    TopicTree(const TopicTree&) = delete;
    TopicTree& operator=(const TopicTree&) = delete;

    void subscribe(const std::string& filter, SubscriberEntry entry);
    void unsubscribe(const std::string& filter, const std::shared_ptr<Session>& session);
    std::vector<SubscriberEntry> lookup(const std::string& topic);

    void set_retained(const std::string& topic, const RetainedMessage& msg);
    std::shared_ptr<RetainedMessage> get_retained(const std::string& topic);
    std::vector<std::pair<std::string, RetainedMessage>> get_all_retained();

private:
    struct Node {
        std::unordered_map<std::string, std::unique_ptr<Node>> children;
        std::unique_ptr<Node> single_wild;    // for '+'
        std::unique_ptr<Node> multi_wild;     // for '#'
        std::vector<SubscriberEntry> subscribers;
        std::shared_ptr<RetainedMessage> retained;
    };

    Node root_;
    mutable std::shared_mutex mutex_;

    Node* ensure_node(Node* parent, const std::string& segment);
    void collect_subscribers(Node* node, std::vector<SubscriberEntry>& result);
};
