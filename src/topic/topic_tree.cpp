#include "topic_tree.h"
#include "session.h"
#include "config.h"
#include <algorithm>
#include <stack>

void TopicTree::subscribe(const std::string& filter, SubscriberEntry entry) {
    std::unique_lock lock(mutex_);
    auto segments = topic::split(filter);
    Node* node = &root_;

    for (const auto& seg : segments) {
        if (seg == "#") {
            if (!node->multi_wild) {
                node->multi_wild = std::make_unique<Node>();
            }
            node = node->multi_wild.get();
        } else if (seg == "+") {
            if (!node->single_wild) {
                node->single_wild = std::make_unique<Node>();
            }
            node = node->single_wild.get();
        } else {
            node = ensure_node(node, seg);
        }
    }

    auto s1 = entry.session.lock();
    auto it = std::find_if(node->subscribers.begin(), node->subscribers.end(),
        [&](const SubscriberEntry& e) {
            auto s2 = e.session.lock();
            return s1 && s2 && s1 == s2;
        });

    if (it != node->subscribers.end()) {
        it->qos = entry.qos;
    } else {
        node->subscribers.push_back(std::move(entry));
    }
}

void TopicTree::unsubscribe(const std::string& filter, const std::shared_ptr<Session>& session) {
    std::unique_lock lock(mutex_);
    auto segments = topic::split(filter);
    Node* node = &root_;

    for (const auto& seg : segments) {
        if (seg == "#") {
            if (!node->multi_wild) return;
            node = node->multi_wild.get();
        } else if (seg == "+") {
            if (!node->single_wild) return;
            node = node->single_wild.get();
        } else {
            auto it = node->children.find(seg);
            if (it == node->children.end()) return;
            node = it->second.get();
        }
    }

    node->subscribers.erase(
        std::remove_if(node->subscribers.begin(), node->subscribers.end(),
            [&](const SubscriberEntry& e) {
                return e.session.lock() == session;
            }),
        node->subscribers.end());
}

std::vector<SubscriberEntry> TopicTree::lookup(const std::string& topic) {
    std::shared_lock lock(mutex_);
    auto segments = topic::split(topic);
    std::vector<SubscriberEntry> result;

    struct Frame {
        Node* node;
        size_t index;
    };
    std::stack<Frame> stack;
    stack.push({&root_, 0});

    while (!stack.empty()) {
        auto [node, index] = stack.top();
        stack.pop();
        if (!node) continue;

        if (index >= segments.size()) {
            collect_subscribers(node, result);
            continue;
        }

        const auto& seg = segments[index];

        if (node->multi_wild) {
            collect_subscribers(node->multi_wild.get(), result);
        }

        auto it = node->children.find(seg);
        if (it != node->children.end()) {
            stack.push({it->second.get(), index + 1});
        }

        if (node->single_wild) {
            stack.push({node->single_wild.get(), index + 1});
        }
    }

    return result;
}

bool TopicTree::set_retained(const std::string& topic, const RetainedMessage& msg) {
    std::unique_lock lock(mutex_);
    auto segments = topic::split(topic);
    Node* node = &root_;

    for (const auto& seg : segments) {
        node = ensure_node(node, seg);
    }

    bool was_empty = !node->retained || node->retained->payload.empty();
    bool new_empty = msg.payload.empty();

    if (new_empty && !was_empty) {
        --retained_count_;
    } else if (!new_empty && was_empty) {
        auto max_retained = Config::instance().max_retained_messages();
        if (retained_count_.load() >= max_retained) {
            return false;
        }
        ++retained_count_;
    }

    node->retained = std::make_shared<RetainedMessage>(msg);
    return true;
}

std::shared_ptr<RetainedMessage> TopicTree::get_retained(const std::string& topic) {
    std::shared_lock lock(mutex_);
    auto segments = topic::split(topic);
    Node* node = &root_;

    for (const auto& seg : segments) {
        auto it = node->children.find(seg);
        if (it == node->children.end()) return nullptr;
        node = it->second.get();
    }

    return node->retained;
}

std::vector<std::pair<std::string, RetainedMessage>> TopicTree::get_all_retained() {
    std::shared_lock lock(mutex_);
    std::vector<std::pair<std::string, RetainedMessage>> result;
    get_all_retained(&root_, "", result);
    return result;
}

void TopicTree::get_all_retained(Node* node, std::string path,
                                  std::vector<std::pair<std::string, RetainedMessage>>& result) {
    if (node->retained) {
        result.emplace_back(path, *node->retained);
    }
    for (const auto& [seg, child] : node->children) {
        get_all_retained(child.get(), path.empty() ? seg : path + "/" + seg, result);
    }
    if (node->single_wild) {
        get_all_retained(node->single_wild.get(), path.empty() ? "+" : path + "/+", result);
    }
}

TopicTree::Node* TopicTree::ensure_node(Node* parent, const std::string& segment) {
    auto it = parent->children.find(segment);
    if (it != parent->children.end()) {
        return it->second.get();
    }
    auto node = std::make_unique<Node>();
    Node* ptr = node.get();
    parent->children[segment] = std::move(node);
    return ptr;
}

void TopicTree::collect_subscribers(Node* node, std::vector<SubscriberEntry>& result) {
    if (!node) return;

    for (const auto& entry : node->subscribers) {
        result.push_back(entry);
    }

    if (node->multi_wild) {
        for (const auto& entry : node->multi_wild->subscribers) {
            result.push_back(entry);
        }
    }
}
