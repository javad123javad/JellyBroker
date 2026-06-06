#pragma once
#include "topic/topic_tree.h"
#include "packet/packets.h"

class Qos0Handler {
public:
    static void deliver(TopicTree& tree, const PublishPacket& pub);
};
