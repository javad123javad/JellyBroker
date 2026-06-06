#pragma once
#include "topic/topic_tree.h"
#include "subscription.h"
#include "auth/authenticator.h"
#include "core/delivery_engine.h"

struct BrokerContext {
    TopicTree* topic_tree = nullptr;
    SubscriptionManager* sub_manager = nullptr;
    Authenticator* auth = nullptr;
    DeliveryEngine* delivery = nullptr;
};
