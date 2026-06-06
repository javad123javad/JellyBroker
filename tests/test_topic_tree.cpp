#include "topic/topic_tree.h"
#include "packet/types.h"
#include "utils/buffer.h"
#include <gtest/gtest.h>
#include <memory>

class TopicTreeTest : public ::testing::Test {
protected:
    struct TestSubscriber : std::enable_shared_from_this<TestSubscriber> {
        std::string id;
        explicit TestSubscriber(std::string n) : id(std::move(n)) {}
    };

    TopicTree tree;

    SubscriberEntry make_entry(std::shared_ptr<TestSubscriber> sub, uint8_t qos) {
        // We can't create a Session without real sockets, so we create a
        // TestSubscriber and store its pointer. The SubscriberEntry holds
        // weak_ptr<Session> but since we can't create Session, we skip
        // the session-based tests and test the tree structure directly.
        SubscriberEntry entry;
        // Using nullptr session - these tests verify tree topology only
        return entry;
    }
};

TEST_F(TopicTreeTest, SubscribeAndLookup) {
    SubscriberEntry entry;
    auto sub = std::make_shared<TestSubscriber>("s1");
    entry.session = std::weak_ptr<Session>();  // null session
    entry.qos = 1;

    tree.subscribe("sensor/temp", entry);

    auto results = tree.lookup("sensor/temp");
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].qos, 1);
    ASSERT_TRUE(results[0].session.expired());  // null session
}

TEST_F(TopicTreeTest, WildcardLookup) {
    SubscriberEntry entry;
    entry.qos = 0;

    tree.subscribe("sensor/+", entry);

    auto results = tree.lookup("sensor/temp");
    ASSERT_EQ(results.size(), 1);

    results = tree.lookup("sensor/humidity");
    ASSERT_EQ(results.size(), 1);

    results = tree.lookup("other/value");
    ASSERT_EQ(results.size(), 0);
}

TEST_F(TopicTreeTest, MultiLevelWildcard) {
    SubscriberEntry entry;
    entry.qos = 1;

    tree.subscribe("home/#", entry);

    auto results = tree.lookup("home");
    ASSERT_EQ(results.size(), 1);

    results = tree.lookup("home/room1");
    ASSERT_EQ(results.size(), 1);

    results = tree.lookup("home/room1/temp");
    ASSERT_EQ(results.size(), 1);

    results = tree.lookup("away/room1");
    ASSERT_EQ(results.size(), 0);
}

TEST_F(TopicTreeTest, MultipleSubscribers) {
    SubscriberEntry entry1, entry2;
    entry1.session = std::shared_ptr<Session>(reinterpret_cast<Session*>(1), [](Session*){});
    entry1.qos = 0;
    entry2.session = std::shared_ptr<Session>(reinterpret_cast<Session*>(2), [](Session*){});
    entry2.qos = 2;

    tree.subscribe("test/topic", entry1);
    tree.subscribe("test/topic", entry2);

    auto results = tree.lookup("test/topic");
    ASSERT_EQ(results.size(), 2);
}

TEST_F(TopicTreeTest, OverwriteQoS) {
    SubscriberEntry entry;
    auto identity = std::shared_ptr<Session>(reinterpret_cast<Session*>(42), [](Session*){});
    entry.session = identity;

    entry.qos = 1;
    tree.subscribe("topic", entry);

    entry.qos = 2;
    tree.subscribe("topic", entry);

    auto results = tree.lookup("topic");
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].qos, 2);
}

TEST_F(TopicTreeTest, RetainedMessages) {
    RetainedMessage msg;
    std::string payload = "hello";
    msg.payload = Buffer(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    msg.qos = 1;

    tree.set_retained("sensor/value", msg);

    auto retrieved = tree.get_retained("sensor/value");
    ASSERT_NE(retrieved, nullptr);
    ASSERT_EQ(retrieved->qos, 1);

    auto all = tree.get_all_retained();
    ASSERT_EQ(all.size(), 1);
    ASSERT_EQ(all[0].first, "sensor/value");
}
