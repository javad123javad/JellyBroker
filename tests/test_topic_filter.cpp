#include "topic/filter.h"
#include <gtest/gtest.h>

TEST(TopicFilterTest, ExactMatch) {
    ASSERT_TRUE(topic::matches("sensor/temp", "sensor/temp"));
    ASSERT_TRUE(topic::matches("a/b/c", "a/b/c"));
    ASSERT_FALSE(topic::matches("sensor/temp", "sensor/humidity"));
    ASSERT_FALSE(topic::matches("a/b", "a/b/c"));
}

TEST(TopicFilterTest, SingleLevelWildcard) {
    ASSERT_TRUE(topic::matches("sensor/temp", "sensor/+"));
    ASSERT_TRUE(topic::matches("sensor/humidity", "sensor/+"));
    ASSERT_TRUE(topic::matches("a/b", "+/b"));
    ASSERT_TRUE(topic::matches("a/b/c", "+/+/c"));
    ASSERT_FALSE(topic::matches("sensor/temp/extra", "sensor/+"));
    ASSERT_FALSE(topic::matches("a/b/c", "+/c"));
}

TEST(TopicFilterTest, MultiLevelWildcard) {
    ASSERT_TRUE(topic::matches("sensor/temp", "sensor/#"));
    ASSERT_TRUE(topic::matches("sensor/temp/inside", "sensor/#"));
    ASSERT_TRUE(topic::matches("a/b/c/d", "a/#"));
    ASSERT_TRUE(topic::matches("anything", "#"));
    ASSERT_TRUE(topic::matches("a/b/c", "#"));
    ASSERT_FALSE(topic::matches("sensor", "sensor/#"));
}

TEST(TopicFilterTest, MixedWildcards) {
    ASSERT_TRUE(topic::matches("home/room1/temp", "home/+/temp"));
    ASSERT_TRUE(topic::matches("home/room1/temp/inside", "home/+/temp/#"));
    ASSERT_FALSE(topic::matches("home/room1/humidity", "home/+/temp"));
}

TEST(TopicFilterTest, IsValidTopic) {
    ASSERT_TRUE(topic::is_valid_topic("sensor/temp"));
    ASSERT_TRUE(topic::is_valid_topic("a"));
    ASSERT_TRUE(topic::is_valid_topic("a/b/c"));
    ASSERT_FALSE(topic::is_valid_topic(""));
    ASSERT_FALSE(topic::is_valid_topic("sensor/+"));
    ASSERT_FALSE(topic::is_valid_topic("sensor/#"));
}

TEST(TopicFilterTest, IsValidFilter) {
    ASSERT_TRUE(topic::is_valid_filter("sensor/temp"));
    ASSERT_TRUE(topic::is_valid_filter("sensor/+"));
    ASSERT_TRUE(topic::is_valid_filter("sensor/#"));
    ASSERT_TRUE(topic::is_valid_filter("#"));
    ASSERT_TRUE(topic::is_valid_filter("+"));
    ASSERT_FALSE(topic::is_valid_filter(""));
    ASSERT_FALSE(topic::is_valid_filter("sensor/#/extra"));
}

TEST(TopicFilterTest, Split) {
    auto segments = topic::split("a/b/c");
    ASSERT_EQ(segments.size(), 3);
    ASSERT_EQ(segments[0], "a");
    ASSERT_EQ(segments[1], "b");
    ASSERT_EQ(segments[2], "c");
}

TEST(TopicFilterTest, EdgeCases) {
    ASSERT_TRUE(topic::matches("+", "+"));
    ASSERT_TRUE(topic::matches("#", "#"));
    ASSERT_TRUE(topic::matches("a", "+"));
}
