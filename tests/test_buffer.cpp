#include "utils/buffer.h"
#include <gtest/gtest.h>

TEST(BufferTest, WriteReadByte) {
    Buffer buf;
    buf.write_byte(0xAB);
    ASSERT_EQ(buf.readable(), 1);
    ASSERT_EQ(buf.read_byte(), 0xAB);
    ASSERT_TRUE(buf.empty());
}

TEST(BufferTest, WriteReadUint16) {
    Buffer buf;
    buf.write_uint16(0x1234);
    ASSERT_EQ(buf.readable(), 2);
    ASSERT_EQ(buf.read_uint16(), 0x1234);
}

TEST(BufferTest, WriteReadString) {
    Buffer buf;
    std::string s = "hello";
    buf.write_string(s);
    ASSERT_EQ(buf.read_string(), s);
}

TEST(BufferTest, RemainingLength) {
    struct TestCase {
        uint32_t value;
        std::vector<uint8_t> encoded;
    };

    std::vector<TestCase> cases = {
        {0, {0x00}},
        {127, {0x7F}},
        {128, {0x80, 0x01}},
        {16383, {0xFF, 0x7F}},
        {2097151, {0xFF, 0xFF, 0x7F}},
        {268435455, {0xFF, 0xFF, 0xFF, 0x7F}},
    };

    for (const auto& tc : cases) {
        Buffer buf;
        buf.write_remaining_length(tc.value);
        ASSERT_EQ(buf.readable(), tc.encoded.size());
        for (size_t i = 0; i < tc.encoded.size(); ++i) {
            ASSERT_EQ(buf.data()[i], tc.encoded[i]);
        }
        ASSERT_EQ(buf.read_remaining_length(), tc.value);
    }
}

TEST(BufferTest, MultipleOperations) {
    Buffer buf;
    buf.write_byte(0x01);
    buf.write_uint16(0x0203);
    buf.write_string("test");

    ASSERT_EQ(buf.read_byte(), 0x01);
    ASSERT_EQ(buf.read_uint16(), 0x0203);
    ASSERT_EQ(buf.read_string(), "test");
}

TEST(BufferTest, Skip) {
    Buffer buf;
    buf.write_byte(0x01);
    buf.write_byte(0x02);
    buf.write_byte(0x03);

    buf.skip(2);
    ASSERT_EQ(buf.read_byte(), 0x03);
}

TEST(BufferTest, Clear) {
    Buffer buf;
    buf.write_byte(0x01);
    ASSERT_FALSE(buf.empty());
    buf.clear();
    ASSERT_TRUE(buf.empty());
    ASSERT_EQ(buf.readable(), 0);
}

TEST(BufferTest, Underflow) {
    Buffer buf;
    ASSERT_THROW(buf.read_byte(), std::out_of_range);
    ASSERT_THROW(buf.read_uint16(), std::out_of_range);
    ASSERT_THROW(buf.read_string(), std::out_of_range);
}

TEST(BufferTest, FromRawData) {
    uint8_t data[] = {0x00, 0x05, 'h', 'e', 'l', 'l', 'o'};
    Buffer buf(data, sizeof(data));
    ASSERT_EQ(buf.read_string(), "hello");
}
