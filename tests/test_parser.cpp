#include "packet/parser.h"
#include "packet/packets.h"
#include "packet/builder.h"
#include "packet/types.h"
#include <gtest/gtest.h>

static Buffer build_connect_packet(const std::string& client_id, bool clean_session) {
    Buffer body;
    body.write_string("MQTT");       // protocol name
    body.write_byte(4);              // protocol level
    uint8_t flags = 0x02;            // clean session
    if (clean_session) flags |= 0x02;
    body.write_byte(flags);
    body.write_uint16(60);           // keepalive
    body.write_string(client_id);    // client ID

    Buffer packet;
    packet.write_byte((static_cast<uint8_t>(PacketType::CONNECT) << 4));
    packet.write_remaining_length(static_cast<uint32_t>(body.size()));
    packet.write(body.data(), body.size());
    return packet;
}

TEST(ParserTest, ParseConnect) {
    auto raw = build_connect_packet("test-client", true);

    PacketParser parser;
    parser.feed(raw.data(), raw.size());

    Buffer out;
    ASSERT_TRUE(parser.try_extract(out));

    // Parse the fixed header
    uint8_t byte1 = out.read_byte();
    ASSERT_EQ((byte1 >> 4) & 0x0F, static_cast<uint8_t>(PacketType::CONNECT));
    uint32_t remaining = out.read_remaining_length();
    ASSERT_GT(remaining, 0);

    auto conn = ConnectPacket::parse(out, remaining);
    ASSERT_EQ(conn.protocol_name, "MQTT");
    ASSERT_EQ(conn.protocol_level, 4);
    ASSERT_TRUE(conn.clean_session);
    ASSERT_EQ(conn.keepalive, 60);
    ASSERT_EQ(conn.client_id, "test-client");
}

TEST(ParserTest, ParseConnectWithAuth) {
    Buffer body;
    body.write_string("MQTT");
    body.write_byte(4);
    uint8_t flags = 0xC6;  // clean + username + password + will
    body.write_byte(flags);
    body.write_uint16(30);
    body.write_string("client1");
    body.write_string("will/topic");
    body.write_string("will message");
    body.write_string("user");
    body.write_string("pass");

    Buffer packet;
    packet.write_byte((static_cast<uint8_t>(PacketType::CONNECT) << 4));
    packet.write_remaining_length(static_cast<uint32_t>(body.size()));
    packet.write(body.data(), body.size());

    PacketParser parser;
    parser.feed(packet.data(), packet.size());

    Buffer out;
    ASSERT_TRUE(parser.try_extract(out));

    out.read_byte();
    uint32_t remaining = out.read_remaining_length();
    auto conn = ConnectPacket::parse(out, remaining);

    ASSERT_TRUE(conn.clean_session);
    ASSERT_TRUE(conn.will_flag);
    ASSERT_EQ(conn.will_topic, "will/topic");
    ASSERT_EQ(conn.will_message, "will message");
    ASSERT_EQ(conn.username, "user");
    ASSERT_EQ(conn.password, "pass");
}

TEST(ParserTest, ParsePublishQoS0) {
    std::string topic = "sensor/temp";
    std::string payload = "23.5";

    Buffer body;
    body.write_string(topic);
    body.write(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

    Buffer packet;
    packet.write_byte((static_cast<uint8_t>(PacketType::PUBLISH) << 4));
    packet.write_remaining_length(static_cast<uint32_t>(body.size()));
    packet.write(body.data(), body.size());

    PacketParser parser;
    parser.feed(packet.data(), packet.size());

    Buffer out;
    ASSERT_TRUE(parser.try_extract(out));

    FixedHeader hdr;
    uint8_t byte1 = out.read_byte();
    hdr.type = static_cast<PacketType>((byte1 >> 4) & 0x0F);
    hdr.qos = (byte1 >> 1) & 0x03;
    hdr.retain = (byte1 & 0x01) != 0;
    hdr.remaining_length = out.read_remaining_length();

    auto pub = PublishPacket::parse(out, hdr);
    ASSERT_EQ(pub.topic, "sensor/temp");
    ASSERT_EQ(pub.qos, 0);
    ASSERT_FALSE(pub.retain);
    ASSERT_EQ(pub.payload.readable(), payload.size());
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(pub.payload.data()), pub.payload.size()), "23.5");
}

TEST(ParserTest, ParseSubscribe) {
    Buffer body;
    body.write_uint16(1);  // packet ID
    body.write_string("sensor/+");
    body.write_byte(1);     // QoS 1
    body.write_string("home/#");
    body.write_byte(0);     // QoS 0

    Buffer packet;
    packet.write_byte((static_cast<uint8_t>(PacketType::SUBSCRIBE) << 4) | 0x02);
    packet.write_remaining_length(static_cast<uint32_t>(body.size()));
    packet.write(body.data(), body.size());

    PacketParser parser;
    parser.feed(packet.data(), packet.size());

    Buffer out;
    ASSERT_TRUE(parser.try_extract(out));

    out.read_byte();
    uint32_t remaining = out.read_remaining_length();
    auto sub = SubscribePacket::parse(out, remaining);

    ASSERT_EQ(sub.packet_id, 1);
    ASSERT_EQ(sub.filters.size(), 2);
    ASSERT_EQ(sub.filters[0].topic_filter, "sensor/+");
    ASSERT_EQ(sub.filters[0].requested_qos, 1);
    ASSERT_EQ(sub.filters[1].topic_filter, "home/#");
    ASSERT_EQ(sub.filters[1].requested_qos, 0);
}

TEST(ParserTest, MultiplePacketsInOneRead) {
    auto pkt1 = build_connect_packet("client1", true);
    auto pkt2 = build_connect_packet("client2", true);

    // Concatenate packets
    Buffer combined;
    combined.write(pkt1.data(), pkt1.size());
    combined.write(pkt2.data(), pkt2.size());

    PacketParser parser;
    parser.feed(combined.data(), combined.size());

    Buffer out1;
    ASSERT_TRUE(parser.try_extract(out1));

    Buffer out2;
    ASSERT_TRUE(parser.try_extract(out2));

    ASSERT_FALSE(parser.try_extract(out2));  // no more packets
}

TEST(ParserTest, Pingreq) {
    Buffer packet;
    packet.write_byte((static_cast<uint8_t>(PacketType::PINGREQ) << 4));
    packet.write_remaining_length(0);

    PacketParser parser;
    parser.feed(packet.data(), packet.size());

    Buffer out;
    ASSERT_TRUE(parser.try_extract(out));

    uint8_t byte1 = out.read_byte();
    ASSERT_EQ((byte1 >> 4) & 0x0F, static_cast<uint8_t>(PacketType::PINGREQ));
}

TEST(ParserTest, BuildAndParsePingresp) {
    auto resp = PacketBuilder::build_pingresp();

    PacketParser parser;
    parser.feed(resp.data(), resp.size());

    Buffer out;
    ASSERT_TRUE(parser.try_extract(out));

    uint8_t byte1 = out.read_byte();
    ASSERT_EQ((byte1 >> 4) & 0x0F, static_cast<uint8_t>(PacketType::PINGRESP));
}

TEST(ParserTest, BuildAndParseConnack) {
    auto ack = PacketBuilder::build_connack(true, ConnackCode::ACCEPTED);

    PacketParser parser;
    parser.feed(ack.data(), ack.size());

    Buffer out;
    ASSERT_TRUE(parser.try_extract(out));

    out.read_byte();  // fixed header
    out.read_remaining_length();
    uint8_t session_present = out.read_byte();
    uint8_t return_code = out.read_byte();

    ASSERT_EQ(session_present, 1);
    ASSERT_EQ(return_code, static_cast<uint8_t>(ConnackCode::ACCEPTED));
}

TEST(ParserTest, BuildAndParseSuback) {
    std::vector<SubackCode> codes = {SubackCode::MAX_QOS0, SubackCode::MAX_QOS1, SubackCode::FAILURE};
    auto suback = PacketBuilder::build_suback(42, codes);

    PacketParser parser;
    parser.feed(suback.data(), suback.size());

    Buffer out;
    ASSERT_TRUE(parser.try_extract(out));

    out.read_byte();
    out.read_remaining_length();
    uint16_t pid = out.read_uint16();
    ASSERT_EQ(pid, 42);

    for (size_t i = 0; i < codes.size(); ++i) {
        ASSERT_EQ(out.read_byte(), static_cast<uint8_t>(codes[i]));
    }
}
