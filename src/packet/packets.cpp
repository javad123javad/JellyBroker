#include "packets.h"
#include "utils/buffer.h"
#include <stdexcept>

ConnectPacket ConnectPacket::parse(Buffer& buf, uint32_t remaining_length) {
    ConnectPacket pkt;
    auto start_pos = buf.read_position();

    pkt.protocol_name = buf.read_string();
    pkt.protocol_level = buf.read_byte();

    uint8_t flags = buf.read_byte();
    pkt.clean_session = (flags & 0x02) != 0;
    pkt.will_flag = (flags & 0x04) != 0;
    pkt.will_qos = (flags >> 3) & 0x03;
    pkt.will_retain = (flags & 0x20) != 0;
    bool has_username = (flags & 0x80) != 0;
    bool has_password = (flags & 0x40) != 0;

    pkt.keepalive = buf.read_uint16();
    pkt.client_id = buf.read_string();

    if (pkt.will_flag) {
        pkt.will_topic = buf.read_string();
        pkt.will_message = buf.read_string();
    }

    if (has_username) {
        pkt.username = buf.read_string();
    }
    if (has_password) {
        pkt.password = buf.read_string();
    }

    return pkt;
}

PublishPacket PublishPacket::parse(Buffer& buf, const FixedHeader& hdr) {
    PublishPacket pkt;
    pkt.dup = hdr.dup;
    pkt.qos = hdr.qos;
    pkt.retain = hdr.retain;

    pkt.topic = buf.read_string();

    size_t consumed = 2 + pkt.topic.size();

    if (pkt.qos > 0) {
        pkt.packet_id = buf.read_uint16();
        consumed += 2;
    }

    size_t payload_len = hdr.remaining_length;
    if (payload_len < consumed) {
        throw std::runtime_error("Invalid publish packet: remaining_length too small");
    }
    payload_len -= consumed;

    if (payload_len > 0) {
        pkt.payload = Buffer(buf.read_head(), payload_len);
        buf.skip(payload_len);
    }

    return pkt;
}

SubscribePacket SubscribePacket::parse(Buffer& buf, uint32_t remaining_length) {
    SubscribePacket pkt;
    pkt.packet_id = buf.read_uint16();

    while (buf.readable() > 0) {
        Filter f;
        f.topic_filter = buf.read_string();
        f.requested_qos = buf.read_byte() & 0x03;
        pkt.filters.push_back(std::move(f));
    }

    return pkt;
}

UnsubscribePacket UnsubscribePacket::parse(Buffer& buf, uint32_t remaining_length) {
    UnsubscribePacket pkt;
    pkt.packet_id = buf.read_uint16();

    while (buf.readable() > 0) {
        pkt.topic_filters.push_back(buf.read_string());
    }

    return pkt;
}

PubackPacket PubackPacket::parse(Buffer& buf) {
    if (buf.readable() < 2) {
        throw std::runtime_error("Invalid puback packet");
    }
    PubackPacket pkt;
    pkt.packet_id = buf.read_uint16();
    return pkt;
}

PubrecPacket PubrecPacket::parse(Buffer& buf) {
    if (buf.readable() < 2) {
        throw std::runtime_error("Invalid pubrec packet");
    }
    PubrecPacket pkt;
    pkt.packet_id = buf.read_uint16();
    return pkt;
}

PubrelPacket PubrelPacket::parse(Buffer& buf) {
    if (buf.readable() < 2) {
        throw std::runtime_error("Invalid pubrel packet");
    }
    PubrelPacket pkt;
    pkt.packet_id = buf.read_uint16();
    return pkt;
}

PubcompPacket PubcompPacket::parse(Buffer& buf) {
    if (buf.readable() < 2) {
        throw std::runtime_error("Invalid pubcomp packet");
    }
    PubcompPacket pkt;
    pkt.packet_id = buf.read_uint16();
    return pkt;
}
