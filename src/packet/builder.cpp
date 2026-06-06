#include "builder.h"
#include <cstring>

Buffer PacketBuilder::build_connack(bool session_present, ConnackCode code) {
    Buffer body;
    body.write_byte(session_present ? 0x01 : 0x00);
    body.write_byte(static_cast<uint8_t>(code));
    return wrap(PacketType::CONNACK, 0, body);
}

Buffer PacketBuilder::build_suback(uint16_t packet_id, const std::vector<SubackCode>& codes) {
    Buffer body;
    body.write_uint16(packet_id);
    for (auto code : codes) {
        body.write_byte(static_cast<uint8_t>(code));
    }
    return wrap(PacketType::SUBACK, 0, body);
}

Buffer PacketBuilder::build_unsuback(uint16_t packet_id) {
    Buffer body;
    body.write_uint16(packet_id);
    return wrap(PacketType::UNSUBACK, 0, body);
}

Buffer PacketBuilder::build_publish(const std::string& topic, const uint8_t* payload,
                                    size_t payload_len, uint8_t qos, bool retain,
                                    bool dup, uint16_t packet_id) {
    Buffer body;
    body.write_string(topic);
    if (qos > 0) {
        body.write_uint16(packet_id);
    }
    if (payload_len > 0) {
        body.write(payload, payload_len);
    }

    uint8_t flags = 0;
    if (dup) flags |= 0x08;
    flags |= (qos & 0x03) << 1;
    if (retain) flags |= 0x01;

    return wrap(PacketType::PUBLISH, flags, body);
}

Buffer PacketBuilder::build_puback(uint16_t packet_id) {
    Buffer body;
    body.write_uint16(packet_id);
    return wrap(PacketType::PUBACK, 0, body);
}

Buffer PacketBuilder::build_pubrec(uint16_t packet_id) {
    Buffer body;
    body.write_uint16(packet_id);
    return wrap(PacketType::PUBREC, 0, body);
}

Buffer PacketBuilder::build_pubrel(uint16_t packet_id) {
    Buffer body;
    body.write_uint16(packet_id);
    return wrap(PacketType::PUBREL, 2, body);
}

Buffer PacketBuilder::build_pubcomp(uint16_t packet_id) {
    Buffer body;
    body.write_uint16(packet_id);
    return wrap(PacketType::PUBCOMP, 0, body);
}

Buffer PacketBuilder::build_pingresp() {
    Buffer body;
    return wrap(PacketType::PINGRESP, 0, body);
}

Buffer PacketBuilder::wrap(PacketType type, uint8_t flags, const Buffer& body) {
    Buffer packet;
    uint8_t byte1 = (static_cast<uint8_t>(type) << 4) | (flags & 0x0F);
    packet.write_byte(byte1);
    packet.write_remaining_length(static_cast<uint32_t>(body.size()));
    if (body.size() > 0) {
        packet.write(body.data(), body.size());
    }
    return packet;
}
