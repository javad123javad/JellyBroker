#pragma once
#include "types.h"
#include "utils/buffer.h"
#include <string>
#include <vector>

class PacketBuilder {
public:
    static Buffer build_connack(bool session_present, ConnackCode code);
    static Buffer build_suback(uint16_t packet_id, const std::vector<SubackCode>& codes);
    static Buffer build_unsuback(uint16_t packet_id);
    static Buffer build_publish(const std::string& topic, const uint8_t* payload,
                                size_t payload_len, uint8_t qos, bool retain,
                                bool dup, uint16_t packet_id);
    static Buffer build_puback(uint16_t packet_id);
    static Buffer build_pubrec(uint16_t packet_id);
    static Buffer build_pubrel(uint16_t packet_id);
    static Buffer build_pubcomp(uint16_t packet_id);
    static Buffer build_pingresp();

private:
    static Buffer wrap(PacketType type, uint8_t flags, const Buffer& body);
};
