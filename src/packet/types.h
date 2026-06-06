#pragma once
#include <cstdint>

enum class PacketType : uint8_t {
    CONNECT = 1,
    CONNACK = 2,
    PUBLISH = 3,
    PUBACK = 4,
    PUBREC = 5,
    PUBREL = 6,
    PUBCOMP = 7,
    SUBSCRIBE = 8,
    SUBACK = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK = 11,
    PINGREQ = 12,
    PINGRESP = 13,
    DISCONNECT = 14
};

struct FixedHeader {
    PacketType type;
    bool dup = false;
    uint8_t qos = 0;
    bool retain = false;
    uint32_t remaining_length = 0;
};

enum class ConnackCode : uint8_t {
    ACCEPTED = 0,
    REFUSED_PROTOCOL = 1,
    REFUSED_ID_REJECTED = 2,
    REFUSED_SERVER_UNAVAIL = 3,
    REFUSED_BAD_USERNAME = 4,
    REFUSED_NOT_AUTHORIZED = 5
};

enum class SubackCode : uint8_t {
    MAX_QOS0 = 0,
    MAX_QOS1 = 1,
    MAX_QOS2 = 2,
    FAILURE = 0x80
};

inline constexpr uint8_t MQTT_PROTOCOL_LEVEL = 4;
inline constexpr const char* MQTT_PROTOCOL_NAME = "MQTT";
