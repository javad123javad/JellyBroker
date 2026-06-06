#pragma once
#include "types.h"
#include "utils/buffer.h"
#include <string>
#include <vector>

struct ConnectPacket {
    std::string protocol_name;
    uint8_t protocol_level = 0;
    bool clean_session = false;
    bool will_flag = false;
    uint8_t will_qos = 0;
    bool will_retain = false;
    std::string will_topic;
    std::string will_message;
    std::string username;
    std::string password;
    uint16_t keepalive = 0;
    std::string client_id;

    static ConnectPacket parse(Buffer& buf, uint32_t remaining_length);
};

struct PublishPacket {
    std::string topic;
    Buffer payload;
    uint8_t qos = 0;
    bool retain = false;
    bool dup = false;
    uint16_t packet_id = 0;

    static PublishPacket parse(Buffer& buf, const FixedHeader& hdr);
};

struct SubscribePacket {
    struct Filter {
        std::string topic_filter;
        uint8_t requested_qos = 0;
    };
    uint16_t packet_id = 0;
    std::vector<Filter> filters;

    static SubscribePacket parse(Buffer& buf, uint32_t remaining_length);
};

struct UnsubscribePacket {
    uint16_t packet_id = 0;
    std::vector<std::string> topic_filters;

    static UnsubscribePacket parse(Buffer& buf, uint32_t remaining_length);
};

struct PubackPacket {
    uint16_t packet_id = 0;
    static PubackPacket parse(Buffer& buf);
};

struct PubrecPacket {
    uint16_t packet_id = 0;
    static PubrecPacket parse(Buffer& buf);
};

struct PubrelPacket {
    uint16_t packet_id = 0;
    static PubrelPacket parse(Buffer& buf);
};

struct PubcompPacket {
    uint16_t packet_id = 0;
    static PubcompPacket parse(Buffer& buf);
};

struct SubackPacket {
    uint16_t packet_id = 0;
    std::vector<SubackCode> return_codes;
};

struct UnsubackPacket {
    uint16_t packet_id = 0;
};
