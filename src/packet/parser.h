#pragma once
#include "types.h"
#include "utils/buffer.h"
#include <string>

class PacketParser {
public:
    PacketParser();

    void feed(const uint8_t* data, size_t len);
    bool try_extract(Buffer& out);

    bool has_error() const;
    std::string error_message() const;
    void reset();

private:
    Buffer buffer_;

    FixedHeader decode_header() const;
    uint32_t decode_remaining_length(size_t& pos) const;

    mutable std::string error_msg_;
};
