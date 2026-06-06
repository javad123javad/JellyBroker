#include "parser.h"
#include "config.h"

PacketParser::PacketParser() {
    reset();
}

void PacketParser::reset() {
    buffer_.clear();
    error_msg_.clear();
}

bool PacketParser::has_error() const {
    return !error_msg_.empty();
}

std::string PacketParser::error_message() const {
    return error_msg_;
}

void PacketParser::feed(const uint8_t* data, size_t len) {
    if (data && len > 0) {
        auto max_buf = Config::instance().max_parser_buffer_size();
        if (buffer_.size() + len > static_cast<size_t>(max_buf)) {
            error_msg_ = "Parser buffer overflow";
            return;
        }
        buffer_.write(data, len);
    }
}

FixedHeader PacketParser::decode_header() const {
    FixedHeader hdr{};
    if (buffer_.readable() < 1) return hdr;

    uint8_t byte1 = buffer_.data()[0];
    uint8_t type_val = (byte1 >> 4) & 0x0F;
    hdr.type = static_cast<PacketType>(type_val);
    hdr.dup = (byte1 & 0x08) != 0;
    hdr.qos = (byte1 >> 1) & 0x03;
    hdr.retain = (byte1 & 0x01) != 0;
    return hdr;
}

uint32_t PacketParser::decode_remaining_length(size_t& pos) const {
    uint32_t value = 0;
    uint32_t multiplier = 1;
    int count = 0;
    pos = 1;

    while (pos < buffer_.size()) {
        uint8_t enc = buffer_.data()[pos];
        value += (enc & 0x7F) * multiplier;
        ++pos;
        ++count;

        if (!(enc & 0x80)) {
            return value;
        }

        // Check overflow BEFORE multiplying
        if (count >= 4 || multiplier > 2097152) {
            error_msg_ = "Malformed remaining length";
            return 0;
        }
        multiplier *= 128;
    }

    pos = 0; // mark as incomplete
    return 0;  // incomplete
}

bool PacketParser::try_extract(Buffer& out) {
    if (has_error()) return false;
    if (buffer_.readable() < 1) return false;

    // Validate packet type
    auto hdr = decode_header();
    uint8_t type_val = static_cast<uint8_t>(hdr.type);
    if (type_val < 1 || type_val > 14) {
        error_msg_ = "Invalid packet type: " + std::to_string(type_val);
        return false;
    }

    // Decode remaining length
    size_t header_end = 0;
    uint32_t remaining = decode_remaining_length(header_end);
    if (has_error()) return false;
    if (remaining == 0 && header_end == 0) return false;  // incomplete

    // Check max packet size
    auto max_size = Config::instance().max_packet_size();
    if (remaining > static_cast<uint32_t>(max_size)) {
        error_msg_ = "Packet too large: " + std::to_string(remaining);
        return false;
    }

    size_t total_size = header_end + remaining;
    if (buffer_.size() < total_size) return false;  // not enough data yet

    // Copy complete packet
    out = Buffer(buffer_.data(), total_size);

    // Remove consumed bytes from internal buffer
    Buffer remaining_data(buffer_.data() + total_size, buffer_.size() - total_size);
    buffer_ = std::move(remaining_data);

    return true;
}
