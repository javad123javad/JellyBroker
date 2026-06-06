#pragma once
#include <cstdint>
#include <string>
#include <vector>

class Buffer {
public:
    Buffer() = default;
    explicit Buffer(size_t reserve_size);
    Buffer(const uint8_t* data, size_t len);

    void write(const uint8_t* src, size_t len);
    void read(uint8_t* dst, size_t len);

    void write_byte(uint8_t b);
    uint8_t read_byte();

    void write_uint16(uint16_t v);
    uint16_t read_uint16();

    void write_string(const std::string& s);
    std::string read_string();

    void write_remaining_length(uint32_t v);
    uint32_t read_remaining_length();

    void skip(size_t n);
    void clear();

    size_t readable() const;
    size_t writable() const;
    bool empty() const;

    const uint8_t* data() const;
    uint8_t* data();

    size_t size() const;
    void reserve(size_t n);

    const uint8_t* read_head() const;
    size_t read_position() const;
    void set_read_position(size_t pos);

    uint8_t* write_head();
    size_t write_capacity() const;
    void ensure_writable(size_t n);
    void advance_write(size_t n);

private:
    std::vector<uint8_t> data_;
    size_t read_pos_ = 0;
    size_t write_pos_ = 0;
};
