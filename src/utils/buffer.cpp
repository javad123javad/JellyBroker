#include "buffer.h"
#include <cstring>
#include <stdexcept>

Buffer::Buffer(size_t reserve_size) {
    data_.reserve(reserve_size);
}

Buffer::Buffer(const uint8_t* data, size_t len)
    : data_(data, data + len), write_pos_(len) {
}

void Buffer::write(const uint8_t* src, size_t len) {
    if (write_pos_ + len > data_.size()) {
        data_.resize(write_pos_ + len);
    }
    std::memcpy(data_.data() + write_pos_, src, len);
    write_pos_ += len;
}

void Buffer::read(uint8_t* dst, size_t len) {
    if (read_pos_ + len > write_pos_) {
        throw std::out_of_range("Buffer underflow");
    }
    std::memcpy(dst, data_.data() + read_pos_, len);
    read_pos_ += len;
}

void Buffer::write_byte(uint8_t b) {
    write(&b, 1);
}

uint8_t Buffer::read_byte() {
    uint8_t b;
    read(&b, 1);
    return b;
}

void Buffer::write_uint16(uint16_t v) {
    uint8_t buf[2];
    buf[0] = static_cast<uint8_t>((v >> 8) & 0xFF);
    buf[1] = static_cast<uint8_t>(v & 0xFF);
    write(buf, 2);
}

uint16_t Buffer::read_uint16() {
    uint8_t buf[2];
    read(buf, 2);
    return (static_cast<uint16_t>(buf[0]) << 8) | buf[1];
}

void Buffer::write_string(const std::string& s) {
    write_uint16(static_cast<uint16_t>(s.size()));
    write(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

std::string Buffer::read_string() {
    uint16_t len = read_uint16();
    std::string s(len, '\0');
    read(reinterpret_cast<uint8_t*>(s.data()), len);
    return s;
}

void Buffer::write_remaining_length(uint32_t v) {
    do {
        uint8_t enc = v % 128;
        v /= 128;
        if (v > 0) enc |= 0x80;
        write_byte(enc);
    } while (v > 0);
}

uint32_t Buffer::read_remaining_length() {
    uint32_t value = 0;
    int multiplier = 1;
    int count = 0;
    uint8_t enc;
    do {
        if (read_pos_ >= write_pos_) {
            throw std::out_of_range("Incomplete remaining length");
        }
        enc = read_byte();
        value += (enc & 0x7F) * multiplier;
        multiplier *= 128;
        if (++count > 4) {
            throw std::runtime_error("Malformed remaining length");
        }
    } while (enc & 0x80);
    return value;
}

void Buffer::skip(size_t n) {
    if (read_pos_ + n > write_pos_) {
        throw std::out_of_range("Buffer skip underflow");
    }
    read_pos_ += n;
}

void Buffer::clear() {
    data_.clear();
    read_pos_ = 0;
    write_pos_ = 0;
}

size_t Buffer::readable() const {
    return write_pos_ - read_pos_;
}

size_t Buffer::writable() const {
    return data_.size() - write_pos_;
}

bool Buffer::empty() const {
    return readable() == 0;
}

const uint8_t* Buffer::data() const {
    return data_.data();
}

uint8_t* Buffer::data() {
    return data_.data();
}

size_t Buffer::size() const {
    return write_pos_;
}

void Buffer::reserve(size_t n) {
    data_.reserve(n);
}

const uint8_t* Buffer::read_head() const {
    return data_.data() + read_pos_;
}

size_t Buffer::read_position() const {
    return read_pos_;
}

void Buffer::set_read_position(size_t pos) {
    if (pos > write_pos_) {
        throw std::out_of_range("Invalid read position");
    }
    read_pos_ = pos;
}

uint8_t* Buffer::write_head() {
    return data_.data() + write_pos_;
}

size_t Buffer::write_capacity() const {
    return data_.size() - write_pos_;
}

void Buffer::ensure_writable(size_t n) {
    if (write_pos_ + n > data_.size()) {
        data_.resize(write_pos_ + n);
    }
}

void Buffer::advance_write(size_t n) {
    if (write_pos_ + n > data_.size()) {
        throw std::out_of_range("Buffer advance overflow");
    }
    write_pos_ += n;
}
