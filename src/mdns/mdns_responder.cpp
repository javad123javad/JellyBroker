#include "mdns_responder.h"
#include <boost/asio/ip/multicast.hpp>
#include <cstring>
#include <string>

static constexpr uint16_t MDNS_PORT = 5353;
static constexpr std::size_t BUF_SIZE = 1500;

// DNS header flags: QR=1 (response), AA=1 (authoritative)
static constexpr uint16_t DNS_FLAG_RESPONSE = 0x8400;

// Resource record types
static constexpr uint16_t RR_TYPE_A = 1;
static constexpr uint16_t RR_TYPE_PTR = 12;
static constexpr uint16_t RR_TYPE_TXT = 16;
static constexpr uint16_t RR_TYPE_SRV = 33;

// Class IN with cache-flush bit for mDNS unicast responses
static constexpr uint16_t RR_CLASS_IN_FLUSH = 0x8001;

// TTLs (seconds)
static constexpr uint32_t TTL_PTR = 4500;
static constexpr uint32_t TTL_SRV_TXT = 120;
static constexpr uint32_t TTL_A = 120;

MdnsResponder::MdnsResponder(boost::asio::io_context& io, uint16_t port,
                             const std::string& hostname, const std::string& ip)
    : io_(io)
    , socket_(io_)
    , recv_buf_(BUF_SIZE)
    , port_(port)
    , ip_(ip)
{
    node_ = hostname + ".local";
    service_ = "_mqtt._tcp.local";
    instance_ = hostname + "." + service_;

    encode_name(encoded_instance_, instance_);
    encode_name(encoded_service_, service_);
    encode_name(encoded_node_, node_);
}

MdnsResponder::~MdnsResponder() {
    stop();
}

void MdnsResponder::start() {
    boost::system::error_code ec;

    socket_.open(boost::asio::ip::udp::v4(), ec);
    if (ec) return;

    socket_.set_option(boost::asio::ip::udp::socket::reuse_address(true), ec);

    socket_.bind(boost::asio::ip::udp::endpoint(
        boost::asio::ip::address_v4::any(), MDNS_PORT), ec);
    if (ec) {
        socket_.close();
        return;
    }

    socket_.set_option(
        boost::asio::ip::multicast::join_group(
            boost::asio::ip::address_v4::from_string("224.0.0.251")), ec);
    if (ec) {
        socket_.close();
        return;
    }

    socket_.set_option(
        boost::asio::ip::multicast::hops(255), ec);

    running_ = true;
    do_receive();
}

void MdnsResponder::stop() {
    if (!running_.exchange(false)) return;
    boost::system::error_code ec;
    socket_.cancel(ec);
    socket_.close(ec);
}

void MdnsResponder::do_receive() {
    if (!running_) return;

    socket_.async_receive_from(
        boost::asio::buffer(recv_buf_), remote_,
        [this](const boost::system::error_code& ec, std::size_t bytes) {
            handle_receive(ec, bytes);
        });
}

void MdnsResponder::handle_receive(const boost::system::error_code& ec, std::size_t bytes) {
    if (ec || !running_) return;

    if (bytes >= 12) {
        const uint8_t* data = recv_buf_.data();

        // Parse header
        uint16_t qdcount = (data[4] << 8) | data[5];

        std::size_t offset = 12;

        // Extract the first question as raw bytes for echoing back
        std::size_t q_start = offset;

        for (uint16_t q = 0; q < qdcount && offset < bytes; ++q) {
            // Skip name
            while (offset < bytes) {
                uint8_t b = data[offset];
                if (b == 0) {
                    offset++;
                    break;
                }
                if ((b & 0xC0) == 0xC0) {
                    offset += 2;
                    break;
                }
                offset += 1 + b;
            }
            if (offset + 4 <= bytes) {
                offset += 4; // skip QTYPE + QCLASS
            }
        }

        std::size_t q_end = offset;

        // Re-parse to decode the name for matching
        offset = q_start;
        std::string qname = decode_name(data, bytes, offset);
        uint16_t qtype = 0;
        uint16_t qclass = 0;
        if (offset + 4 <= bytes) {
            qtype = (data[offset] << 8) | data[offset + 1];
            qclass = (data[offset + 2] << 8) | data[offset + 3];
        }

        // Check if query matches our service
        bool match = false;
        if (qname == service_ && qtype == RR_TYPE_PTR) match = true;
        else if (qname == instance_ && (qtype == RR_TYPE_SRV || qtype == RR_TYPE_TXT)) match = true;
        else if (qname == node_ && qtype == RR_TYPE_A) match = true;
        else if (qtype == 255) match = true; // ANY query

        if (match) {
            // Extract raw question bytes for echoing
            std::vector<uint8_t> question(recv_buf_.begin() + q_start,
                                          recv_buf_.begin() + q_end);

            uint16_t id = (data[0] << 8) | data[1];
            send_response(remote_, id, question);
        }
    }

    do_receive();
}

void MdnsResponder::send_response(const boost::asio::ip::udp::endpoint& dest,
                                  uint16_t id, const std::vector<uint8_t>& question) {
    std::vector<uint8_t> buf;
    buf.reserve(512);

    // Write header (placeholder sizes)
    auto write_u16 = [&](uint16_t v) {
        buf.push_back(static_cast<uint8_t>(v >> 8));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto write_u32 = [&](uint32_t v) {
        buf.push_back(static_cast<uint8_t>(v >> 24));
        buf.push_back(static_cast<uint8_t>(v >> 16));
        buf.push_back(static_cast<uint8_t>(v >> 8));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };

    write_u16(id);
    write_u16(DNS_FLAG_RESPONSE);
    write_u16(1);  // QDCOUNT (echo one question)
    write_u16(1);  // ANCOUNT (one answer: PTR)
    write_u16(0);  // NSCOUNT
    write_u16(3);  // ARCOUNT (SRV + TXT + A)

    // Echo back the question
    buf.insert(buf.end(), question.begin(), question.end());

    // --- Answer: PTR record ---
    // NAME = _mqtt._tcp.local (pointer back to question name if possible,
    // but we'll just re-encode it)
    buf.insert(buf.end(), encoded_service_.begin(), encoded_service_.end());
    write_u16(RR_TYPE_PTR);
    write_u16(RR_CLASS_IN_FLUSH);
    write_u32(TTL_PTR);

    // RDLENGTH + RDATA = instance name
    size_t rdlen_pos = buf.size();
    write_u16(0); // placeholder
    size_t rdata_start = buf.size();
    buf.insert(buf.end(), encoded_instance_.begin(), encoded_instance_.end());
    size_t rdlen = buf.size() - rdata_start;
    buf[rdlen_pos] = static_cast<uint8_t>(rdlen >> 8);
    buf[rdlen_pos + 1] = static_cast<uint8_t>(rdlen & 0xFF);

    // --- Additional: SRV record ---
    buf.insert(buf.end(), encoded_instance_.begin(), encoded_instance_.end());
    write_u16(RR_TYPE_SRV);
    write_u16(RR_CLASS_IN_FLUSH);
    write_u32(TTL_SRV_TXT);

    // RDLENGTH placeholder
    rdlen_pos = buf.size();
    write_u16(0);
    rdata_start = buf.size();
    write_u16(0);    // Priority
    write_u16(0);    // Weight
    write_u16(port_); // Port
    buf.insert(buf.end(), encoded_node_.begin(), encoded_node_.end()); // Target
    rdlen = buf.size() - rdata_start;
    buf[rdlen_pos] = static_cast<uint8_t>(rdlen >> 8);
    buf[rdlen_pos + 1] = static_cast<uint8_t>(rdlen & 0xFF);

    // --- Additional: TXT record ---
    buf.insert(buf.end(), encoded_instance_.begin(), encoded_instance_.end());
    write_u16(RR_TYPE_TXT);
    write_u16(RR_CLASS_IN_FLUSH);
    write_u32(TTL_SRV_TXT);

    // RDATA: empty TXT record (single zero byte)
    rdlen_pos = buf.size();
    write_u16(0);
    rdata_start = buf.size();
    buf.push_back(0); // empty TXT
    rdlen = buf.size() - rdata_start;
    buf[rdlen_pos] = static_cast<uint8_t>(rdlen >> 8);
    buf[rdlen_pos + 1] = static_cast<uint8_t>(rdlen & 0xFF);

    // --- Additional: A record ---
    buf.insert(buf.end(), encoded_node_.begin(), encoded_node_.end());
    write_u16(RR_TYPE_A);
    write_u16(RR_CLASS_IN_FLUSH);
    write_u32(TTL_A);

    // RDATA: 4-byte IP
    rdlen_pos = buf.size();
    write_u16(4);
    boost::system::error_code ec;
    auto addr = boost::asio::ip::address_v4::from_string(ip_, ec);
    if (!ec) {
        auto bytes = addr.to_bytes();
        buf.insert(buf.end(), bytes.begin(), bytes.end());
    } else {
        buf.push_back(127);
        buf.push_back(0);
        buf.push_back(0);
        buf.push_back(1);
    }

    socket_.send_to(boost::asio::buffer(buf), dest);
}

void MdnsResponder::encode_name(std::vector<uint8_t>& buf, const std::string& name) const {
    std::size_t start = 0;
    while (start < name.size()) {
        auto dot = name.find('.', start);
        if (dot == std::string::npos) dot = name.size();
        auto label_len = dot - start;
        buf.push_back(static_cast<uint8_t>(label_len));
        for (std::size_t i = 0; i < label_len; ++i) {
            buf.push_back(static_cast<uint8_t>(name[start + i]));
        }
        start = dot + 1;
    }
    buf.push_back(0); // root label
}

std::string MdnsResponder::decode_name(const uint8_t* data, std::size_t len, std::size_t& offset) {
    std::string name;
    bool first = true;

    while (offset < len) {
        uint8_t b = data[offset];
        if (b == 0) {
            offset++;
            return name;
        }
        if ((b & 0xC0) == 0xC0) {
            if (offset + 2 > len) return name;
            uint16_t ptr = ((static_cast<uint16_t>(b & 0x3F)) << 8) | data[offset + 1];
            offset += 2;
            std::size_t saved = offset;
            offset = ptr;
            std::string rest = decode_name(data, len, offset);
            offset = saved;
            if (!first) name += '.';
            name += rest;
            return name;
        }
        offset++;
        if (!first) name += '.';
        first = false;
        for (int i = 0; i < b; ++i) {
            if (offset + i >= len) return name;
            char c = static_cast<char>(data[offset + i]);
            name += (c >= 'A' && c <= 'Z') ? (c - 'A' + 'a') : c;
        }
        offset += b;
    }

    return name;
}
