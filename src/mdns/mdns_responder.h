#pragma once
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/udp.hpp>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

class MdnsResponder {
public:
    MdnsResponder(boost::asio::io_context& io, uint16_t port,
                  const std::string& hostname, const std::string& ip);
    ~MdnsResponder();

    void start();
    void stop();

private:
    void do_receive();
    void handle_receive(const boost::system::error_code& ec, std::size_t bytes);
    void send_response(const boost::asio::ip::udp::endpoint& dest,
                       uint16_t id, const std::vector<uint8_t>& question);

    void encode_name(std::vector<uint8_t>& buf, const std::string& name) const;
    static std::string decode_name(const uint8_t* data, std::size_t len, std::size_t& offset);

    boost::asio::io_context& io_;
    boost::asio::ip::udp::socket socket_;
    boost::asio::ip::udp::endpoint remote_;
    std::vector<uint8_t> recv_buf_;

    uint16_t port_;
    std::string instance_;
    std::string service_;
    std::string node_;
    std::string ip_;

    std::atomic<bool> running_{false};

    // Pre-encoded DNS names (for response construction)
    std::vector<uint8_t> encoded_instance_;
    std::vector<uint8_t> encoded_service_;
    std::vector<uint8_t> encoded_node_;
};
