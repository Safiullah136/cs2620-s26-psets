#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <memory>
#include <string>

#include <msgpack.hpp>

#include "rpcgame.hh"

static constexpr uint32_t MSG_REQUEST = 0;
static constexpr uint32_t MSG_RESPONSE = 1;

class RPCGameClient
{
public:
    RPCGameClient(int sockfd) : _fd(sockfd)
    {
    }

    void send_try(const char *name, size_t name_len, uint64_t count)
    {
        uint64_t msgid = _serial++;

        // Build `Try` request: [0, id, "Try", [serial, name, count]]
        msgpack::sbuffer buffer;
        {
            msgpack::packer<msgpack::sbuffer> pk(&buffer);
            pk.pack_array(4);
            pk.pack(MSG_REQUEST);
            pk.pack(msgid);
            pk.pack_str(3);
            pk.pack_str_body("Try", 3);
            pk.pack_array(3);
            pk.pack(_serial - 1); // serial of this Try
            pk.pack_str(name_len);
            pk.pack_str_body(name, name_len);
            pk.pack(count);
        }
        send_frame(buffer);

        // Read the response frame.
        std::string frame = recv_frame();

        msgpack::object_handle handle;
        try
        {
            handle = msgpack::unpack(frame.data(), frame.size());
        }
        catch (...)
        {
            std::cerr << "protocol error: bad response\n";
            close(_fd);
            exit(1);
        }
        if (handle.get().type != msgpack::type::ARRAY)
        {
            std::cerr << "protocol error: response is not an array\n";
            close(_fd);
            exit(1);
        }
        const msgpack::object_array &array = handle.get().via.array;
        if (array.size < 4)
        {
            std::cerr << "protocol error: short response\n";
            close(_fd);
            exit(1);
        }
        if (array.ptr[0].as<uint32_t>() != MSG_RESPONSE)
        {
            std::cerr << "protocol error: not a response\n";
            close(_fd);
            exit(1);
        }
        uint64_t resp_id = array.ptr[1].as<uint64_t>();
        if (resp_id != msgid)
        {
            std::cerr << "protocol error: mismatched message id\n";
            close(_fd);
            exit(1);
        }
        // array.ptr[2] is the error; on success it is nil.
        if (array.ptr[2].type != msgpack::type::NIL)
        {
            std::cerr << "rpc error: " << array.ptr[2].as<std::string>() << "\n";
            close(_fd);
            exit(1);
        }
        uint64_t value = array.ptr[3].as<uint64_t>();

        client_recv_try_response(value);
    }

    void finish()
    {
        uint64_t msgid = _serial++;

        // Build `Done` request: [0, id, "Done", []]
        msgpack::sbuffer buffer;
        {
            msgpack::packer<msgpack::sbuffer> pk(&buffer);
            pk.pack_array(4);
            pk.pack(MSG_REQUEST);
            pk.pack(msgid);
            pk.pack_str(4);
            pk.pack_str_body("Done", 4);
            pk.pack_array(0);
        }
        send_frame(buffer);

        std::string frame = recv_frame();

        msgpack::object_handle handle;
        try
        {
            handle = msgpack::unpack(frame.data(), frame.size());
        }
        catch (...)
        {
            std::cerr << "protocol error: bad response\n";
            close(_fd);
            exit(1);
        }
        if (handle.get().type != msgpack::type::ARRAY)
        {
            std::cerr << "protocol error: response is not an array\n";
            close(_fd);
            exit(1);
        }
        const msgpack::object_array &array = handle.get().via.array;
        if (array.size < 4)
        {
            std::cerr << "protocol error: short response\n";
            close(_fd);
            exit(1);
        }
        if (array.ptr[0].as<uint32_t>() != MSG_RESPONSE)
        {
            std::cerr << "protocol error: not a response\n";
            close(_fd);
            exit(1);
        }
        if (array.ptr[2].type != msgpack::type::NIL)
        {
            std::cerr << "rpc error: " << array.ptr[2].as<std::string>() << "\n";
            close(_fd);
            exit(1);
        }
        if (array.ptr[3].type != msgpack::type::ARRAY)
        {
            std::cerr << "protocol error: bad Done result\n";
            close(_fd);
            exit(1);
        }
        const msgpack::object_array &checks = array.ptr[3].via.array;
        if (checks.size < 2)
        {
            std::cerr << "protocol error: short Done result\n";
            close(_fd);
            exit(1);
        }
        std::string server_client_csum = checks.ptr[0].as<std::string>();
        std::string server_server_csum = checks.ptr[1].as<std::string>();

        std::string my_client_checksum = client_checksum(),
                    my_server_checksum = server_checksum();
        bool ok = my_client_checksum == server_client_csum && my_server_checksum == server_server_csum;
        std::cout << "client checksums: "
                  << my_client_checksum << "/" << server_client_csum
                  << "\nserver checksums: "
                  << my_server_checksum << "/" << server_server_csum
                  << "\nmatch: " << (ok ? "true\n" : "false\n");

        close(_fd);
    }

private:
    // void send_frame(msgpack::sbuffer& buffer) {
    //     uint32_t len = htonl(static_cast<uint32_t>(buffer.size()));
    //     size_t sent = 0;
    //     // Send the 4-byte length prefix.
    //     const char* prefix = reinterpret_cast<char*>(&len);
    //     while (sent < sizeof(len)) {
    //         ssize_t n = send(_fd, prefix + sent, sizeof(len) - sent, 0);
    //         if (n <= 0) {
    //             std::cerr << "send: " << strerror(errno) << "\n";
    //             close(_fd);
    //             exit(1);
    //         }
    //         sent += static_cast<size_t>(n);
    //     }
    //     sent = 0;
    //     while (sent < buffer.size()) {
    //         ssize_t n = send(_fd, buffer.data() + sent, buffer.size() - sent, 0);
    //         if (n <= 0) {
    //             std::cerr << "send: " << strerror(errno) << "\n";
    //             close(_fd);
    //             exit(1);
    //         }
    //         sent += static_cast<size_t>(n);
    //     }
    // }

    void send_frame(msgpack::sbuffer &buffer)
    {
        std::string frame;
        frame.reserve(4 + buffer.size());
        uint32_t len = htonl(static_cast<uint32_t>(buffer.size()));
        frame.append(reinterpret_cast<char *>(&len), 4);
        frame.append(buffer.data(), buffer.size());

        size_t sent = 0;
        while (sent < frame.size())
        {
            ssize_t n = send(_fd, frame.data() + sent, frame.size() - sent, MSG_NOSIGNAL);
            if (n <= 0)
            {
                std::cerr << "send: " << strerror(errno) << "\n";
                close(_fd);
                exit(1);
            }
            sent += static_cast<size_t>(n);
        }
    }

    std::string recv_frame()
    {
        char lengthbuf[4];
        size_t got = 0;
        while (got < sizeof(lengthbuf))
        {
            ssize_t n = recv(_fd, lengthbuf + got, sizeof(lengthbuf) - got, 0);
            if (n <= 0)
            {
                std::cerr << "recv: " << strerror(errno) << "\n";
                close(_fd);
                exit(1);
            }
            got += static_cast<size_t>(n);
        }
        uint32_t length;
        std::memcpy(&length, lengthbuf, sizeof(length));
        length = ntohl(length);
        if (length == 0 || length > (1u << 28))
        {
            std::cerr << "protocol error: bad frame length\n";
            close(_fd);
            exit(1);
        }
        std::string payload(length, '\0');
        got = 0;
        while (got < length)
        {
            ssize_t n = recv(_fd, &payload[got], length - got, 0);
            if (n <= 0)
            {
                std::cerr << "recv: " << strerror(errno) << "\n";
                close(_fd);
                exit(1);
            }
            got += static_cast<size_t>(n);
        }
        return payload;
    }

    int _fd;
    uint64_t _serial = 1;
};

static std::unique_ptr<RPCGameClient> client;

void client_connect(std::string address)
{
    size_t colon = address.rfind(':');
    if (colon == std::string::npos)
    {
        std::cerr << "invalid address: " << address << "\n";
        exit(1);
    }
    std::string host = address.substr(0, colon);
    uint16_t port = from_str_chars<uint16_t>(address.substr(colon + 1));

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
        std::cerr << "socket: " << strerror(errno) << "\n";
        exit(1);
    }

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host == "localhost")
    {
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    }
    else if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1)
    {
        std::cerr << "invalid host: " << host << "\n";
        exit(1);
    }

    if (connect(sockfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        std::cerr << "connect " << address << ": " << strerror(errno) << "\n";
        exit(1);
    }

    client = std::make_unique<RPCGameClient>(sockfd);
}

void client_send_try(const char *name, size_t name_len, uint64_t count)
{
    client->send_try(name, name_len, count);
}

void client_finish()
{
    client->finish();
}