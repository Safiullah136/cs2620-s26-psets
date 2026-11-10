#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <msgpack.hpp>

#include "rpcgame.hh"

// The reserve of the embedded serialization buffer.
static constexpr size_t kDefaultBufferSize = 4096;

// MessagePack-RPC message types.
static constexpr uint32_t MSG_REQUEST = 0;
static constexpr uint32_t MSG_RESPONSE = 1;

// A collectively allocated MessagePack-RPC message. Requests arrive on the
// wire as `[0, id, "method", params...]`; responses are sent back as
// `[1, id, error, result]` where the error is `null` on success.
struct wire_message
{
    uint32_t type;
    uint64_t msgid;
    std::string method;
    msgpack::object params;
};

// Set to signal that the server should stop accepting connections.
static std::atomic<bool> server_shutdown{false};

// The listening socket, closed by the shutdown thread so a blocking `accept`
// in the main thread is unblocked and the server can exit cleanly.
static std::atomic<int> g_listenfd{-1};

// Serialize a MessagePack-RPC response `[1, id, null, result]`.
static bool serialize_response(uint64_t msgid, const msgpack::object &result,
                               msgpack::sbuffer &buffer)
{
    try
    {
        msgpack::packer<msgpack::sbuffer> pk(&buffer);
        pk.pack_array(4);
        pk.pack(MSG_RESPONSE); // type
        pk.pack(msgid);
        pk.pack(msgpack::type::nil_t()); // error
        pk.pack(result);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

// Serialize a MessagePack-RPC error response `[1, id, error, nil]`.
static bool serialize_error(uint64_t msgid, const std::string &message,
                            msgpack::sbuffer &buffer)
{
    try
    {
        msgpack::packer<msgpack::sbuffer> pk(&buffer);
        pk.pack_array(4);
        pk.pack(MSG_RESPONSE); // type
        pk.pack(msgid);
        pk.pack_str(message.size()); // error
        pk.pack_str_body(message.data(), message.size());
        pk.pack(msgpack::type::nil_t()); // result
        return true;
    }
    catch (...)
    {
        return false;
    }
}

// Parse a `[type, id, method, params...]` array into `message`.
static bool parse_message(msgpack::object_handle &handle, wire_message &message)
{
    if (handle.get().type != msgpack::type::ARRAY)
    {
        return false;
    }
    const msgpack::object_array &array = handle.get().via.array;
    if (array.size < 4)
    {
        return false;
    }
    message.type = array.ptr[0].as<uint32_t>();
    message.msgid = array.ptr[1].as<uint64_t>();
    message.method = array.ptr[2].as<std::string>();
    message.params = array.ptr[3];
    return true;
}

class RPCGameServiceImpl
{
public:
    // Handle a single decoded request and write the response into `out`.
    static bool dispatch(const wire_message &message, msgpack::sbuffer &out)
    {
        if (message.type != MSG_REQUEST)
        {
            return false;
        }

        if (message.method == "Try")
        {
            // params are [serial, name, count]
            if (message.params.type != msgpack::type::ARRAY || message.params.via.array.size < 3)
            {
                out.clear();
                return serialize_error(message.msgid, "invalid Try params", out);
            }
            const msgpack::object_array &args = message.params.via.array;
            uint64_t serial = args.ptr[0].as<uint64_t>();
            std::string name = args.ptr[1].as<std::string>();
            uint64_t count = args.ptr[2].as<uint64_t>();

            uint64_t value = server_process_try(serial, name.data(), name.size(),
                                                count);

            out.clear();
            return serialize_response(message.msgid, msgpack::object(value), out);
        }

        if (message.method == "Done")
        {
            std::string client_csum = client_checksum();
            std::string server_csum = server_checksum();

            // Represent the checksums as a 2-element array of strings.
            msgpack::zone result_zone;
            std::vector<msgpack::object> elements;
            elements.push_back(msgpack::object(client_csum, result_zone));
            elements.push_back(msgpack::object(server_csum, result_zone));
            msgpack::object result_object;
            result_object.type = msgpack::type::ARRAY;
            result_object.via.array.ptr = elements.data();
            result_object.via.array.size = elements.size();

            out.clear();
            bool ok = serialize_response(message.msgid, result_object, out);

            // Shut down the server shortly after the response is flushed:
            // stop accepting new connections so pending work is never cut short.
            if (ok)
            {
                std::thread shutdown_thread([]()
                                            {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(100ms);
                    server_shutdown.store(true, std::memory_order_release);
                    int fd = g_listenfd.exchange(-1);
                    if (fd >= 0)
                    {
                        shutdown(fd, SHUT_RDWR);
                        close(fd);
                    } });
                shutdown_thread.detach();
            }
            return ok;
        }

        out.clear();
        return serialize_error(message.msgid, "unknown method", out);
    }

    static void run(int connfd)
    {
        // Collectively receive a full message frame: a 4-byte big-endian
        // length prefix followed by that many bytes of msgpack data.
        int sockfd = connfd;
        char lengthbuf[4];
        while (true)
        {
            size_t got = 0;
            while (got < sizeof(lengthbuf))
            {
                ssize_t n = recv(sockfd, lengthbuf + got,
                                 sizeof(lengthbuf) - got, 0);
                if (n <= 0)
                {
                    close(sockfd);
                    return;
                }
                got += static_cast<size_t>(n);
            }

            uint32_t length;
            std::memcpy(&length, lengthbuf, sizeof(length));
            length = ntohl(length);
            if (length == 0 || length > (1u << 28))
            {
                close(sockfd);
                return;
            }

            std::string payload(length, '\0');
            got = 0;
            while (got < length)
            {
                ssize_t n = recv(sockfd, &payload[got], length - got, 0);
                if (n <= 0)
                {
                    close(sockfd);
                    return;
                }
                got += static_cast<size_t>(n);
            }

            // Unpack the request.
            msgpack::object_handle handle;
            try
            {
                handle = msgpack::unpack(payload.data(), payload.size());
            }
            catch (...)
            {
                close(sockfd);
                return;
            }

            wire_message message;
            if (!parse_message(handle, message) || message.type != MSG_REQUEST)
            {
                close(sockfd);
                return;
            }

            // Build and send the response.
            msgpack::sbuffer out(kDefaultBufferSize);
            if (!dispatch(message, out))
            {
                close(sockfd);
                return;
            }

            uint32_t outlen = htonl(static_cast<uint32_t>(out.size()));
            std::string frame;
            frame.reserve(sizeof(outlen) + out.size());
            frame.append(reinterpret_cast<char *>(&outlen), sizeof(outlen));
            frame.append(out.data(), out.size());

            size_t sent = 0;
            while (sent < frame.size())
            {
                ssize_t n = send(sockfd, frame.data() + sent,
                                 frame.size() - sent, 0);
                if (n <= 0)
                {
                    close(sockfd);
                    return;
                }
                sent += static_cast<size_t>(n);
            }
        }
    }
};

void server_start(std::string address)
{
    // Parse the `host:port` address into a sockaddr.
    size_t colon = address.rfind(':');
    if (colon == std::string::npos)
    {
        std::cerr << "invalid address: " << address << "\n";
        exit(1);
    }
    std::string host = address.substr(0, colon);
    uint16_t port = from_str_chars<uint16_t>(address.substr(colon + 1));

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0)
    {
        std::cerr << "socket: " << strerror(errno) << "\n";
        exit(1);
    }
    int one = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host == "localhost" || host == "0.0.0.0")
    {
        addr.sin_addr.s_addr = host == "0.0.0.0" ? htonl(INADDR_ANY)
                                                 : htonl(INADDR_LOOPBACK);
    }
    else if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1)
    {
        std::cerr << "invalid host: " << host << "\n";
        exit(1);
    }

    if (bind(listenfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        std::cerr << "bind " << address << ": " << strerror(errno) << "\n";
        exit(1);
    }
    if (listen(listenfd, 16) < 0)
    {
        std::cerr << "listen: " << strerror(errno) << "\n";
        exit(1);
    }

    std::cout << "Server listening on " << address << "\n";
    g_listenfd.store(listenfd);

    while (!server_shutdown.load(std::memory_order_acquire))
    {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int connfd = accept(listenfd,
                            reinterpret_cast<sockaddr *>(&client_addr),
                            &client_len);
        if (connfd < 0)
        {
            // The shutdown thread closed listenfd to unblock this accept.
            if (server_shutdown.load(std::memory_order_acquire))
            {
                break;
            }
            if (errno == EINTR)
            {
                continue;
            }
            std::cerr << "accept: " << strerror(errno) << "\n";
            continue;
        }
        std::thread([](int fd)
                    { RPCGameServiceImpl::run(fd); }, connfd)
            .detach();
    }

    g_listenfd.store(-1);
    close(listenfd);
    std::cout << "Server exiting\n";
}