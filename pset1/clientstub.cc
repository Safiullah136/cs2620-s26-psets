#include <grpcpp/grpcpp.h>
#include <condition_variable>
#include <thread>

#include "rpcgame.grpc.pb.h"
#include "rpcgame.hh"

class RPCGameClient
{
private:
    struct AsyncTryCall
    {
        uint64_t serial;

        grpc::ClientContext context;
        TryRequest request;
        TryResponse response;
        grpc::Status status;

        std::unique_ptr<
            grpc::ClientAsyncResponseReader<TryResponse>>
            reader;
    };

public:
    RPCGameClient(std::shared_ptr<grpc::Channel> channel)
        : _stub(RPCGame::NewStub(channel))
    {

        _worker = std::thread(&RPCGameClient::poll_cq, this);
    }

    ~RPCGameClient()
    {
        {
            std::unique_lock<std::mutex> lock(_mutex);

            _closing = true;

            // Wait until all outstanding RPCs complete.
            _cv.wait(lock, [this]
                     { return _outstanding == 0; });
        }

        // Tell CQ no more events will arrive.
        _cq.Shutdown();

        // Wait for polling thread to exit.
        if (_worker.joinable())
        {
            _worker.join();
        }
    }

    void send_try(const char *name, size_t name_len, uint64_t count)
    {
        // construct request
        auto call = std::make_unique<AsyncTryCall>();

        {
            std::lock_guard<std::mutex> lock(_mutex);

            if (_closing)
            {
                return;
            }

            call->serial = _serial++;

            ++_outstanding;
        }

        call->request.set_serial(std::to_string(call->serial));

        call->request.set_name(std::string(name, name_len));

        call->request.set_count(std::to_string(count));

        // Initiate asynchronous RPC.
        call->reader = _stub->AsyncTry(&call->context, call->request, &_cq);

        // Use the AsyncTryCall object itself as the tag.
        AsyncTryCall *tag = call.release();

        // Register completion notification.
        tag->reader->Finish(&tag->response, &tag->status, static_cast<void *>(tag));
    }

    void finish()
    {
        {
            std::unique_lock<std::mutex> lock(_mutex);

            _closing = true;

            // Wait for all Try RPCs to finish.
            _cv.wait(lock, [this]
                     { return _outstanding == 0; });
        }

        // ready space for response
        DoneResponse response;

        // send request, get response
        grpc::ClientContext context;
        grpc::Status status = _stub->Done(&context, DoneRequest(), &response);
        if (!status.ok())
        {
            std::cerr << status.error_code() << ": " << status.error_message()
                      << "\n";
            exit(1);
        }

        // parse response
        std::string my_client_checksum = client_checksum(),
                    my_server_checksum = server_checksum();
        bool ok = my_client_checksum == response.client_checksum() && my_server_checksum == response.server_checksum();
        std::cout << "client checksums: "
                  << my_client_checksum << "/" << response.client_checksum()
                  << "\nserver checksums: "
                  << my_server_checksum << "/" << response.server_checksum()
                  << "\nmatch: " << (ok ? "true\n" : "false\n");
    }

private:
    // Dedicated CQ consumer thread.
    void poll_cq()
    {
        void *got_tag;
        bool ok;

        while (_cq.Next(&got_tag, &ok))
        {
            // std::cout << "I never come";

            // Recover the AsyncTryCall associated with this event.
            std::unique_ptr<AsyncTryCall> call(
                static_cast<AsyncTryCall *>(got_tag));

            if (ok && call->status.ok())
            {

                std::string valuestr = call->response.value();

                uint64_t value =
                    from_str_chars<uint64_t>(valuestr);

                // Notify application.
                client_recv_try_response(value);
            }
            else
            {
                std::cerr << "RPC failed for serial "
                          << call->serial << "\n";
            }

            // Call object is deleted when unique_ptr goes out of scope.

            {
                std::lock_guard<std::mutex> lock(_mutex);
                --_outstanding;
            }

            _cv.notify_all();
        }
    }

    std::unique_ptr<RPCGame::Stub> _stub;
    uint64_t _serial = 1;

    grpc::CompletionQueue _cq;

    // CQ consumer.
    std::thread _worker;

    std::mutex _mutex;
    std::condition_variable _cv;

    size_t _outstanding = 0;

    bool _closing = false;
};

static std::unique_ptr<RPCGameClient> client;

void client_connect(std::string address)
{
    // Request a compressed channel
    grpc::ChannelArguments args;
    args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);
    client = std::make_unique<RPCGameClient>(
        grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args));
}

void client_send_try(const char *name, size_t name_len, uint64_t count)
{
    client->send_try(name, name_len, count);
}

void client_finish()
{
    client->finish();
}
