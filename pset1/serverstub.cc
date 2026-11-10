#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "rpcgame.grpc.pb.h"
#include "rpcgame.hh"

#include <thread>

static std::unique_ptr<grpc::Server> server;

class CallBase
{
public:
    virtual ~CallBase() = default;
    virtual void Proceed(bool ok) = 0;
};

class TryCall;

static std::map<uint64_t, TryCall *> pending;
static uint64_t next_serial = 1;

class TryCall final : public CallBase
{
public:
    TryCall(RPCGame::AsyncService *svc, grpc::ServerCompletionQueue *cq)
        : _svc(svc), _cq(cq), _responder(&_ctx)
    {
        Proceed(true);
    }

    // void Proceed(bool ok) override
    // {
    //     if (_state == CREATE)
    //     {
    //         _state = PROCESS;
    //         // Ask gRPC to deliver the next incoming Try to this object.
    //         _svc->RequestTry(&_ctx, &_req, &_responder, _cq, _cq, this);
    //     }
    //     else if (_state == PROCESS)
    //     {
    //         if (!ok)
    //         {
    //             delete this;
    //             return;
    //         }

    //         // Spawn a fresh listener so new Try RPCs can be accepted.
    //         new TryCall(_svc, _cq);

    //         std::string serialstr = _req.serial();
    //         uint64_t serial = from_str_chars<uint64_t>(serialstr);
    //         std::string name = _req.name();
    //         std::string countstr = _req.count();
    //         uint64_t count = from_str_chars<uint64_t>(countstr);

    //         uint64_t value = server_process_try(serial, name.data(),
    //                                             name.size(), count);
    //         _resp.set_value(std::to_string(value));

    //         _state = FINISH;
    //         _responder.Finish(_resp, grpc::Status::OK, this);
    //     }
    //     else
    //     {
    //         delete this;
    //     }
    // }

    void Proceed(bool ok)
    {
        if (_state == CREATE)
        {
            _state = PROCESS;
            _svc->RequestTry(&_ctx, &_req, &_responder, _cq, _cq, this);
        }
        else if (_state == PROCESS)
        {
            if (!ok)
            {
                delete this;
                return;
            }

            new TryCall(_svc, _cq); // keep listening

            _serial = from_str_chars<uint64_t>(_req.serial());
            pending.emplace(_serial, this);

            // Drain everything that is now in order.
            for (auto it = pending.find(next_serial); it != pending.end();
                 it = pending.find(next_serial))
            {
                it->second->Execute();
                pending.erase(it);
                ++next_serial;
            }
        }
        else
        {
            delete this;
        }
    }

private:
    void Execute()
    {
        std::string name = _req.name();
        uint64_t count = from_str_chars<uint64_t>(_req.count());
        uint64_t value = server_process_try(_serial, name.data(), name.size(), count);
        _resp.set_value(std::to_string(value));
        _state = FINISH;
        _responder.Finish(_resp, grpc::Status::OK, this);
    }

    uint64_t _serial = 0;
    RPCGame::AsyncService *_svc;
    grpc::ServerCompletionQueue *_cq;
    grpc::ServerContext _ctx;
    TryRequest _req;
    TryResponse _resp;
    grpc::ServerAsyncResponseWriter<TryResponse> _responder;
    enum
    {
        CREATE,
        PROCESS,
        FINISH
    } _state = CREATE;
};

class DoneCall final : public CallBase
{
public:
    DoneCall(RPCGame::AsyncService *svc, grpc::ServerCompletionQueue *cq)
        : _svc(svc), _cq(cq), _responder(&_ctx)
    {
        Proceed(true);
    }

    void Proceed(bool ok) override
    {
        if (_state == CREATE)
        {
            _state = PROCESS;
            _svc->RequestDone(&_ctx, &_req, &_responder, _cq, _cq, this);
        }
        else if (_state == PROCESS)
        {
            if (!ok)
            {
                delete this;
                return;
            }

            new DoneCall(_svc, _cq);

            _resp.set_client_checksum(client_checksum());
            _resp.set_server_checksum(server_checksum());

            _state = FINISH;
            _responder.Finish(_resp, grpc::Status::OK, this);
        }
        else
        {
            // Response has been sent. Now shut down.
            // Must NOT call Shutdown() on the CQ thread (it would deadlock),
            // so use a separate thread.
            auto *cq = _cq;
            std::thread([cq]
                        {
                server->Shutdown();
                cq->Shutdown(); })
                .detach();
            delete this;
        }
    }

private:
    RPCGame::AsyncService *_svc;
    grpc::ServerCompletionQueue *_cq;
    grpc::ServerContext _ctx;
    DoneRequest _req;
    DoneResponse _resp;
    grpc::ServerAsyncResponseWriter<DoneResponse> _responder;
    enum
    {
        CREATE,
        PROCESS,
        FINISH
    } _state = CREATE;
};

void server_start(std::string address)
{
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    grpc::ServerBuilder builder;
    // Request a compressed channel
    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_GZIP);
    // Listen on the given address without any authentication
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    // Register `service` as the instance through which we'll communicate with
    // clients
    RPCGame::AsyncService service;
    builder.RegisterService(&service);
    std::unique_ptr<grpc::ServerCompletionQueue> cq = builder.AddCompletionQueue();
    // Finally assemble the server
    server = builder.BuildAndStart();
    std::cout << "Server listening on " << address << "\n";

    // Seed one listener per method.
    new TryCall(&service, cq.get());
    new DoneCall(&service, cq.get());

    void *tag;
    bool ok;
    while (cq->Next(&tag, &ok))
    {
        static_cast<CallBase *>(tag)->Proceed(ok);
    }

    std::cout << "Server exiting\n";
}
