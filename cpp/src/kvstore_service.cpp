#include "kvstore_service.h"
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <rocksdb/utilities/transaction.h>

// Base class for async RPC handlers following gRPC official pattern
class CallData {
public:
    CallData() : status_(CREATE) {}
    virtual ~CallData() {}
    virtual void Proceed() = 0;

protected:
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;
};

// Get operation handler
class GetCallData : public CallData {
public:
    GetCallData(KvStoreService* service, kvstore::KVStore::AsyncService* async_service, 
                grpc::ServerCompletionQueue* cq)
        : service_(service), async_service_(async_service), cq_(cq), responder_(&ctx_) {
        Proceed();
    }

    void Proceed() override {
        if (status_ == CREATE) {
            status_ = PROCESS;
            async_service_->RequestGet(&ctx_, &request_, &responder_, cq_, cq_, this);
        } else if (status_ == PROCESS) {
            // Spawn a new CallData instance to serve new clients
            new GetCallData(service_, async_service_, cq_);
            
            // Process the request
            grpc::Status status = service_->ProcessGet(&request_, &response_);
            
            status_ = FINISH;
            responder_.Finish(response_, status, this);
        } else {
            // FINISH state - cleanup
            delete this;
        }
    }

private:
    KvStoreService* service_;
    kvstore::KVStore::AsyncService* async_service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    grpc::ServerAsyncResponseWriter<kvstore::GetResponse> responder_;
    kvstore::GetRequest request_;
    kvstore::GetResponse response_;
};

// Put operation handler
class PutCallData : public CallData {
public:
    PutCallData(KvStoreService* service, kvstore::KVStore::AsyncService* async_service, 
                grpc::ServerCompletionQueue* cq)
        : service_(service), async_service_(async_service), cq_(cq), responder_(&ctx_) {
        Proceed();
    }

    void Proceed() override {
        if (status_ == CREATE) {
            status_ = PROCESS;
            async_service_->RequestPut(&ctx_, &request_, &responder_, cq_, cq_, this);
        } else if (status_ == PROCESS) {
            new PutCallData(service_, async_service_, cq_);
            
            grpc::Status status = service_->ProcessPut(&request_, &response_);
            
            status_ = FINISH;
            responder_.Finish(response_, status, this);
        } else {
            delete this;
        }
    }

private:
    KvStoreService* service_;
    kvstore::KVStore::AsyncService* async_service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    grpc::ServerAsyncResponseWriter<kvstore::PutResponse> responder_;
    kvstore::PutRequest request_;
    kvstore::PutResponse response_;
};

// Delete operation handler
class DeleteCallData : public CallData {
public:
    DeleteCallData(KvStoreService* service, kvstore::KVStore::AsyncService* async_service, 
                   grpc::ServerCompletionQueue* cq)
        : service_(service), async_service_(async_service), cq_(cq), responder_(&ctx_) {
        Proceed();
    }

    void Proceed() override {
        if (status_ == CREATE) {
            status_ = PROCESS;
            async_service_->RequestDelete(&ctx_, &request_, &responder_, cq_, cq_, this);
        } else if (status_ == PROCESS) {
            new DeleteCallData(service_, async_service_, cq_);
            
            grpc::Status status = service_->ProcessDelete(&request_, &response_);
            
            status_ = FINISH;
            responder_.Finish(response_, status, this);
        } else {
            delete this;
        }
    }

private:
    KvStoreService* service_;
    kvstore::KVStore::AsyncService* async_service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    grpc::ServerAsyncResponseWriter<kvstore::DeleteResponse> responder_;
    kvstore::DeleteRequest request_;
    kvstore::DeleteResponse response_;
};

// ListKeys operation handler
class ListKeysCallData : public CallData {
public:
    ListKeysCallData(KvStoreService* service, kvstore::KVStore::AsyncService* async_service, 
                     grpc::ServerCompletionQueue* cq)
        : service_(service), async_service_(async_service), cq_(cq), responder_(&ctx_) {
        Proceed();
    }

    void Proceed() override {
        if (status_ == CREATE) {
            status_ = PROCESS;
            async_service_->RequestListKeys(&ctx_, &request_, &responder_, cq_, cq_, this);
        } else if (status_ == PROCESS) {
            new ListKeysCallData(service_, async_service_, cq_);
            
            grpc::Status status = service_->ProcessListKeys(&request_, &response_);
            
            status_ = FINISH;
            responder_.Finish(response_, status, this);
        } else {
            delete this;
        }
    }

private:
    KvStoreService* service_;
    kvstore::KVStore::AsyncService* async_service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    grpc::ServerAsyncResponseWriter<kvstore::ListKeysResponse> responder_;
    kvstore::ListKeysRequest request_;
    kvstore::ListKeysResponse response_;
};

// Ping operation handler
class PingCallData : public CallData {
public:
    PingCallData(KvStoreService* service, kvstore::KVStore::AsyncService* async_service, 
                 grpc::ServerCompletionQueue* cq)
        : service_(service), async_service_(async_service), cq_(cq), responder_(&ctx_) {
        Proceed();
    }

    void Proceed() override {
        if (status_ == CREATE) {
            status_ = PROCESS;
            async_service_->RequestPing(&ctx_, &request_, &responder_, cq_, cq_, this);
        } else if (status_ == PROCESS) {
            new PingCallData(service_, async_service_, cq_);
            
            grpc::Status status = service_->ProcessPing(&request_, &response_);
            
            status_ = FINISH;
            responder_.Finish(response_, status, this);
        } else {
            delete this;
        }
    }

private:
    KvStoreService* service_;
    kvstore::KVStore::AsyncService* async_service_;
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;
    grpc::ServerAsyncResponseWriter<kvstore::PingResponse> responder_;
    kvstore::PingRequest request_;
    kvstore::PingResponse response_;
};

// Server implementation following gRPC official pattern
class ServerImpl {
public:
    ServerImpl(KvStoreService* service_instance) : service_instance_(service_instance) {}

    ~ServerImpl() {
        server_->Shutdown();
        // Always shutdown the completion queue after the server
        cq_->Shutdown();
    }

    void Run(const std::string& server_address) {
        grpc::ServerBuilder builder;
        
        // Listen on the given address without any authentication mechanism
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        
        // Register async service
        builder.RegisterService(&service_);
        
        // Configure server options for better performance
        builder.SetMaxReceiveMessageSize(4 * 1024 * 1024);  // 4MB
        builder.SetMaxSendMessageSize(4 * 1024 * 1024);     // 4MB
        
        // Get hold of the completion queue used for the asynchronous communication
        cq_ = builder.AddCompletionQueue();
        
        // Finally assemble the server
        server_ = builder.BuildAndStart();
        
        if (!server_) {
            throw std::runtime_error("Failed to start server");
        }
        
        std::cout << "C++ async gRPC server listening on " << server_address << std::endl;
        
        // Proceed to the server's main loop
        HandleRpcs();
    }

private:
    void HandleRpcs() {
        // Spawn new CallData instances to serve new clients
        new GetCallData(service_instance_, &service_, cq_.get());
        new PutCallData(service_instance_, &service_, cq_.get());
        new DeleteCallData(service_instance_, &service_, cq_.get());
        new ListKeysCallData(service_instance_, &service_, cq_.get());
        new PingCallData(service_instance_, &service_, cq_.get());
        
        void* tag;  // uniquely identifies a request
        bool ok;
        
        while (true) {
            // Block waiting to read the next event from the completion queue
            // The event is uniquely identified by its tag, which in this case is the
            // memory address of a CallData instance
            // The return value of Next should always be checked
            if (!cq_->Next(&tag, &ok)) {
                break;  // Queue is shutting down
            }
            
            if (ok) {
                static_cast<CallData*>(tag)->Proceed();
            }
        }
    }

    KvStoreService* service_instance_;
    kvstore::KVStore::AsyncService service_;
    std::unique_ptr<grpc::ServerCompletionQueue> cq_;
    std::unique_ptr<grpc::Server> server_;
};

// Internal implementation class for KvStoreService
class KvStoreService::AsyncServiceImpl {
public:
    std::unique_ptr<ServerImpl> server_impl_;
};

KvStoreService::KvStoreService(const std::string& db_path)
    : async_service_(std::make_unique<AsyncServiceImpl>()) {
    
    // Set up RocksDB options
    rocksdb::Options opts;
    opts.create_if_missing = true;
    
    // Set up transaction database options
    rocksdb::TransactionDBOptions txn_db_opts;
    
    // Open transaction database
    rocksdb::TransactionDB* txn_db;
    rocksdb::Status status = rocksdb::TransactionDB::Open(opts, txn_db_opts, db_path, &txn_db);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
    
    db_.reset(txn_db);
    
    // Set up read/write options
    read_options_.verify_checksums = true;
    write_options_.sync = false;  // For better performance
}

KvStoreService::~KvStoreService() {
    // ServerImpl destructor will handle proper shutdown
}

bool KvStoreService::acquire_read_permit() {
    int current = active_reads_.load();
    while (current < MAX_READ_CONCURRENCY) {
        if (active_reads_.compare_exchange_weak(current, current + 1)) {
            return true;
        }
    }
    return false;
}

void KvStoreService::release_read_permit() {
    active_reads_.fetch_sub(1);
}

bool KvStoreService::acquire_write_permit() {
    int current = active_writes_.load();
    while (current < MAX_WRITE_CONCURRENCY) {
        if (active_writes_.compare_exchange_weak(current, current + 1)) {
            return true;
        }
    }
    return false;
}

void KvStoreService::release_write_permit() {
    active_writes_.fetch_sub(1);
}

int64_t KvStoreService::get_current_timestamp_micros() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

grpc::Status KvStoreService::ProcessGet(const kvstore::GetRequest* request,
                                        kvstore::GetResponse* response) {
    if (!acquire_read_permit()) {
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many concurrent reads");
    }
    
    try {
        std::string value;
        rocksdb::Status status = db_->Get(read_options_, request->key(), &value);
        
        if (status.ok()) {
            response->set_value(value);
            response->set_found(true);
        } else if (status.IsNotFound()) {
            response->set_found(false);
        } else {
            release_read_permit();
            return grpc::Status(grpc::StatusCode::INTERNAL, 
                              "Database error: " + status.ToString());
        }
        
        release_read_permit();
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        release_read_permit();
        return grpc::Status(grpc::StatusCode::INTERNAL, 
                          "Exception: " + std::string(e.what()));
    }
}

grpc::Status KvStoreService::ProcessPut(const kvstore::PutRequest* request,
                                        kvstore::PutResponse* response) {
    if (!acquire_write_permit()) {
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many concurrent writes");
    }
    
    try {
        std::unique_ptr<rocksdb::Transaction> txn(db_->BeginTransaction(write_options_, txn_options_));
        
        rocksdb::Status status = txn->Put(request->key(), request->value());
        if (status.ok()) {
            status = txn->Commit();
        }
        
        if (status.ok()) {
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_error("Failed to put key-value: " + status.ToString());
        }
        
        release_write_permit();
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        release_write_permit();
        return grpc::Status(grpc::StatusCode::INTERNAL, 
                          "Exception: " + std::string(e.what()));
    }
}

grpc::Status KvStoreService::ProcessDelete(const kvstore::DeleteRequest* request,
                                           kvstore::DeleteResponse* response) {
    if (!acquire_write_permit()) {
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many concurrent writes");
    }
    
    try {
        std::unique_ptr<rocksdb::Transaction> txn(db_->BeginTransaction(write_options_, txn_options_));
        
        rocksdb::Status status = txn->Delete(request->key());
        if (status.ok()) {
            status = txn->Commit();
        }
        
        if (status.ok()) {
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_error("Failed to delete key: " + status.ToString());
        }
        
        release_write_permit();
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        release_write_permit();
        return grpc::Status(grpc::StatusCode::INTERNAL, 
                          "Exception: " + std::string(e.what()));
    }
}

grpc::Status KvStoreService::ProcessListKeys(const kvstore::ListKeysRequest* request,
                                             kvstore::ListKeysResponse* response) {
    if (!acquire_read_permit()) {
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many concurrent reads");
    }
    
    try {
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options_));
        
        int count = 0;
        int limit = request->limit();
        if (limit <= 0) {
            limit = 1000;  // Default limit
        }
        
        if (request->prefix().empty()) {
            // List all keys
            for (it->SeekToFirst(); it->Valid() && count < limit; it->Next()) {
                if (!it->status().ok()) {
                    release_read_permit();
                    return grpc::Status(grpc::StatusCode::INTERNAL, 
                                      "Iterator error: " + it->status().ToString());
                }
                response->add_keys(it->key().ToString());
                count++;
            }
        } else {
            // List keys with prefix
            for (it->Seek(request->prefix()); it->Valid() && count < limit; it->Next()) {
                if (!it->status().ok()) {
                    release_read_permit();
                    return grpc::Status(grpc::StatusCode::INTERNAL, 
                                      "Iterator error: " + it->status().ToString());
                }
                
                std::string key = it->key().ToString();
                if (key.substr(0, request->prefix().length()) != request->prefix()) {
                    break;  // No more keys with this prefix
                }
                
                response->add_keys(key);
                count++;
            }
        }
        
        if (!it->status().ok()) {
            release_read_permit();
            return grpc::Status(grpc::StatusCode::INTERNAL, 
                              "Iterator error: " + it->status().ToString());
        }
        
        release_read_permit();
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        release_read_permit();
        return grpc::Status(grpc::StatusCode::INTERNAL, 
                          "Exception: " + std::string(e.what()));
    }
}

grpc::Status KvStoreService::ProcessPing(const kvstore::PingRequest* request,
                                         kvstore::PingResponse* response) {
    try {
        int64_t server_timestamp = get_current_timestamp_micros();
        
        response->set_message(request->message());
        response->set_timestamp(request->timestamp());
        response->set_server_timestamp(server_timestamp);
        
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, 
                          "Exception: " + std::string(e.what()));
    }
}

void KvStoreService::Run(const std::string& server_address) {
    async_service_->server_impl_ = std::make_unique<ServerImpl>(this);
    async_service_->server_impl_->Run(server_address);
}
