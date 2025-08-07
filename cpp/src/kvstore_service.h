#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/alarm.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction_db.h>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

#include "kvstore.grpc.pb.h"

class KvStoreService final {
public:
    explicit KvStoreService(const std::string& db_path);
    ~KvStoreService();

    void Run(const std::string& server_address);
    
    // Process methods called by async handlers
    grpc::Status ProcessGet(const kvstore::GetRequest* request, kvstore::GetResponse* response);
    grpc::Status ProcessPut(const kvstore::PutRequest* request, kvstore::PutResponse* response);
    grpc::Status ProcessDelete(const kvstore::DeleteRequest* request, kvstore::DeleteResponse* response);
    grpc::Status ProcessListKeys(const kvstore::ListKeysRequest* request, kvstore::ListKeysResponse* response);
    grpc::Status ProcessPing(const kvstore::PingRequest* request, kvstore::PingResponse* response);

private:
    // Async service implementation
    class AsyncServiceImpl;
    std::unique_ptr<AsyncServiceImpl> async_service_;
    
    std::unique_ptr<rocksdb::TransactionDB> db_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;
    rocksdb::TransactionOptions txn_options_;
    
    // Concurrency control - using atomic for better performance
    static constexpr int MAX_READ_CONCURRENCY = 32;
    static constexpr int MAX_WRITE_CONCURRENCY = 16;
    
    std::atomic<int> active_reads_{0};
    std::atomic<int> active_writes_{0};
    
    bool acquire_read_permit();
    void release_read_permit();
    bool acquire_write_permit();
    void release_write_permit();
    
    int64_t get_current_timestamp_micros();
};
