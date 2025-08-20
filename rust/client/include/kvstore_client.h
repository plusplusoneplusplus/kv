#ifndef KVSTORE_CLIENT_H
#define KVSTORE_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

// Opaque handle types
typedef void* KvClientHandle;
typedef void* KvTransactionHandle;
typedef void* KvReadTransactionHandle;
typedef void* KvFutureHandle;

// Error codes
typedef enum {
    KV_SUCCESS = 0,
    KV_ERROR_TRANSPORT = 1000,
    KV_ERROR_PROTOCOL = 1001,
    KV_ERROR_TRANSACTION_NOT_FOUND = 2000,
    KV_ERROR_TRANSACTION_CONFLICT = 2001,
    KV_ERROR_TRANSACTION_TIMEOUT = 2002,
    KV_ERROR_INVALID_KEY = 3000,
    KV_ERROR_INVALID_VALUE = 3001,
    KV_ERROR_NETWORK = 4000,
    KV_ERROR_SERVER = 5000,
    KV_ERROR_UNKNOWN = 9999
} KvErrorCode;

// Result structure
typedef struct {
    int success;           // 1 for success, 0 for failure
    int error_code;        // Error code from KvErrorCode enum
    char* error_message;   // Error message (must be freed with kv_result_free)
} KvResult;

// Key-value pair structure
typedef struct {
    char* key;     // Must be freed with kv_string_free
    char* value;   // Must be freed with kv_string_free
} KvPair;

// Array of key-value pairs for range operations
typedef struct {
    KvPair* pairs;  // Array of pairs (must be freed with kv_pair_array_free)
    size_t count;   // Number of pairs in the array
} KvPairArray;

// Library initialization and cleanup
int kv_init(void);
void kv_shutdown(void);

// Client management
KvClientHandle kv_client_create(const char* address);
void kv_client_destroy(KvClientHandle client);

// Transaction management
KvFutureHandle kv_transaction_begin(KvClientHandle client, int timeout_seconds);
KvFutureHandle kv_read_transaction_begin(KvClientHandle client, int64_t read_version);

// Future operations
int kv_future_poll(KvFutureHandle future);  // Returns: 1=ready, 0=pending, -1=error
KvTransactionHandle kv_future_get_transaction(KvFutureHandle future);
KvReadTransactionHandle kv_future_get_read_transaction(KvFutureHandle future);
KvResult kv_future_get_void_result(KvFutureHandle future);
KvResult kv_future_get_string_result(KvFutureHandle future, char** value);
KvResult kv_future_get_range_result(KvFutureHandle future, KvPairArray* pairs);

// Transaction operations
KvFutureHandle kv_transaction_get(KvTransactionHandle transaction, const char* key, const char* column_family);
KvFutureHandle kv_transaction_set(KvTransactionHandle transaction, const char* key, const char* value, const char* column_family);
KvFutureHandle kv_transaction_delete(KvTransactionHandle transaction, const char* key, const char* column_family);
KvFutureHandle kv_transaction_get_range(KvTransactionHandle transaction, const char* start_key, const char* end_key, int limit, const char* column_family);
KvFutureHandle kv_transaction_commit(KvTransactionHandle transaction);
KvFutureHandle kv_transaction_abort(KvTransactionHandle transaction);

// Read transaction operations
KvFutureHandle kv_read_transaction_get(KvReadTransactionHandle transaction, const char* key, const char* column_family);
KvFutureHandle kv_read_transaction_get_range(KvReadTransactionHandle transaction, const char* start_key, const char* end_key, int limit, const char* column_family);
void kv_read_transaction_destroy(KvReadTransactionHandle transaction);

// Conflict detection
KvFutureHandle kv_transaction_add_read_conflict(KvTransactionHandle transaction, const char* key, const char* column_family);
KvFutureHandle kv_transaction_add_read_conflict_range(KvTransactionHandle transaction, const char* start_key, const char* end_key, const char* column_family);

// Versionstamped operations
KvFutureHandle kv_transaction_set_versionstamped_key(KvTransactionHandle transaction, const char* key_prefix, const char* value, const char* column_family);
KvFutureHandle kv_transaction_set_versionstamped_value(KvTransactionHandle transaction, const char* key, const char* value_prefix, const char* column_family);

// Utility functions
void kv_string_free(char* s);
void kv_result_free(KvResult* result);
void kv_pair_array_free(KvPairArray* pairs);

// Health check
KvFutureHandle kv_client_ping(KvClientHandle client, const char* message);

// Synchronous convenience functions (blocking wrappers)
KvResult kv_transaction_get_sync(KvTransactionHandle transaction, const char* key, const char* column_family, char** value);
KvResult kv_transaction_set_sync(KvTransactionHandle transaction, const char* key, const char* value, const char* column_family);
KvResult kv_transaction_delete_sync(KvTransactionHandle transaction, const char* key, const char* column_family);
KvResult kv_transaction_commit_sync(KvTransactionHandle transaction);

// Connection pool support (for high-throughput applications)
typedef void* KvClientPoolHandle;

KvClientPoolHandle kv_client_pool_create(const char* address, size_t pool_size);
void kv_client_pool_destroy(KvClientPoolHandle pool);
KvFutureHandle kv_client_pool_transaction_begin(KvClientPoolHandle pool, int timeout_seconds);
KvFutureHandle kv_client_pool_read_transaction_begin(KvClientPoolHandle pool, int64_t read_version);

#ifdef __cplusplus
}
#endif

#endif // KVSTORE_CLIENT_H