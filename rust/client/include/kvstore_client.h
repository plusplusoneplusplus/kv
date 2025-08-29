#ifndef KVSTORE_CLIENT_H
#define KVSTORE_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>
#include <string.h>

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

// Binary data structure (following FoundationDB pattern)
typedef struct {
    uint8_t* data;    // Binary data (must be freed with kv_binary_free)
    int length;       // Length in bytes
} KvBinaryData;

// Key-value pair structure with binary support
typedef struct {
    KvBinaryData key;      // Binary key data
    KvBinaryData value;    // Binary value data
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
KvResult kv_future_get_value_result(KvFutureHandle future, KvBinaryData* value);
KvResult kv_future_get_kv_array_result(KvFutureHandle future, KvPairArray* pairs);

// Transaction operations (FoundationDB-style binary interface)
KvFutureHandle kv_transaction_get(KvTransactionHandle transaction,
                                  const uint8_t* key_data, int key_length,
                                  const char* column_family);
KvFutureHandle kv_transaction_set(KvTransactionHandle transaction,
                                  const uint8_t* key_data, int key_length,
                                  const uint8_t* value_data, int value_length,
                                  const char* column_family);
KvFutureHandle kv_transaction_delete(KvTransactionHandle transaction,
                                     const uint8_t* key_data, int key_length,
                                     const char* column_family);
KvFutureHandle kv_transaction_get_range(KvTransactionHandle transaction,
                                        const uint8_t* start_key_data, int start_key_length,
                                        const uint8_t* end_key_data, int end_key_length,
                                        int limit, const char* column_family);
KvFutureHandle kv_transaction_commit(KvTransactionHandle transaction);
KvFutureHandle kv_transaction_abort(KvTransactionHandle transaction);

// Read transaction operations (FoundationDB-style binary interface)
KvFutureHandle kv_read_transaction_get(KvReadTransactionHandle transaction,
                                       const uint8_t* key_data, int key_length,
                                       const char* column_family);
KvFutureHandle kv_read_transaction_get_range(KvReadTransactionHandle transaction,
                                             const uint8_t* start_key_data, int start_key_length,
                                             const uint8_t* end_key_data, int end_key_length,
                                             int limit, const char* column_family);
void kv_read_transaction_destroy(KvReadTransactionHandle transaction);

// Conflict detection (FoundationDB-style binary interface)
KvFutureHandle kv_transaction_add_read_conflict(KvTransactionHandle transaction,
                                                const uint8_t* key_data, int key_length,
                                                const char* column_family);
KvFutureHandle kv_transaction_add_read_conflict_range(KvTransactionHandle transaction,
                                                      const uint8_t* start_key_data, int start_key_length,
                                                      const uint8_t* end_key_data, int end_key_length,
                                                      const char* column_family);

// Versionstamped operations (FoundationDB-style binary interface)
KvFutureHandle kv_transaction_set_versionstamped_key(KvTransactionHandle transaction,
                                                     const uint8_t* key_prefix_data, int key_prefix_length,
                                                     const uint8_t* value_data, int value_length,
                                                     const char* column_family);
KvFutureHandle kv_transaction_set_versionstamped_value(KvTransactionHandle transaction,
                                                       const uint8_t* key_data, int key_length,
                                                       const uint8_t* value_prefix_data, int value_prefix_length,
                                                       const char* column_family);

// Utility functions
void kv_binary_free(KvBinaryData* data);        // Free binary data allocated by the library
void kv_result_free(KvResult* result);          // Free result structure
void kv_pair_array_free(KvPairArray* pairs);    // Free array of key-value pairs

// Convenience functions for string operations
KvBinaryData kv_binary_from_string(const char* str);  // Create binary data from null-terminated string
KvBinaryData kv_binary_copy(const uint8_t* data, int length);  // Create copy of binary data

// Convenience macros for string literals (following FoundationDB pattern)
#define KV_STR(str) ((const uint8_t*)(str)), ((int)strlen(str))
#define KV_BIN(data, len) ((const uint8_t*)(data)), (len)

// Example usage:
// kv_transaction_set(tx, KV_STR("my_key"), KV_STR("my_value"), NULL);
// kv_transaction_set(tx, KV_BIN(binary_key, key_len), KV_BIN(binary_value, value_len), NULL);

// Health check (FoundationDB-style binary interface)
KvFutureHandle kv_client_ping(KvClientHandle client,
                              const uint8_t* message_data, int message_length);

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