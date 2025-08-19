use tonic::{Request, Response, Status};
use crate::db::TransactionalKvDatabase;
use crate::kvstore::{
    GetRequest, GetResponse, PutRequest, PutResponse, DeleteRequest, DeleteResponse,
    ListKeysRequest, ListKeysResponse, PingRequest, PingResponse,
};
use crate::kvstore::kv_store_server::KvStore;

pub struct KvStoreGrpcService {
    db: TransactionalKvDatabase,
}

impl KvStoreGrpcService {
    pub fn new(db: TransactionalKvDatabase) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl KvStore for KvStoreGrpcService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        
        match self.db.get(&req.key).await {
            Ok(result) => Ok(Response::new(GetResponse {
                value: result.value,
                found: result.found,
            })),
            Err(e) => Err(Status::invalid_argument(e)),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        
        let result = self.db.put(&req.key, &req.value).await;
        Ok(Response::new(PutResponse {
            success: result.success,
            error: result.error,
        }))
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        
        let result = self.db.delete(&req.key).await;
        Ok(Response::new(DeleteResponse {
            success: result.success,
            error: result.error,
        }))
    }

    async fn list_keys(&self, request: Request<ListKeysRequest>) -> Result<Response<ListKeysResponse>, Status> {
        let req = request.into_inner();
        
        let limit = if req.limit >= 0 { req.limit as u32 } else { 0 };
        match self.db.list_keys(&req.prefix, limit).await {
            Ok(keys) => Ok(Response::new(ListKeysResponse { keys })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        
        // Get current timestamp in microseconds
        let server_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::internal("failed to get system time"))?
            .as_micros() as i64;
        
        // Echo back the message with timestamps
        Ok(Response::new(PingResponse {
            message: req.message,
            timestamp: req.timestamp,
            server_timestamp,
        }))
    }
}