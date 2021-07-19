use ecc_proto::ecc_rpc_server::{EccRpc, EccRpcServer};
use ecc_proto::{GetReply, GetRequest, SetReply, SetRequest};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

pub mod ecc_proto {
  tonic::include_proto!("ecc_proto");
}

pub struct EccRpcService {
  storage: RwLock<HashMap<String, String>>,
}

impl EccRpcService {
  pub fn new() -> EccRpcService {
    EccRpcService {
      storage: RwLock::new(HashMap::new()),
    }
  }
}

#[tonic::async_trait]
impl EccRpc for EccRpcService {
  async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetReply>, Status> {
    let request = request.into_inner();
    println!("Got a set request: {:?}", request.clone());
    let key = request.key;
    let value = request.value;

    let mut storage = self.storage.write().await;
    storage.insert(key, value);

    let reply = SetReply {
      status: "success".into(),
    };

    Ok(Response::new(reply))
  }
  async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
    let request = request.into_inner();
    println!("Got a get request: {:?}", request.clone());
    let key = request.key;

    let storage = self.storage.read().await;
    let value = storage.get(&key).map(|val| val.clone());

    let reply = GetReply { value };

    Ok(Response::new(reply))
  }
}

pub async fn start_server(address: String) {
  let addr = address.parse().unwrap();
  let service = EccRpcService::new();
  Server::builder()
    .add_service(EccRpcServer::new(service))
    .serve(addr)
    .await;
}
