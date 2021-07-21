use crate::ecc::client::EccClient;
use crate::ecc::get_ecc_settings;
use ecc_proto::ecc_rpc_server::{EccRpc, EccRpcServer};
use ecc_proto::{GetKeysReply, GetKeysRequest, GetReply, GetRequest, SetReply, SetRequest};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

pub mod ecc_proto {
  tonic::include_proto!("ecc_proto");
}

pub struct EccRpcService {
  id: usize,
  storage: RwLock<HashMap<String, String>>,
}

impl EccRpcService {
  pub async fn new(id: usize, recover: bool) -> Result<EccRpcService, Box<dyn std::error::Error>> {
    let res = EccRpcService {
      id,
      storage: RwLock::new(HashMap::new()),
    };

    if recover {
      res.recover().await;
    }
    Ok(res)
  }

  async fn recover(&self) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Probably want to recover while no transacations are in flight
    println!("RECOVER for {:?}", self.id);
    let (k, n, block_size, servers) = get_ecc_settings();
    let target = (self.id + 1) % servers.len();

    // Get all keys to fill
    let client = EccClient::new(k, n, block_size, servers.clone()).await;
    let keys = client.get_keys_once(servers[target].clone()).await?;

    // Get all values
    let mut storage = self.storage.write().await;
    for key in keys {
      let codeword = client.get_codeword(key.clone()).await?.unwrap();
      let parity = serde_json::to_string(&codeword[self.id]).unwrap();
      storage.insert(key, parity);
    }

    println!("RECOVERED {:?}", storage);
    Ok(())
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

  async fn get_keys(&self, _: Request<GetKeysRequest>) -> Result<Response<GetKeysReply>, Status> {
    let storage = self.storage.read().await;
    let keys = storage.keys().cloned().collect::<Vec<String>>();
    let keys = serde_json::to_string(&keys).unwrap();
    let reply = GetKeysReply { keys };
    Ok(Response::new(reply))
  }
}

pub async fn start_server(
  id: usize,
  addr: String,
  recover: bool,
) -> Result<(), Box<dyn std::error::Error>> {
  let addr = addr.parse().unwrap();
  let service = EccRpcService::new(id, recover).await?;
  println!("Starting ecc cache node at {:?}", addr);
  Server::builder()
    .add_service(EccRpcServer::new(service))
    .serve(addr)
    .await?;
  Ok(())
}

pub async fn start_many_servers(addresses: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
  let mut futures = Vec::new();
  for (id, addr) in addresses.clone().into_iter().enumerate() {
    let future = start_server(id, addr, false);
    futures.push(future);
  }
  join_all(futures).await;

  Ok(())
}
