use crate::ecc::client::EccClient;
// use crate::ecc::get_ecc_settings;
use ecc_proto::ecc_rpc_server::{EccRpc, EccRpcServer};
use ecc_proto::*;
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tonic::{transport::Server, Request, Response, Status};

pub mod ecc_proto {
  tonic::include_proto!("ecc_proto");
}

enum State {
  Ready,
  NotReady,
}

// Write ahead log lock
#[derive(Clone)]
struct Lock {
  lock: String,
  value: String,
}

pub struct EccRpcService {
  id: usize,
  servers: Vec<String>,
  state: State,
  storage: RwLock<HashMap<String, String>>,
  lock_table: RwLock<HashMap<String, Lock>>,
  healthy_servers: RwLock<HashSet<String>>,
  client: RwLock<EccClient>,
}

impl EccRpcService {
  pub async fn new(
    id: usize,
    servers: Vec<String>,
    recover: bool,
  ) -> Result<EccRpcService, Box<dyn std::error::Error>> {
    let client = EccClient::new().await;
    let res = EccRpcService {
      id,
      servers,
      healthy_servers: Default::default(),
      state: State::Ready,
      storage: Default::default(),
      lock_table: Default::default(),
      client: RwLock::new(client),
    };

    if recover {
      res.recover().await?;
    }

    Ok(res)
  }

  async fn recover(&self) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Probably want to recover while no transacations are in flight
    println!("RECOVER for {:?}", self.id);
    let target = (self.id + 1) % self.servers.len();
    let client = self.client.write().await;

    // Get all keys to fill
    let keys = client.get_keys_once(self.servers[target].clone()).await?;

    // Get all values
    let mut storage = self.storage.write().await;
    for key in keys {
      let codeword = client.get_codeword(key.clone()).await?.unwrap();
      let parity = serde_json::to_string(&codeword[self.id]).unwrap();
      storage.insert(key, parity);
    }
    Ok(())
  }

  async fn get_cluster_status(&self) {
    let mut client = self.client.write().await;
    let healthy_servers_new = client.send_heartbeats().await;
    let mut healthy_servers = self.healthy_servers.write().await;

    // Only if healthy servers changed
    if healthy_servers
      .symmetric_difference(&healthy_servers_new)
      .into_iter()
      .count()
      > 0
    {
      *healthy_servers = healthy_servers_new;
    }
  }
}

#[tonic::async_trait]
impl EccRpc for Arc<EccRpcService> {
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

  async fn heartbeat(
    &self,
    request: Request<HeartbeatRequest>,
  ) -> Result<Response<HeartbeatReply>, Status> {
    let state = match self.state {
      State::NotReady => "NotReady".to_string(),
      State::Ready => "Ready".to_string(),
    };
    Ok(Response::new(HeartbeatReply { state }))
  }

  async fn prepare(
    &self,
    request: Request<PrepareRequest>,
  ) -> Result<Response<PrepareReply>, Status> {
    let request = request.into_inner();
    println!("Got a prepare request: {:?}", request.clone());
    let tid = request.tid;
    let value = request.value;
    let key = request.key;

    let mut lock_table = self.lock_table.write().await;
    let healthy_servers = self.healthy_servers.read().await.clone();
    let healthy_servers = serde_json::to_string(&healthy_servers).unwrap();

    // Grant lock if : 1. Not locked OR 2. TID matches
    let grant_lock = match lock_table.get(&key) {
      Some(existing_lock) => {
        existing_lock.clone().lock == tid && existing_lock.clone().value == value
      }
      None => true,
    };

    if grant_lock {
      let lock = Lock { lock: tid, value };
      lock_table.insert(key, lock);

      let reply = PrepareReply {
        lock_acquired: true,
        healthy_servers,
      };
      Ok(Response::new(reply))
    } else {
      let reply = PrepareReply {
        lock_acquired: false,
        healthy_servers,
      };
      Ok(Response::new(reply))
    }
  }

  // Commit transacation to storage
  async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitReply>, Status> {
    let request = request.into_inner();
    println!("Got a commit request: {:?}", request.clone());
    let tid = request.tid;
    let key = request.key;

    let mut lock_table = self.lock_table.write().await;
    let mut storage = self.storage.write().await;

    // insert into kv store
    let new_value = lock_table.get(&key).unwrap().clone().value;
    storage.insert(key.clone(), new_value);

    // free lock
    lock_table.remove(&key);

    let reply = CommitReply { success: true };
    Ok(Response::new(reply))
  }

  // Abort new transaction
  async fn abort(&self, request: Request<AbortRequest>) -> Result<Response<AbortReply>, Status> {
    let request = request.into_inner();
    println!("Got an abort request: {:?}", request.clone());
    let tid = request.tid;
    let key = request.key;

    let mut lock_table = self.lock_table.write().await;
    let can_abort = match lock_table.get(&key) {
      Some(existing_lock) => existing_lock.clone().lock == tid,
      None => false,
    };

    if can_abort {
      lock_table.remove(&key);
      let reply = AbortReply { success: true };
      Ok(Response::new(reply))
    } else {
      let reply = AbortReply { success: false };
      Ok(Response::new(reply))
    }
  }
}

pub async fn start_server(
  id: usize,
  addr: String,
  servers: Vec<String>,
  heartbeat_timeout_ms: usize,
  recover: bool,
) -> Result<(), Box<dyn std::error::Error>> {
  let addr = addr.parse().unwrap();
  let service = EccRpcService::new(id, servers, recover).await?;
  println!("Starting ecc cache node at {:?}", addr);

  let service = Arc::new(service);

  let server_future = Server::builder()
    .add_service(EccRpcServer::new(service.clone()))
    .serve(addr);

  let handle = Handle::current();
  handle.spawn(async move {
    loop {
      service.get_cluster_status().await;
      sleep(Duration::from_millis(heartbeat_timeout_ms as u64)).await;
      println!("{:?}", service.healthy_servers.read().await);
    }
  });

  server_future.await?;
  Ok(())
}

pub async fn start_many_servers(
  servers: Vec<String>,
  heartbeat_timeout_ms: usize,
) -> Result<(), Box<dyn std::error::Error>> {
  let mut futures = Vec::new();
  for (id, addr) in servers.clone().into_iter().enumerate() {
    let future = start_server(id, addr, servers.clone(), heartbeat_timeout_ms, false);
    futures.push(future);
  }
  join_all(futures).await;

  Ok(())
}
