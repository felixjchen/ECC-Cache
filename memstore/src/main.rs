mod lib;
mod network;
mod server;
use async_raft::config::Config;
use async_raft::{NodeId, Raft};
use futures::join;
use std::collections::HashSet;
use std::sync::Arc;

#[tokio::main]
async fn main() {
  let config1 = Arc::new(
    Config::build("primary-raft-group".into())
      .validate()
      .expect("failed to build Raft config"),
  );
  let config2 = Arc::new(
    Config::build("primary-raft-group".into())
      .validate()
      .expect("failed to build Raft config"),
  );

  let node_1: NodeId = 1;
  let node_1_addr = "0.0.0.0:5001".to_string();
  let node_2: NodeId = 2;
  let node_2_addr = "0.0.0.0:5002".to_string();

  let mut members = HashSet::new();
  members.insert(node_1);
  members.insert(node_2);

  let network1 = Arc::new(network::TonicgRPCNetwork::new());
  let network2 = Arc::new(network::TonicgRPCNetwork::new());
  network1.add_route(node_1, node_1_addr.clone()).await;
  network1.add_route(node_2, node_2_addr.clone()).await;

  network2.add_route(node_1, node_1_addr.clone()).await;
  network2.add_route(node_2, node_2_addr.clone()).await;

  let storage1 = Arc::new(lib::MemStore::new(node_1));
  let storage2 = Arc::new(lib::MemStore::new(node_2));
  let raft1 = Raft::new(node_1, config1, network1, storage1.clone());
  let raft2 = Raft::new(node_2, config2, network2, storage2.clone());

  let r1 = raft1.initialize(members.clone()).await;
  let r2 = raft2.initialize(members.clone()).await;
  println!("{:?} {:?}", r1, r2);

  let s1 = server::start_server(raft1, storage1, node_1_addr.clone());
  let s2 = server::start_server(raft2, storage2, node_2_addr.clone());

  join!(s1, s2);
}
