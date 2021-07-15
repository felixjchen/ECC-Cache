mod lib;
mod network;
mod server;
use async_raft::config::Config;
use async_raft::{NodeId, Raft};
use futures::future::join_all;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[tokio::main]
async fn main() {
  let node_ids: [NodeId; 4] = [1, 2, 3, 4];
  let addresses = [
    "0.0.0.0:5001",
    "0.0.0.0:5002",
    "0.0.0.0:5003",
    "0.0.0.0:5004",
  ];
  let mut members = HashSet::new();
  let mut routing_table = HashMap::new();
  for (i, &id) in node_ids.iter().enumerate() {
    members.insert(id);
    routing_table.insert(id, addresses[i].to_string());
  }
  println!("{:?} ", members);
  println!("{:?} ", routing_table);

  // Create storages and networks
  let mut networks = HashMap::new();
  let mut storages = HashMap::new();
  for id in node_ids {
    let network = Arc::new(network::TonicgRPCNetwork::new(routing_table.clone()));
    networks.insert(id, network);
    let storage = Arc::new(lib::MemStore::new(id));
    storages.insert(id, storage);
  }

  let mut rafts: HashMap<NodeId, server::MyRaft> = HashMap::new();
  let config = Arc::new(
    Config::build("primary-raft-group".into())
      .validate()
      .expect("failed to build Raft config"),
  );
  for id in node_ids {
    let raft = Raft::new(
      id,
      config.clone(),
      networks.get(&id).unwrap().clone(),
      storages.get(&id).unwrap().clone(),
    );
    rafts.insert(id, raft);
  }

  // intialize
  for raft in rafts.values() {
    raft.initialize(members.clone()).await;
  }

  // await all servers
  let mut server_futures = Vec::new();
  for (id, raft) in rafts.into_iter() {
    let future = server::start_server(
      raft,
      storages.get(&id).unwrap().clone(),
      routing_table.get(&id).unwrap().clone(),
    );
    server_futures.push(future);
  }

  join_all(server_futures).await;
}
