use crate::raft::network;
use crate::raft::server;
use crate::raft::storage;
use async_raft::config::{Config};
use async_raft::{NodeId, Raft};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub async fn start_raft(
  id: u64,
  node_ids: Vec<NodeId>,
  servers: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
  let mut members = HashSet::new();
  let mut routing_table = HashMap::new();
  for (i, &id) in node_ids.iter().enumerate() {
    members.insert(id);
    routing_table.insert(id, servers[i].to_string());
  }

  let network = Arc::new(network::TonicgRPCNetwork::new(routing_table.clone()));
  let storage = Arc::new(storage::MemStore::new(id));
  let config = Arc::new(
    Config::build("primary-raft-group".into())
      .validate()
      .expect("failed to build Raft config"),
  );

  let raft = Raft::new(id, config.clone(), network.clone(), storage.clone());
  raft.initialize(members.clone()).await?;
  server::start_server(
    raft,
    storage.clone(),
    routing_table.get(&id).unwrap().clone(),
  )
  .await?;
  Ok(())
}

pub async fn start_rafts(
  node_ids: Vec<NodeId>,
  servers: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
  let mut members = HashSet::new();
  let mut routing_table = HashMap::new();
  for (i, &id) in node_ids.iter().enumerate() {
    members.insert(id);
    routing_table.insert(id, servers[i].to_string());
  }

  // Create storages and networks
  let mut networks = HashMap::new();
  let mut storages = HashMap::new();
  for id in node_ids.clone() {
    let network = Arc::new(network::TonicgRPCNetwork::new(routing_table.clone()));
    networks.insert(id, network);
    let storage = Arc::new(storage::MemStore::new(id));
    storages.insert(id, storage);
  }

  // Start rafts
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

  // Intialize rafts
  let mut futures = Vec::new();
  for raft in rafts.values() {
    let future = raft.initialize(members.clone());
    futures.push(future);
  }
  join_all(futures).await;

  // Await all servers
  let mut futures = Vec::new();
  for (id, raft) in rafts.into_iter() {
    let future = server::start_server(
      raft,
      storages.get(&id).unwrap().clone(),
      routing_table.get(&id).unwrap().clone(),
    );
    futures.push(future);
  }

  join_all(futures).await;
  Ok(())
}
