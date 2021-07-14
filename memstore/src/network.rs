use crate::lib::ClientRequest;
use anyhow::Result;

use std::collections::HashMap;
// use std::sync::Arc;

use async_raft::async_trait::async_trait;
// use async_raft::config::Config;
use async_raft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use async_raft::{NodeId, RaftNetwork};

use tokio::sync::RwLock;

use raft_proto::raft_rpc_client::RaftRpcClient;
use raft_proto::{
  AppendEntriesRpcReply, AppendEntriesRpcRequest, InstallSnapshotRpcReply, InstallSnapshotRpcRequest, VoteRequestRpcReply, VoteRequestRpcRequest,
};

pub mod raft_proto {
  tonic::include_proto!("raft_proto");
}

/// A type which emulates a network transport and implements the `RaftNetwork` trait.
pub struct TonicgRPCNetwork {
  routing_table: RwLock<HashMap<NodeId, String>>,
}

impl TonicgRPCNetwork {
  /// Create a new instance.
  pub fn new() -> Self {
    Self { routing_table: Default::default() }
  }

  pub async fn add_route(&self, peer: NodeId, address: String) {
    let mut routing_table = self.routing_table.write().await;
    routing_table.insert(peer, address);
  }

  pub async fn get_route(&self, peer: NodeId) -> Result<String> {
    let routing_table = self.routing_table.write().await;
    Ok(routing_table.get(&peer).cloned().unwrap())
  }

  // pub async fn del_route(&self, peer: NodeId) {
  //   let mut routing_table = self.routing_table.write().await;
  //   routing_table.remove(&peer);
  // }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for TonicgRPCNetwork {
  /// Send an AppendEntries RPC to the target Raft node (§5).
  async fn append_entries(&self, target: NodeId, rpc: AppendEntriesRequest<ClientRequest>) -> Result<AppendEntriesResponse> {
    let address = self.get_route(target).await?;
    let address = format!("http://{}", address);

    // TODO Open Client once
    let mut client = RaftRpcClient::connect(address).await?;

    let serialized = serde_json::to_string(&rpc).unwrap();
    let request = tonic::Request::new(AppendEntriesRpcRequest { data: serialized.into() });

    let response = client.append_entries(request).await?;
    let serialized = response.into_inner().data;
    let deserialized: AppendEntriesResponse = serde_json::from_str(&serialized).unwrap();

    println!("RESPONSE={:?}", deserialized);

    Ok(deserialized)
  }

  /// Send an InstallSnapshot RPC to the target Raft node (§7).
  async fn install_snapshot(&self, target: NodeId, rpc: InstallSnapshotRequest) -> Result<InstallSnapshotResponse> {
    let address = self.get_route(target).await?;
    let address = format!("http://{}", address);

    // TODO Open Client once
    let mut client = RaftRpcClient::connect(address).await?;

    let serialized = serde_json::to_string(&rpc).unwrap();
    let request = tonic::Request::new(InstallSnapshotRpcRequest { data: serialized.into() });

    let response = client.install_snapshot(request).await?;
    let serialized = response.into_inner().data;
    let deserialized: InstallSnapshotResponse = serde_json::from_str(&serialized).unwrap();

    println!("RESPONSE={:?}", deserialized);

    Ok(deserialized)
  }

  /// Send a RequestVote RPC to the target Raft node (§5).
  async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
    let address = self.get_route(target).await?;
    let address = format!("http://{}", address);

    // TODO Open Client once
    let mut client = RaftRpcClient::connect(address).await?;
    let serialized = serde_json::to_string(&rpc).unwrap();
    let request = tonic::Request::new(VoteRequestRpcRequest { data: serialized.into() });

    let response = client.vote_request(request).await?;
    let serialized = response.into_inner().data;
    let deserialized: VoteResponse = serde_json::from_str(&serialized).unwrap();

    println!("RESPONSE={:?}", deserialized);

    Ok(deserialized)
  }
}
