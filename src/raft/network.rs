use crate::raft::storage::ClientRequest;
use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::raft::{
  AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
  VoteRequest, VoteResponse,
};
use async_raft::{NodeId, RaftNetwork};
use raft_proto::raft_rpc_client::RaftRpcClient;
use raft_proto::{AppendEntriesRpcRequest, InstallSnapshotRpcRequest, VoteRequestRpcRequest};
use std::collections::HashMap;
use std::env;
use tokio::sync::RwLock;
use tonic::transport::Channel;

pub mod raft_proto {
  tonic::include_proto!("raft_proto");
}

/// A type which emulates a network transport and implements the `RaftNetwork` trait.
pub struct TonicgRPCNetwork {
  routing_table: RwLock<HashMap<NodeId, String>>,
  client_table: RwLock<HashMap<NodeId, RaftRpcClient<Channel>>>,
}

impl TonicgRPCNetwork {
  /// Create a new instance.
  pub fn new(routing_table: HashMap<NodeId, String>) -> Self {
    let routing_table = RwLock::new(routing_table);
    let client_table = Default::default();
    Self {
      routing_table,
      client_table,
    }
  }

  #[allow(dead_code)]
  pub async fn add_route(&self, peer: NodeId, address: String) {
    let mut routing_table = self.routing_table.write().await;
    routing_table.insert(peer, address);
  }

  pub async fn get_route(&self, peer: NodeId) -> Result<String> {
    let routing_table = self.routing_table.write().await;
    Ok(routing_table.get(&peer).cloned().unwrap())
  }

  pub async fn get_client(&self, peer: NodeId) -> RaftRpcClient<Channel> {
    let mut client_table = self.client_table.write().await;

    // Need to create connection
    if !client_table.contains_key(&peer) {
      let address = self.get_route(peer).await.unwrap();

      let address = match env::var_os("DOCKER_HOSTNAME") {
        Some(hostname) => format!(
          "http://{}",
          address.replace("0.0.0.0", &hostname.into_string().unwrap())
        ),
        None => format!("http://{}", address),
      };

      let client = RaftRpcClient::connect(address).await.unwrap();
      client_table.insert(peer, client);
    }

    // Return connection
    client_table.get(&peer).map(|c| c.clone()).unwrap()
  }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for TonicgRPCNetwork {
  /// Send an AppendEntries RPC to the target Raft node (§5).
  async fn append_entries(
    &self,
    target: NodeId,
    rpc: AppendEntriesRequest<ClientRequest>,
  ) -> Result<AppendEntriesResponse> {
    let mut client = self.get_client(target).await;

    let serialized = serde_json::to_string(&rpc).unwrap();
    let request = tonic::Request::new(AppendEntriesRpcRequest {
      data: serialized.into(),
    });

    let response = client.append_entries(request).await?;
    let serialized = response.into_inner().data;
    let deserialized: AppendEntriesResponse = serde_json::from_str(&serialized).unwrap();

    Ok(deserialized)
  }

  /// Send an InstallSnapshot RPC to the target Raft node (§7).
  async fn install_snapshot(
    &self,
    target: NodeId,
    rpc: InstallSnapshotRequest,
  ) -> Result<InstallSnapshotResponse> {
    let mut client = self.get_client(target).await;

    let serialized = serde_json::to_string(&rpc).unwrap();
    let request = tonic::Request::new(InstallSnapshotRpcRequest {
      data: serialized.into(),
    });

    let response = client.install_snapshot(request).await?;
    let serialized = response.into_inner().data;
    let deserialized: InstallSnapshotResponse = serde_json::from_str(&serialized).unwrap();

    Ok(deserialized)
  }

  /// Send a RequestVote RPC to the target Raft node (§5).
  async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
    let mut client = self.get_client(target).await;
    let serialized = serde_json::to_string(&rpc).unwrap();

    let request = tonic::Request::new(VoteRequestRpcRequest {
      data: serialized.into(),
    });
    let response = client.vote_request(request).await?;
    let serialized = response.into_inner().data;
    let deserialized: VoteResponse = serde_json::from_str(&serialized).unwrap();

    Ok(deserialized)
  }
}
