use raft_proto::raft_rpc_client::RaftRpcClient;
use raft_proto::{ClientReadRpcRequest, ClientWriteRpcRequest};

pub mod raft_proto {
  tonic::include_proto!("raft_proto");
}
