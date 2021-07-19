use ecc_proto::ecc_rpc_client::EccRpcClient;
use ecc_proto::{GetReply, GetRequest, SetReply, SetRequest};

pub mod ecc_proto {
  tonic::include_proto!("ecc_proto");
}

pub struct EccClient {}

impl EccClient {
  pub fn new() -> EccClient {
    EccClient
  }
}
