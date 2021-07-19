use ecc_proto::ecc_rpc_client::EccRpcClient;
use ecc_proto::{GetReply, GetRequest, SetReply, SetRequest};

pub mod ecc_proto {
  tonic::include_proto!("ecc_proto");
}

pub struct EccClient {
  k: u8,
  n: u8,
  block_size: u8,
}

impl EccClient {
  pub fn new() -> EccClient {
    EccClient {}
  }

  pub fn read(key: String) -> String {

  }

  pub write(key: String, value: String) {

  }
}
