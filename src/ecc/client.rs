use ecc_proto::ecc_rpc_client::EccRpcClient;
use ecc_proto::{GetReply, GetRequest, SetReply, SetRequest};

pub mod ecc_proto {
  tonic::include_proto!("ecc_proto");
}

pub struct EccClient {
  k: u8,
  n: u8,
  block_size: u8,
  node_address: Vec<String>,
}

impl EccClient {
  pub fn new(k: u8, n: u8, block_size: u8, node_address: Vec<String>) -> EccClient {
    EccClient {
      k,
      n,
      block_size,
      node_address,
    }
  }

  pub fn read(key: String) -> String {
    unimplemented!()
  }

  pub fn write(key: String, value: String) {
    unimplemented!()
  }
}
