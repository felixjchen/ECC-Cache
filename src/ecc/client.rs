use ecc_proto::ecc_rpc_client::EccRpcClient;
use ecc_proto::{GetReply, GetRequest, SetReply, SetRequest};
use std::collections::HashMap;
use std::env;
use tonic::transport::Channel;

pub mod ecc_proto {
  tonic::include_proto!("ecc_proto");
}

pub struct EccClient {
  k: u8,
  n: u8,
  block_size: u8,
  client_table: HashMap<String, EccRpcClient<Channel>>,
}

impl EccClient {
  pub async fn new(k: u8, n: u8, block_size: u8, servers: Vec<String>) -> EccClient {
    let mut client_table = HashMap::new();
    for addr in servers {
      let client = EccRpcClient::connect(format!("http://{}", addr))
        .await
        .unwrap();
      client_table.insert(addr, client);
    }
    println!("Maximum value size is {}", k * block_size);
    EccClient {
      k,
      n,
      block_size,
      client_table,
    }
  }

  async fn write_once(
    &self,
    address: String,
    key: String,
    value: String,
  ) -> Result<(), Box<dyn std::error::Error>> {
    println!("{:?}", self.client_table);
    let mut client = self.client_table.get(&address).map(|c| c.clone()).unwrap();

    let request = tonic::Request::new(SetRequest { key, value });
    client.set(request).await?;
    Ok(())
  }

  pub fn write(&self, key: String, value: String) {
    // convert value to
  }
  pub fn read(&self, key: String) -> String {
    unimplemented!()
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let mut servers: Vec<String> = env::args().collect();
  servers.remove(0);

  let k = 1;
  let n = 2;
  let block_size = 16;
  println!("{:?}", servers);

  let client = EccClient::new(k, n, block_size, servers.clone()).await;

  client
    .write_once(servers[1].clone(), "yes".to_string(), "2".to_string())
    .await?;

  Ok(())
}
