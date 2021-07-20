#[macro_use]
extern crate simple_error;
use ecc_proto::ecc_rpc_client::EccRpcClient;
use ecc_proto::{GetReply, GetRequest, SetReply, SetRequest};
use futures::future::join_all;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::collections::HashMap;
use std::env;
use std::str;
use tonic::transport::Channel;

pub mod ecc_proto {
  tonic::include_proto!("ecc_proto");
}

pub struct EccClient {
  k: usize,
  n: usize,
  block_size: usize,
  message_size: usize,
  codeword_size: usize,
  ecc: reed_solomon_erasure::ReedSolomon<reed_solomon_erasure::galois_8::Field>,
  client_table: HashMap<String, EccRpcClient<Channel>>,
  servers: Vec<String>,
}

impl EccClient {
  pub async fn new(k: usize, n: usize, block_size: usize, servers: Vec<String>) -> EccClient {
    let mut client_table = HashMap::new();
    for addr in servers.clone() {
      let client = EccRpcClient::connect(format!("http://{}", addr))
        .await
        .unwrap();
      client_table.insert(addr, client);
    }
    let message_size = k * block_size;
    let codeword_size = n * block_size;
    let ecc = ReedSolomon::new(k, n - k).unwrap();
    println!("Maximum message size is {}", message_size);
    EccClient {
      k,
      n,
      block_size,
      message_size,
      ecc,
      codeword_size,
      client_table,
      servers,
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

  pub async fn write(&self, key: String, value: String) -> Result<(), Box<dyn std::error::Error>> {
    // convert value to array of bytes "[u8]"
    let mut bytes = value.into_bytes();

    // too long ...
    if bytes.len() > self.message_size.into() {
      bail!("message too long")
    } else {
      // pad with zeros
      let pad_size = self.codeword_size - bytes.len();
      let mut pad = vec![0; pad_size];
      bytes.append(&mut pad);

      // chunk into vec of vecs
      let mut codeword: Vec<Vec<u8>> = bytes.chunks(self.block_size).map(|x| x.to_vec()).collect();
      println!("{:?} 1", codeword);

      // calculate parity
      self.ecc.encode(&mut codeword).unwrap();
      println!("{:?} 2", codeword);

      // map to strings
      let codeword: Vec<String> = codeword
        .into_iter()
        .map(|x| serde_json::to_string(&x).unwrap())
        .collect();
      println!("{:?} 3", codeword);

      // Create futures and send to all nodes
      let mut futures = Vec::new();
      for (i, addr) in self.servers.iter().enumerate() {
        let future = self.write_once(addr.clone(), key.clone(), codeword[i].clone());
        futures.push(future);
      }
      join_all(futures).await;

      // let s = str::from_utf8(&bytes).unwrap();
      // println!("{:?}", s);
      Ok(())
    }
  }
  pub fn read(&self, key: String) -> String {
    unimplemented!()
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let mut servers: Vec<String> = env::args().collect();
  servers.remove(0);

  let k = 2;
  let n = 3;
  let block_size = 8;
  println!("{:?}", servers);

  let client = EccClient::new(k, n, block_size, servers.clone()).await;

  // client
  //   .write_once(servers[1].clone(), "yes".to_string(), "2".to_string())
  //   .await?;

  client
    .write("yes".to_string(), "&femkfjez".to_string())
    .await;
  // let mut converter = convert::Convert::new(2, 256);
  // let output = converter.convert::<u8, u8>(&vec![1, 1, 1, 1, 1, 1, 0, 1, 1]);
  // println!("{:?}", output);

  Ok(())
}
