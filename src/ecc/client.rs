#[macro_use]
extern crate simple_error;
use ecc_proto::ecc_rpc_client::EccRpcClient;
use ecc_proto::{GetReply, GetRequest, SetReply, SetRequest};
use futures::future::join_all;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::collections::HashMap;
use std::env;
use std::str;
use std::time::Duration;
use tokio::time::sleep;
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
  servers: Vec<String>,
  index_table: HashMap<String, usize>,
  client_table: HashMap<String, Option<EccRpcClient<Channel>>>,
}

impl EccClient {
  pub async fn new(k: usize, n: usize, block_size: usize, servers: Vec<String>) -> EccClient {
    let mut client_table = HashMap::new();
    let mut index_table = HashMap::new();
    for (i, addr) in servers.clone().into_iter().enumerate() {
      let client = EccRpcClient::connect(format!("http://{}", addr)).await;
      let client = match client {
        Ok(i) => Some(i),
        _ => None,
      };
      client_table.insert(addr.clone(), client);
      index_table.insert(addr, i);
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
      index_table,
      servers,
    }
  }

  async fn write_once(
    &self,
    address: String,
    key: String,
    value: String,
  ) -> Result<(), Box<dyn std::error::Error>> {
    println!("Set {:?} {:?} {:?}", address, key, value);
    let client = self.client_table.get(&address).map(|c| c.clone()).unwrap();

    match client {
      Some(mut client) => {
        let request = tonic::Request::new(SetRequest { key, value });
        client.set(request).await?;
      }
      _ => {
        println!("Ignoring {:?} as client could not connect", address)
      }
    }
    Ok(())
  }

  async fn get_once(
    &self,
    address: String,
    key: String,
  ) -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    let client = self.client_table.get(&address).map(|c| c.clone()).unwrap();

    match client {
      Some(mut client) => {
        let request = tonic::Request::new(GetRequest { key: key.clone() });
        let response = client.get(request).await?;
        let value = response.into_inner().value;
        println!("Get {:?} {:?} {:?}", address, key, value);
        Ok((address, value))
      }
      _ => {
        sleep(Duration::from_millis(10)).await;
        bail!("Ignoring {:?} as client could not connect", address)
      }
    }
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

      // calculate parity
      self.ecc.encode(&mut codeword).unwrap();

      // map to strings
      let codeword: Vec<String> = codeword
        .into_iter()
        .map(|x| serde_json::to_string(&x).unwrap())
        .collect();

      // Create futures and send to all nodes
      let mut futures = Vec::new();
      for (i, addr) in self.servers.iter().enumerate() {
        let future = self.write_once(addr.clone(), key.clone(), codeword[i].clone());
        futures.push(future);
      }
      join_all(futures).await;

      Ok(())
    }
  }

  pub async fn read(&self, key: String) -> Result<String, Box<dyn std::error::Error>> {
    // Get first k responses
    let mut futures = Vec::new();
    for addr in self.servers.clone() {
      let future = self.get_once(addr.clone(), key.clone());
      futures.push(future);
    }
    let futures = futures.into_iter().collect::<FuturesUnordered<_>>();
    let first_k = futures.take(self.k).collect::<Vec<_>>().await;

    // Empty codeword
    let mut codeword: Vec<Option<Vec<u8>>> = vec![None; self.n];
    println!("{:?}", first_k);

    // Fill in codeword
    for response in first_k {
      match response {
        Ok((addr, result)) => {
          // Get server index
          let i = self.index_table.get(&addr).unwrap().clone();
          // String to vec of u8
          let result: Option<Vec<u8>> = result.map(|x| serde_json::from_str(&x).unwrap());
          codeword[i] = result;
        }
        _ => bail!("Get error"),
      }
    }

    // Reconstruct message
    println!("{:?}", codeword.clone());
    self.ecc.reconstruct(&mut codeword).unwrap();
    let codeword: Vec<_> = codeword.into_iter().filter_map(|x| x).collect();
    println!("{:?}", codeword.clone());

    // Process into string
    let flattened = codeword.into_iter().flatten().collect::<Vec<u8>>();
    let mut flattened: Vec<u8> = (&flattened[..self.message_size]).to_vec();
    // pop padding
    while let Some(0) = flattened.last() {
      flattened.pop();
    }

    println!("{:?}", flattened.clone());
    let message = str::from_utf8(&flattened).unwrap();
    println!("{:?}", message);

    Ok(message.to_string())
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
  // .write("josie".to_string(), "&femkfjez💖".to_string())
  // .await?;

  // client.write("yes2".to_string(), "¢".to_string()).await;

  // client
  //   .get_once(servers[1].clone(), "yes".to_string())
  //   .await?;

  client.read("josie".to_string()).await;

  // let mut converter = convert::Convert::new(2, 256);
  // let output = converter.convert::<u8, u8>(&vec![1, 1, 1, 1, 1, 1, 0, 1, 1]);
  // println!("{:?}", output);

  Ok(())
}
