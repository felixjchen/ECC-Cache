use ecc_proto::ecc_rpc_client::EccRpcClient;
use ecc_proto::{GetKeysRequest, GetRequest, SetRequest};
use futures::future::join_all;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use reed_solomon_erasure::galois_8::ReedSolomon;
use simple_error::bail;
use std::collections::HashMap;
use std::str;
use std::time::Duration;
use tokio::sync::RwLock;
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
  client_table: RwLock<HashMap<String, Option<EccRpcClient<Channel>>>>,
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
    let client_table = RwLock::new(client_table);
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

  async fn get_client(&self, addr: String) -> Option<EccRpcClient<Channel>> {
    // If dead try to reconnect
    let mut client_table = self.client_table.write().await;
    match client_table.get(&addr) {
      Some(client_option) => match client_option {
        Some(client) => Some(client.clone()),
        None => {
          let client_option = EccRpcClient::connect(format!("http://{}", addr.clone())).await;
          match client_option {
            Ok(client) => {
              client_table.insert(addr.clone(), Some(client.clone()));
              Some(client)
            }
            _ => None,
          }
        }
      },
      None => None,
    }
  }

  async fn set_once(
    &self,
    address: String,
    key: String,
    value: String,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let client = self.get_client(address.clone()).await;

    match client {
      Some(mut client) => {
        println!("Set {:?} {:?} {:?}", address, key, value);
        let request = tonic::Request::new(SetRequest { key, value });
        client.set(request).await?;
      }
      _ => {
        println!("Ignoring {:?} as client could not connect", address)
      }
    }
    Ok(())
  }

  pub async fn set(
    &mut self,
    key: String,
    value: String,
  ) -> Result<(), Box<dyn std::error::Error>> {
    // convert value to array of bytes "[u8]"
    let mut bytes = value.into_bytes();

    // too long ...
    if bytes.len() > self.message_size.into() {
      bail!(
        "message too long, {:?} larger then {:?}",
        bytes.len(),
        self.message_size
      )
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
        let future = self.set_once(addr.clone(), key.clone(), codeword[i].clone());
        futures.push(future);
      }
      join_all(futures).await;

      Ok(())
    }
  }

  async fn get_once(
    &self,
    address: String,
    key: String,
  ) -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    let client = self.get_client(address.clone()).await;

    match client {
      Some(mut client) => {
        let request = tonic::Request::new(GetRequest { key: key.clone() });
        let response = client.get(request).await?;
        let value = response.into_inner().value;
        println!("Get {:?} {:?} {:?}", address, key, value);
        Ok((address, value))
      }
      _ => {
        sleep(Duration::from_secs(2)).await;
        bail!("Ignoring {:?} as client could not connect", address)
      }
    }
  }

  pub async fn get_keys_once(
    &self,
    address: String,
  ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = self.get_client(address.clone()).await;

    match client {
      Some(mut client) => {
        let request = tonic::Request::new(GetKeysRequest {});
        let response = client.get_keys(request).await?;
        let keys = response.into_inner().keys;
        let keys: Vec<String> = serde_json::from_str(&keys).unwrap();
        Ok(keys)
      }
      _ => {
        bail!("Ignoring {:?} as client could not connect", address)
      }
    }
  }

  pub async fn get_codeword(
    &self,
    key: String,
  ) -> Result<Option<Vec<Vec<u8>>>, Box<dyn std::error::Error>> {
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

    // Fill in codeword
    let mut all_none = true;
    for response in first_k {
      match response {
        Ok((addr, result)) => {
          // Check if result is none
          all_none = all_none && result.is_none();
          // Get server index
          let i = self.index_table.get(&addr).unwrap().clone();
          // String to vec of u8
          let result: Option<Vec<u8>> = result.map(|x| serde_json::from_str(&x).unwrap());
          codeword[i] = result;
        }
        _ => bail!("Didn't get k responses..."),
      }
    }

    if all_none {
      return Ok(None);
    }

    // Reconstruct message
    self.ecc.reconstruct(&mut codeword).unwrap();
    let codeword: Vec<_> = codeword.into_iter().map(|x| x.unwrap()).collect();
    Ok(Some(codeword))
  }

  pub async fn get(&self, key: String) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match self.get_codeword(key).await? {
      Some(codeword) => {
        // Process into string
        let flattened: Vec<u8> = codeword.into_iter().flatten().collect();
        let mut flattened: Vec<u8> = (&flattened[..self.message_size]).to_vec();
        // pop padding
        while let Some(0) = flattened.last() {
          flattened.pop();
        }
        let message = str::from_utf8(&flattened).unwrap();
        Ok(Some(message.to_string()))
      }
      None => Ok(None),
    }
  }
}
