use crate::ecc::get_ecc_settings;
use ecc_proto::ecc_rpc_client::EccRpcClient;
use ecc_proto::*;
use futures::future::join_all;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use reed_solomon_erasure::galois_8::ReedSolomon;
use simple_error::bail;
use std::collections::HashMap;
use std::collections::HashSet;
use std::str;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::Request;
use uuid::Uuid;

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
  pub async fn new() -> EccClient {
    let (k, n, heartbeat_timeout_ms, block_size, servers) = get_ecc_settings();
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
        let request = Request::new(SetRequest { key, value });
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
        let request = Request::new(GetRequest { key: key.clone() });
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
    println!("{:?}", first_k);

    // Fill in codeword
    let mut all_none = true;
    for response in first_k {
      match response {
        Ok((addr, result)) => {
          // Check if result is none
          all_none = all_none && result.is_none();
          // Get server index
          let i = self.index_table.get(&addr).unwrap().clone();
          println!("{:?}", result);
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

  pub async fn get_keys_once(
    &self,
    address: String,
  ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = self.get_client(address.clone()).await;

    match client {
      Some(mut client) => {
        let request = Request::new(GetKeysRequest {});
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

  async fn send_heartbeat(&self, addr: String) -> (String, String) {
    let client = self.get_client(addr.clone()).await;

    match client {
      Some(mut client) => {
        let request = Request::new(HeartbeatRequest {});
        let response = client.heartbeat(request).await;
        match response {
          Ok(response) => {
            let state = response.into_inner().state;
            (state, addr)
          }
          Err(_) => ("NotReady".to_string(), addr),
        }
      }
      _ => ("NotReady".to_string(), addr),
    }
  }

  pub async fn send_heartbeats(&mut self) -> HashSet<String> {
    let mut futures = Vec::new();
    for addr in self.servers.clone() {
      let future = self.send_heartbeat(addr);
      futures.push(future)
    }
    let res = join_all(futures).await;
    let mut healthy_servers = HashSet::new();
    for (state, addr) in res {
      if state == "Ready" {
        healthy_servers.insert(addr);
      }
    }
    healthy_servers
  }

  pub async fn two_phase_commit(
    &self,
    key: String,
    value: String,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let tid = Uuid::new_v4().to_string();
    println!("Beginning 2pc for {:?} {:?} {:?}", tid, key, value);
    let mut bytes = value.into_bytes();

    // too long ...
    if bytes.len() > self.message_size.into() {
      bail!(
        "message too long, {:?} larger then {:?}",
        bytes.len(),
        self.message_size
      );
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

      // Prepare, try to acquire H >= k locks, make sure the healthy server clique is agreed upon
      let mut futures = Vec::new();
      for (i, addr) in self.servers.iter().enumerate() {
        let future = self.send_prepare(addr.clone(), tid.clone(), key.clone(), codeword[i].clone());
        futures.push(future)
      }
      let res = join_all(futures).await;

      // Clique matches and all locks acquired
      // Acquired >= k locks
      let mut clique = true;
      let mut all_healthy_servers: HashSet<String> = HashSet::new();
      let mut first_healthy_servers = true;
      let mut all_locks_acquired = true;
      let mut num_locks_acquired = 0;
      for i in res {
        match i {
          Err(_) => (),
          Ok((healthy_servers, locks_acquired)) => {
            if first_healthy_servers {
              all_healthy_servers = healthy_servers.clone();
              first_healthy_servers = false;
            }

            clique = clique && (all_healthy_servers == healthy_servers);
            all_locks_acquired = all_locks_acquired && locks_acquired;
            if locks_acquired {
              num_locks_acquired += 1
            };
          }
        }
      }

      let go_commit = clique && num_locks_acquired >= self.k && all_locks_acquired;
      println!("Can commit {:?}", go_commit);

      if go_commit {
        let mut futures = Vec::new();
        for addr in self.servers.clone() {
          let future = self.send_commit(addr.clone(), tid.clone(), key.clone());
          futures.push(future);
        }
        let res = join_all(futures).await;
        println!("{:?}", res);
      } else {
        let mut futures = Vec::new();
        for addr in self.servers.clone() {
          let future = self.send_abort(addr.clone(), tid.clone(), key.clone());
          futures.push(future);
        }
        let res = join_all(futures).await;
        println!("{:?}", res);
      }
      Ok(())
    }
  }

  async fn send_prepare(
    &self,
    addr: String,
    tid: String,
    key: String,
    value: String,
  ) -> Result<(HashSet<String>, bool), Box<dyn std::error::Error>> {
    let client = self.get_client(addr.clone()).await;
    match client {
      Some(mut client) => {
        let request = Request::new(PrepareRequest { tid, key, value });
        let response = client.prepare(request).await?.into_inner();
        let lock_acquired = response.lock_acquired;
        let healthy_servers = response.healthy_servers;
        let healthy_servers: HashSet<String> = serde_json::from_str(&healthy_servers).unwrap();
        Ok((healthy_servers, lock_acquired))
      }
      _ => {
        bail!("Ignoring {:?} as client could not connect", addr)
      }
    }
  }

  async fn send_commit(
    &self,
    addr: String,
    tid: String,
    key: String,
  ) -> Result<bool, Box<dyn std::error::Error>> {
    let client = self.get_client(addr.clone()).await;
    match client {
      Some(mut client) => {
        let request = Request::new(CommitRequest { key, tid });
        let response = client.commit(request).await?.into_inner();
        let success = response.success;
        Ok(success)
      }
      _ => {
        bail!("Ignoring {:?} as client could not connect", addr)
      }
    }
  }

  async fn send_abort(
    &self,
    addr: String,
    tid: String,
    key: String,
  ) -> Result<bool, Box<dyn std::error::Error>> {
    let client = self.get_client(addr.clone()).await;
    match client {
      Some(mut client) => {
        let request = Request::new(AbortRequest { key, tid });
        let response = client.abort(request).await?.into_inner();
        let success = response.success;
        Ok(success)
      }
      _ => {
        bail!("Ignoring {:?} as client could not connect", addr)
      }
    }
  }
}
