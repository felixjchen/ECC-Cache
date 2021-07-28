use async_raft::NodeId;
use raft_proto::raft_rpc_client::RaftRpcClient;
use raft_proto::{ClientReadRpcRequest, ClientWriteRpcRequest};
use std::collections::HashMap;
use std::env;
use tonic::transport::Channel;

pub mod raft_proto {
  tonic::include_proto!("raft_proto");
}

pub struct RaftClient {
  client_table: HashMap<NodeId, Option<RaftRpcClient<Channel>>>,
  last_leader: NodeId,
  n: u64,
}

impl RaftClient {
  pub async fn new(node_ids: Vec<NodeId>, addresses: Vec<String>) -> RaftClient {
    let mut client_table = HashMap::new();
    let last_leader = node_ids[0].clone();
    let n = client_table.len() as u64;

    for (id, addr) in node_ids.iter().zip(addresses.iter()) {
      let addr = match env::var_os("DOCKER_HOSTNAME") {
        Some(hostname) => format!(
          "http://{}",
          addr.replace("0.0.0.0", &hostname.into_string().unwrap())
        ),
        None => format!("http://{}", addr),
      };
      

      let client = RaftRpcClient::connect(addr.clone()).await;
      let client = match client {
        Ok(i) => Some(i),
        _ => None,
      };
      client_table.insert(id.clone(), client);
    }

    RaftClient {
      client_table,
      last_leader,
      n,
    }
  }

  pub async fn set(
    &mut self,
    key: String,
    value: String,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let mut write = false;
    while !write {
      let client = self
        .client_table
        .get(&self.last_leader)
        .map(|c| c.clone())
        .unwrap();

      match client {
        Some(mut client) => {
          let request = tonic::Request::new(ClientWriteRpcRequest {
            key: key.clone(),
            value: value.clone(),
          });
          let response = client.client_write(request).await?;
          let response = response.into_inner();

          // Check if leader has changed, if not we're good!
          
          match response.leader_id {
            Some(new_leader) => {
              self.last_leader = new_leader;
            }
            None => {
              write = true;
            }
          }
        }
        _ => {
          self.last_leader += 1;
          self.last_leader = self.last_leader % self.n;
        }
      }
    }

    Ok(())
  }

  pub async fn get(&mut self, key: String) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let mut read = false;
    let mut res = None;

    while !read {
      let client = self
        .client_table
        .get(&self.last_leader)
        .map(|c| c.clone())
        .unwrap();

      match client {
        Some(mut client) => {
          let request = tonic::Request::new(ClientReadRpcRequest { key: key.clone() });
          let response = client.client_read(request).await?;
          let response = response.into_inner();

          // Check if leader has changed, if not we're good!
          
          match response.leader_id {
            Some(new_leader) => {
              self.last_leader = new_leader;
            }
            None => {
              res = response.value;
              read = true;
            }
          }
        }
        _ => {
          self.last_leader += 1;
          self.last_leader = self.last_leader % self.n;
        }
      }
    }

    Ok(res)
  }
}
