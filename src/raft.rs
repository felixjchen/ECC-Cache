pub mod client;
pub mod network;
pub mod raft;
pub mod server;
pub mod storage;
use async_raft::NodeId;

pub fn get_raft_settings() -> (Vec<NodeId>, Vec<String>) {
  let mut settings = config::Config::default();
  settings.merge(config::File::with_name("config")).unwrap();

  let servers = settings
    .get_array("servers")
    .unwrap()
    .iter()
    .map(|x| x.to_string())
    .collect::<Vec<String>>();

  let mut node_ids = Vec::new();
  for i in 0..servers.len() {
    let i = i as NodeId;
    node_ids.push(i);
  }

  (node_ids, servers)
}
