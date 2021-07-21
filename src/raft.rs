pub mod client;
pub mod network;
pub mod raft;
pub mod server;
pub mod storage;
use async_raft::NodeId;

pub fn get_raft_settings() -> (usize, Vec<NodeId>, Vec<String>) {
  let mut settings = config::Config::default();
  settings.merge(config::File::with_name("config")).unwrap();

  let servers = settings
    .get_array("servers")
    .unwrap()
    .iter()
    .map(|x| x.to_string())
    .collect::<Vec<String>>();

  let n = settings.get_int("n").unwrap() as usize;

  let mut node_ids = Vec::new();
  for i in 0..n {
    let i = i as NodeId;
    node_ids.push(i);
  }

  (n, node_ids, servers)
}
