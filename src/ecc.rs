pub mod client;
pub mod server;

pub fn get_ecc_settings() -> (usize, usize, usize, Vec<String>) {
  let mut settings = config::Config::default();
  settings.merge(config::File::with_name("config")).unwrap();

  let servers = settings
    .get_array("servers")
    .unwrap()
    .iter()
    .map(|x| x.to_string())
    .collect::<Vec<String>>();

  let k = settings.get_int("k").unwrap() as usize;
  let n = settings.get_int("n").unwrap() as usize;
  let block_size = settings.get_int("block_size").unwrap() as usize;

  (k, n, block_size, servers)
}
