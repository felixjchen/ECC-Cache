use clap::{App, Arg, SubCommand};
mod ecc;
mod replication;
use ecc::client::EccClient;
use ecc::server::start_many_servers;
use simple_error::bail;

fn get_ecc_cache_settings() -> (usize, usize, usize, Vec<String>) {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let matches = App::new("Distributed Cache")
    .version(env!("CARGO_PKG_VERSION"))
    .author(env!("CARGO_PKG_AUTHORS"))
    .about("Distributed cache with two modes for fault tolerence. Standard replication or error correcting codes.")
    .subcommand(
      SubCommand::with_name("ecc")
        .about("ECC Cache")
        .subcommand(SubCommand::with_name("server")
          .about("ECC Cache Server")
          .subcommand(
            SubCommand::with_name("startAll")
              .about("Start all nodes from config.json")
          )
          .subcommand(
            SubCommand::with_name("startOne")
              .about("Start a single node from config.json. Meant to test restoring nodes.")
              .arg(Arg::with_name("address").help("a").required(true).index(1)),
          )
        )
        .subcommand(SubCommand::with_name("client")
          .about("ECC Cache Client")
          .subcommand(
            SubCommand::with_name("set")
              .about("Set key with value")
              .arg(Arg::with_name("key").help("k").required(true).index(1))
              .arg(Arg::with_name("value").help("v").required(true).index(2)),
          )
          .subcommand(
            SubCommand::with_name("get")
              .about("Get key's value")
              .arg(Arg::with_name("key").help("k").required(true).index(1)),
          )
        )
    )
    .subcommand(
      SubCommand::with_name("TODO")
    )
    .get_matches();

  if let Some(matches) = matches.subcommand_matches("ecc") {
    // Read ecc settings from config.json
    let (k, n, block_size, servers) = get_ecc_cache_settings();
    // ECC Server CLI
    if let Some(matches) = matches.subcommand_matches("server") {
      // Start all ECC servers
      if let Some(_) = matches.subcommand_matches("startAll") {
        start_many_servers(servers.clone()).await?;
      }
      // Start one ECC server node during restore
      if let Some(_) = matches.subcommand_matches("startOne") {}
    }
    // ECC Client CLI
    if let Some(matches) = matches.subcommand_matches("client") {
      let client = EccClient::new(k, n, block_size, servers.clone()).await;
      // SET KV
      if let Some(matches) = matches.subcommand_matches("set") {
        let key = matches.value_of("key").unwrap().to_string();
        let value = matches.value_of("value").unwrap().to_string();
        client.write(key, value).await?;
      }
      // GET K
      if let Some(matches) = matches.subcommand_matches("get") {
        let key = matches.value_of("key").unwrap().to_string();
        println!("{:?}", client.read(key).await?);
      }
    }
  }

  Ok(())
}
