use clap::{App, Arg, SubCommand};
mod ecc;
mod raft;
use ecc::client::EccClient;
use ecc::server::{start_many_servers, start_server};
use simple_error::bail;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let matches = App::new("Distributed Cache")
    .version(env!("CARGO_PKG_VERSION"))
    .author(env!("CARGO_PKG_AUTHORS"))
    .about("Distributed cache with two modes for fault tolerence. Raft or error correcting codes.")
    .subcommand(
      SubCommand::with_name("ecc")
        .about("ECC Cache")
        .subcommand(
          SubCommand::with_name("server")
            .about("ECC Cache Server")
            .subcommand(SubCommand::with_name("startAll").about("Start all nodes from config.json"))
            .subcommand(
              SubCommand::with_name("startOne")
                .about("Start a single node from config.json. Meant to test restoring nodes.")
                .arg(Arg::with_name("address").help("a").required(true).index(1))
                .arg(
                  Arg::with_name("recover")
                    .help("a")
                    .required(false)
                    .index(2)
                    .default_value("no"),
                ),
            ),
        )
        .subcommand(
          SubCommand::with_name("client")
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
            ),
        ),
    )
    .subcommand(
      SubCommand::with_name("raft")
        .about("Raft Cache")
        .subcommand(
          SubCommand::with_name("server")
            .about("Raft Cache Server")
            .subcommand(SubCommand::with_name("startAll").about("Starts a n node raft kv cache"))
            .subcommand(
              SubCommand::with_name("startOne")
                .about("Add a new raft member")
                .arg(Arg::with_name("id").help("i").required(true).index(1)),
            ),
        )
        .subcommand(
          SubCommand::with_name("client")
            .about("Raft Cache Client")
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
            ),
        ),
    )
    .get_matches();

  // ECC
  if let Some(matches) = matches.subcommand_matches("ecc") {
    // Read ecc settings from config.json
    let (k, n, heartbeat_timeout_ms, block_size, servers) = ecc::get_ecc_settings();
    // ECC Server CLI
    if let Some(matches) = matches.subcommand_matches("server") {
      // Start all ECC servers
      if let Some(_) = matches.subcommand_matches("startAll") {
        start_many_servers(servers.clone(), heartbeat_timeout_ms)
          .await
          .unwrap();
      }
      // Start one ECC server node during restore
      if let Some(matches) = matches.subcommand_matches("startOne") {
        let addr = matches.value_of("address").unwrap().to_string();
        let recover = matches.value_of("recover").unwrap().to_string() == "recover";
        match servers.iter().position(|i| i.clone() == addr) {
          Some(id) => start_server(id, addr, servers.clone(), heartbeat_timeout_ms, recover)
            .await
            .unwrap(),
          None => bail!("server address not found in config"),
        }
      }
    }
    // ECC Client CLI
    if let Some(matches) = matches.subcommand_matches("client") {
      let mut client = EccClient::new(true).await;
      // SET KV
      if let Some(matches) = matches.subcommand_matches("set") {
        let key = matches.value_of("key").unwrap().to_string();
        let value = matches.value_of("value").unwrap().to_string();
        client.two_phase_commit(key, value).await.unwrap();
      }
      // GET K
      if let Some(matches) = matches.subcommand_matches("get") {
        let key = matches.value_of("key").unwrap().to_string();
        println!("{:?}", client.get(key).await.unwrap());
      }
    }
  }

  // Raft
  if let Some(matches) = matches.subcommand_matches("raft") {
    let (node_ids, servers) = raft::get_raft_settings();
    if let Some(matches) = matches.subcommand_matches("server") {
      if let Some(_) = matches.subcommand_matches("startAll") {
        raft::raft::start_rafts(node_ids.clone(), servers.clone()).await?;
      }
      if let Some(matches) = matches.subcommand_matches("startOne") {
        let id = matches.value_of("id").unwrap().to_string();
        raft::raft::start_raft(
          id.parse::<u64>().unwrap(),
          node_ids.clone(),
          servers.clone(),
        )
        .await?;
      }
    }
    if let Some(matches) = matches.subcommand_matches("client") {
      let mut client = raft::client::RaftClient::new(node_ids.clone(), servers.clone()).await;
      // SET KV
      if let Some(matches) = matches.subcommand_matches("set") {
        let key = matches.value_of("key").unwrap().to_string();
        let value = matches.value_of("value").unwrap().to_string();
        client.set(key, value).await.unwrap();
      }
      // GET K
      if let Some(matches) = matches.subcommand_matches("get") {
        let key = matches.value_of("key").unwrap().to_string();
        println!("{:?}", client.get(key).await.unwrap());
      }
    }
  };

  Ok(())
}
