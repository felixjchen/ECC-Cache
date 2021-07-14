use clap::{App, Arg};

#[path = "lib/server_raft.rs"]
// mod server;
mod server_raft;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let matches = App::new("Distributed Cache")
    .version("1.0")
    .author("Felix C. <felixchen1998@gmail.com>")
    .about("Distributed caching with two modes for fault tolerence. Standard sharding with replicating or error correcting codes.")
    .arg(
      Arg::new("mode")
        .short('m')
        .long("mode")
        .value_name("STRING")
        .about("Sets fault tolerence mode")
        .default_value("standard")
        .possible_values(&["standard",  "ecc"])
    ).arg(
      Arg::new("port")
        .short('p')
        .long("port")
        .value_name("STRING")
        .about("Port number")
        .required(true)
        .default_value("2000")
    )
    .get_matches();

  if let Some(p) = matches.value_of("port") {
    println!("Value for port: {}", p);
  }
  if let Some(m) = matches.value_of("mode") {
    println!("Value for mode: {}", m);
  }

  Ok(())
}
