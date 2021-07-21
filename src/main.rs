use clap::{App, Arg};
use config::Config;
mod ecc;
mod replication;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let matches = App::new("Distributed Cache")
    .version("1.0")
    .author("Felix C. <felixchen1998@gmail.com>")
    .about("Distributed cache with two modes for fault tolerence. Standard replication or error correcting codes.")
    .arg(
      Arg::new("mode")
        .short('m')
        .long("mode")
        .value_name("STRING")
        .about("Sets fault tolerence mode")
        .default_value("standard")
        .possible_values(&["standard",  "ecc"])
    )
    .get_matches();

  if let Some(m) = matches.value_of("mode") {
    println!("Value for mode: {}", m);
  }

  Ok(())
}
