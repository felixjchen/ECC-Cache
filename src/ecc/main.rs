#[macro_use(shards)]
extern crate reed_solomon_erasure;
mod client;
mod ecc;
mod server;
use futures::future::join_all;

#[tokio::main]
async fn main() {
  let addresses = ["0.0.0.0:3001", "0.0.0.0:3002", "0.0.0.0:3003"];

  // await all servers
  let mut server_futures = Vec::new();
  for i in addresses {
    let future = server::start_server(i.to_string());
    server_futures.push(future);
  }

  join_all(server_futures).await;
}
