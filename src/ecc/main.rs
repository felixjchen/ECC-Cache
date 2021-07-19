mod server;

#[tokio::main]
async fn main() {
  server::start_server("0.0.0.0:3001".to_string()).await;
}
