#[path = "lib/server.rs"]
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  server::main().await
}
