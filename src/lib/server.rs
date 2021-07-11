use tonic::{transport::Server, Request, Response, Status};

use hello_world::greeter_client::GreeterClient;
use hello_world::greeter_server::{Greeter, GreeterServer};

use hello_world::{HelloReply, HelloRequest};

use futures::join;

pub mod hello_world {
  tonic::include_proto!("helloworld"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
  async fn say_hello(
    &self,
    request: Request<HelloRequest>, // Accept request of type HelloRequest
  ) -> Result<Response<HelloReply>, Status> {
    // Return an instance of type HelloReply
    println!("Got a request: {:?}", request);

    let reply = hello_world::HelloReply {
      message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
    };

    Ok(Response::new(reply)) // Send back our formatted greeting
  }
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
  let addr = "0.0.0.0:50051".parse()?;
  let greeter = MyGreeter::default();

  Server::builder()
    .add_service(GreeterServer::new(greeter))
    .serve(addr)
    .await?;

  Ok(())
}
async fn query() -> Result<(), Box<dyn std::error::Error>> {
  let mut client = GreeterClient::connect("http://0.0.0.0:50051").await?;

  let request = tonic::Request::new(HelloRequest {
    name: "Tonic".into(),
  });

  let response = client.say_hello(request).await?;

  println!("RESPONSE={:?}", response);

  Ok(())
}

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let f1 = serve();
  let f2 = query();

  join!(f1, f2);

  Ok(())
}
