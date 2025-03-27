use std::env;
use tfs::chunkserver::RpcServer;
use tfs::net::utils::my_name;

#[tokio::main]
async fn main() {
    let hostname = my_name().unwrap();
    let server = RpcServer::new(hostname.clone()).unwrap();
    println!("Starting chunkserver on {}", hostname);
    server.serve().await.unwrap();
}
