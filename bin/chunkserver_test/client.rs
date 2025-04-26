use std::env;
use std::time::Instant;
use tfs::client::RpcClient;
use tfs::net::utils::my_name;
use tfs::net::{HostName, TokioTcpConnectionManager};

#[tokio::main]
async fn main() {
    let hostname = my_name().unwrap();
    let peer = env::var("PEER_HOSTNAME").expect("PEER_HOSTNAME is not set");

    // Create RPC client
    let mut client = RpcClient::<TokioTcpConnectionManager>::new(
        HostName::RegularName(hostname.clone()),
        peer.clone(),
    )
    .await
    .unwrap();

    // Prepare test data
    let test_data = b"Hello, world!";

    client.connect_ib().await.unwrap();
    println!("Sending PutRequest...");
    let start = Instant::now();

    // Send PutRequest
    let result = client.send_put_request(1, 1, test_data).await;

    let duration = start.elapsed();
    println!("Time taken: {:?}", duration);
    match result {
        Ok(_) => println!("PutRequest successful!"),
        Err(e) => println!("PutRequest failed: {:?}", e),
    }

    // Send GetRequest
    let start = Instant::now();
    let result = client.send_get_request(1, 1, test_data.len()).await;
    let duration = start.elapsed();
    println!("Time taken: {:?}", duration);
    match result {
        Ok(_) => println!("GetRequest successful!"),
        Err(e) => println!("GetRequest failed: {:?}", e),
    }
}
