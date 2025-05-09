use futures::future::join_all;
use std::env;
use std::time::{Duration, Instant};
use tfs::chunkserver::CHUNK_SIZE;
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
    let test_data: Vec<u8> = vec![65; CHUNK_SIZE];

    client.connect_ib().await.unwrap();
    println!("Sending PutRequest...");
    let start = Instant::now();

    // Send PutRequest
    let result = client.send_put_request(1, 1, &test_data).await;

    let duration = start.elapsed();
    println!("Time taken: {:?}", duration);
    match result {
        Ok(_) => println!("PutRequest successful!"),
        Err(e) => println!("PutRequest failed: {:?}", e),
    }

    // Send 1000 concurrent GetRequests
    // Each reads 10MB of data
    // Total data read = 10GB
    let start = Instant::now();
    let handles = (0..1000)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move {
                let test_data = vec![0; CHUNK_SIZE];
                // Time the latency of the request
                let start = Instant::now();
                let result = client.send_get_request(1, 1, test_data.len()).await;
                let duration = start.elapsed();
                duration
            })
        })
        .collect::<Vec<_>>();
    let durations = join_all(handles).await;
    println!("Total time taken: {:?}", start.elapsed());
    let mut total_duration = Duration::from_secs(0);
    let mut max_duration = Duration::from_secs(0);
    for duration in &durations {
        let d = duration.as_ref().expect("Request failed").clone();
        total_duration += d;
        if d > max_duration {
            max_duration = d;
        }
    }
    println!("Max latency: {:?}", max_duration);
    let average_duration = (total_duration - max_duration) / ((durations.len() - 1) as u32);
    println!("Average latency: {:?}", average_duration);

    // Request when there is no server pressure

    let test_data = vec![0; CHUNK_SIZE];
    // Time the latency of the request
    let start = Instant::now();
    let result = client.send_get_request(1, 1, test_data.len()).await;
    let duration = start.elapsed();
    println!("Latency of a request when there is no server pressure: {:?}", duration);
}
