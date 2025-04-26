use serial_test::serial;
use tfs::chunkserver::RpcServer;
use tfs::client::RpcClient;
use tfs::net::{HostName, TokioTcpConnectionManager};
use tokio::time::{Duration, sleep};

#[tokio::test]
#[serial]
async fn read_write_test() {
    // Start the server at 127.0.0.1
    let server_addr = "127.0.0.1".to_string();
    let server_task = tokio::spawn(async move {
        let server =
            RpcServer::<TokioTcpConnectionManager>::new(HostName::RegularName(server_addr.clone()))
                .await
                .expect("Failed to start server");
        server.serve().await.expect("Server crashed unexpectedly");
    });

    // Give the server a moment to start
    sleep(Duration::from_millis(300)).await;

    // Start the client at 127.0.0.3
    let client_addr = "127.0.0.3".to_string();
    let mut client = RpcClient::<TokioTcpConnectionManager>::new(
        HostName::RegularName(client_addr.clone()),
        "127.0.0.1".to_string(),
    )
    .await
    .expect("Failed to create client");

    client.connect_ib().await.expect("Failed to connect IB");

    let test_data = b"Hello, world!";
    client
        .send_put_request(1, 1, test_data)
        .await
        .expect("Put request failed");
    let get_result = client
        .send_get_request(1, 1, test_data.len())
        .await
        .expect("Get request failed");
    assert_eq!(&get_result[..], test_data);

    // Simulate server crash
    server_task.abort();
    // Optionally, check that the server task is indeed aborted
    assert!(
        server_task.await.is_err(),
        "Server did not crash as expected"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn read_write_crash_test() {
    println!("Starting read_write_crash_test");
    let server_addr = "127.0.0.1".to_string();
    let server1_task = tokio::spawn(async move {
        let server =
            RpcServer::<TokioTcpConnectionManager>::new(HostName::RegularName(server_addr.clone()))
                .await
                .expect("Failed to start server");
        server.serve().await.expect("Server crashed unexpectedly");
    });

    // Give server 1 time to start to guarantee that it is the head of the chain
    sleep(Duration::from_millis(1000)).await;

    let server_addr = "127.0.0.2".to_string();
    let server2_task = tokio::spawn(async move {
        let server =
            RpcServer::<TokioTcpConnectionManager>::new(HostName::RegularName(server_addr.clone()))
                .await
                .expect("Failed to start server");
        server.serve().await.expect("Server crashed unexpectedly");
    });

    let client_addr = "127.0.0.3".to_string();
    let mut client1 = RpcClient::<TokioTcpConnectionManager>::new(
        HostName::RegularName(client_addr.clone()),
        "127.0.0.1".to_string(),
    )
    .await
    .expect("Failed to create client");

    client1.connect_ib().await.expect("Failed to connect IB");

    let client_addr = "127.0.0.4".to_string();
    let mut client2 = RpcClient::<TokioTcpConnectionManager>::new(
        HostName::RegularName(client_addr.clone()),
        "127.0.0.2".to_string(),
    )
    .await
    .expect("Failed to create client");

    client2.connect_ib().await.expect("Failed to connect IB");

    let test_data = b"Hello, world!";
    client1
        .send_put_request(1, 1, test_data)
        .await
        .expect("Put request failed");
    let get_result = client1
        .send_get_request(1, 1, test_data.len())
        .await
        .expect("Get request failed");
    assert_eq!(&get_result[..], test_data);

    // Simulate server crash
    server1_task.abort();
    assert!(
        server1_task.await.is_err(),
        "Server did not crash as expected"
    );

    // client 2 should still be able to read the data
    let get_result = client2
        .send_get_request(1, 1, test_data.len())
        .await
        .expect("Get request failed");
    assert_eq!(&get_result[..], test_data);
}
