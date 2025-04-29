// Zookeeper needs to be manually started for these tests to work.
// SLURM_NODELIST=`hostname` ./zookeeper/launch.py start
// Also, this file should be run with the test-config feature enabled.
// Example: cargo test --features test-config read_write_test -- --nocapture

use futures::future::join_all;
use serial_test::serial;
use tfs::chunkserver::RpcServer;
use tfs::client::RpcClient;
use tfs::net::{HostName, TokioTcpConnectionManager};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

/// Spawns N servers and N clients, each with a unique 127.0.0.* address.
/// Each client[i] connects to server[i].
pub async fn spawn_servers_and_clients(
    n: usize,
) -> (
    Vec<JoinHandle<()>>,
    Vec<RpcClient<TokioTcpConnectionManager>>,
) {
    let mut server_tasks = Vec::with_capacity(n);
    let mut clients = Vec::with_capacity(n);

    // Start servers
    for i in 1..=n {
        let server_addr = format!("127.0.0.{}", i);
        let task = tokio::spawn(async move {
            let mut server = RpcServer::<TokioTcpConnectionManager>::new(HostName::RegularName(
                server_addr.clone(),
            ))
            .await
            .expect("Failed to start server");
            server.serve().await.expect("Server crashed unexpectedly");
        });
        // Give server 1 time to start to have a deterministic chain
        sleep(Duration::from_millis(500)).await;
        server_tasks.push(task);
    }

    // Start clients
    for i in 1..=n {
        let client_addr = format!("127.0.0.{}", n + i);
        let server_addr = format!("127.0.0.{}", i);
        let mut client = RpcClient::<TokioTcpConnectionManager>::new(
            HostName::RegularName(client_addr.clone()),
            server_addr.clone(),
        )
        .await
        .expect("Failed to create client");
        println!("Connecting to {}", server_addr);
        client.connect_ib().await.expect("Failed to connect IB");
        clients.push(client);
    }

    (server_tasks, clients)
}

#[tokio::test]
#[serial]
async fn read_write_test() {
    // Start the server at 127.0.0.1
    let server_addr = "127.0.0.1".to_string();
    let server_task = tokio::spawn(async move {
        let mut server =
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
    let (mut server_tasks, clients) = spawn_servers_and_clients(2).await;

    let test_data = b"Hello, world!";
    clients[0]
        .send_put_request(1, 1, test_data)
        .await
        .expect("Put request failed");
    let get_result = clients[0]
        .send_get_request(1, 1, test_data.len())
        .await
        .expect("Get request failed");
    assert_eq!(&get_result[..], test_data);

    // Simulate server crash
    let handle = server_tasks.remove(0);
    handle.abort();
    assert!(handle.await.is_err(), "Server did not crash as expected");

    // client 2 should still be able to read the data
    let get_result = clients[1]
        .send_get_request(1, 1, test_data.len())
        .await
        .expect("Get request failed");
    assert_eq!(&get_result[..], test_data);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn reconfigure_head_test() {
    let (mut server_tasks, clients) = spawn_servers_and_clients(2).await;

    let test_data = b"Hello, world!";
    clients[0]
        .send_put_request(1, 1, test_data)
        .await
        .expect("Put request failed");

    // Simulate server crash
    let handle = server_tasks.remove(0);
    handle.abort();
    assert!(handle.await.is_err(), "Server did not crash as expected");

    // client 2 should still be able to read the data
    let get_result = clients[1]
        .send_get_request(1, 1, test_data.len())
        .await
        .expect("Get request failed");
    assert_eq!(&get_result[..], test_data);

    sleep(Duration::from_millis(6000)).await;

    // Client 2 should now be accessing the new head
    // TODO: Include the client onwership of the message in the request,
    // and reject the message if it is not the head.
    clients[1]
        .send_put_request(2, 1, test_data)
        .await
        .expect("Put request failed");
}

// - Concurrent put requests -- only one is processed
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn concurrent_put_test() {
    let (mut server_tasks, clients) = spawn_servers_and_clients(3).await;

    let mut tasks = Vec::new();
    for i in 0..5 {
        let client = clients[0].clone();
        tasks.push(tokio::spawn(async move {
            client
                .send_put_request(1, 1, format!("Hello, world! {}", i).as_bytes())
                .await
                .expect("Put request failed");
        }));
    }

    let res = join_all(tasks).await;
    for r in res {
        assert!(r.is_ok(), "Put request failed");
    }

    // Check that only one of the requests was processed
    let get_result = clients[0]
        .send_get_request(1, 1, 15)
        .await
        .expect("Get request failed");
    let get_result2 = clients[1]
        .send_get_request(1, 1, 15)
        .await
        .expect("Get request failed");
    let get_result3 = clients[2]
        .send_get_request(1, 1, 15)
        .await
        .expect("Get request failed");
    println!("get_result: {:?}", String::from_utf8_lossy(&get_result));
    println!("get_result2: {:?}", String::from_utf8_lossy(&get_result2));
    println!("get_result3: {:?}", String::from_utf8_lossy(&get_result3));
    assert_eq!(get_result, get_result2);
    assert_eq!(get_result, get_result3);
}

// - Dirty version handling
// #[tokio::test(flavor = "multi_thread")]
// #[serial]
// async fn dirty_put_test() {
//     let (mut server_tasks, clients) = spawn_servers_and_clients(3).await;

//     let test_data = b"Hello, world!";
//     let client = clients[0].clone();
//     client.send_put_request(1, 1, test_data).await.expect("Put request failed");
//     let t1 = tokio::spawn(async move {
//         clients
//             .send_put_request(1, 1, b"Hello, world! 2")
//             .await
//             .expect("Put request failed");
//     });

//     // Simulate server crash
//     let handle = server_tasks.remove(1);
//     let t2 = tokio::spawn(async move {
//         handle.abort();
//     });

//     tokio::join!(t1, t2);

//     // client 3 should still be able to read the data
//     let get_result = clients[2]
//         .send_get_request(1, 1, test_data.len())
//         .await
//         .expect("Get request failed");
//     assert_eq!(&get_result[..], test_data);

//     // client 1 should see stale data
//     let get_result = clients[0]
//         .send_get_request(1, 1, test_data.len())
//         .await;
//     assert_ne!(&get_result[..], test_data);
// }
