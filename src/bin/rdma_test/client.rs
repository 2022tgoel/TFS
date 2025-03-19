use std::env;
use std::time::Duration;
use std::time::Instant;
use tfs::net::{IBSocket, RpcSocket, rpc_socket::MemRegionInfo, utils::my_name};

#[tokio::main]
async fn main() {
    // Create two IB sockets
    let ib = IBSocket::new().unwrap();

    let hostname = my_name().unwrap();
    let peer = env::var("PEER_HOSTNAME").expect("PEER_HOSTNAME is not set");
    let mut rpc = RpcSocket::new(ib, hostname, peer);

    rpc.request_send(8).await.unwrap();

    let len = 100000000; // 10MB
    let mut mr = rpc.register_memory(len).await.unwrap();

    mr[0] = b'H';
    mr[1] = b'e';
    mr[2] = b'l';
    mr[3] = b'l';
    mr[4] = b'o';
    mr[5] = b',';
    mr[6] = b' ';
    mr[7] = b'w';
    mr[8] = b'o';
    mr[9] = b'r';
    mr[10] = b'l';
    mr[11] = b'd';
    mr[12] = b'!';
    mr[13] = b'\x00';

    let buffer_len = unsafe { (*mr.mr).length };
    assert!(buffer_len == len, "Buffer len: {:?}", buffer_len);
    // Test read request
    let mem_region = MemRegionInfo {
        addr: unsafe { (*mr.mr).addr } as u64,
        rkey: mr.rkey().key,
        length: len,
    };

    let start = Instant::now();
    let result = rpc.request_read(mem_region).await;
    assert!(result.is_ok(), "Test failed: {:?}", result.err());
    let duration = start.elapsed();
    println!("Time taken: {:?}", duration);
}
