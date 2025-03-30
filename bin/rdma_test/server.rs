use std::env;
use tfs::net::{IBSocket, RpcSocket, utils::my_name};

#[tokio::main]
async fn main() {
    // Create two IB sockets
    let ib = IBSocket::new().unwrap();

    let hostname = my_name().unwrap();
    let peer = env::var("PEER_HOSTNAME").expect("PEER_HOSTNAME is not set");
    let rpc = RpcSocket::new(ib, hostname, peer);

    rpc.serve().await.unwrap();
}
