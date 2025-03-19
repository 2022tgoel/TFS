pub mod net;

use crate::net::IBSocket;
use crate::net::utils::{my_name, slurm_nodelist};
use ibverbs;
use tokio;

#[tokio::main]
async fn main() {
    let hostname = my_name().unwrap();
    let peers = slurm_nodelist().unwrap();
    println!("Starting up server {}. Peers: {:?}.", &hostname, peers);

    println!(
        "Number of infiniband devices: {}",
        ibverbs::devices().unwrap().len()
    );
    let mut socket = IBSocket::new().unwrap();
    // socket.connect(peers[0].clone()).await.unwrap();
    // println!("Connected to peer {}", peers[0]);
}
