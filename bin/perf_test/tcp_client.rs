
// benchmark the latency of sending 10GB of data over TCP between two servers

use std::net::TcpStream;
use std::io::{Write};
use std::time::Instant;
use std::env;

const TOTAL_BYTES: usize = 1000 * 10000000; // 10GB

fn main() -> std::io::Result<()> {
    let peer = env::var("PEER_HOSTNAME").expect("PEER_HOSTNAME is not set");
    let mut stream = TcpStream::connect(format!("{}:4000", peer))?; 
    let buffer = [4u8; 10000000]; // 1MB buffer
    let mut sent: usize = 0;

    println!("Connected to server. Sending data...");
    let start = Instant::now();

    while sent < TOTAL_BYTES {
        let to_send = std::cmp::min(TOTAL_BYTES - sent, buffer.len());
        stream.write_all(&buffer[..to_send])?;
        sent += to_send;
    }

    let duration = start.elapsed();
    println!("Sent 10GB in {:.2?}", duration);
    Ok(())
}