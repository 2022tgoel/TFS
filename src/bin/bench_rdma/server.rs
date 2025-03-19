use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
use std::env;
use std::net::ToSocketAddrs;
use std::time::Instant;
use std::{
    alloc::Layout,
    io::{self, Write},
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

const BENCHMARKING_SIZE: usize = 5 * 1024 * 1024;
const WARMUP_ITERATIONS: usize = 5;
const BENCH_ITERATIONS: usize = 20;

async fn server(addr: SocketAddr) -> io::Result<()> {
    println!("Starting RDMA server on {}", addr);

    // Configure RDMA for maximum performance
    let rdma = RdmaBuilder::default().listen(addr).await?;

    println!("RDMA server listening with optimized settings on {}", addr);

    // Pre-allocate with proper alignment
    let layout = Layout::from_size_align(BENCHMARKING_SIZE, 4096).unwrap(); // 4KB aligned
    let mut lmr = rdma.alloc_local_mr(layout)?;
    println!("Allocated local MR of size {}", BENCHMARKING_SIZE);

    // Wait for warmup iterations
    for i in 0..WARMUP_ITERATIONS {
        if let Ok(rmr) = rdma.receive_local_mr().await {
            println!("Warmup iteration {} completed", i);
        }
    }

    println!("\nStarting benchmark measurements...");

    // Benchmark phase
    for i in 0..BENCH_ITERATIONS {
        if let Ok(rmr) = rdma.receive_local_mr().await {
            if (i + 1) % 5 == 0 {
                println!("Completed {} iterations", i + 1);
            }
        }
    }

    // Wait for final signal
    if let Ok(_) = rdma.receive_local_mr().await {
        println!("Benchmark completed");
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let hostname = env::var("SERVER_NAME").unwrap();
    let port = 5555;

    let socket_addr = format!("{}:{}", hostname, port)
        .to_socket_addrs()
        .and_then(|mut socket_addrs| {
            socket_addrs
                .next()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No address found"))
        })
        .unwrap();

    server(socket_addr)
        .await
        .map_err(|err| println!("Server error: {}", err))
        .unwrap();
}
