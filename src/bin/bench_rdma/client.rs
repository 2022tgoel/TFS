use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, QueuePairOptions, RdmaBuilder};
use std::env;
use std::net::ToSocketAddrs;
use std::time::Instant;
use std::{
    alloc::Layout,
    io::{self, Write},
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

const BENCHMARKING_SIZE: usize = 5 * 1024 * 1024; // Match server's size
const WARMUP_ITERATIONS: usize = 5;
const BENCH_ITERATIONS: usize = 20;

async fn client(addr: SocketAddr) -> io::Result<()> {
    println!("Connecting to RDMA server at {}", addr);
    let layout = Layout::from_size_align(BENCHMARKING_SIZE, 4096).unwrap(); // 4KB aligned
    let rdma = RdmaBuilder::default().connect(addr).await?;

    println!("Connected to server, starting warmup...");

    // Pre-allocate MRs for reuse
    let mut lmr = rdma.alloc_local_mr(layout)?;
    let mut rmr = rdma.request_remote_mr(layout).await?;

    // Fill local MR with test data
    lmr.as_mut_slice().write(&vec![1_u8; BENCHMARKING_SIZE])?;

    // Warmup phase
    for i in 0..WARMUP_ITERATIONS {
        rdma.write(&lmr, &mut rmr).await?;
        println!("Warmup iteration {} completed", i);
    }

    println!("\nStarting benchmark measurements...");
    let mut latencies = Vec::with_capacity(BENCH_ITERATIONS);

    // Benchmark phase
    for i in 0..BENCH_ITERATIONS {
        let start = Instant::now();
        rdma.write(&lmr, &mut rmr).await?;
        let duration = start.elapsed();
        latencies.push(duration);

        if (i + 1) % 5 == 0 {
            println!("Completed {} iterations", i + 1);
        }
    }

    // Calculate statistics
    let total_time: Duration = latencies.iter().sum();
    let avg_latency = total_time / BENCH_ITERATIONS as u32;
    let min_latency = latencies.iter().min().unwrap();
    let max_latency = latencies.iter().max().unwrap();

    let throughput = (BENCHMARKING_SIZE as f64 / avg_latency.as_secs_f64()) / 1_000_000.0; // MB/s

    println!("\nBenchmark Results:");
    println!(
        "Transfer Size: {} MB",
        BENCHMARKING_SIZE as f64 / 1_000_000.0
    );
    println!("Average Latency: {:?}", avg_latency);
    println!("Min Latency: {:?}", min_latency);
    println!("Max Latency: {:?}", max_latency);
    println!("Throughput: {:.2} MB/s", throughput);

    // Send final signal to server
    rdma.send_remote_mr(rmr).await?;

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

    client(socket_addr)
        .await
        .map_err(|err| println!("Client error: {}", err))
        .unwrap();
}
