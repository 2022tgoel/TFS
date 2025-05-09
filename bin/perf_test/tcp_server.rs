// benchmark the latency of sending 10GB of data over TCP between two servers

use std::net::TcpListener;
use std::io::{Read};
use std::time::Instant;

const TOTAL_BYTES: usize = 10 * 1000 * 1000000; // 10GB

fn main() -> std::io::Result<()> {
    // 1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
    //     link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    //     inet 127.0.0.1/8 scope host lo
    //     valid_lft forever preferred_lft forever
    //     inet6 ::1/128 scope host 
    //     valid_lft forever preferred_lft forever
    // 2: eno12399np0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc mq state UP group default qlen 1000
    //     link/ether d4:04:e6:74:74:b0 brd ff:ff:ff:ff:ff:ff
    //     inet 10.1.16.6/16 brd 10.1.255.255 scope global noprefixroute eno12399np0
    //     valid_lft forever preferred_lft forever
    //     inet6 fe80::d604:e6ff:fe74:74b0/64 scope link 
    //     valid_lft forever preferred_lft forever
    // 3: eno12409np1: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc mq state DOWN group default qlen 1000
    //     link/ether d4:04:e6:74:74:b1 brd ff:ff:ff:ff:ff:ff
    // 4: ib0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP group default qlen 256
    //     link/infiniband 00:00:00:d9:fe:80:00:00:00:00:00:00:58:a2:e1:03:00:f5:0c:a6 brd 00:ff:ff:ff:ff:12:40:1b:ff:ff:00:00:00:00:00:00:ff:ff:ff:ff
    //     inet 172.16.16.6/16 brd 172.16.255.255 scope global noprefixroute ib0
    //     valid_lft forever preferred_lft forever
    //     inet6 fe80::5aa2:e103:f5:ca6/64 scope link 
    //     valid_lft forever preferred_lft forever
    
    let listener = TcpListener::bind("172.16.16.6:4000")?;
    println!("Server listening on port 4000...");

    for stream in listener.incoming() {
        let mut stream = stream?;
        let mut received: usize = 0;
        let mut buffer = [0u8; 10000000]; // 1MB buffer

        println!("Client connected. Receiving data...");
        let start = Instant::now();

        while received < TOTAL_BYTES {
            let n = stream.read(&mut buffer)?;
            if n == 0 { break; }
            received += n;
        }

        let duration = start.elapsed();
        println!("Received 10GB in {:.2?}", duration);
        break;
    }
    Ok(())
}