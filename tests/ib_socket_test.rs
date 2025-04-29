use anyhow::Result;
use tfs::net::{HostName, IBSocket, ManagerCreate, TokioTcpConnectionManager};

#[tokio::test]
async fn test_ib_socket_rdma_op() -> Result<()> {
    // Create two IB sockets
    let mut socket1 = IBSocket::<<TokioTcpConnectionManager as ManagerCreate>::Listener>::new()?;
    let mut socket2 = IBSocket::<<TokioTcpConnectionManager as ManagerCreate>::Listener>::new()?;

    // Use different loopback IPs for local/remote nodes
    let addr1 = "127.0.0.1".to_string();
    let addr2 = "127.0.0.2".to_string();

    // Connect sockets to each other (bi-directional)
    // socket1 is server on addr1, connects to addr2
    // socket2 is server on addr2, connects to addr1
    let c1 = socket1.connect(HostName::RegularName(addr1.clone()), addr2.clone());
    let c2 = socket2.connect(HostName::RegularName(addr2.clone()), addr1.clone());
    let (r1, r2) = tokio::join!(c1, c2);
    r1?;
    r2?;

    // Register memory regions
    let mut mr1 = socket1.register_memory(16)?;
    let mr2 = socket2.register_memory(16)?;

    // Write some data to mr1
    let src_data = b"hello_ib_socket!";
    mr1[..src_data.len()].copy_from_slice(src_data);

    // Get remote addr/rkey for mr2
    let raddr = unsafe { (*mr2.mr).addr } as u64;
    let rkey = unsafe { (*mr2.mr).rkey };

    // Perform RDMA write from socket1 to socket2's memory
    let wr_id = socket1.rdma_write(&addr2, &mut mr1, raddr, rkey, src_data.len())?;
    socket1.get_completion(wr_id)?;

    // Check that the data was written to mr2
    assert_eq!(&mr2[..src_data.len()], src_data);

    Ok(())
}
