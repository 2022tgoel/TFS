use super::IBSocket;
use anyhow::Result;
use ibverbs::MemoryRegion;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
const RPC_PORT: u16 = 7777;

#[derive(Debug, Serialize, Deserialize)]
pub struct MemRegionInfo {
    pub addr: u64,
    pub rkey: u32,
    pub length: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SendInfo {
    pub length: usize,
}

#[derive(Debug, Serialize, Deserialize)]
enum RpcMessage {
    ReadRequest(MemRegionInfo),
    SendRequest(SendInfo),
    Response(),
}

#[derive(PartialEq)]
enum IBSocketState {
    Created,
    Connected,
}

pub struct RpcSocket {
    local_addr: String,
    remote_addr: String,
    ib_socket: Arc<Mutex<IBSocket>>,
    ib_socket_state: IBSocketState,
}

impl RpcSocket {
    pub fn new(ib_socket: IBSocket, local_addr: String, remote_addr: String) -> Self {
        RpcSocket {
            ib_socket: Arc::new(Mutex::new(ib_socket)),
            ib_socket_state: IBSocketState::Created,
            local_addr,
            remote_addr,
        }
    }

    pub async fn init_ib_socket(&mut self) -> Result<()> {
        if self.ib_socket_state == IBSocketState::Created {
            self.ib_socket
                .lock()
                .await
                .connect(self.local_addr.clone(), self.remote_addr.clone())
                .await?;
            self.ib_socket_state = IBSocketState::Connected;
        }
        Ok(())
    }

    /// Start the RPC server to handle incoming read requests
    pub async fn serve(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("{}:{}", self.local_addr, RPC_PORT)).await?;
        println!("RPC server listening on port {}", RPC_PORT);
        while let Ok((socket, _)) = listener.accept().await {
            let ib_socket = self.ib_socket.clone();
            let local_addr = self.local_addr.clone();
            let remote_addr = self.remote_addr.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    Self::handle_connection(socket, ib_socket, local_addr, remote_addr).await
                {
                    eprintln!("Error handling RPC connection: {}", e);
                }
            });
        }
        Ok(())
    }

    async fn handle_connection(
        mut socket: TcpStream,
        ib_socket: Arc<Mutex<IBSocket>>,
        local_addr: String,
        remote_addr: String,
    ) -> Result<()> {
        let mut ib = ib_socket.lock().await;
        ib.connect(local_addr.clone(), remote_addr.clone()).await?;

        let mut buf = vec![0; 1024];
        let n = socket.read(&mut buf).await?;
        let msg: RpcMessage = serde_json::from_slice(&buf[..n])?;

        match msg {
            RpcMessage::ReadRequest(region) => {
                let start = Instant::now();
                let mut buffer = ib.register_memory(region.length)?;
                let duration = start.elapsed();
                println!("Time taken to register memory: {:?}", duration);
                // Perform RDMA read using the provided memory region info
                let start = Instant::now();
                let id = ib.rdma_read(&remote_addr, &mut buffer, region.addr, region.rkey)?;
                ib.get_completion(id)?;
                let duration = start.elapsed();
                println!("Time taken to read: {:?}", duration);
                dbg!(format!("Read {} bytes from {}", buffer.len(), region.addr));
                io::stdout().flush().unwrap();

                let resp = RpcMessage::Response();
                socket.write_all(&serde_json::to_vec(&resp)?).await?;
            }
            RpcMessage::SendRequest(info) => {
                let mut mr = ib.register_memory(info.length)?;
                let id = ib.post_send(&remote_addr, &mut mr)?;
                ib.get_completion(id)?;
                let resp = RpcMessage::Response();
                socket.write_all(&serde_json::to_vec(&resp)?).await?;
            }
            _ => return Err(anyhow::anyhow!("Unexpected message type")),
        }
        Ok(())
    }

    pub async fn register_memory(&self, size: usize) -> Result<MemoryRegion<u8>> {
        self.ib_socket.lock().await.register_memory(size)
    }

    pub async fn request_send(&mut self, length: usize) -> Result<()> {
        let mut stream = TcpStream::connect(format!("{}:{}", self.remote_addr, RPC_PORT)).await?;
        self.init_ib_socket().await?;

        let mut ib = self.ib_socket.lock().await;
        let mut mr = ib.register_memory(length)?;
        let id = ib.post_receive(&self.remote_addr, &mut mr)?;

        let request = RpcMessage::SendRequest(SendInfo { length });
        let request_bytes = serde_json::to_vec(&request)?;
        stream.write_all(&request_bytes).await?;
        dbg!("Send send request");

        ib.get_completion(id)?;

        let mut buf = vec![0; 1024];
        let n = stream.read(&mut buf).await?;
        let response: RpcMessage = serde_json::from_slice(&buf[..n])?;

        match response {
            RpcMessage::Response() => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    /// Request a read from a remote node
    pub async fn request_read(&mut self, mem_region: MemRegionInfo) -> Result<()> {
        let mut stream = TcpStream::connect(format!("{}:{}", self.remote_addr, RPC_PORT)).await?;

        self.init_ib_socket().await?;

        let request = RpcMessage::ReadRequest(mem_region);
        let request_bytes = serde_json::to_vec(&request)?;
        stream.write_all(&request_bytes).await?;
        dbg!("Send read request");

        let mut buf = vec![0; 1024];
        let n = stream.read(&mut buf).await?;
        let response: RpcMessage = serde_json::from_slice(&buf[..n])?;

        match response {
            RpcMessage::Response() => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }
}
