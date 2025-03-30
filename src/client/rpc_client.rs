use crate::chunkserver::{CHUNK_SIZE, QPInfo, PutInfo, RpcMessage, SERVER_PORT};
use crate::net::IBSocket;
use anyhow::Result;
use ibverbs::MemoryRegion;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct RpcClient {
    local_addr: String,
    server_name: String,
    ib_socket: IBSocket,
    stream: TcpStream,
    buffer: MemoryRegion<u8>,
}

impl RpcClient {
    pub async fn new(local_addr: String, server_name: String) -> Result<Self> {
        let ib_socket = IBSocket::new()?;
        let buffer = ib_socket.register_memory(CHUNK_SIZE)?;
        let stream = TcpStream::connect(format!("{}:{}", server_name, SERVER_PORT)).await?;
        Ok(Self {
            local_addr,
            server_name,
            ib_socket,
            stream,
            buffer,
        })
    }

    pub async fn connect_ib(&mut self) -> Result<()> {
        let request = RpcMessage::CreateQueuePair(QPInfo {
            addr: self.local_addr.clone(),
        });
        let request_bytes = serde_json::to_vec(&request)?;
        self.stream.write_all(&request_bytes).await?;

        println!(
            "Connecting to {} from {}",
            self.server_name, self.local_addr
        );
        self.ib_socket
            .connect(self.local_addr.clone(), self.server_name.clone())
            .await?;
        let mut buf = vec![0; 1024];
        let n = self.stream.read(&mut buf).await?;
        let response: RpcMessage = serde_json::from_slice(&buf[..n])?;

        match response {
            RpcMessage::Response() => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    pub async fn send_put_request(
        &mut self,
        file_id: u64,
        chunk_id: u64,
        data: &[u8],
    ) -> Result<()> {
        println!(
            "Sending PutRequest to {}",
            format!("{}:{}", self.server_name, SERVER_PORT)
        );

        self.buffer[..data.len()].copy_from_slice(data);

        let put_request = RpcMessage::PutRequest(PutInfo {
            client_name: self.local_addr.clone(),
            file_id,
            chunk_id,
            remote_addr: unsafe { (*self.buffer.mr).addr } as u64,
            rkey: self.buffer.rkey().key,
            version: 0, // This is only for the servers to use, client should always set to 0
        });
        let request_bytes = serde_json::to_vec(&put_request)?;
        self.stream.write_all(&request_bytes).await?;

        let mut buf = vec![0; 1024];
        let n = self.stream.read(&mut buf).await?;
        let response: RpcMessage = serde_json::from_slice(&buf[..n])?;

        match response {
            RpcMessage::Response() => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }
}
