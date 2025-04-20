use crate::chunkserver::{CHUNK_SIZE, GetInfo, PutInfo, QPInfo, RpcMessage, SERVER_PORT};
use crate::net::{IBSocket, TcpConnectionManager, create_tcp_pool};
use anyhow::Result;
use bb8::Pool;
use ibverbs::MemoryRegion;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const NUM_BUFFERS: usize = 100; // Number of requests that can be simultaneously handled
const NUM_CONNS: u32 = 100; // Number of connections to keep open

#[derive(Clone)]
pub struct RpcClient {
    local_addr: String,
    server_name: String,
    ib_socket: Arc<Mutex<IBSocket>>,
    pool: Pool<TcpConnectionManager>,
    buffers: Vec<Arc<Mutex<MemoryRegion<u8>>>>,
}

impl RpcClient {
    pub async fn new(local_addr: String, server_name: String) -> Result<Self> {
        let ib_socket = IBSocket::new()?;
        let buffers = (0..NUM_BUFFERS)
            .map(|_| {
                ib_socket
                    .register_memory(CHUNK_SIZE)
                    .map(|mem| Arc::new(Mutex::new(mem)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let pool = create_tcp_pool(server_name.clone(), SERVER_PORT, NUM_CONNS).await?;
        Ok(Self {
            local_addr,
            server_name,
            ib_socket: Arc::new(Mutex::new(ib_socket)),
            pool,
            buffers,
        })
    }

    pub async fn connect_ib(&mut self) -> Result<()> {
        let request = RpcMessage::CreateQueuePair(QPInfo {
            addr: self.local_addr.clone(),
        });
        let request_bytes = serde_json::to_vec(&request)?;
        let mut conn = self.pool.get().await?;
        conn.write_all(&request_bytes).await?;

        println!(
            "Connecting to {} from {}",
            self.server_name, self.local_addr
        );
        self.ib_socket
            .lock()
            .await
            .connect(self.local_addr.clone(), self.server_name.clone())
            .await?;
        let mut buf = vec![0; 1024];
        let n = conn.read(&mut buf).await?;
        let response: RpcMessage = serde_json::from_slice(&buf[..n])?;

        match response {
            RpcMessage::Response(_) => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    // These are kind of unsafe at the moment, they should not be issued concurrently
    pub async fn send_put_request(&self, file_id: u64, chunk_id: u64, data: &[u8]) -> Result<()> {
        println!(
            "Sending PutRequest to {}",
            format!("{}:{}", self.server_name, SERVER_PORT)
        );

        // Pick an available local buffer from the ring
        let mut buffer;
        'outer: loop {
            for i in 0..NUM_BUFFERS {
                let buffer_tmp = self.buffers[i].try_lock();
                match buffer_tmp {
                    Ok(buffer_tmp) => {
                        buffer = buffer_tmp;
                        break 'outer;
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_micros(1)).await;
                        continue;
                    }
                }
            }
        }

        buffer[..data.len()].copy_from_slice(data);

        let put_request = RpcMessage::PutRequest(PutInfo {
            client_name: self.local_addr.clone(),
            file_id,
            chunk_id,
            remote_addr: unsafe { (*buffer.mr).addr } as u64,
            rkey: buffer.rkey().key,
            version: 0, // This is only for the servers to use, client should always set to 0
            size: data.len(),
        });
        let request_bytes = serde_json::to_vec(&put_request)?;
        let mut conn = self.pool.get().await?;
        conn.write_all(&request_bytes).await?;

        let mut buf = vec![0; 1024];
        let n = conn.read(&mut buf).await?;
        let response: RpcMessage = serde_json::from_slice(&buf[..n])?;

        match response {
            RpcMessage::Response(_) => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    pub async fn send_get_request(
        &self,
        file_id: u64,
        chunk_id: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        println!(
            "Sending GetRequest to {}",
            format!("{}:{}", self.server_name, SERVER_PORT)
        );

        let mut conn = self.pool.get().await?;
        // Pick an available local buffer from the ring
        let mut buffer;
        'outer: loop {
            for i in 0..NUM_BUFFERS {
                let buffer_tmp = self.buffers[i].try_lock();
                match buffer_tmp {
                    Ok(buffer_tmp) => {
                        buffer = buffer_tmp;
                        break 'outer;
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_micros(1)).await;
                        continue;
                    }
                }
            }
        }

        let get_request = RpcMessage::GetRequest(GetInfo {
            client_name: self.local_addr.clone(),
            file_id,
            chunk_id,
            remote_addr: unsafe { (*buffer.mr).addr } as u64,
            rkey: buffer.rkey().key,
            size,
        });
        let request_bytes = serde_json::to_vec(&get_request)?;

        conn.write_all(&request_bytes).await?;

        let mut buf = vec![0; 1024];
        let n = conn.read(&mut buf).await?;
        let response: RpcMessage = serde_json::from_slice(&buf[..n])?;

        // Print the data in the buffer
        println!("Data in buffer: {:?}", buffer[..15].to_vec());

        match response {
            RpcMessage::Response(_) => Ok(buffer[..size].to_vec()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }
}
