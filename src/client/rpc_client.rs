use crate::chunkserver::{CHUNK_SIZE, GetInfo, PutInfo, QPInfo, RpcMessage, SERVER_PORT};
use crate::net::{HostName, IBSocket, ManagerCreate, NUM_CONNS, create_tcp_pool};
use anyhow::Result;
use bb8::{ManageConnection, Pool};
use ibverbs::MemoryRegion;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::info;
// For testing, we need to limit the number of buffers, since
// the servers and clients are sharing the same physical hardware
// Reduces startup time.
#[cfg(feature = "test-config")]
const NUM_BUFFERS: usize = 5;

#[cfg(not(feature = "test-config"))]
const NUM_BUFFERS: usize = 100;

pub struct RpcClient<TcpConnectionManager>
where
    TcpConnectionManager: ManagerCreate + ManageConnection<Error = io::Error> + Clone,
{
    local_addr: HostName,
    server_name: String,
    ib_socket: Arc<Mutex<IBSocket<TcpConnectionManager::Listener>>>,
    pool: Pool<TcpConnectionManager>,
    buffers: Vec<Arc<Mutex<MemoryRegion<u8>>>>,
}

impl<TcpConnectionManager> Clone for RpcClient<TcpConnectionManager>
where
    TcpConnectionManager: ManagerCreate + ManageConnection<Error = io::Error> + Clone,
    TcpConnectionManager::Connection: AsyncReadExt + AsyncWriteExt + Unpin,
{
    fn clone(&self) -> Self {
        Self {
            local_addr: self.local_addr.clone(),
            server_name: self.server_name.clone(),
            ib_socket: self.ib_socket.clone(),
            pool: self.pool.clone(),
            buffers: self.buffers.clone(),
        }
    }
}

impl<TcpConnectionManager> RpcClient<TcpConnectionManager>
where
    TcpConnectionManager: ManagerCreate + ManageConnection<Error = io::Error> + Clone,
    TcpConnectionManager::Connection: AsyncReadExt + AsyncWriteExt + Unpin,
{
    pub async fn new(local_addr: HostName, server_name: String) -> Result<Self> {
        let ib_socket = IBSocket::<TcpConnectionManager::Listener>::new()?;
        let buffers = (0..NUM_BUFFERS)
            .map(|_| {
                ib_socket
                    .register_memory(CHUNK_SIZE)
                    .map(|mem| Arc::new(Mutex::new(mem)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        tracing::info!("Creating TCP pool");
        let pool =
            create_tcp_pool::<TcpConnectionManager>(server_name.clone(), SERVER_PORT, NUM_CONNS)
                .await?;
        tracing::info!("TCP pool created");
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
            addr: self.local_addr.clone().get_name().to_string(),
        });
        let request_bytes = serde_json::to_vec(&request)?;
        let mut conn = self.pool.get().await?;
        conn.write_all(&request_bytes).await?;

        info!(
            "Connecting to {} from {}",
            self.server_name,
            self.local_addr.get_name()
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
        info!(
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
            client_name: self.local_addr.get_name().to_string(),
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
        info!(
            "Sending GetRequest to {}",
            format!("{}:{}", self.server_name, SERVER_PORT)
        );

        let mut conn = self.pool.get().await?;
        // Pick an available local buffer from the ring
        let buffer;
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
            client_name: self.local_addr.get_name().to_string(),
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

        match response {
            RpcMessage::Response(_) => Ok(buffer[..size].to_vec()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }
}
