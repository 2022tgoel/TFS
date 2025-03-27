use super::{CompletionsManager, PendingCompletion};
use crate::net::IBSocket;
use anyhow::Result;
use ibverbs::MemoryRegion;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::fs::File;

pub const SERVER_PORT: u16 = 7777;
const NUM_BUFFERS: usize = 16; // Number of requests that can be simultaneously handled
pub const CHUNK_SIZE: usize = 64000; // 64KB chunk size

/// Put the chunk on the servers file system storage
#[derive(Debug, Serialize, Deserialize)]
pub struct PutInfo {
    pub client_name: String,
    pub file_id: u64,
    pub chunk_id: u64,
    pub remote_addr: u64,
    pub rkey: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
    PutRequest(PutInfo),
    GetRequest(),
    Response(),
}

pub struct RpcServer {
    local_addr: String,
    ib_socket: Arc<Mutex<IBSocket>>,
    completions_manager: Arc<Mutex<CompletionsManager>>,
    buffers: Vec<Arc<Mutex<MemoryRegion<u8>>>>,
}

impl RpcServer {
    pub fn new(local_addr: String) -> Result<Self> {
        let ib_socket = IBSocket::new()?;
        let buffers = (0..NUM_BUFFERS)
            .map(|_| {
                ib_socket
                    .register_memory(CHUNK_SIZE)
                    .map(|mem| Arc::new(Mutex::new(mem)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ib_socket = Arc::new(Mutex::new(ib_socket));
        let completions_manager = CompletionsManager::new();
        let ib_socket_clone = ib_socket.clone();
        let map_view = completions_manager.map_view();
        tokio::spawn(async move {
            CompletionsManager::manage_completions(ib_socket_clone, map_view).await
        });
        // Create a ring of memory regions to read data from the client

        Ok(Self {
            local_addr,
            ib_socket,
            completions_manager: Arc::new(Mutex::new(completions_manager)),
            buffers,
        })
    }

    pub async fn serve(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("{}:{}", self.local_addr, SERVER_PORT)).await?;
        println!("RPC server listening on port {}", SERVER_PORT);
        while let Ok((socket, _)) = listener.accept().await {
            let local_addr = self.local_addr.clone();
            let ib_socket = self.ib_socket.clone();
            let buffers = self.buffers.clone();
            let completions_manager = self.completions_manager.clone();
            
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(
                    socket,
                    ib_socket,
                    local_addr,
                    buffers,
                    completions_manager,
                )
                .await
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
        buffers: Vec<Arc<Mutex<MemoryRegion<u8>>>>,
        completions_manager: Arc<Mutex<CompletionsManager>>,
    ) -> Result<()> {
        let mut buf = vec![0; 1024];
        while let Ok(_) = socket.peer_addr() {
            // Handle requests synchronously until the client disconnects
            let n = socket.read(&mut buf).await?;
            let msg: RpcMessage = serde_json::from_slice(&buf[..n])?;
            match msg {
                RpcMessage::PutRequest(put_info) => {
                    println!("Received PutRequest from {}", put_info.client_name);
                    // Pick an available local buffer from the ring
                    let mut buffer;
                    'outer: loop {
                        for i in 0..NUM_BUFFERS {
                            let buffer_tmp = buffers[i].try_lock();
                            match buffer_tmp {
                                Ok(buffer_tmp) => {
                                    buffer = buffer_tmp;
                                    break 'outer;
                                }
                                Err(_) => {
                                    std::thread::yield_now(); 
                                    continue;
                                }
                            }
                        }
                    }

                    // Connect the infiniband device
                    println!("Connecting to {} from {}", put_info.client_name, local_addr);
                    let mut ib = ib_socket.lock().await;
                    ib.connect(local_addr.clone(), put_info.client_name.clone())
                        .await?;

                    // Issue an rdma read
                    let wr_id = ib.rdma_read(
                        &put_info.client_name,
                        &mut buffer,
                        put_info.remote_addr,
                        put_info.rkey,
                    )?;
                    drop(ib);

                    let (tx, mut rx) = mpsc::channel(1);

                    // Inform the completions queue manager of this event
                    completions_manager
                        .lock()
                        .await
                        .submit_request(PendingCompletion {
                            wr_id,
                            channel: tx,
                        });
                    // Wait for the completion of the request
                    let _resp = rx
                        .recv()
                        .await
                        .ok_or(anyhow::anyhow!("Error receiving from mpsc channel"))?;

                    // Save the file to /scratch/{file_id}_{chunk_id}.txt
                    let file_path = format!("/scratch/{}_{}.txt", put_info.file_id, put_info.chunk_id);
                    let mut file = File::create(file_path).await?;
                    file.write_all(&buffer[..CHUNK_SIZE]).await?;

                    let resp = RpcMessage::Response();
                    socket.write_all(&serde_json::to_vec(&resp)?).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}
