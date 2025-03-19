use serde::{Deserialize, Serialize};
use crate::net::IBSocket;
use anyhow::Result;
use super::{CompletionsManager, PendingCompletion, FileChunkMetadata};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use ibverbs::MemoryRegion;

const SERVER_PORT: u16 = 7777;
const NUM_BUFFERS: usize = 32; // Number of requests that can be simultaneously handled
const CHUNK_SIZE: usize = 64000000; // 64MB chunk size

/// Put the chunk on the servers file system storage
#[derive(Debug, Serialize, Deserialize)]
struct PutInfo {
    client_name: String,
    file_id: u64,
    chunk_id: u64,
    remote_addr: u64,
    rkey: u32,
}


#[derive(Debug, Serialize, Deserialize)]
enum RpcMessage {
    PutRequest(PutInfo),
    GetRequest(),
    Response(Result<()>),
}


struct RPCServer {
    local_addr: String,
    ib_socket: Arc<Mutex<IBSocket>>,
    completions_manager: CompletionsManager,
    buffers: Vec<Arc<Mutex<MemoryRegion<u8>>>>,
    buffer_index: usize,
}


impl RPCServer {
    pub fn new(local_addr: String) -> Result<Self> {
        let ib_socket = Arc::new(Mutex::new(IBSocket::new()?));
        let completions_manager = CompletionsManager::new();
        let ib_socket_clone = ib_socket.clone();
        let map_view = completions_manager.map_view();
        tokio::spawn(async move {
            CompletionsManager::manage_completions(ib_socket_clone, map_view).await
        });
        // Create a ring of memory regions to read data from the client
        let buffers = (0..NUM_BUFFERS).map(|_| Arc::new(Mutex::new(ib_socket.register_memory(CHUNK_SIZE)?))).collect::<Result<Vec<_>>>()?;
        Ok(Self { local_addr, ib_socket, completions_manager, buffers, buffer_index: 0 })
    }

    pub async fn serve(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("{}:{}", self.local_addr, SERVER_PORT)).await?;
        println!("RPC server listening on port {}", SERVER_PORT);
        while let Ok((socket, _)) = listener.accept().await {
            let ib_socket = self.ib_socket.clone();
            let local_addr = self.local_addr.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(socket, ib_socket, local_addr).await {
                    eprintln!("Error handling RPC connection: {}", e);
                }
            });
        }
    }

    async fn handle_connection(socket: TcpStream, ib_socket: IBSocket, local_addr: String) -> Result<()> {
        let mut buf = vec![0; 1024];
        let n = socket.read(&mut buf).await?;
        let msg: RpcMessage = serde_json::from_slice(&buf[..n])?;
        while let Ok(_) = socket.peer_addr() { // Handle requests synchronously until the client disconnects
            match msg {
                RpcMessage::PutRequest(put_info) => {

                    // Pick an available local buffer from the ring 
                    let mut buffer;
                    'outer: loop {
                        for i in 0..NUM_BUFFERS {
                            let mut buffer_tmp = self.buffers[i].try_lock();
                            match buffer_tmp {
                                Ok(buffer_tmp) => {
                                    buffer = buffer_tmp;
                                    break 'outer;
                                }
                                Err(_) => continue,
                            }
                        }
                    }

                    // Connect the infiniband device
                    let mut ib = ib_socket.lock().await;
                    ib.connect(local_addr.clone(), remote_addr.clone()).await?;

                    // Issue an rdma read
                    let wr_id = ib.rdma_read(put_info.client_name, buffer, put_info.remote_addr, put_info.rkey);
                    drop(ib);

                    let (tx, rx) = mspc::channel(1);
                    // Inform the completions queue manager of this event
                    self.completions_manager.submit_request(PendingCompletion {
                        wr_id,
                        metadata: FileChunkMetadata {
                            file_id: put_info.file_id,
                            chunk_id: put_info.chunk_id,
                            buffer: buffer,
                            channel: tx
                        }
                    });
                    // Wait for the completion of the request
                    let resp = rx.recv().await?;
                    let resp = RpcMessage::Response(Ok());
                    socket.write_all(&serde_json::to_vec(&resp)?).await?;
                }
            }
        }
    }
}

