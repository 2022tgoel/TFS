use super::{ChunkVersionTable, CompletionsManager, PendingCompletion, State, ZookeeperClient};
use crate::net::IBSocket;
use anyhow::Result;
use ibverbs::MemoryRegion;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, timeout};

pub const SERVER_PORT: u16 = 7777;
const NUM_BUFFERS: usize = 16; // Number of requests that can be simultaneously handled
pub const CHUNK_SIZE: usize = 64000; // 64KB chunk size
const RPC_TIMEOUT: Duration = Duration::from_secs(15); // How long to wait for an RPC response before assuming server failure

#[derive(Debug, Serialize, Deserialize)]
pub struct QPInfo {
    pub addr: String,
}

/// Put the chunk on the servers file system storage
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PutInfo {
    pub client_name: String,
    pub file_id: u64,
    pub chunk_id: u64,
    pub remote_addr: u64,
    pub rkey: u32,
    pub version: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetInfo {
    pub client_name: String,
    pub file_id: u64,
    pub chunk_id: u64,
    pub remote_addr: u64,
    pub rkey: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Status {
    Success,
    StaleData,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
    CreateQueuePair(QPInfo),
    PutRequest(PutInfo),
    GetRequest(GetInfo),
    Response(Status),
}

pub struct RpcServer {
    local_addr: String,
    ib_socket: Arc<Mutex<IBSocket>>,
    completions_manager: Arc<Mutex<CompletionsManager>>,
    buffers: Vec<Arc<Mutex<MemoryRegion<u8>>>>,
    chunk_version_table: Arc<ChunkVersionTable>,
    zookeeper: Arc<Mutex<ZookeeperClient>>,
}

impl RpcServer {
    pub async fn new(local_addr: String) -> Result<Self> {
        // make /scratch/files if it doesn't exist
        if !std::path::Path::new("/scratch/files").exists() {
            std::fs::create_dir_all("/scratch/files")?;
        }

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
        let zookeeper = ZookeeperClient::new().await?;

        let (is_head, _) = zookeeper.get_chain_info().await?;
        if is_head {
            println!("{} is the head of the chain", local_addr);
        }
        Ok(Self {
            local_addr,
            ib_socket,
            completions_manager: Arc::new(Mutex::new(completions_manager)),
            buffers,
            chunk_version_table: Arc::new(ChunkVersionTable::new()),
            zookeeper: Arc::new(Mutex::new(zookeeper)),
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
            let chunk_version_table = self.chunk_version_table.clone();
            let zookeeper = self.zookeeper.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(
                    socket,
                    ib_socket,
                    local_addr,
                    buffers,
                    completions_manager,
                    chunk_version_table,
                    zookeeper,
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
        chunk_version_table: Arc<ChunkVersionTable>,
        zookeeper: Arc<Mutex<ZookeeperClient>>,
    ) -> Result<()> {
        let mut buf = vec![0; 1024];
        while let Ok(_) = socket.peer_addr() {
            // Handle requests synchronously until the client disconnects
            let n = socket.read(&mut buf).await;
            if n.is_err() {
                eprintln!("Client disconnected");
                break;
            }
            let n = n?;
            let msg: RpcMessage = serde_json::from_slice(&buf[..n])?;
            match msg {
                RpcMessage::CreateQueuePair(qp_info) => {
                    println!("Received CreateQueuePair from {}", qp_info.addr);
                    let mut ib = ib_socket.lock().await;
                    ib.connect(local_addr.clone(), qp_info.addr).await?;
                    let resp = RpcMessage::Response(Status::Success);
                    socket.write_all(&serde_json::to_vec(&resp)?).await?;
                }
                RpcMessage::PutRequest(put_info) => {
                    println!("Received PutRequest from {}", put_info.client_name);
                    // Query zookeeper for chain information
                    let (is_head, next_node) = zookeeper.lock().await.get_chain_info().await?;
                    let version = if is_head {
                        chunk_version_table.get_new_version(put_info.file_id, put_info.chunk_id)
                    } else {
                        put_info.version
                    };
                    println!("Will forward to {:?}", next_node);
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

                    let mut ib = ib_socket.lock().await;
                    // Issue an rdma read
                    let wr_id = ib.rdma_read(
                        &put_info.client_name,
                        &mut buffer,
                        put_info.remote_addr,
                        put_info.rkey,
                    )?;

                    let must_connect =
                        next_node.is_some() && !ib.is_connected(&next_node.clone().unwrap());
                    drop(ib);

                    let (tx, mut rx) = mpsc::channel(1);

                    // Inform the completions queue manager of this event
                    completions_manager
                        .lock()
                        .await
                        .submit_request(PendingCompletion { wr_id, channel: tx });
                    // Wait for the completion of the request
                    let _resp = rx
                        .recv()
                        .await
                        .ok_or(anyhow::anyhow!("Error receiving from mpsc channel"))?;

                    println!("Received completion for wr_id {}", wr_id);

                    // Forward the request to the next node in the chain
                    if let Some(next_node) = next_node {
                        let mut next_socket =
                            TcpStream::connect(format!("{}:{}", next_node, SERVER_PORT)).await?;
                        if must_connect {
                            let resp = RpcMessage::CreateQueuePair(QPInfo {
                                addr: local_addr.clone(),
                            });
                            println!("Sending CreateQueuePair to {}", next_node);
                            next_socket.write_all(&serde_json::to_vec(&resp)?).await?;
                            let mut ib = ib_socket.lock().await;
                            ib.connect(local_addr.clone(), next_node.clone()).await?;
                            drop(ib);
                            let mut buf = vec![0; 1024];
                            let n = next_socket.read(&mut buf).await?;
                            let response: RpcMessage = serde_json::from_slice(&buf[..n])?;
                            println!("Received CreateQueuePair response from {}", next_node);
                        }
                        let resp = RpcMessage::PutRequest(PutInfo {
                            client_name: local_addr.clone(),
                            file_id: put_info.file_id,
                            chunk_id: put_info.chunk_id,
                            remote_addr: unsafe { (*buffer.mr).addr } as u64,
                            rkey: buffer.rkey().key,
                            version: version,
                        });
                        next_socket.write_all(&serde_json::to_vec(&resp)?).await?;
                        let mut buf = vec![0; 1024];
                        let n = timeout(RPC_TIMEOUT, next_socket.read(&mut buf))
                            .await
                            .map_err(|_| {
                                anyhow::anyhow!(format!(
                                    "Timeout waiting for response from next node {}",
                                    next_node
                                ))
                            })??;
                        let msg: RpcMessage = serde_json::from_slice(&buf[..n])?;
                        match msg {
                            RpcMessage::Response(_) => {}
                            _ => {
                                eprintln!("Error forwarding request to next node");
                            }
                        }
                    }

                    let file_path = format!(
                        "/scratch/files/{}_{}_{}.txt",
                        put_info.file_id, put_info.chunk_id, version
                    );
                    let mut file = File::create(file_path.clone()).await?;
                    file.write_all(&buffer[..CHUNK_SIZE]).await?;
                    if let State::Aborted = chunk_version_table.commit_version(
                        put_info.file_id,
                        put_info.chunk_id,
                        version,
                    )? {
                        std::fs::remove_file(file_path)?;
                    }
                    let resp = RpcMessage::Response(Status::Success);
                    socket.write_all(&serde_json::to_vec(&resp)?).await?;
                }
                RpcMessage::GetRequest(get_info) => {
                    println!("Received GetRequest from {}", get_info.client_name);
                    if let Some(version) =
                        chunk_version_table.get_version(get_info.file_id, get_info.chunk_id)
                    {
                        let file_path = format!(
                            "/scratch/files/{}_{}_{}.txt",
                            get_info.file_id, get_info.chunk_id, version
                        );
                        let mut file = File::open(file_path.clone()).await?;
                        // Read the chunk into a buffer
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
                        file.read(&mut buffer).await?;
                        // Issue an rdma write
                        let mut ib = ib_socket.lock().await;
                        let wr_id = ib.rdma_write(
                            &get_info.client_name,
                            &mut buffer,
                            get_info.remote_addr,
                            get_info.rkey,
                        )?;
                        drop(ib);

                        let (tx, mut rx) = mpsc::channel(1);

                        // Inform the completions queue manager of this event
                        completions_manager
                            .lock()
                            .await
                            .submit_request(PendingCompletion { wr_id, channel: tx });
                        // Wait for the completion of the request
                        let _resp = rx
                            .recv()
                            .await
                            .ok_or(anyhow::anyhow!("Error receiving from mpsc channel"))?;
                        let resp = RpcMessage::Response(Status::Success);
                        socket.write_all(&serde_json::to_vec(&resp)?).await?;
                    } else {
                        let resp = RpcMessage::Response(Status::StaleData);
                        socket.write_all(&serde_json::to_vec(&resp)?).await?;
                        continue;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
