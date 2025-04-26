use super::{ChunkVersionTable, CompletionsManager, PendingCompletion, State, ZookeeperClient};
use crate::net::{HostName, IBSocket, Listener, ManagerCreate, NUM_CONNS, create_tcp_pool};
use anyhow::Result;
use bb8::{ManageConnection, Pool};
use ibverbs::MemoryRegion;
use serde::{Deserialize, Serialize};
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, timeout};
use tracing::{Level, info, span};

pub const SERVER_PORT: u16 = 7777;
const RPC_TIMEOUT: Duration = Duration::from_secs(15); // How long to wait for an RPC response before assuming server failure

// For testing, we need to limit the number of buffers, since
// the servers and clients are sharing the same physical hardware
// Reduces startup time.
#[cfg(test)]
const NUM_BUFFERS: usize = 5;
#[cfg(test)]
pub const CHUNK_SIZE: usize = 1000;

#[cfg(not(test))]
const NUM_BUFFERS: usize = 100;
#[cfg(not(test))]
pub const CHUNK_SIZE: usize = 10000000; // 10MB chunk size

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
    pub size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetInfo {
    pub client_name: String,
    pub file_id: u64,
    pub chunk_id: u64,
    pub remote_addr: u64,
    pub rkey: u32,
    pub size: usize,
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

pub struct PeerServer<TcpConnectionManager>
where
    TcpConnectionManager: ManagerCreate + ManageConnection<Error = io::Error> + Clone,
{
    name: String,
    pool: Pool<TcpConnectionManager>,
}

pub struct RpcServer<TcpConnectionManager>
where
    TcpConnectionManager: ManagerCreate + ManageConnection<Error = io::Error> + Clone,
{
    local_addr: HostName,
    ib_socket: Arc<Mutex<IBSocket<TcpConnectionManager::Listener>>>,
    completions_manager: Arc<Mutex<CompletionsManager>>,
    buffers: Vec<Arc<Mutex<MemoryRegion<u8>>>>,
    chunk_version_table: Arc<ChunkVersionTable>,
    zookeeper: Arc<Mutex<ZookeeperClient>>,
    peer: Arc<Mutex<Option<PeerServer<TcpConnectionManager>>>>,
    request_id: AtomicU64,
}

impl<TcpConnectionManager> RpcServer<TcpConnectionManager>
where
    TcpConnectionManager: ManagerCreate + ManageConnection<Error = io::Error> + Clone,
    TcpConnectionManager::Connection: AsyncReadExt + AsyncWriteExt + Unpin,
{
    pub async fn new(local_addr: HostName) -> Result<Self> {
        // make /scratch/files if it doesn't exist
        if !std::path::Path::new("/scratch/files").exists() {
            std::fs::create_dir_all("/scratch/files")?;
        }

        let ib_socket = IBSocket::<TcpConnectionManager::Listener>::new()?;
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
        let zookeeper = ZookeeperClient::new(local_addr.get_name().to_string())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create zookeeper client: {}", e))?;

        let (is_head, _) = zookeeper.get_chain_info().await?;
        if is_head {
            tracing::info!("{} is the head of the chain", local_addr.get_name());
        }
        Ok(Self {
            local_addr,
            ib_socket,
            completions_manager: Arc::new(Mutex::new(completions_manager)),
            buffers,
            chunk_version_table: Arc::new(ChunkVersionTable::new()),
            zookeeper: Arc::new(Mutex::new(zookeeper)),
            peer: Arc::new(Mutex::new(None)),
            request_id: AtomicU64::new(0),
        })
    }

    pub async fn serve(&self) -> Result<()> {
        let listener = if self.local_addr.is_turmoil() {
            TcpConnectionManager::Listener::bind((IpAddr::from(Ipv4Addr::UNSPECIFIED), SERVER_PORT))
                .await?
        } else {
            TcpConnectionManager::Listener::bind(format!(
                "{}:{}",
                self.local_addr.get_name(),
                SERVER_PORT
            ))
            .await?
        };
        println!("RPC server listening on port {}", SERVER_PORT);
        while let Ok((socket, _)) = listener.accept().await {
            let local_addr = self.local_addr.clone();
            let ib_socket = self.ib_socket.clone();
            let buffers = self.buffers.clone();
            let completions_manager = self.completions_manager.clone();
            let chunk_version_table = self.chunk_version_table.clone();
            let zookeeper = self.zookeeper.clone();
            let peer = self.peer.clone();
            let request_id = self.request_id.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(
                    socket,
                    ib_socket,
                    local_addr,
                    buffers,
                    completions_manager,
                    chunk_version_table,
                    zookeeper,
                    peer,
                    request_id,
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
        mut socket: <TcpConnectionManager::Listener as Listener>::Stream,
        ib_socket: Arc<Mutex<IBSocket<TcpConnectionManager::Listener>>>,
        local_addr: HostName,
        buffers: Vec<Arc<Mutex<MemoryRegion<u8>>>>,
        completions_manager: Arc<Mutex<CompletionsManager>>,
        chunk_version_table: Arc<ChunkVersionTable>,
        zookeeper: Arc<Mutex<ZookeeperClient>>,
        peer: Arc<Mutex<Option<PeerServer<TcpConnectionManager>>>>,
        _request_id: u64,
    ) -> Result<()> {
        let mut buf = vec![0; 1024];
        loop {
            // Handle requests synchronously until the client disconnects
            let n = socket.read(&mut buf).await;
            if n.is_err() {
                break;
            }
            let n = n?;
            if n == 0 {
                break;
            }
            let msg: RpcMessage = serde_json::from_slice(&buf[..n])?;
            match msg {
                RpcMessage::CreateQueuePair(qp_info) => {
                    info!("Received CreateQueuePair from {}", qp_info.addr);
                    let mut ib = ib_socket.lock().await;
                    ib.connect(local_addr.clone(), qp_info.addr).await?;
                    let resp = RpcMessage::Response(Status::Success);
                    socket.write_all(&serde_json::to_vec(&resp)?).await?;
                }
                RpcMessage::PutRequest(put_info) => {
                    // Query zookeeper for chain information
                    let (is_head, next_node) = zookeeper.lock().await.get_chain_info().await?;
                    let is_tail = next_node.is_none();
                    println!(
                        "Received PutRequest from {} is_head: {}, next_node: {:?}",
                        put_info.client_name, is_head, next_node
                    );
                    let version = if is_head {
                        chunk_version_table.get_new_version(put_info.file_id, put_info.chunk_id)
                    } else {
                        if let Err(e) = chunk_version_table.insert_version(
                            put_info.file_id,
                            put_info.chunk_id,
                            put_info.version,
                        ) {
                            println!("Insertion failed: {}", e);
                            let resp = RpcMessage::Response(Status::StaleData);
                            socket.write_all(&serde_json::to_vec(&resp)?).await?;
                            continue;
                        }
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
                                    tokio::time::sleep(Duration::from_micros(1)).await;
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
                        put_info.size,
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
                        let mut peer = peer.lock().await;
                        if peer.is_none() || peer.as_ref().unwrap().name != next_node {
                            // The peer changed. Create a new TCP pool.
                            *peer = Some(PeerServer {
                                name: next_node.clone(),
                                pool: create_tcp_pool::<TcpConnectionManager>(
                                    next_node.clone(),
                                    SERVER_PORT,
                                    NUM_CONNS,
                                )
                                .await?,
                            });
                        }
                        let pool = peer.as_ref().unwrap().pool.clone();
                        let mut next_socket = pool.get().await?;
                        drop(peer);
                        if must_connect {
                            let resp = RpcMessage::CreateQueuePair(QPInfo {
                                addr: local_addr.clone().get_name().to_string(),
                            });
                            println!("Sending CreateQueuePair to {}", next_node);
                            next_socket.write_all(&serde_json::to_vec(&resp)?).await?;
                            let mut ib = ib_socket.lock().await;
                            ib.connect(local_addr.clone(), next_node.clone()).await?;
                            drop(ib);
                            let mut buf = vec![0; 1024];
                            let n = next_socket.read(&mut buf).await?;
                            let _response: RpcMessage = serde_json::from_slice(&buf[..n])?;
                            println!("Received CreateQueuePair response from {}", next_node);
                        }
                        let resp = RpcMessage::PutRequest(PutInfo {
                            client_name: local_addr.clone().get_name().to_string(),
                            file_id: put_info.file_id,
                            chunk_id: put_info.chunk_id,
                            remote_addr: unsafe { (*buffer.mr).addr } as u64,
                            rkey: buffer.rkey().key,
                            version: version,
                            size: put_info.size,
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
                            RpcMessage::Response(Status::Success) => {}
                            RpcMessage::Response(Status::StaleData) => {
                                let resp = RpcMessage::Response(Status::StaleData);
                                println!("Sending StaleData to {}", local_addr.get_name());
                                socket.write_all(&serde_json::to_vec(&resp)?).await?;
                                continue;
                            }
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
                    file.write_all(&buffer[..put_info.size]).await?;
                    // If tail, update the size of the file in zookeeper
                    if is_tail {
                        zookeeper
                            .lock()
                            .await
                            .update_size(put_info.file_id, put_info.size)
                            .await?;
                    }
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
                        {
                            let get_span = span!(Level::INFO, "get_request");
                            let _get_guard = get_span.enter();

                            let file_path = format!(
                                "/scratch/files/{}_{}_{}.txt",
                                get_info.file_id, get_info.chunk_id, version
                            );
                            if !std::path::Path::new(&file_path).exists() {
                                eprintln!("File {} does not exist", file_path);
                            }
                            let span = span!(Level::INFO, "open_file");
                            let _guard = span.enter();
                            let mut file = File::open(file_path.clone()).await?;

                            let mut buffer;
                            {
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
                            }
                            let read_span = span!(Level::INFO, "file_read");
                            let _read_guard = read_span.enter();
                            file.read(&mut buffer[..get_info.size]).await?;
                            let rdma_span = span!(Level::INFO, "issue_rdma_write");
                            let _rdma_guard = rdma_span.enter();
                            let mut ib = ib_socket.lock().await;
                            let wr_id = ib.rdma_write(
                                &get_info.client_name,
                                &mut buffer,
                                get_info.remote_addr,
                                get_info.rkey,
                                get_info.size,
                            )?;
                            drop(ib);

                            let (tx, mut rx) = mpsc::channel(1);

                            completions_manager
                                .lock()
                                .await
                                .submit_request(PendingCompletion { wr_id, channel: tx });

                            let _resp = rx
                                .recv()
                                .await
                                .ok_or(anyhow::anyhow!("Error receiving from mpsc channel"))?;
                        }
                        {
                            let response_span = span!(Level::INFO, "send_response");
                            let _response_guard = response_span.enter();

                            let resp = RpcMessage::Response(Status::Success);
                            socket.write_all(&serde_json::to_vec(&resp)?).await?;
                        }
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
