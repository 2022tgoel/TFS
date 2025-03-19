use crate::net::{IBSocket, MIN_WR_ID};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use ibverbs::{MemoryRegion, ibv_wc};
use dashmap::DashMap;

const MAX_POLLED_COMPLETIONS: u64 = 32;

struct FileChunkMetadata {
    file_id: u64,
    chunk_id: u64,
    buffer: MutexGuard<MemoryRegion>,
    channel: mspc::Sender<u64>,
}

struct PendingCompletion {
    wr_id: u64, 
    metadata: FileChunkMetadata,
}

struct CompletionsManager {
    requests: DashMap<u64, FileChunkMetadata>,
}

impl CompletionsManager {
    pub fn new() -> CompletionsManager {
        CompletionsManager {
            requests: Arc::new(DashMap::new())
        }
    }

    pub fn submit_request(&mut self, request: PendingCompletion) {
        self.requests.insert(request.wr_id, request.metadata)
    }

    pub fn map_view(&self) -> Arc<DashMap> {
        return self.requests.clone()
    }

    pub async fn manage_completions(ib_socket: Arc<Mutex<IBSocket>>, requests: Arc<DashMap>) {
        let mut completions = [ibverbs::ibv_wc::default(); 32];
        loop {
            let mut ib = ib_socket.lock().await;
            if let Some(completed) = ib.poll(&mut completions) {
                drop(ib);
                for completion in completed.iter() {
                    let wr_id = completion.wr_id();
                    if wr_id > MIN_WR_ID {
                        loop { // Wait until the request is submitted by the rpc_server
                            if let Some(metadata) = requests.get(wr_id) {
                                requests.remove(wr_id);
                                if let Err(_) = metadata.channel.send(0).await {
                                    eprintln("Reciever incorrectly dropped");
                                }
                                break;
                            }
                        }
                    }
                }
            } else {
                eprintln!("Error polling the completions queue");
            }
        }
    }
}