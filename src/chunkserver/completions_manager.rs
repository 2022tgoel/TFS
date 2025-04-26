use crate::net::{IBSocket, Listener, MIN_WR_ID, parse_completion_error};
use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Duration;

const MAX_POLLED_COMPLETIONS: usize = 32;

pub struct PendingCompletion {
    pub wr_id: u64,
    pub channel: mpsc::Sender<u64>,
}

pub struct CompletionsManager {
    requests: Arc<DashMap<u64, mpsc::Sender<u64>>>,
}

impl CompletionsManager {
    pub fn new() -> CompletionsManager {
        CompletionsManager {
            requests: Arc::new(DashMap::new()),
        }
    }

    pub fn submit_request(&mut self, request: PendingCompletion) {
        self.requests.insert(request.wr_id, request.channel);
    }

    pub fn map_view(&self) -> Arc<DashMap<u64, mpsc::Sender<u64>>> {
        return self.requests.clone();
    }

    pub async fn manage_completions<L: Listener>(
        ib_socket: Arc<Mutex<IBSocket<L>>>,
        requests: Arc<DashMap<u64, mpsc::Sender<u64>>>,
    ) {
        let mut completions = [ibverbs::ibv_wc::default(); MAX_POLLED_COMPLETIONS];
        loop {
            let ib = ib_socket.lock().await;
            if let Ok(n_completions) = ib.poll_completions(&mut completions) {
                drop(ib);
                for i in 0..n_completions {
                    let completion = &completions[i];
                    let wr_id = completion.wr_id();
                    if let Err(e) = parse_completion_error(completion) {
                        eprintln!("Error with RDMA operation: {}", e);
                    }
                    if wr_id >= MIN_WR_ID {
                        loop {
                            // Wait until the request is submitted by the rpc_server
                            let mut finished = false;
                            if let Some(channel) = requests.get(&wr_id) {
                                if let Err(_) = channel.send(0).await {
                                    eprintln!("Reciever incorrectly dropped");
                                }
                                finished = true;
                            }
                            if finished {
                                requests.remove(&wr_id);
                                break;
                            }
                            tokio::time::sleep(Duration::from_micros(1)).await;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            } else {
                eprintln!("Error polling the completions queue");
            }
        }
    }
}
