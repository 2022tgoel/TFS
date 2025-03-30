use crate::net::utils::my_name;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_zookeeper::*;

const ZOOKEERPER_CLIENT_PORT: u16 = 2181;
const ZOOKEEPER_SESSION_TIMEOUT: u64 = 4000; // ms, this is twice the tickTime in zoo.cfg
// Reference: https://zookeeper.apache.org/doc/current/zookeeperStarted.html

pub struct ZookeeperClient {
    node_path: String,
    heartbeat_path: String,
    client: ZooKeeper,
    // event_stream: Pin<Box<dyn Stream<Item = WatchedEvent> + Send>>,
    heartbeat_time: Arc<Mutex<Instant>>,
}

impl ZookeeperClient {
    pub async fn new() -> Result<Self> {
        let addr = format!("{}:{}", "127.0.0.1", ZOOKEERPER_CLIENT_PORT);
        let (client, default_watcher) = ZooKeeper::connect(&addr.parse()?).await?;
        let name: Vec<u8> = my_name()?.into_bytes();
        let my_name: &'static [u8] = Box::leak(Box::new(name));
        // Construct the chain of chunkserver nodes in zookeeper
        let heartbeat_time = Arc::new(Mutex::new(Instant::now()));
        let path = client
            .create(
                "/chunkservers",
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await?;
        match path {
            Err(error::Create::NodeExists) => {}
            _ => {
                path?;
            }
        }
        let path = client
            .create(
                "/leases",
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await?;
        match path {
            Err(error::Create::NodeExists) => {}
            _ => {
                path?;
            }
        }
        let node_path = client
            .create(
                "/chunkservers/node",
                my_name,
                Acl::open_unsafe(),
                CreateMode::EphemeralSequential,
            )
            .await??;
        let heartbeat_path = client
            .create(
                "/leases/heartbeat",
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::EphemeralSequential,
            )
            .await??;
        tokio::spawn(Self::send_hearbeats(
            client.clone(),
            heartbeat_path.clone(),
            heartbeat_time.clone(),
        ));
        Ok(Self {
            node_path,
            heartbeat_path,
            client,
            // event_stream: Box::pin(default_watcher),
            heartbeat_time,
        })
    }

    /// Check if the current node is the head of the chain
    /// Returns a tuple of (is_head, next_node)
    pub async fn get_chain_info(&self) -> Result<(bool, Option<String>)> {
        loop {
            let children = self.client.get_children("/chunkservers").await?;
            let Some(children) = children else {
                return Err(anyhow::anyhow!(
                    "Failed to get children from zookeeper, chunkserver path doesn't exist"
                ));
            };
            // sort the children
            let mut children: Vec<String> = children.into_iter().collect();
            children.sort();
            // Get the node lexicographically larger than the current node
            let node_path = self
                .node_path
                .split('/')
                .last()
                .ok_or(anyhow::anyhow!("Failed to parse node path"))?;
            let next_node: Option<&String> = children
                .iter()
                .find(|child| child > &&node_path.to_string());
            dbg!(&children, &node_path, &next_node);
            if let Some(next_node) = next_node {
                match self
                    .client
                    .get_data(&format!("/chunkservers/{}", next_node))
                    .await?
                {
                    Some((node_name, _)) => {
                        return Ok((
                            children[0] == self.node_path,
                            Some(String::from_utf8(node_name)?),
                        ));
                    }
                    None => {
                        // The file was deleted in between the get_children and get_data
                        // This is a rare case, but it can happen
                        // Just retry, at some point the chain should be stable
                        dbg!("File was deleted in zookeeper, retrying",);
                    }
                }
            } else {
                return Ok((children[0] == node_path, None));
            }
        }
    }

    /// Send hearbeats to the zookeeper to maintain 'leases' on the session
    pub async fn send_hearbeats(
        client: ZooKeeper,
        heartbeat_path: String,
        heartbeat_time: Arc<Mutex<Instant>>,
    ) -> Result<()> {
        loop {
            let time = Instant::now();
            client.set_data(&heartbeat_path, None, &b"Hi"[..]).await??;
            *heartbeat_time.lock().await = time;
            // Have the lease renewal loop run slightly faster than the lease timeout
            tokio::time::sleep(Duration::from_millis(ZOOKEEPER_SESSION_TIMEOUT / 2)).await;
        }
    }

    /// Check if the current node is still alive in zookeeper (by checking that the lease is still valid)
    /// If the node is deleted in zookeeper, it cannot respond to requests.
    pub async fn still_alive(&self) -> Result<bool> {
        let elapsed = self.heartbeat_time.lock().await.elapsed();
        Ok(elapsed < Duration::from_millis(ZOOKEEPER_SESSION_TIMEOUT))
    }
}
