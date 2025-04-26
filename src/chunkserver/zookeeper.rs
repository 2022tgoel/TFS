use crate::net::utils::my_name;
use anyhow::Result;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_zookeeper::*;
use tracing::info;

pub const ZOOKEEPER_CLIENT_PORT: u16 = 2181;
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
    pub async fn new(name: String) -> Result<Self> {
        let addr = format!("{}:{}", "127.0.0.1", ZOOKEEPER_CLIENT_PORT);
        let (client, _default_watcher) = ZooKeeper::connect(&addr.parse()?)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to zookeeper: {}", e))?;
        let name: Vec<u8> = name.into_bytes();
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
            Err(error::Create::NodeExists) => {
                tracing::info!("chunkservers path already exists, skipping");
            }
            _ => {
                path.map_err(|e| anyhow::anyhow!("Failed to create chunkservers path: {}", e))?;
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
            Err(error::Create::NodeExists) => {
                tracing::info!("leases path already exists, skipping");
            }
            _ => {
                path.map_err(|e| anyhow::anyhow!("Failed to create leases path: {}", e))?;
            }
        }
        let node_path = client
            .create(
                "/chunkservers/node",
                my_name,
                Acl::open_unsafe(),
                CreateMode::EphemeralSequential,
            )
            .await?
            .map_err(|e| anyhow::anyhow!("Failed to create node path: {}", e))?;
        let heartbeat_path = client
            .create(
                "/leases/heartbeat",
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::EphemeralSequential,
            )
            .await?
            .map_err(|e| anyhow::anyhow!("Failed to create heartbeat path: {}", e))?;
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
            tracing::info!("Getting children");
            let children = self.client.get_children("/chunkservers").await?;
            tracing::info!("Children: {:?}", children);
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
            if let Some(next_node) = next_node {
                match self
                    .client
                    .get_data(&format!("/chunkservers/{}", next_node))
                    .await?
                {
                    Some((node_name, _)) => {
                        return Ok((
                            children[0] == node_path,
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

    /// Update the size of the inode
    pub async fn update_size(&self, inode: u64, size: usize) -> Result<usize> {
        let path = format!("/inode{:010}", inode);
        let data = self.client.get_data(&path).await?;
        if let Some((data, _)) = data {
            // split by comma and get the second element
            let data = String::from_utf8(data)?;
            let name = data.split(",").nth(0).unwrap();
            let new_data: Cow<'static, [u8]> =
                Cow::Owned(format!("{},{}", name, size).as_bytes().to_vec());
            self.client.set_data(&path, None, new_data).await??;
            Ok(size)
        } else {
            // This file was created without the fuse client, by writing chunks directly
            // Therefore, we don't need to worry about filenames
            // let new_data: Cow<'static, [u8]> = Cow::Owned(format!(",{}", size).as_bytes().to_vec());
            // self.client.set_data(&path, None, new_data).await??;
            Ok(0)
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
