use crate::chunkserver::ZOOKEEPER_CLIENT_PORT;
use anyhow::Result;
use log::info;
use std::borrow::Cow;
use std::net::ToSocketAddrs;
use tokio_zookeeper::*;

pub struct ZookeeperClient {
    client: ZooKeeper,
}

impl ZookeeperClient {
    pub async fn new(name: String) -> Result<Self> {
        let addr = format!("{}:{}", name, ZOOKEEPER_CLIENT_PORT);
        // Resolving the hostname to a SocketAddr
        let socket_addr = addr
            .to_socket_addrs()?
            .next() // Get the first resolved address (there could be multiple)
            .ok_or(anyhow::anyhow!("Failed to resolve address"))?;
        info!("Connecting to {}", socket_addr);
        let (client, _default_watcher) = ZooKeeper::connect(&socket_addr).await?;
        // create a dummy inode for the parent directory
        let _ = client
            .create(
                "/inode",
                b"",
                Acl::open_unsafe(),
                CreateMode::PersistentSequential,
            )
            .await??;
        let _ = client
            .create(
                "/files",
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await?;
        Ok(Self { client })
    }

    pub async fn create_inode(&self, name: &str) -> Result<u64> {
        let data: Cow<'static, [u8]> = Cow::Owned(format!("{},0", name).as_bytes().to_vec());

        let multi_builder = self
            .client
            .multi()
            .create(
                &format!("/files/{}", name),
                b"",
                Acl::open_unsafe().clone(),
                CreateMode::Persistent,
            )
            .create(
                "/inode",
                data,
                Acl::open_unsafe(),
                CreateMode::PersistentSequential,
            );
        let responses = multi_builder.run().await?;
        let Ok(MultiResponse::Create(inode)) = &responses[1] else {
            return Err(anyhow::anyhow!("Failed to create inode"));
        };
        let inode = inode.split("/inode").last().unwrap();
        let ino = inode.parse::<u64>()?;
        // write the inode to the file

        let data: Cow<'static, [u8]> = Cow::Owned(format!("{}", ino).as_bytes().to_vec());
        self.client
            .set_data(&format!("/files/{}", name), None, data)
            .await??;
        Ok(ino)
    }

    pub async fn get_size(&self, inode: u64) -> Result<u64> {
        let path = format!("/inode{:010}", inode);
        let data = self.client.get_data(&path).await?;
        let Some((data, _)) = data else {
            return Err(anyhow::anyhow!("Failed to get inode data from zookeeper"));
        };
        // split by comma and get the second element
        let data = String::from_utf8(data)?;
        let size = data.split(",").nth(1).unwrap();
        Ok(size.parse::<u64>()?)
    }

    pub async fn get_ino(&self, name: &str) -> Result<u64> {
        let path = format!("/files/{}", name);
        info!("Getting inode for {}", path);
        let Some((ino, _)) = self.client.get_data(&path).await? else {
            return Err(anyhow::anyhow!("Failed to get file data from zookeeper"));
        };
        Ok(String::from_utf8(ino)?.parse::<u64>()?)
    }

    pub async fn readdir(&self) -> Result<Vec<(u64, String)>> {
        let files = self.client.get_children("/files").await?;
        if let Some(files) = files {
            let mut result = Vec::new();
            for file in files {
                let ino = self.client.get_data(&format!("/files/{}", file)).await?;
                let Some((ino, _)) = ino else {
                    return Err(anyhow::anyhow!("Failed to get file data from zookeeper"));
                };
                result.push((String::from_utf8(ino)?.parse::<u64>()?, file));
            }
            Ok(result)
        } else {
            Err(anyhow::anyhow!("Failed to get children from zookeeper"))
        }
    }
}
