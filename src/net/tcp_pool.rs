use anyhow::Result;
use bb8::{ManageConnection, Pool};
use std::io;
use tokio::net::TcpStream as TokioTcpStream;

pub async fn create_tcp_pool(
    host: String,
    port: u16,
    size: u32,
) -> Result<Pool<TcpConnectionManager>> {
    let manager = TcpConnectionManager::new(host, port);
    let pool = bb8::Pool::builder().max_size(size).build(manager).await?;
    Ok(pool)
}

#[derive(Debug, Clone)]
pub struct TcpConnectionManager {
    host: String,
    port: u16,
}

impl TcpConnectionManager {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
}

impl ManageConnection for TcpConnectionManager {
    type Connection = TokioTcpStream;
    type Error = io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let addr = format!("{}:{}", self.host, self.port);
        TokioTcpStream::connect(&addr).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // Use peek instead of try_read to avoid consuming data
        match conn.writable().await {
            Ok(()) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        return false;
    }
}
