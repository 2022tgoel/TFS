use anyhow::Result;
use async_trait::async_trait;
use bb8::{ManageConnection, Pool};
use core::net::SocketAddr;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::ToSocketAddrs as TokioToSocketAddrs;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
use turmoil::ToSocketAddrs as TurmoilToSocketAddrs;
use turmoil::net::{TcpListener as TurmoilTcpListener, TcpStream as TurmoilTcpStream};

pub const NUM_CONNS: u32 = 100; // Number of connections to keep open

#[async_trait]
pub trait ConnectStream: Sized {
    async fn connect(addr: &str) -> std::io::Result<Self>;
}

#[async_trait]
impl ConnectStream for TokioTcpStream {
    async fn connect(addr: &str) -> std::io::Result<Self> {
        TokioTcpStream::connect(addr).await
    }
}

#[async_trait]
impl ConnectStream for TurmoilTcpStream {
    async fn connect(addr: &str) -> std::io::Result<Self> {
        TurmoilTcpStream::connect(addr).await
    }
}

#[async_trait]
pub trait Listener: Send + Sync + Unpin + Sized {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + ConnectStream;

    async fn bind<T: TokioToSocketAddrs + TurmoilToSocketAddrs + Send>(
        addr: T,
    ) -> std::io::Result<Self>;
    async fn accept(&self) -> std::io::Result<(Self::Stream, SocketAddr)>;
    fn local_addr(&self) -> std::io::Result<SocketAddr>;
}

#[async_trait]
impl Listener for TokioTcpListener {
    type Stream = TokioTcpStream;

    async fn bind<T: TokioToSocketAddrs + TurmoilToSocketAddrs + Send>(
        addr: T,
    ) -> std::io::Result<Self> {
        TokioTcpListener::bind(addr).await
    }

    async fn accept(&self) -> std::io::Result<(Self::Stream, SocketAddr)> {
        self.accept().await
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.local_addr()
    }
}

#[async_trait]
impl Listener for TurmoilTcpListener {
    type Stream = TurmoilTcpStream;

    async fn bind<T: TokioToSocketAddrs + TurmoilToSocketAddrs + Send>(
        addr: T,
    ) -> std::io::Result<Self> {
        TurmoilTcpListener::bind(addr).await
    }

    async fn accept(&self) -> std::io::Result<(Self::Stream, SocketAddr)> {
        self.accept().await
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.local_addr()
    }
}

pub async fn create_tcp_pool<TcpConnectionManager>(
    host: String,
    port: u16,
    size: u32,
) -> Result<Pool<TcpConnectionManager>>
where
    TcpConnectionManager: ManagerCreate + ManageConnection<Error = io::Error> + Clone,
{
    let manager = TcpConnectionManager::new(host, port);
    let pool = bb8::Pool::builder().max_size(size).build(manager).await?;
    Ok(pool)
}

pub trait ManagerCreate {
    type Listener: Listener;
    fn new(host: String, port: u16) -> Self;
}

#[derive(Debug, Clone)]
pub struct TokioTcpConnectionManager {
    host: String,
    port: u16,
}

impl ManagerCreate for TokioTcpConnectionManager {
    type Listener = TokioTcpListener;
    fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
}

impl ManageConnection for TokioTcpConnectionManager {
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

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        return false;
    }
}

#[derive(Debug, Clone)]
pub struct TurmoilTcpConnectionManager {
    host: String,
    port: u16,
}

impl ManagerCreate for TurmoilTcpConnectionManager {
    type Listener = TurmoilTcpListener;
    fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
}

impl ManageConnection for TurmoilTcpConnectionManager {
    type Connection = TurmoilTcpStream;
    type Error = io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let addr = format!("{}:{}", self.host, self.port);
        TurmoilTcpStream::connect(&addr).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // Use peek instead of try_read to avoid consuming data
        match conn.writable().await {
            Ok(()) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        return false;
    }
}
