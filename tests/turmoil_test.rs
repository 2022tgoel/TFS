// I never really got this to work. :(
// I would need a simulator for Zookeeper to be able to use turmoil.
// Really cool concept though!

use std::{
    assert_eq,
    io::{self},
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use once_cell::sync::Lazy;
use serial_test::serial;
use tfs::chunkserver::RpcServer;
use tfs::client::RpcClient;
use tfs::net::{HostName, TurmoilTcpConnectionManager};
use tokio::time::sleep;
use tokio::time::timeout;
use tracing::info;
use turmoil::{
    Builder, Result,
    net::{TcpListener, TcpStream},
};

const PORT: u16 = 1738;

fn assert_error_kind<T>(res: io::Result<T>, kind: io::ErrorKind) {
    assert_eq!(res.err().map(|e| e.kind()), Some(kind));
}

async fn bind_to_v4(port: u16) -> std::result::Result<TcpListener, std::io::Error> {
    TcpListener::bind((IpAddr::from(Ipv4Addr::UNSPECIFIED), port)).await
}

async fn bind() -> std::result::Result<TcpListener, std::io::Error> {
    bind_to_v4(PORT).await
}

static INIT: Lazy<()> = Lazy::new(|| {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
});

#[test]
#[serial]
fn network_partitions_during_connect() -> Result {
    Lazy::force(&INIT);

    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let listener = bind().await?;
        loop {
            let _ = listener.accept().await;
        }
    });

    sim.client("client", async {
        turmoil::partition("client", "server");
        info!("Partitioned");

        sleep(Duration::from_secs(5)).await;

        assert_error_kind(
            TcpStream::connect(("server", PORT)).await,
            io::ErrorKind::ConnectionRefused,
        );

        turmoil::repair("client", "server");
        turmoil::hold("client", "server");
        info!("Holding");
        assert!(
            timeout(Duration::from_secs(1), TcpStream::connect(("server", PORT)))
                .await
                .is_err()
        );

        Ok(())
    });

    sim.run()
}

#[test]
#[serial]
fn rpc_server_connect_test() -> Result {
    Lazy::force(&INIT);
    let mut sim = Builder::new().enable_tokio_io().build();

    sim.host("server", || async {
        let server = RpcServer::<TurmoilTcpConnectionManager>::new(HostName::TurmoilName(
            "server".to_string(),
        ))
        .await?;
        tracing::info!("Server started");
        server.serve().await?;
        Ok(())
    });

    sim.client("client", async {
        turmoil::partition("client", "server");
        info!("Partitioned");

        sleep(Duration::from_secs(5)).await;

        assert_error_kind(
            TcpStream::connect(("server", PORT)).await,
            io::ErrorKind::ConnectionRefused,
        );

        turmoil::repair("client", "server");
        turmoil::hold("client", "server");
        info!("Holding");
        assert!(
            timeout(Duration::from_secs(1), TcpStream::connect(("server", PORT)))
                .await
                .is_err()
        );

        Ok(())
    });

    sim.run()
}

#[test]
#[serial]
fn rpc_client_connect_test() -> Result {
    Lazy::force(&INIT);
    let mut sim = Builder::new().enable_tokio_io().build();

    sim.host("server", || async {
        let server = RpcServer::<TurmoilTcpConnectionManager>::new(HostName::TurmoilName(
            "server".to_string(),
        ))
        .await?;
        tracing::info!("Server started");
        server.serve().await?;
        Ok(())
    });

    sim.client("client", async {
        turmoil::partition("client", "server");
        info!("Partitioned");

        sleep(Duration::from_secs(5)).await;

        assert_error_kind(
            TcpStream::connect(("server", PORT)).await,
            io::ErrorKind::ConnectionRefused,
        );

        turmoil::repair("client", "server");
        info!("Repairing");
        let mut client = RpcClient::<TurmoilTcpConnectionManager>::new(
            HostName::TurmoilName("client".to_string()),
            "server".to_string(),
        )
        .await?;
        tracing::info!("Client started");
        client.connect_ib().await?;
        Ok(())
    });

    sim.run()
}
