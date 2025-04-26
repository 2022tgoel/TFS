use tfs::chunkserver::RpcServer;
use tfs::net::{HostName, TokioTcpConnectionManager, my_name};
use tokio::signal;
use tracing_chrome::{ChromeLayerBuilder, TraceStyle};
use tracing_flame::FlameLayer;
use tracing_log::LogTracer;
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt, prelude::*};

#[tokio::main]
async fn main() {
    // let filter = EnvFilter::builder()
    //     .with_default_directive(LevelFilter::INFO.into())
    //     .parse("tfs=info,tokio-zookeeper=off")
    //     .unwrap();

    // // let layer = PerfettoLayer::new(std::sync::Mutex::new(
    // //     std::fs::File::create("./trace.pftrace").unwrap(),
    // // ));
    // // // .with_filter(filter);
    // // let (flame_layer, _guard) = FlameLayer::with_file("./tracing.folded").unwrap();

    // let (chrome_layer, _guard) = ChromeLayerBuilder::new().trace_style(TraceStyle::Async).build();

    // tracing_subscriber::registry()
    //     // .with(fmt_layer)
    //     .with(chrome_layer.with_filter(filter))
    //     .init();

    let hostname = my_name().unwrap();
    let server =
        RpcServer::<TokioTcpConnectionManager>::new(HostName::RegularName(hostname.clone()))
            .await
            .unwrap();
    println!("Starting chunkserver on {}", hostname);
    // Set up Ctrl+C handler
    tokio::select! {
        result = server.serve() => {
            if let Err(e) = result {
                eprintln!("Server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
    }
}
