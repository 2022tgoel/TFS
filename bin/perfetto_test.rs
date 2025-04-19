use futures::future::join_all;
use tokio::time::{Duration, sleep};
use tracing::{Instrument, Level, event, info, info_span, instrument, span};
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::{EnvFilter, prelude::*};

async fn function() {
    let span = span!(Level::INFO, "my_block", some_field = 42);
    let _enter = span.enter();
    println!("my_block complete");
    drop(_enter);

    let nested_span = info_span!("nested");
    async {
        info!("doing nested work");
        // sleep(Duration::from_millis(50)).await;
        println!("nested work complete");
    }
    .instrument(nested_span)
    .await;
}

#[tokio::main]
async fn main() {
    // Set up the Perfetto layer
    let layer = PerfettoLayer::new(std::sync::Mutex::new(
        std::fs::File::create("./perfetto_demo.pftrace").unwrap(),
    ));

    let filter = EnvFilter::builder()
        .parse("tfs=info,tokio-zookeeper=off")
        .unwrap();

    tracing_subscriber::registry().with(layer).init();

    // Create a top-level span for the entire program
    let program_span = span!(Level::INFO, "perfetto_demo");
    let _guard = program_span.enter();

    // Spawn but don't block on the function
    let function_span = info_span!("function");
    tokio::spawn(function().instrument(function_span));

    // Demonstrate nested spans
    {
        let outer_span = span!(Level::INFO, "outer_operation", type = "demo");
        let _outer_guard = outer_span.enter();

        event!(Level::INFO, "starting outer operation");
        sleep(Duration::from_millis(100)).await;

        {
            let inner_span = span!(Level::INFO, "inner_operation", count = 3);
            let _inner_guard = inner_span.enter();

            for i in 0..3 {
                event!(Level::DEBUG, iteration = i, "processing item");
                sleep(Duration::from_millis(50)).await;
            }
        }

        event!(Level::INFO, "outer operation complete");
    }

    // Demonstrate concurrent spans
    let concurrent_tasks = async {
        let span = span!(Level::INFO, "concurrent_operations");
        let _guard = span.enter();

        let mut tasks = Vec::new();
        for i in 0..10 {
            let task_span = span!(Level::INFO, "task");
            let task = async move {
                let span = span!(Level::INFO, "my_block", some_field = 42);
                // let _enter = span.enter();
                async {
                    sleep(Duration::from_millis(50 + (i * 20) as u64)).await;
                }
                .instrument(span)
                .await;
            };
            tasks.push(tokio::spawn(task.instrument(task_span)));
        }

        let _ = join_all(tasks).await;
    };

    // Run our async demonstrations
    concurrent_tasks.await;

    // Demonstrate span attributes
    {
        let span = span!(
            Level::INFO,
            "attribute_demo",
            user = "test_user",
            operation_id = 12345,
            metadata = ?vec!["tag1", "tag2"]
        );
        let _guard = span.enter();

        event!(
            Level::INFO,
            value = 42,
            message = "demonstrating structured logging"
        );

        sleep(Duration::from_millis(50)).await;
    }

    println!("Trace written to ./perfetto_demo.pftrace");
}
