///
/// A `DataFusion`-based query server that serves SQL queries and table metadata
///
use clap::Parser;
use datafusion::prelude::*;
use routes::configure_routes;
use std::sync::Arc;
use tables::register_tables;
use tracing::info;

mod routes;
mod tables;

/// Command-line arguments for the query server
#[derive(Parser, Debug)]
#[command(name = "plano-serv")]
struct Args {
    /// One or more table-specs in the form
    ///   name=path[:col1,col2,...]
    ///
    /// e.g. --table-spec events=/data/parquet/events:year,month,day
    #[arg(long, short, action = clap::ArgAction::Append, required=true)]
    table_spec: Vec<String>,

    /// Address to bind the server to
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,
}

async fn start_server(
    bind: String,
    routes: impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection>
        + Clone
        + std::marker::Sync
        + std::marker::Send
        + 'static,
) -> anyhow::Result<()> {
    info!("Serving on http://{}", bind);
    let addr: std::net::SocketAddr = bind.parse()?;
    warp::serve(routes).run(addr).await;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let ctx = Arc::new(SessionContext::new());
    let cache = routes::initialize_cache(100);

    register_tables(&ctx, &args.table_spec).await?;

    let routes = configure_routes(ctx, cache);

    start_server(args.bind, routes).await?;

    Ok(())
}
