///
/// A `DataFusion`-based query server that serves SQL queries and table metadata
///
use cached_stats::AtomicIntCacheStats;
use clap::Parser;
use datafusion::{common::HashSet, prelude::*};
use deadpool_postgres::{Config as DeadpoolConfig, Pool, Runtime};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_object_store::MetricsObjectStore;
use object_store::parse_url;
use ocra::{memory::InMemoryCache, ReadThroughCache};
use routes::{configure_routes, rds_route};
use std::{net::SocketAddr, sync::Arc};
use tables::{register_tables, TableSpec};
use tokio::spawn;
use tokio_postgres::{config::Host, NoTls};
use tracing::info;
use url::Url;
use warp::Filter;

mod cached_stats;
mod metrics_object_store;
mod routes;
mod tables;

/// Command-line arguments for the query server
#[derive(Parser, Debug, Clone)]
#[command(name = "plano-serv")]
struct Args {
    /// One or more table-specs in the form
    ///   name=path[:col1,col2,...]
    ///
    /// e.g. --table-spec events=/data/parquet/events:year,month,day
    #[arg(long, short, action = clap::ArgAction::Append)]
    table_spec: Vec<String>,

    /// Address to bind the server to
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,
}

fn make_pg_pool() -> Pool {
    // Get the database URL from the environment variable
    let db_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL environment variable must be set");

    // Parse the database URL
    let parsed_url = db_url
        .parse::<tokio_postgres::Config>()
        .expect("Invalid DATABASE_URL");
    let mut cfg = DeadpoolConfig::new();
    cfg.dbname = parsed_url.get_dbname().map(|n| n.to_string());
    let host_opt = parsed_url.get_hosts().first().cloned();
    cfg.host = match host_opt {
        Some(Host::Tcp(addr)) => Some(addr.to_string()),
        _ => None,
    };
    cfg.user = parsed_url.get_user().map(|u| u.to_string());
    cfg.port = parsed_url.get_ports().first().cloned();
    info!(
        "Parsed from database URL: host: {:?} port {:?} dbname {:?} user {:?}",
        cfg.host, cfg.port, cfg.dbname, cfg.user
    );
    cfg.password = String::from_utf8(parsed_url.get_password().expect("haha").to_vec()).ok();

    #[allow(clippy::expect_used)]
    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("failed to create pg pool")
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

fn parse_table_spec(s: Args) -> Result<Vec<TableSpec>, String> {
    s.table_spec
        .into_iter()
        .map(|spec| TableSpec::parse(&spec))
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let ctx = Arc::new(SessionContext::new());
    let cache = routes::initialize_cache(100);

    let pg_pool = make_pg_pool();
    let rds_route = rds_route(ctx.clone(), pg_pool);

    #[allow(clippy::expect_used)]
    let recorder_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    // 2) Expose it at /metrics on port 9898
    let metrics_route = warp::path("metrics").map(move || recorder_handle.render());
    let addr: SocketAddr = ([0, 0, 0, 0], 9898).into();
    spawn(async move {
        warp::serve(metrics_route).run(addr).await;
    });

    let table_specs: Vec<TableSpec> = parse_table_spec(args.clone()).unwrap_or_else(|e| {
        panic!("Failed to parse table specs: {e}");
    });

    let mut seen_roots = HashSet::new();
    for spec in &table_specs {
        let root = &spec.root;
        // avoid double‐registering the same URI
        if !seen_roots.insert(root.clone()) {
            continue;
        }

        let url = Url::parse(root)?;

        #[allow(clippy::expect_used)]
        let (cache, _path) = parse_url(&url).expect("Failed to parse URL");
        let cache = Arc::new(cache);
        // wrap in caching + metrics
        let base_store = Arc::new(MetricsObjectStore::new(cache));

        let stats = AtomicIntCacheStats::new(); // e.g. 500 MB max
        let cache_size = 500 * 1024 * 1024;
        let cache_backend = Arc::new(
            InMemoryCache::builder(cache_size)
                //.max_capacity_bytes(stats.max_capacity())
                .build(),
        );
        let cached_store =
            ReadThroughCache::new_with_stats(base_store, cache_backend, Arc::new(stats));
        ctx.register_object_store(&url, Arc::new(cached_store));
    }

    // Register tables based on the provided table specifications.
    //
    // These specifications enable datafusion to dynamically create glob specs and lazily read
    // partitioned filesets into in-memory tables to satisfy newly arriving queries.
    register_tables(&ctx, &table_specs).await?;

    let routes = configure_routes(ctx, cache, rds_route);

    start_server(args.bind, routes).await?;

    Ok(())
}
