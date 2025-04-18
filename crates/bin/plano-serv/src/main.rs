/// A simple DataFusion-based query server that serves SQL queries and table metadata
use clap::Parser;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use lru::LruCache;
use plano_core::format::{format_batches, OutputFormat};
use std::num::NonZero;
use std::{collections::HashMap, sync::Arc};
use table_spec::TableSpec;
use tokio::sync::Mutex;
use tracing::{debug, info};
use warp::http::{HeaderMap, Response, StatusCode};
use warp::Filter;

mod table_spec;

// Cache up to 100 distinct queries in memory
type QueryCache = Arc<Mutex<LruCache<String, Vec<RecordBatch>>>>;

/// Command-line arguments for the query server
#[derive(Parser, Debug)]
#[command(name = "plano-serv")]
struct Args {
    /// One or more table-specs in the form
    ///   name=path[:col1,col2,...]
    /// e.g. --table-spec events=/data/parquet/events:year,month,day
    #[arg(long, short, action = clap::ArgAction::Append, required=true)]
    table_spec: Vec<String>,

    /// Address to bind the server to
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,
}

/// Handles the `/tables` endpoint to list tables and their row counts
async fn handle_tables(
    ctx: Arc<SessionContext>,
    headers: HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(warp::reject::not_found)?;

    let schema = catalog
        .schema("public")
        .ok_or_else(warp::reject::not_found)?;

    let mut table_names = Vec::new();
    let mut row_counts = Vec::new();

    for table_name in schema.table_names() {
        let count_query = format!("SELECT COUNT(*) AS cnt FROM {table_name}");
        let df = ctx.sql(&count_query).await.map_err(|_| warp::reject())?;
        let batches = df.collect().await.map_err(|_| warp::reject())?;

        let count_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(warp::reject::not_found)?;

        table_names.push(table_name.to_string());
        row_counts.push(count_array.value(0));
    }

    // Create RecordBatch from collected data
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(table_names)),
            Arc::new(Int64Array::from(row_counts)),
        ],
    )
    .map_err(|_| warp::reject())?;

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let output_format = match accept {
        "text/csv" => OutputFormat::Csv,
        "text/plain" => OutputFormat::Text,
        _ => OutputFormat::Json,
    };

    let content_type = match output_format {
        OutputFormat::Csv => "text/csv",
        OutputFormat::Text => "text/plain",
        OutputFormat::Json => "application/json",
    };
    let body = format_batches(&[batch], output_format).map_err(|_| warp::reject())?;

    Ok(warp::reply::with_header(body, "Content-Type", content_type))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let ctx = Arc::new(SessionContext::new());

    let cache_size = NonZero::new(100).unwrap_or_else(|| {
        panic!("Cache size must be a non-zero value");
    });

    let cache: QueryCache = Arc::new(Mutex::new(LruCache::new(cache_size)));

    // Parse each spec and register
    for raw in &args.table_spec {
        let spec = TableSpec::parse(raw).map_err(|e| anyhow::anyhow!(e))?;
        // 1) Wrap in the proper URL type:
        let store_url = if spec.root.starts_with("s3://") {
            spec.root.clone()
        } else {
            let abs = std::fs::canonicalize(&spec.root)?;
            format!("file://{}", abs.display())
        };
        // Normalize root into a directory prefix (with trailing slash)
        let raw_root = &spec.root;
        let mut root_prefix = if raw_root.starts_with("s3://") {
            raw_root.clone()
        } else {
            // canonicalize to absolute path
            let abs = std::fs::canonicalize(raw_root)?;
            abs.display().to_string()
        };
        // make sure it ends with '/'
        if !root_prefix.ends_with('/') {
            root_prefix.push('/');
        }

        // Now parse that as a ListingTableUrl.
        // Note: `ListingTableUrl::parse` will detect "s3://" vs plain local path.
        let listing_url = ListingTableUrl::parse(&root_prefix)?;

        let listing_opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            // map each partition name to a (column_name, DataType) tuple:
            .with_table_partition_cols(
                spec.partitions
                    .iter()
                    .map(|col| (col.clone(), DataType::Utf8))
                    .collect::<Vec<(String, DataType)>>(),
            );
        // Prepend file:// if it isn't already an object store URL
        let store_url = if spec.root.starts_with("s3://") {
            spec.root.clone()
        } else {
            // ensure absolute paths are file://
            let abs = std::fs::canonicalize(&spec.root)?;
            format!("file://{}", abs.display())
        };

        // Build the base config
        let config = ListingTableConfig::new(listing_url).with_listing_options(listing_opts);

        // This async call _returns_ a ListingTableConfig with its schema set for you
        let config = config.infer_schema(&ctx.state()).await?;
        // 4) Finally, turn it into a ListingTable and register:
        let listing_table = ListingTable::try_new(config)?;
        ctx.register_table(&spec.name, Arc::new(listing_table))?;

        println!("Registered table `{}` at `{}`", spec.name, store_url);
    }

    // Parse

    let ctx_filter = warp::any().map(move || ctx.clone());
    //let paths_filter = warp::any().map(move || shared_paths.clone());
    let cache_filter = warp::any().map(move || cache.clone());

    // Define the routes
    let query_route = warp::path("query")
        .and(warp::post())
        .and(warp::body::form())
        .and(ctx_filter.clone())
        //.and(paths_filter.clone())
        .and(cache_filter.clone())
        .and(warp::header::headers_cloned())
        .and_then(handle_query);

    let tables_route = warp::path("tables")
        .and(warp::get())
        .and(ctx_filter.clone())
        .and(warp::header::headers_cloned())
        .and_then(handle_tables);

    // Combine the routes
    let routes = query_route.or(tables_route).with(warp::log("plano-serv"));
    info!("Serving on http://{}", args.bind);
    let addr: std::net::SocketAddr = args.bind.parse()?;
    warp::serve(routes).run(addr).await;

    Ok(())
}

/// Handles the `/query` endpoint to execute SQL queries
async fn handle_query(
    form: HashMap<String, String>,
    ctx: Arc<SessionContext>,
    cache: QueryCache,
    headers: HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    // Check
    let Some(query) = form.get("sql") else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Missing 'sql'".into())
            .map_or_else(|_| Err(warp::reject()), Ok);
    };

    // Try hit using query as cache key
    if let Some(cached_batches) = cache.lock().await.get(query) {
        debug!("Cache hit for {}", &query);
        let body =
            format_batches(cached_batches, OutputFormat::Json).map_err(|_| warp::reject())?;
        return Response::builder()
            .status(StatusCode::OK)
            .body(body)
            .map_or_else(|_| Err(warp::reject()), Ok);
    }

    let df = ctx.sql(query).await.map_err(|_| warp::reject())?;
    let results = df.collect().await.map_err(|_| warp::reject())?;

    // store in cache
    cache.lock().await.put(query.clone(), results.clone());

    // then format & reply as before
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/plain");

    let format = match accept {
        "application/json" => OutputFormat::Json,
        "text/csv" => OutputFormat::Csv,
        _ => OutputFormat::Text,
    };

    let content_type = match &format {
        OutputFormat::Json => "application/json",
        OutputFormat::Csv => "text/csv",
        OutputFormat::Text => "text/plain",
    };

    let body = format_batches(&results, format).map_err(|_| warp::reject())?;
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", content_type)
        .body(body)
        .map_or_else(|_| Err(warp::reject()), Ok)
}
