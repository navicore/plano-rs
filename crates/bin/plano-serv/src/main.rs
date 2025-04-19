/// A simple DataFusion-based query server that serves SQL queries and table metadata
use bytes::Bytes;
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
use std::collections::HashSet;
use std::fmt::Display;
use std::num::NonZero;
use std::{collections::HashMap, sync::Arc};
use table_spec::TableSpec;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
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

// In Cargo.toml, add:
// bytes = "1.4"
// serde_urlencoded = "0.7"

#[derive(Debug)]
struct PlanoServerError {
    pub reason: String,
}
impl warp::reject::Reject for PlanoServerError {}
impl Display for PlanoServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Plano Server Error: {}", self.reason)
    }
}

#[derive(Debug)]
struct PlanoBadRequest {
    pub reason: String,
}

impl Display for PlanoBadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bad Request: {}", self.reason)
    }
}

impl warp::reject::Reject for PlanoBadRequest {}
/// Unified query handler that first captures raw bytes,
/// optionally logs them, then parses as form and delegates.
async fn handle_query_bytes(
    raw_body: Bytes,
    ctx: Arc<SessionContext>,
    cache: QueryCache,
    headers: HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    // 1) Debug print the raw payload if you like:
    debug!("QUERY RAW BODY = {:?}", std::str::from_utf8(&raw_body));

    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&raw_body).map_err(|e| {
        warn!("form parse error: {}", e);
        warp::reject::custom(PlanoBadRequest {
            reason: e.to_string(),
        })
    })?;

    // 3) Call your existing handler
    handle_query(form, ctx, cache, headers).await
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
        let raw_root = &spec.root;
        let mut root_prefix = if raw_root.starts_with("s3://") {
            raw_root.clone()
        } else {
            let abs = std::fs::canonicalize(raw_root)?;
            abs.display().to_string()
        };
        if !root_prefix.ends_with('/') {
            root_prefix.push('/');
        }

        let store_url = if spec.root.starts_with("s3://") {
            spec.root.clone()
        } else {
            // ensure absolute paths are file://
            let abs = std::fs::canonicalize(&spec.root)?;
            format!("file://{}", abs.display())
        };

        let _ = register_table(&ctx, &spec).await;

        println!("Registered table `{}` at `{}`", spec.name, store_url);
    }

    // Parse

    let ctx_filter = warp::any().map(move || ctx.clone());

    let cache_filter = warp::any().map(move || cache.clone());

    let query_route = warp::path("query")
        .and(warp::post())
        .and(warp::body::bytes()) // grab raw bytes once
        .and(ctx_filter.clone())
        .and(cache_filter.clone())
        .and(warp::header::headers_cloned())
        .and_then(handle_query_bytes);

    let tables_route = warp::path("tables")
        .and(warp::get())
        .and(ctx_filter.clone())
        .and(warp::header::headers_cloned())
        .and_then(handle_tables);

    let routes = query_route // your old form‑based route
        .or(tables_route)
        .with(warp::log("plano-serv"));

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
            .map_or_else(
                |e| {
                    Err(warp::reject::custom(PlanoBadRequest {
                        reason: e.to_string(),
                    }))
                },
                Ok,
            );
    };

    // Try hit using query as cache key
    if let Some(cached_batches) = cache.lock().await.get(query) {
        debug!("Cache hit for {}", &query);
        let body =
            format_batches(cached_batches, OutputFormat::Json).map_err(|_| warp::reject())?;
        return Response::builder()
            .status(StatusCode::OK)
            .body(body)
            //.map_or_else(|_| Err(warp::reject()), Ok);
            .map_or_else(
                |e| {
                    Err(warp::reject::custom(PlanoBadRequest {
                        reason: e.to_string(),
                    }))
                },
                Ok,
            );
    }

    debug!("handle_query: {query}");
    //let df = ctx.sql(query).await.map_err(|_| warp::reject())?;

    let df = match ctx.sql(query).await {
        Ok(df) => df,
        Err(e) => {
            warn!("❌ DataFusion `ctx.sql` error for '{}':\n  {}", query, e);
            return Err(PlanoServerError {
                reason: e.to_string(),
            }
            .into());
        }
    };

    let results = df.collect().await.map_err(|_| warp::reject())?;
    debug!("handle_query results: {results:?}");

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

// Registers a table in the DataFusion context using a `ListingTableConfig`
//
// The complexity is due to we use partition keys based on file data but once we start using a
// file column as a partition key datafusion will fail in sql planning because it can't deal with
// duplicate cols in the schema.  We need to scrub the file column when we are adding  a
// partition key.
async fn register_table(
    ctx: &SessionContext,
    spec: &TableSpec, // your own struct that holds name, path, partition list …
) -> datafusion::error::Result<()> {
    // 1. Prepare the base listing options -------------------------------
    let base_opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_table_partition_cols(
            spec.partitions
                .iter()
                .map(|c| (c.clone(), DataType::Utf8))
                .collect(),
        );

    let table_url = ListingTableUrl::parse(&spec.root)?;

    let session_state = ctx.state();
    let file_schema = base_opts.infer_schema(&session_state, &table_url).await?;

    let part_set: HashSet<&str> = spec.partitions.iter().map(String::as_str).collect();

    // filter out the file columns that are also partition keys
    let clean_fields: Vec<Field> = file_schema
        .fields()
        .iter()
        .filter(|f| !part_set.contains(f.name().as_str()))
        .map(|f| (**f).clone()) // <‑‑ convert Arc<Field> → Field
        .collect();

    let clean_schema = Arc::new(Schema::new(clean_fields));

    let cfg = ListingTableConfig::new(table_url)
        .with_listing_options(base_opts)
        .with_schema(clean_schema); // <‑‑ this is the key :contentReference[oaicite:0]{index=0}

    let table = ListingTable::try_new(cfg)?;
    ctx.register_table(&spec.name, Arc::new(table))?;

    Ok(())
}
