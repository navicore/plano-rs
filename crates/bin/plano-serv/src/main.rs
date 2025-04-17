/// A simple DataFusion-based query server that serves SQL queries and table metadata
use clap::Parser;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use glob::glob;
use plano_core::format::{format_batches, OutputFormat};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing::{error, info};
use warp::http::HeaderMap;
use warp::http::{Response, StatusCode};
use warp::Filter;

/// Command-line arguments for the query server
#[derive(Parser, Debug)]
#[command(name = "plano-serv")]
struct Args {
    /// List of tables to register, in the format "name=glob"
    #[arg(short, long, required = true, value_parser = parse_table)]
    table: Vec<(String, String)>,

    /// Address to bind the server to
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,
}

/// Parses a table definition in the format "name=glob"
fn parse_table(s: &str) -> Result<(String, String), String> {
    let parts: Vec<_> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err("Expected format: name=glob".to_string());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
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
    let mut table_paths: HashMap<String, Vec<String>> = HashMap::new();

    // Load tables from glob patterns
    for (name, pattern) in &args.table {
        #[allow(clippy::expect_used)]
        let files: Vec<_> = glob(pattern)
            .expect("Invalid glob pattern")
            .filter_map(Result::ok)
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        if files.is_empty() {
            error!("No files matched for table '{name}': {pattern}");
            continue;
        }

        table_paths.insert(name.clone(), files);
    }

    let shared_paths = Arc::new(RwLock::new(table_paths));

    // Explicitly load tables at startup
    {
        let table_paths = shared_paths.read().await;
        for (name, files) in table_paths.iter() {
            if ctx.table(name).await.is_err() {
                let df = ctx
                    .read_parquet(files.clone(), ParquetReadOptions::default())
                    .await?;
                ctx.register_table(name, df.into_view())?;
            }
        }
    }

    let ctx_filter = warp::any().map(move || ctx.clone());
    let paths_filter = warp::any().map(move || shared_paths.clone());

    // Define the routes
    let query_route = warp::path("query")
        .and(warp::post())
        .and(warp::body::form())
        .and(ctx_filter.clone())
        .and(paths_filter.clone())
        .and(warp::header::headers_cloned())
        .and_then(handle_query);

    let tables_route = warp::path("tables")
        .and(warp::get())
        .and(ctx_filter.clone())
        .and(warp::header::headers_cloned())
        .and_then(handle_tables);

    // Combine the routes
    let routes = query_route.or(tables_route);

    info!("Serving on http://{}", args.bind);
    let addr: std::net::SocketAddr = args.bind.parse()?;
    warp::serve(routes).run(addr).await;

    Ok(())
}

/// Handles the `/query` endpoint to execute SQL queries
async fn handle_query(
    form: HashMap<String, String>,
    ctx: Arc<SessionContext>,
    paths: Arc<RwLock<HashMap<String, Vec<String>>>>,
    headers: warp::http::HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    let Some(query) = form.get("sql") else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Missing 'sql'".into())
            .map_or_else(|_| Err(warp::reject()), Ok);
    };

    for (name, files) in paths.read().await.iter() {
        if ctx.table(name).await.is_err() {
            let df = ctx
                .read_parquet(files.clone(), ParquetReadOptions::default())
                .await
                .map_err(|_| warp::reject())?;
            ctx.register_table(name, df.into_view())
                .map_err(|_| warp::reject())?;
        }
    }

    let df = ctx.sql(query).await.map_err(|_| warp::reject())?;
    let results = df.collect().await.map_err(|_| warp::reject())?;

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
