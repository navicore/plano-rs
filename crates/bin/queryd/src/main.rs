use clap::Parser;
use datafusion::prelude::*;
use glob::glob;
use plano_core::format::{format_batches, OutputFormat};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use warp::http::{Response, StatusCode};
use warp::Filter;

#[derive(Parser, Debug)]
#[command(name = "queryd")]
struct Args {
    #[arg(short, long, required = true, value_parser = parse_table)]
    table: Vec<(String, String)>,

    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,
}

fn parse_table(s: &str) -> Result<(String, String), String> {
    let parts: Vec<_> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err("Expected format: name=glob".to_string());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let ctx = Arc::new(SessionContext::new());
    let mut table_paths: HashMap<String, Vec<String>> = HashMap::new();

    for (name, pattern) in &args.table {
        #[allow(clippy::expect_used)]
        let files: Vec<_> = glob(pattern)
            .expect("Invalid glob pattern")
            .filter_map(Result::ok)
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        if files.is_empty() {
            eprintln!("No files matched for table '{name}': {pattern}");
            continue;
        }

        table_paths.insert(name.clone(), files);
    }

    let shared_paths = Arc::new(RwLock::new(table_paths));

    let ctx_filter = warp::any().map(move || ctx.clone());
    let paths_filter = warp::any().map(move || shared_paths.clone());

    let query_route = warp::path("query")
        .and(warp::post())
        .and(warp::body::form())
        .and(ctx_filter)
        .and(paths_filter)
        .and(warp::header::headers_cloned()) // <- add this
        .and_then(handle_query);

    println!("Serving on http://{}", args.bind);
    let addr: std::net::SocketAddr = args.bind.parse()?;
    warp::serve(query_route).run(addr).await;

    Ok(())
}

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
