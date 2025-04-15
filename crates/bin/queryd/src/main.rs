use clap::Parser;
use datafusion::{arrow, prelude::*};
use glob::glob;
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
        let files: Vec<_> = glob(pattern)
            .expect("Invalid glob pattern")
            .filter_map(Result::ok)
            .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        if files.is_empty() {
            eprintln!("No files matched for table '{}': {}", name, pattern);
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
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Missing 'sql'".into())
            .unwrap());
    };

    let path_map = paths.read().await;
    for (name, files) in path_map.iter() {
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

    let response = if accept == "application/json" {
        use datafusion::arrow::json::writer::LineDelimitedWriter;
        use std::io::Cursor;

        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = LineDelimitedWriter::new(&mut buffer);
            for batch in &results {
                writer.write(batch).map_err(|_| warp::reject())?;
            }
            writer.finish().map_err(|_| warp::reject())?;
        }

        let json_string = String::from_utf8(buffer.into_inner()).map_err(|_| warp::reject())?;

        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(json_string)
            .unwrap()
    } else if accept == "text/csv" {
        use datafusion::arrow::csv::writer::WriterBuilder;
        use std::io::Cursor;

        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = WriterBuilder::new().build(&mut buffer);

            for batch in &results {
                writer.write(batch).map_err(|_| warp::reject())?;
            }
        }

        let csv_string = String::from_utf8(buffer.into_inner()).map_err(|_| warp::reject())?;

        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/csv")
            .body(csv_string)
            .unwrap()
    } else {
        let formatted = arrow::util::pretty::pretty_format_batches(&results)
            .map_err(|_| warp::reject())?
            .to_string();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain")
            .body(formatted)
            .unwrap()
    };

    Ok(response)
}
