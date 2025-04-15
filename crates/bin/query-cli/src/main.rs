use clap::Parser;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;
use glob::glob;

/// Run SQL queries against one or more Parquet files using DataFusion
#[derive(Parser, Debug)]
#[command(name = "query-cli")]
struct Args {
    /// Glob pattern to the Parquet file(s)
    #[arg(short, long)]
    file: String,

    /// SQL query to run
    #[arg(short, long)]
    query: String,
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args = Args::parse();
    let ctx = SessionContext::new();

    // Collect matched files as String paths
    let file_paths: Vec<_> = glob(&args.file)
        .expect("Invalid glob pattern")
        .filter_map(Result::ok)
        .filter(|path| {
            path.extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
        })
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    if file_paths.is_empty() {
        eprintln!("No parquet files matched the pattern: {}", args.file);
        std::process::exit(1);
    }

    // Read and register all matched parquet files as one table
    let df = ctx
        .read_parquet(file_paths.clone(), ParquetReadOptions::default())
        .await?;
    ctx.register_table("data", df.into_view())?;

    // Execute the query
    let df = ctx.sql(&args.query).await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    Ok(())
}
