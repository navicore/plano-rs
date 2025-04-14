use clap::Parser;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "query-cli")]
#[command(about = "Run SQL queries against a local Parquet file using DataFusion")]
struct Args {
    /// Path to the Parquet file
    #[arg(short, long)]
    file: PathBuf,

    /// SQL query to run
    #[arg(short, long)]
    query: String,
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args = Args::parse();

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "data",
        args.file.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx.sql(&args.query).await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    Ok(())
}
