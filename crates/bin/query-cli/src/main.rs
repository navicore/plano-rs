use clap::Parser;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;
use glob::glob;

/// Run SQL queries against one or more Parquet files using DataFusion
#[derive(Parser, Debug)]
#[command(name = "query-cli")]
struct Args {
    /// One or more --table name=glob_pattern entries
    #[arg(short, long, required = true, value_parser = parse_table)]
    table: Vec<(String, String)>,

    /// SQL query to run
    #[arg(short, long)]
    query: String,
}

fn parse_table(s: &str) -> Result<(String, String), String> {
    let parts: Vec<_> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err("Expected format: name=glob".to_string());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args = Args::parse();
    let ctx = SessionContext::new();

    for (table_name, pattern) in &args.table {
        let file_paths: Vec<_> = glob(pattern)
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
            eprintln!(
                "No parquet files matched pattern for table '{}': {}",
                table_name, pattern
            );
            std::process::exit(1);
        }

        let df = ctx
            .read_parquet(file_paths.clone(), ParquetReadOptions::default())
            .await?;
        ctx.register_table(table_name, df.into_view())?;
    }

    // Execute the query
    let df = ctx.sql(&args.query).await?;
    let results = df.collect().await?;
    print_batches(&results)?;

    Ok(())
}
