use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to your synced Parquet file
    let path = "/tmp/users.parquet";

    // Register the file as a table
    ctx.register_parquet("users", path, ParquetReadOptions::default())
        .await?;

    // Run a query
    let df = ctx
        .sql("SELECT id, name FROM users WHERE email IS NOT NULL")
        .await?;

    let batches = df.collect().await?;
    print_batches(&batches)?;

    Ok(())
}
