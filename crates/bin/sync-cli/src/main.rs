use arrow::util::pretty::print_batches;
use clap::*;
use rds_sync::{infer_arrow_schema, sync_table};
use sqlx::postgres::PgPoolOptions;
use std::env;

#[derive(Parser, Debug)]
#[command(name = "sync-cli")]
#[command(about = "Synchronize a table from Postgres and display it as a RecordBatch", long_about = None)]
struct Args {
    /// Name of the table to sync
    #[arg(short, long)]
    table: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new().connect(&db_url).await?;
    let args = Args::parse();

    let schema = infer_arrow_schema(&args.table, &pool).await?;
    let batch = sync_table(&args.table, &schema, &pool).await?;

    println!(
        "RecordBatch: {} rows, {} columns",
        batch.num_rows(),
        batch.num_columns()
    );

    for (i, col) in batch.columns().iter().enumerate() {
        println!(
            "Column {}: {} ({:?})",
            i,
            schema.field(i).name(),
            col.data_type()
        );
    }

    print_batches(&[batch])?;

    Ok(())
}
