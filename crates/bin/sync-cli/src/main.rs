use arrow::util::pretty::print_batches;
use clap::{arg, command, Parser};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rds_sync::{infer_arrow_schema, sync_table};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::fs::File;
use std::sync::Arc;
use tracing::info;

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
    tracing_subscriber::fmt::init();
    let db_url = env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new().connect(&db_url).await?;
    let args = Args::parse();

    let schema = infer_arrow_schema(&args.table, &pool).await?;
    let batch = sync_table(&args.table, &schema, &pool).await?;

    let output_path = format!("/tmp/{}.parquet", args.table);
    let file = File::create(&output_path)?;
    let props = WriterProperties::builder().build();

    let mut writer = ArrowWriter::try_new(file, Arc::new(schema.as_ref().clone()), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    print_batches(&[batch])?;
    info!("Wrote Parquet file to {output_path}");
    Ok(())
}
