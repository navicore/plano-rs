///
/// Synchronize a Postgres table and write to Parquet with optional partitioning
///
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use clap::{ArgAction, Parser};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use partitions::{validate_partition_keys, write_partitioned_files};
use rds_sync::{infer_arrow_schema, sync_table};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

mod partitions;

/// Command-line arguments for the sync CLI
#[derive(Parser, Debug, Default)]
#[command(name = "plano-sync")]
#[command(about = "Synchronize a table from Postgres and write Parquet with optional partitioning", long_about = None)]
struct Args {
    /// Name of the table to sync
    #[arg(short, long)]
    table: String,

    /// Print the `RecordBatch` to stdout
    #[arg(long)]
    print: bool,

    /// Directory in which to write Parquet files (default: /tmp)
    #[arg(long, short, default_value = "/tmp")]
    output_dir: String,

    /// Partition keys (can repeat).
    /// If using reserved time keys (year, month, day, hour), must set --timestamp-col.
    #[arg(long, short, action = ArgAction::Append)]
    partition_by: Vec<String>,

    /// When partitioning by timestamp components, select which timestamp column to break down.
    #[arg(long)]
    timestamp_col: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let pool = initialize_db_pool().await?;

    validate_partition_keys(&args);

    let schema_ref = infer_arrow_schema(&args.table, &pool).await?;
    let batch = sync_table(&args.table, &schema_ref, &pool).await?;

    handle_output(&args, &schema_ref, &batch)?;

    Ok(())
}

async fn initialize_db_pool() -> sqlx::Result<sqlx::Pool<sqlx::Postgres>> {
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| panic!("DATABASE_URL environment variable not set"));
    PgPoolOptions::new().connect(&db_url).await
}

fn handle_output(args: &Args, schema_ref: &Arc<Schema>, batch: &RecordBatch) -> anyhow::Result<()> {
    if args.partition_by.is_empty() {
        write_single_file(args, schema_ref, batch)?;
    } else {
        write_partitioned_files(args, schema_ref, batch)?;
    }
    if args.print {
        print_batches(&[batch.clone()])?;
    }
    Ok(())
}

#[allow(clippy::expect_used)]
fn write_single_file(
    args: &Args,
    schema_ref: &Arc<Schema>,
    batch: &RecordBatch,
) -> anyhow::Result<()> {
    let output_path = Path::new(&args.output_dir).join(format!("{}.parquet", args.table));
    fs::create_dir_all(
        output_path
            .parent()
            .expect("Output directory must have a parent"),
    )?;
    let file = File::create(&output_path)?;
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, Arc::new(schema_ref.as_ref().clone()), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    info!("Wrote Parquet file to {}", output_path.display());
    Ok(())
}
