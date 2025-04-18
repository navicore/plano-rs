/// Synchronize a Postgres table and write to Parquet with optional partitioning
use arrow::util::pretty::print_batches;
use clap::{ArgAction, Parser};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rds_sync::{infer_arrow_schema, sync_table};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

// Arrow imports for partition logic
use arrow::array::{ArrayRef, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::prelude::*;
use std::collections::HashMap;

/// Command-line arguments for the sync CLI
#[derive(Parser, Debug)]
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
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| panic!("DATABASE_URL environment variable not set"));
    let pool = PgPoolOptions::new().connect(&db_url).await?;

    // Validate reserved keys only with timestamp_col
    let reserved = ["year", "month", "day", "hour"];
    if args.timestamp_col.is_none() {
        for key in &args.partition_by {
            if reserved.contains(&key.as_str()) {
                eprintln!("error: reserved partition key `{key}`, but --timestamp-col is not set");
                std::process::exit(1);
            }
        }
    }

    // Fetch schema and full batch
    let schema_ref = infer_arrow_schema(&args.table, &pool).await?;
    let batch = sync_table(&args.table, &schema_ref, &pool).await?;

    // Determine output behavior
    if args.partition_by.is_empty() {
        // Single file output
        let output_path = Path::new(&args.output_dir).join(format!("{}.parquet", args.table));
        #[allow(clippy::unwrap_used)]
        fs::create_dir_all(output_path.parent().unwrap())?;
        let file = File::create(&output_path)?;
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(file, Arc::new(schema_ref.as_ref().clone()), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        if args.print {
            print_batches(&[batch.clone()])?;
        }
        info!("Wrote Parquet file to {}", output_path.display());
    } else {
        // Partitioned output
        let schema_clone: Schema = schema_ref.as_ref().clone();
        partition_and_write(&batch, &schema_clone, &args)?;
        if args.print {
            print_batches(&[batch])?;
        }
    }

    Ok(())
}

/// Partition and write the batch according to `args.partition_by` and `args.timestamp_col`
fn partition_and_write(batch: &RecordBatch, schema: &Schema, args: &Args) -> anyhow::Result<()> {
    // Build column index map
    let mut idx_map = HashMap::new();
    for (i, field) in schema.fields().iter().enumerate() {
        idx_map.insert(field.name().clone(), i);
    }

    // Group rows by partition values
    let mut groups: HashMap<String, Vec<u32>> = HashMap::new();
    for row in 0..batch.num_rows() {
        let mut parts = Vec::new();
        for key in &args.partition_by {
            #[allow(clippy::expect_used)]
            #[allow(clippy::unwrap_used)]
            if ["year", "month", "day", "hour"].contains(&key.as_str()) {
                // timestamp component
                let col_name = args.timestamp_col.as_ref().unwrap();

                let col_idx = *idx_map.get(col_name).expect("timestamp col not found");
                let ts_arr = batch
                    .column(col_idx)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("timestamp type mismatch");
                let ts = ts_arr.value_as_datetime(row).unwrap();
                let val = match key.as_str() {
                    "year" => ts.year().to_string(),
                    "month" => format!("{:02}", ts.month()),
                    "day" => format!("{:02}", ts.day()),
                    "hour" => format!("{:02}", ts.hour()),
                    _ => unreachable!(),
                };
                parts.push(format!("{key}={val}"));
            } else {
                // regular string column
                let col_idx = *idx_map.get(key).expect("column not found");
                let arr = batch
                    .column(col_idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string type mismatch");
                let val = arr.value(row);
                parts.push(format!("{key}={val}"));
            }
        }
        let group_key = parts.join("/");
        groups
            .entry(group_key)
            .or_default()
            .push(u32::try_from(row)?);
    }

    // Write each partition
    for (grp, indices) in groups {
        let dir = Path::new(&args.output_dir).join(&args.table).join(grp);
        fs::create_dir_all(&dir)?;
        let file_path = dir.join("part-00000.parquet");
        let file = File::create(&file_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))?;

        let idx_arr = UInt32Array::from(indices);
        // Slice each column array by the row indices
        let arrays: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|array| take(array.as_ref(), &idx_arr, None))
            .collect::<arrow::error::Result<Vec<_>>>()?;
        // Build a new RecordBatch for this partition
        let sliced_batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)?;
        writer.write(&sliced_batch)?;
        writer.close()?;
        info!("Wrote partitioned file to {}", file_path.display());
    }

    Ok(())
}
