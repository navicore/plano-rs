///
/// Synchronize a Postgres table and write to Parquet with optional partitioning
///
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
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

use crate::Args;

// TODO it should only look for reserved words when a timestamp_col is not set
pub fn validate_partition_keys(args: &Args) {
    let reserved = ["year", "month", "day", "hour"];
    if args.timestamp_col.is_none() {
        for key in &args.partition_by {
            if reserved.contains(&key.as_str()) {
                eprintln!("error: reserved partition key `{key}`, but --timestamp-col is not set");
                std::process::exit(1);
            }
        }
    }
}

pub fn write_partitioned_files(
    args: &Args,
    schema_ref: &Arc<Schema>,
    batch: &RecordBatch,
) -> anyhow::Result<()> {
    let schema_clone: Schema = schema_ref.as_ref().clone();
    partition_and_write(batch, &schema_clone, args)
}

fn partition_and_write(batch: &RecordBatch, schema: &Schema, args: &Args) -> anyhow::Result<()> {
    let idx_map = build_column_index_map(schema);
    let groups = group_rows_by_partition(batch, args, &idx_map)?;

    for (grp, indices) in groups {
        write_partition(grp, indices, batch, schema, args)?;
    }

    Ok(())
}

fn build_column_index_map(schema: &Schema) -> HashMap<String, usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name().clone(), i))
        .collect()
}

fn group_rows_by_partition(
    batch: &RecordBatch,
    args: &Args,
    idx_map: &HashMap<String, usize>,
) -> anyhow::Result<HashMap<String, Vec<u32>>> {
    let mut groups: HashMap<String, Vec<u32>> = HashMap::new();

    for row in 0..batch.num_rows() {
        let group_key = build_partition_key(row, batch, args, idx_map);
        groups
            .entry(group_key)
            .or_default()
            .push(u32::try_from(row)?);
    }

    Ok(groups)
}

fn build_partition_key(
    row: usize,
    batch: &RecordBatch,
    args: &Args,
    idx_map: &HashMap<String, usize>,
) -> String {
    let mut parts = Vec::new();

    #[allow(clippy::expect_used)]
    for key in &args.partition_by {
        if ["year", "month", "day", "hour"].contains(&key.as_str()) {
            let col_name = args
                .timestamp_col
                .as_ref()
                .expect("timestamp_col must be set for time-based partitioning");
            let col_idx = *idx_map.get(col_name).expect("timestamp col not found");
            let ts_arr = batch
                .column(col_idx)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp type mismatch");
            let ts = ts_arr
                .value_as_datetime(row)
                .expect("invalid timestamp value");
            let val = match key.as_str() {
                "year" => ts.year().to_string(),
                "month" => format!("{:02}", ts.month()),
                "day" => format!("{:02}", ts.day()),
                "hour" => format!("{:02}", ts.hour()),
                _ => unreachable!(),
            };
            parts.push(format!("{key}={val}"));
        } else {
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

    parts.join("/")
}

fn write_partition(
    grp: String,
    indices: Vec<u32>,
    batch: &RecordBatch,
    schema: &Schema,
    args: &Args,
) -> anyhow::Result<()> {
    let dir = Path::new(&args.output_dir).join(&args.table).join(grp);
    fs::create_dir_all(&dir)?;
    // TODO: support collisions and successive adding to // the same partition
    let file_path = dir.join("part-00000.parquet");
    let file = File::create(&file_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))?;

    let idx_arr = UInt32Array::from(indices);
    let arrays: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|array| take(array.as_ref(), &idx_arr, None))
        .collect::<arrow::error::Result<Vec<_>>>()?;
    let sliced_batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)?;
    writer.write(&sliced_batch)?;
    writer.close()?;
    info!("Wrote partitioned file to {}", file_path.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_build_column_index_map() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let idx_map = build_column_index_map(&schema);
        assert_eq!(idx_map.get("col1"), Some(&0));
        assert_eq!(idx_map.get("col2"), Some(&1));
    }

    #[test]
    fn test_group_rows_by_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(StringArray::from(vec!["1", "2", "3"])),
            ],
        )
        .unwrap();
        let args = Args {
            partition_by: vec!["key".to_string()],
            ..Default::default()
        };
        let idx_map = build_column_index_map(&schema);
        let groups = group_rows_by_partition(&batch, &args, &idx_map).unwrap();
        assert_eq!(groups.get("key=a").unwrap().len(), 2);
        assert_eq!(groups.get("key=b").unwrap().len(), 1);
    }

    #[test]
    fn test_write_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(StringArray::from(vec!["1", "2", "3"])),
            ],
        )
        .unwrap();
        let dir = tempdir().unwrap();
        let args = Args {
            output_dir: dir.path().to_str().unwrap().to_string(),
            table: "test_table".to_string(),
            ..Default::default()
        };
        write_partition("key=a".to_string(), vec![0, 2], &batch, &schema, &args).unwrap();
        let output_path = dir.path().join("test_table/key=a/part-00000.parquet");
        assert!(
            output_path.exists(),
            "Expected partition file to be written"
        );
    }
}
