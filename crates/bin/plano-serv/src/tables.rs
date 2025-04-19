///
/// This module provides functionality to register multiple tables in a `DataFusion` context
///
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;

/// A single table registration spec:
/// name        — the SQL name clients will use (e.g. "events")
/// root        — a file:// or s3:// URI pointing at the top-level directory
/// partitions  — zero or more folder-key names
#[derive(Debug)]
struct TableSpec {
    pub name: String,
    pub root: String,
    pub partitions: Vec<String>,
}

impl TableSpec {
    /// Parse strings of the form
    ///   name=path[:col1,col2,...]
    /// Examples:
    ///   events=/data/parquet/events:year,month,day
    ///   users=s3://bucket/users
    pub fn parse(s: &str) -> Result<Self, String> {
        // split off name=rest
        let (name, rest) = s
            .split_once('=')
            .ok_or_else(|| format!("Invalid table-spec `{s}`"))?;
        // split off optional :part1,part2
        let (root, parts) = if let Some((r, p)) = rest.split_once(':') {
            (r, p)
        } else {
            (rest, "")
        };
        let partitions = if parts.is_empty() {
            Vec::new()
        } else {
            parts.split(',').map(ToString::to_string).collect()
        };
        Ok(Self {
            name: name.to_string(),
            root: root.to_string(),
            partitions,
        })
    }
}

// Registers a table in the DataFusion context using a `ListingTableConfig`
//
// The complexity is due to we use partition keys based on file data but once we start using a
// file column as a partition key datafusion will fail in sql planning because it can't deal with
// duplicate cols in the schema.  We need to scrub the file column when we are adding  a
// partition key.
async fn register_table(
    ctx: &SessionContext,
    spec: &TableSpec, // your own struct that holds name, path, partition list …
) -> datafusion::error::Result<()> {
    let base_opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_table_partition_cols(
            spec.partitions
                .iter()
                .map(|c| (c.clone(), DataType::Utf8))
                .collect(),
        );

    let table_url = ListingTableUrl::parse(&spec.root)?;

    let session_state = ctx.state();
    let file_schema = base_opts.infer_schema(&session_state, &table_url).await?;

    let part_set: HashSet<&str> = spec.partitions.iter().map(String::as_str).collect();

    // filter out the file columns that are also partition keys
    let clean_fields: Vec<Field> = file_schema
        .fields()
        .iter()
        .filter(|f| !part_set.contains(f.name().as_str()))
        .map(|f| (**f).clone()) // <‑‑ convert Arc<Field> → Field
        .collect();

    let clean_schema = Arc::new(Schema::new(clean_fields));

    let cfg = ListingTableConfig::new(table_url)
        .with_listing_options(base_opts)
        .with_schema(clean_schema); // <‑‑ this is the key :contentReference[oaicite:0]{index=0}

    let table = ListingTable::try_new(cfg)?;
    ctx.register_table(&spec.name, Arc::new(table))?;

    Ok(())
}

// MODULE ENTRY POINT

/// Registers multiple tables in the `DataFusion` context based on a list of table specs.
/// Each spec should be in the format:
/// name=path[:col1,col2,...]
pub async fn register_tables(
    ctx: &Arc<SessionContext>,
    table_specs: &[String],
) -> anyhow::Result<()> {
    for raw in table_specs {
        let spec = TableSpec::parse(raw).map_err(anyhow::Error::msg)?;
        let root_prefix = if spec.root.starts_with("s3://") {
            spec.root.clone()
        } else {
            format!("{}/", std::fs::canonicalize(&spec.root)?.display())
        };

        let store_url = if spec.root.starts_with("s3://") {
            spec.root.clone()
        } else {
            format!("file://{root_prefix}")
        };

        register_table(ctx, &spec).await?;
        println!("Registered table `{}` at `{}`", spec.name, store_url);
    }
    Ok(())
}
