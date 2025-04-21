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
use tracing::info;

/// A single table registration spec:
/// name        — the SQL name clients will use (e.g. "events")
/// root        — a file:// or s3:// URI pointing at the top-level directory
/// partitions  — zero or more folder-key names
#[derive(Debug)]
pub struct TableSpec {
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
        let (root, parts) = rest.rfind(':').map_or_else(
            || (rest.to_string(), String::new()),
            |idx| {
                if rest.get(idx + 1..idx + 2) == Some("/") {
                    (rest.to_string(), String::new()) // Treat the entire string as root
                } else {
                    let root = &rest[..idx]; // Everything before the last valid ':'
                    let parts = &rest[idx + 1..]; // Everything after the last valid ':'
                    (root.to_string(), parts.to_string())
                }
            },
        );

        // ensure root is not empty
        let partitions = if parts.is_empty() {
            Vec::new()
        } else {
            parts.split(',').map(ToString::to_string).collect()
        };

        Ok(Self {
            name: name.to_string(),
            root,
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
    table_specs: &[TableSpec],
) -> anyhow::Result<()> {
    for spec in table_specs {
        register_table(ctx, spec).await?;
        info!("Registered table `{}` at `{}`", spec.name, spec.root);
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_valid_spec() {
        let spec = TableSpec::parse("events=/data/parquet/events:year,month,day").unwrap();
        assert_eq!(spec.name, "events");
        assert_eq!(spec.root, "/data/parquet/events");
        assert_eq!(spec.partitions, vec!["year", "month", "day"]);
    }

    #[test]
    fn test_parse_no_partitions() {
        let spec = TableSpec::parse("users=s3://bucket/users").unwrap();
        assert_eq!(spec.name, "users");
        assert_eq!(spec.root, "s3://bucket/users");
        assert!(spec.partitions.is_empty());
    }

    #[test]
    fn test_parse_error_message() {
        let result = TableSpec::parse("invalid_spec");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid table-spec `invalid_spec`"
        );
    }

    #[test]
    fn test_parse_invalid_spec() {
        let result = TableSpec::parse("invalid_spec");
        assert!(result.is_err());
    }
    #[test]
    fn test_parse_single_partition() {
        let spec = TableSpec::parse("data=/path/to/data:year").unwrap();
        assert_eq!(spec.partitions, vec!["year"]);
    }

    #[test]
    fn test_parse_multiple_partitions() {
        let spec = TableSpec::parse("data=/path/to/data:year,month,day").unwrap();
        assert_eq!(spec.partitions, vec!["year", "month", "day"]);
    }
    #[test]
    fn test_parse_empty_input() {
        let result = TableSpec::parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_malformed_input() {
        let result = TableSpec::parse("invalid_spec");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_complex_uri() {
        let spec = TableSpec::parse("data=s3://bucket-name/folder:year,month").unwrap();
        assert_eq!(spec.name, "data");
        assert_eq!(spec.root, "s3://bucket-name/folder");
        assert_eq!(spec.partitions, vec!["year", "month"]);
    }

    // #[tokio::test]
    // async fn test_handle_query_bytes_valid() {
    //     let raw_body = Bytes::from("sql=SELECT%20*%20FROM%20test");
    //     let ctx = Arc::new(SessionContext::new());
    //     let cache = initialize_cache(10);
    //     let headers = HeaderMap::new();
    //
    //     let result = handle_query_bytes(raw_body, ctx, cache, headers).await;
    //     assert!(result.is_ok());
    // }

    // #[tokio::test]
    // async fn test_handle_query_bytes_invalid() {
    //     let raw_body = Bytes::from("invalid_body");
    //     let ctx = Arc::new(SessionContext::new());
    //     let cache = initialize_cache(10);
    //     let headers = HeaderMap::new();
    //
    //     let result = handle_query_bytes(raw_body, ctx, cache, headers).await;
    //     assert!(result.is_err());
    // }

    // use crate::routes::query_route::handle_query_bytes;
    // use warp::{http::HeaderMap, Filter};
    // #[tokio::test]
    // async fn test_query_endpoint() {
    //     let raw_body = Bytes::from("sql=SELECT%20*%20FROM%20test");
    //     let ctx = Arc::new(SessionContext::new());
    //     let cache = initialize_cache(10);
    //     let headers = HeaderMap::new();
    //
    //     let route = warp::post().and(warp::body::bytes()).and_then(move |body| {
    //         handle_query_bytes(body, ctx.clone(), cache.clone(), headers.clone())
    //     });
    //
    //     let response = warp::test::request()
    //         .method("POST")
    //         .body(raw_body)
    //         .reply(&route)
    //         .await;
    //
    //     assert_eq!(response.status(), StatusCode::OK);
    // }
}
