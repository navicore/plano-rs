///
/// This module provides functionality to inspect tables in a `DataFusion` context
///
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::SchemaProvider;
use datafusion::prelude::*;
use plano_core::format::{format_batches, OutputFormat};
use std::sync::Arc;
use warp::http::HeaderMap;

fn get_schema(ctx: &Arc<SessionContext>) -> Result<Arc<dyn SchemaProvider>, warp::Rejection> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(warp::reject::not_found)?;

    catalog.schema("public").ok_or_else(warp::reject::not_found)
}

async fn get_table_data(
    ctx: Arc<SessionContext>,
    schema: Arc<dyn SchemaProvider>,
) -> Result<(Vec<String>, Vec<i64>), warp::Rejection> {
    let mut table_names = Vec::new();
    let mut row_counts = Vec::new();

    for table_name in schema.table_names() {
        let count_query = format!("SELECT COUNT(*) AS cnt FROM {table_name}");
        let df = ctx.sql(&count_query).await.map_err(|_| warp::reject())?;
        let batches = df.collect().await.map_err(|_| warp::reject())?;

        let count_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(warp::reject::not_found)?;

        table_names.push(table_name.to_string());
        row_counts.push(count_array.value(0));
    }

    Ok((table_names, row_counts))
}

fn create_record_batch(
    table_names: Vec<String>,
    row_counts: Vec<i64>,
) -> Result<RecordBatch, warp::Rejection> {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(table_names)),
            Arc::new(Int64Array::from(row_counts)),
        ],
    )
    .map_err(|_| warp::reject())
}

fn determine_output_format(headers: &HeaderMap) -> (OutputFormat, &'static str) {
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    match accept {
        "text/csv" => (OutputFormat::Csv, "text/csv"),
        "text/plain" => (OutputFormat::Text, "text/plain"),
        _ => (OutputFormat::Json, "application/json"),
    }
}

// ENTRY POINT

/// Handles the `/tables` route to list tables and their row counts.
pub async fn handle_tables(
    ctx: Arc<SessionContext>,
    headers: HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    let schema = get_schema(&ctx)?;
    let (table_names, row_counts) = get_table_data(ctx, schema).await?;
    let batch = create_record_batch(table_names, row_counts)?;

    let (output_format, content_type) = determine_output_format(&headers);
    let body = format_batches(&[batch], output_format).map_err(|_| warp::reject())?;

    Ok(warp::reply::with_header(body, "Content-Type", content_type))
}
