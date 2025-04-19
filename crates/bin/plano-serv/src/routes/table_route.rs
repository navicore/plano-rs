///
/// This module provides functionality to inspect tables in a `DataFusion` context
///
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use plano_core::format::{format_batches, OutputFormat};
use std::fmt::Display;
use std::sync::Arc;
use warp::http::HeaderMap;

/// Handles the `/tables` endpoint to list tables and their row counts
pub async fn handle_tables(
    ctx: Arc<SessionContext>,
    headers: HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(warp::reject::not_found)?;

    let schema = catalog
        .schema("public")
        .ok_or_else(warp::reject::not_found)?;

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

    // Create RecordBatch from collected data
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(table_names)),
            Arc::new(Int64Array::from(row_counts)),
        ],
    )
    .map_err(|_| warp::reject())?;

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let output_format = match accept {
        "text/csv" => OutputFormat::Csv,
        "text/plain" => OutputFormat::Text,
        _ => OutputFormat::Json,
    };

    let content_type = match output_format {
        OutputFormat::Csv => "text/csv",
        OutputFormat::Text => "text/plain",
        OutputFormat::Json => "application/json",
    };
    let body = format_batches(&[batch], output_format).map_err(|_| warp::reject())?;

    Ok(warp::reply::with_header(body, "Content-Type", content_type))
}

#[derive(Debug)]
pub struct PlanoServerError {
    pub reason: String,
}
impl warp::reject::Reject for PlanoServerError {}
impl Display for PlanoServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Plano Server Error: {}", self.reason)
    }
}

#[derive(Debug)]
pub struct PlanoBadRequest {
    pub reason: String,
}

impl Display for PlanoBadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bad Request: {}", self.reason)
    }
}

impl warp::reject::Reject for PlanoBadRequest {}
