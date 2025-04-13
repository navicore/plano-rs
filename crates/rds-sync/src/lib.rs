use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sqlx::{PgPool, Row};

pub async fn sync_table(table: &str, pool: &PgPool) -> Result<RecordBatch> {
    let query = format!("SELECT id, name FROM {}", table); // adjust fields later
    let rows = sqlx::query(&query).fetch_all(pool).await?;

    let mut ids = Vec::with_capacity(rows.len());
    let mut names = Vec::with_capacity(rows.len());

    for row in rows {
        ids.push(row.get::<i64, _>("id"));
        names.push(row.get::<String, _>("name"));
    }

    let id_array: ArrayRef = Arc::new(Int64Array::from(ids));
    let name_array: ArrayRef = Arc::new(StringArray::from(names));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(schema, vec![id_array, name_array])?;
    Ok(batch)
}
