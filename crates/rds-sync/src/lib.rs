/// RDS Sync Library
use anyhow::{bail, Result};
use arrow::{
    array::{ArrayRef, BooleanBuilder, PrimitiveBuilder, RecordBatch, StringBuilder},
    datatypes::{
        DataType, Field, Float64Type, Int32Type, Int64Type, Schema, TimeUnit,
        TimestampMicrosecondType,
    },
};
use sqlx::{types::chrono, PgPool, Row};
use std::sync::Arc;

/// # Errors
///
/// Will return `Err` if the table does not exist or if the schema cannot be inferred.
pub async fn infer_arrow_schema(table: &str, pool: &PgPool) -> Result<Arc<Schema>> {
    let query = r"
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
    ";

    let rows = sqlx::query(query).bind(table).fetch_all(pool).await?;

    let mut fields = Vec::with_capacity(rows.len());

    for row in rows {
        let name: String = row.get("column_name");
        let sql_type: String = row.get("data_type");
        let nullable: bool = row.get::<String, _>("is_nullable") == "YES";

        let arrow_type = match sql_type.as_str() {
            "integer" | "int4" => DataType::Int32,
            "bigint" | "int8" => DataType::Int64,
            "smallint" | "int2" => DataType::Int16,
            "text" | "character varying" | "varchar" => DataType::Utf8,
            "boolean" => DataType::Boolean,
            "timestamp without time zone" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "date" => DataType::Date32,
            "numeric" | "decimal" | "double precision" => DataType::Float64, // lossy fallback
            other => bail!("Unsupported SQL type: {}", other),
        };

        fields.push(Field::new(&name, arrow_type, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Synchronizes a table from Postgres into an Arrow `RecordBatch`
/// # Errors
///
/// Will return `Err` if the table does not exist or if the schema cannot be inferred.
pub async fn sync_table(table: &str, schema: &Schema, pool: &PgPool) -> Result<RecordBatch> {
    let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let select_clause = column_names.join(", ");
    let query = format!("SELECT {select_clause} FROM {table}");

    let rows = sqlx::query(&query).fetch_all(pool).await?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let name = field.name().as_str();
        let data_type = field.data_type();

        let array: ArrayRef = match data_type {
            DataType::Float64 => {
                let mut builder = PrimitiveBuilder::<Float64Type>::with_capacity(rows.len());
                for row in &rows {
                    let value = row.try_get::<Option<f64>, _>(name)?;
                    builder.append_option(value);
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = PrimitiveBuilder::<Int64Type>::with_capacity(rows.len());
                for row in &rows {
                    let value = row.try_get::<Option<i64>, _>(name)?;
                    builder.append_option(value);
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in &rows {
                    let value = row.try_get::<Option<String>, _>(name)?;
                    builder.append_option(value.as_deref());
                }
                Arc::new(builder.finish())
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(rows.len());
                for row in &rows {
                    let value = row.try_get::<Option<bool>, _>(name)?;
                    builder.append_option(value);
                }
                Arc::new(builder.finish())
            }
            DataType::Int32 => {
                let mut builder = PrimitiveBuilder::<Int32Type>::with_capacity(rows.len());
                for row in &rows {
                    let value = row.try_get::<Option<i32>, _>(name)?;
                    builder.append_option(value);
                }
                Arc::new(builder.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut builder =
                    PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(rows.len());
                for row in &rows {
                    let dt = row.try_get::<Option<chrono::NaiveDateTime>, _>(name)?;
                    let ts = dt.map(|v| v.and_utc().timestamp_micros());
                    builder.append_option(ts);
                }
                Arc::new(builder.finish())
            }
            other => bail!("Unsupported data type for '{}': {:?}", name, other),
        };

        columns.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
    Ok(batch)
}
