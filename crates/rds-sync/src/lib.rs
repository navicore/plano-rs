use anyhow::{bail, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use sqlx::{PgPool, Row};
use std::sync::Arc;

pub async fn infer_arrow_schema(table: &str, pool: &PgPool) -> Result<Arc<Schema>> {
    let query = r#"
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
    "#;

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
            "numeric" | "decimal" => DataType::Float64, // lossy fallback
            other => bail!("Unsupported SQL type: {}", other),
        };

        fields.push(Field::new(&name, arrow_type, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}
