use rds_sync::{infer_arrow_schema, sync_table};
use sqlx::postgres::PgPoolOptions;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = env::var("DATABASE_URL")?;
    let table = "users";

    let pool = PgPoolOptions::new().connect(&db_url).await?;

    let schema = infer_arrow_schema(table, &pool).await?;
    let batch = sync_table(table, &schema, &pool).await?;

    println!(
        "RecordBatch: {} rows, {} columns",
        batch.num_rows(),
        batch.num_columns()
    );

    for (i, col) in batch.columns().iter().enumerate() {
        println!(
            "Column {}: {} ({:?})",
            i,
            schema.field(i).name(),
            col.data_type()
        );
    }

    println!("{batch:?}");

    Ok(())
}
