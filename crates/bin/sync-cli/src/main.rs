use rds_sync::sync_table;
use sqlx::postgres::PgPoolOptions;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = env::var("DATABASE_URL").expect("Please set DATABASE_URL in your environment");

    let pool = PgPoolOptions::new().connect(&db_url).await?;
    let batch = sync_table("your_table_name", &pool).await?;

    println!("Synced {} rows", batch.num_rows());
    Ok(())
}
