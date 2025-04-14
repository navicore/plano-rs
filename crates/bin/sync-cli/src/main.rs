use rds_sync::infer_arrow_schema;
use sqlx::postgres::PgPoolOptions;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = env::var("DATABASE_URL")?;
    let table = "users";

    let pool = PgPoolOptions::new().connect(&db_url).await?;
    let schema = infer_arrow_schema(table, &pool).await?;

    println!("Schema for {table}:\n{:#?}", schema);
    Ok(())
}
