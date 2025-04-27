// src/routes/rds.rs
use datafusion::arrow::{
    array::{ArrayRef, Int64Builder, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{datasource::MemTable, prelude::SessionContext};
use deadpool_postgres::Pool;
use plano_core::format::format_batches;
use std::convert::Infallible;
use std::sync::Arc;
use tokio_postgres::Row;
use warp::{Filter, Rejection, Reply};

/// Defines a filter that injects your PG pool into handlers
pub fn with_pg_pool(pool: Pool) -> impl Filter<Extract = (Pool,), Error = Infallible> + Clone {
    warp::any().map(move || pool.clone())
}

/// The `/rds` GET endpoint: fetches from RDS, registers in DF, returns JSON rows
pub fn rds_route(
    ctx: Arc<SessionContext>,
    pool: Pool,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let ctx_filter = warp::any().map(move || ctx.clone());
    // matches GET /rds
    warp::path("rds")
        .and(warp::get())
        .and(ctx_filter)
        .and(with_pg_pool(pool))
        .and_then(handle_rds)
}

async fn handle_rds(ctx: Arc<SessionContext>, pool: Pool) -> Result<impl Reply, Rejection> {
    // 1) fetch rows from Postgres
    let rows: Vec<Row> = pool
        .get()
        .await
        .map_err(|_| warp::reject())?
        .query("SELECT id, username, value FROM events LIMIT 1000", &[])
        .await
        .map_err(|_| warp::reject())?;

    // 2) build Arrow arrays
    let mut id_b = Int64Builder::new();
    let mut user_b = StringBuilder::new();
    let mut val_b = Int64Builder::new();
    for row in rows {
        id_b.append_value(row.get::<_, i64>("id"));
        user_b.append_value(row.get::<_, &str>("username"));
        val_b.append_value(row.get::<_, i64>("value"));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("username", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_b.finish()) as ArrayRef,
            Arc::new(user_b.finish()) as ArrayRef,
            Arc::new(val_b.finish()) as ArrayRef,
        ],
    )
    .map_err(|_| warp::reject())?;

    // 3) register as an in-memory DataFusion table
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(|_| warp::reject())?;
    ctx.deregister_table("events").ok(); // if already exists
    ctx.register_table("events", Arc::new(table))
        .map_err(|_| warp::reject())?;

    // 4) run a simple SQL (or you could read a query param)
    let df = ctx
        .sql("SELECT * FROM events")
        .await
        .map_err(|_| warp::reject())?;
    let result = df.collect().await.map_err(|_| warp::reject())?;

    //let (output_format, content_type) = determine_output_format(&headers);
    let (output_format, content_type) =
        (plano_core::format::OutputFormat::Json, "application/json");
    // return as JSON
    //Ok(warp::reply::json(&result))
    let body = format_batches(&result, output_format).map_err(|_| warp::reject())?;

    Ok(warp::reply::with_header(body, "Content-Type", content_type))
}
