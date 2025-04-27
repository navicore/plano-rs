// src/routes/rds.rs
use datafusion::arrow::{
    array::{ArrayRef, Int32Builder, StringBuilder},
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
        .query(
            "SELECT name,uuid,navigation_speedoverground_value FROM signalk_2 LIMIT 1000;",
            &[],
        )
        .await
        .map_err(|_| warp::reject())?;

    // 2) build Arrow arrays
    let mut name = StringBuilder::new();
    let mut uuid = StringBuilder::new();
    let mut speed = Int32Builder::new();
    for row in rows {
        name.append_value(row.get::<_, &str>("name"));
        uuid.append_value(row.get::<_, &str>("uuid"));
        speed.append_value(row.get::<_, i32>("navigation_speedoverground_value"));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("uuid", DataType::Utf8, false),
        Field::new("speed", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(name.finish()) as ArrayRef,
            Arc::new(uuid.finish()) as ArrayRef,
            Arc::new(speed.finish()) as ArrayRef,
        ],
    )
    .map_err(|_| warp::reject())?;

    // 3) register as an in-memory DataFusion table
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(|_| warp::reject())?;
    ctx.deregister_table("signalk_2").ok(); // if already exists
    ctx.register_table("signalk_2", Arc::new(table))
        .map_err(|_| warp::reject())?;

    // 4) run a simple SQL (or you could read a query param)
    let df = ctx
        .sql("SELECT * FROM signalk_2 LIMIT 10")
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
