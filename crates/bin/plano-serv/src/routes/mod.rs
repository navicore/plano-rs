///
/// This module provides `http` route implementations for the `Plano query server`
///
use crate::routes::query_route::handle_query_bytes;
use crate::routes::table_route::handle_tables;
use crate::Arc;
use datafusion::prelude::SessionContext;
pub use query_route::initialize_cache;
use query_route::QueryCache;
pub use rds::rds_route;
use std::fmt::Display;
use warp::reject::Rejection;
use warp::reply::Reply;
use warp::Filter;

mod query_route;
mod rds;
mod table_route;

#[derive(Debug, Eq, PartialEq)]
pub struct PlanoServerError {
    pub reason: String,
}
impl warp::reject::Reject for PlanoServerError {}
impl Display for PlanoServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Plano Server Error: {}", self.reason)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PlanoBadRequest {
    pub reason: String,
}

impl Display for PlanoBadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bad Request: {}", self.reason)
    }
}

impl warp::reject::Reject for PlanoBadRequest {}

pub fn configure_routes<
    T: Filter<Extract = (impl Reply,), Error = Rejection> + Clone + Send + Sync + 'static,
>(
    ctx: Arc<SessionContext>,
    cache: QueryCache,
    rds_filter: T,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let ctx_filter = warp::any().map(move || ctx.clone());

    let cache_filter = warp::any().map(move || cache.clone());

    let query_route = warp::path("query")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(ctx_filter.clone())
        .and(cache_filter)
        .and(warp::header::headers_cloned())
        .and_then(handle_query_bytes);

    let tables_route = warp::path("tables")
        .and(warp::get())
        .and(ctx_filter)
        .and(warp::header::headers_cloned())
        .and_then(handle_tables);

    query_route
        .or(tables_route)
        .or(rds_filter)
        .with(warp::log("plano-serv"))
}
