///
/// This module provides query handling functionality for the `Plano server`
///
use crate::routes::{PlanoBadRequest, PlanoServerError};
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;
use lru::LruCache;
use plano_core::format::{format_batches, OutputFormat};
use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};
use warp::http::{HeaderMap, Response, StatusCode};

// Cache distinct queries in memory
pub type QueryCache = Arc<Mutex<LruCache<String, Vec<RecordBatch>>>>;

pub fn initialize_cache(size: usize) -> QueryCache {
    #[allow(clippy::expect_used)]
    Arc::new(Mutex::new(LruCache::new(
        NonZero::new(size).expect("Cache size must be a non-zero value"),
    )))
}

/// Handles the `/query` endpoint to execute SQL queries
async fn handle_query(
    form: HashMap<String, String>,
    ctx: Arc<SessionContext>,
    cache: QueryCache,
    headers: HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    // Check
    let Some(query) = form.get("sql") else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Missing 'sql'".into())
            .map_or_else(
                |e| {
                    Err(warp::reject::custom(PlanoBadRequest {
                        reason: e.to_string(),
                    }))
                },
                Ok,
            );
    };

    // Try hit using query as cache key
    if let Some(cached_batches) = cache.lock().await.get(query) {
        debug!("Cache hit for {}", &query);
        let body =
            format_batches(cached_batches, OutputFormat::Json).map_err(|_| warp::reject())?;
        return Response::builder()
            .status(StatusCode::OK)
            .body(body)
            .map_or_else(
                |e| {
                    Err(warp::reject::custom(PlanoBadRequest {
                        reason: e.to_string(),
                    }))
                },
                Ok,
            );
    }

    debug!("handle_query: {query}");

    let df = match ctx.sql(query).await {
        Ok(df) => df,
        Err(e) => {
            warn!("❌ DataFusion `ctx.sql` error for '{}':\n  {}", query, e);
            return Err(PlanoServerError {
                reason: e.to_string(),
            }
            .into());
        }
    };

    let results = df.collect().await.map_err(|_| warp::reject())?;
    debug!("handle_query results: {results:?}");

    // store in cache
    cache.lock().await.put(query.clone(), results.clone());

    // then format & reply as before
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/plain");

    let format = match accept {
        "application/json" => OutputFormat::Json,
        "text/csv" => OutputFormat::Csv,
        _ => OutputFormat::Text,
    };

    let content_type = match &format {
        OutputFormat::Json => "application/json",
        OutputFormat::Csv => "text/csv",
        OutputFormat::Text => "text/plain",
    };

    let body = format_batches(&results, format).map_err(|_| warp::reject())?;
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", content_type)
        .body(body)
        .map_or_else(|_| Err(warp::reject()), Ok)
}

/// Unified query handler that first captures raw bytes,
/// optionally logs them, then parses as form and delegates.
/// Naturally we will lock this down or remove sql support all together in a production capable
/// server.  For POC we're using sql in the API.
pub async fn handle_query_bytes(
    raw_body: Bytes,
    ctx: Arc<SessionContext>,
    cache: QueryCache,
    headers: HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&raw_body).map_err(|e| {
        warn!("form parse error: {}", e);
        warp::reject::custom(PlanoBadRequest {
            reason: e.to_string(),
        })
    })?;

    handle_query(form, ctx, cache, headers).await
}
