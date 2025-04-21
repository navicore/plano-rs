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
) -> Result<Response<String>, warp::Rejection> {
    let format = determine_output_format(&headers);
    let content_type = determine_content_type(&format);

    let Ok(query) = extract_query(&form) else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body::<String>("Can not extract 'sql' from input".into())
            .map_or_else(
                |e| {
                    Err(warp::reject::custom(PlanoBadRequest {
                        reason: e.to_string(),
                    }))
                },
                Ok,
            );
    };

    if let Some(cached_batches) = check_cache(&cache, query).await {
        return build_response(&cached_batches, format, content_type);
    }

    let results = match execute_query(&ctx, query).await {
        Ok(results) => results,
        Err(err) => return Err(err.into()),
    };

    cache.lock().await.put(query.clone(), results.clone());
    build_response(&results, format, content_type)
}

fn determine_output_format(headers: &HeaderMap) -> OutputFormat {
    match headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/plain")
    {
        "application/json" => OutputFormat::Json,
        "text/csv" => OutputFormat::Csv,
        _ => OutputFormat::Text,
    }
}

const fn determine_content_type(format: &OutputFormat) -> &'static str {
    match format {
        OutputFormat::Json => "application/json",
        OutputFormat::Csv => "text/csv",
        OutputFormat::Text => "text/plain",
    }
}

fn extract_query(form: &HashMap<String, String>) -> Result<&String, warp::Rejection> {
    form.get("sql").ok_or_else(|| {
        let emsg = "no 'sql' key in request";
        debug!(emsg);
        warp::reject::custom(PlanoBadRequest {
            reason: emsg.to_string(),
        })
    })
}

async fn check_cache(cache: &QueryCache, query: &str) -> Option<Vec<RecordBatch>> {
    cache.lock().await.get(query).map_or_else(
        || None,
        |cached_batches| {
            debug!("Cache hit for {}", query);
            Some(cached_batches.clone())
        },
    )
}

async fn execute_query(
    ctx: &Arc<SessionContext>,
    query: &str,
) -> Result<Vec<RecordBatch>, PlanoServerError> {
    match ctx.sql(query).await {
        Ok(df) => df.collect().await.map_err(|e| PlanoServerError {
            reason: e.to_string(),
        }),
        Err(e) => {
            warn!("❌ DataFusion `ctx.sql` error for '{}':\n  {}", query, e);
            Err(PlanoServerError {
                reason: e.to_string(),
            })
        }
    }
}

fn build_response(
    batches: &[RecordBatch],
    format: OutputFormat,
    content_type: &str,
) -> Result<Response<String>, warp::Rejection> {
    let body = format_batches(batches, format).map_err(|_| warp::reject())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    pub async fn setup_session_context() -> Arc<SessionContext> {
        Arc::new(SessionContext::new())
    }
    use lru::LruCache;
    use tokio::sync::Mutex;

    pub type QueryCache = Arc<Mutex<LruCache<String, Vec<RecordBatch>>>>;

    pub fn setup_query_cache(capacity: usize) -> QueryCache {
        Arc::new(Mutex::new(LruCache::new(NonZero::new(capacity).unwrap())))
    }
    use warp::http::HeaderMap;

    use std::collections::HashMap;

    pub fn setup_form(sql: &str) -> HashMap<String, String> {
        let mut form = HashMap::new();
        form.insert("sql".to_string(), sql.to_string());
        form
    }

    #[tokio::test]
    async fn test_handle_query_cache_hit() {
        let ctx = setup_session_context().await;
        let cache = setup_query_cache(10);
        let headers = HeaderMap::new();
        // Simulate a cache hit
        let sql = "SELECT * FROM test_table";
        let record_batches = vec![]; // Mocked RecordBatch
        cache
            .lock()
            .await
            .put(sql.to_string(), record_batches.clone());

        let form = setup_form(sql);
        let result = handle_query(form, ctx, cache, headers).await.unwrap();
        use warp::Reply;
        let response = result.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_handle_query_missing_sql() {
        let ctx = setup_session_context().await;
        let cache = setup_query_cache(10);
        let headers = HeaderMap::new();

        let form = HashMap::new(); // No "sql" key
        let result = handle_query(form, ctx, cache, headers).await.unwrap();
        use warp::Reply;
        let response = result.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_determine_output_format() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", "application/json".parse().unwrap());
        assert_eq!(determine_output_format(&headers), OutputFormat::Json);

        headers.insert("accept", "text/csv".parse().unwrap());
        assert_eq!(determine_output_format(&headers), OutputFormat::Csv);

        headers.insert("accept", "text/plain".parse().unwrap());
        assert_eq!(determine_output_format(&headers), OutputFormat::Text);

        headers.clear();
        assert_eq!(determine_output_format(&headers), OutputFormat::Text);
    }

    #[test]
    fn test_determine_content_type() {
        assert_eq!(
            determine_content_type(&OutputFormat::Json),
            "application/json"
        );
        assert_eq!(determine_content_type(&OutputFormat::Csv), "text/csv");
        assert_eq!(determine_content_type(&OutputFormat::Text), "text/plain");
    }

    #[test]
    fn test_extract_query_success() {
        let mut form = HashMap::new();
        form.insert("sql".to_string(), "SELECT * FROM test_table".to_string());
        match extract_query(&form) {
            Ok(query) => assert_eq!(query, "SELECT * FROM test_table"),
            Err(_) => panic!("Expected to extract query successfully"),
        }
    }

    #[test]
    fn test_extract_query_failure() {
        let form: HashMap<String, String> = HashMap::new();
        let result = extract_query(&form);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_check_cache_hit() {
        let cache = Arc::new(Mutex::new(LruCache::new(NonZero::new(10).unwrap())));
        let query = "SELECT * FROM test_table";
        let record_batches = vec![]; // Mocked RecordBatch
        cache
            .lock()
            .await
            .put(query.to_string(), record_batches.clone());

        let result = check_cache(&cache, query).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), record_batches);
    }

    #[tokio::test]
    async fn test_check_cache_miss() {
        let cache = Arc::new(Mutex::new(LruCache::new(NonZero::new(10).unwrap())));
        let query = "SELECT * FROM test_table";

        let result = check_cache(&cache, query).await;
        assert!(result.is_none());
    }
}
