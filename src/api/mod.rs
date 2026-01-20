mod auth;
mod materialize;

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event as SseEvent, KeepAlive, KeepAliveStream},
        IntoResponse, Response, Sse,
    },
    routing::{delete, get, post},
    Json, Router,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::broadcast::Broadcaster;
use crate::db::Pool;
use crate::service::{QueryOptions, QueryResult, SyncStatus};

pub use auth::AdminApiKey;

#[derive(Clone)]
pub struct AppState {
    /// Map of chain_id -> pool
    pub pools: HashMap<u64, Pool>,
    /// Default chain_id (first chain)
    pub default_chain_id: u64,
    pub broadcaster: Arc<Broadcaster>,
}

impl AppState {
    fn get_pool(&self, chain_id: Option<u64>) -> Option<&Pool> {
        let id = chain_id.unwrap_or(self.default_chain_id);
        self.pools.get(&id)
    }
}

pub fn router(pools: HashMap<u64, Pool>, default_chain_id: u64, broadcaster: Arc<Broadcaster>) -> Router {
    router_with_admin_key(pools, default_chain_id, broadcaster, None)
}

pub fn router_with_admin_key(
    pools: HashMap<u64, Pool>,
    default_chain_id: u64,
    broadcaster: Arc<Broadcaster>,
    admin_api_key: Option<String>,
) -> Router {
    let state = AppState { pools, default_chain_id, broadcaster };

    let mut router = Router::new()
        .route("/health", get(handle_health))
        .route("/status", get(handle_status))
        .route("/query", get(handle_query))
        .route("/logs/{signature}", get(handle_logs));

    // Add protected /materialize routes if admin key is configured
    if let Some(key) = admin_api_key {
        router = router
            .route(
                "/materialize",
                post(materialize::handle_create_view).get(materialize::handle_list_views),
            )
            .route("/materialize", delete(materialize::handle_delete_view))
            .layer(axum::Extension(AdminApiKey(key)));
    }

    router
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn handle_health() -> &'static str {
    "OK"
}

#[derive(Serialize)]
struct StatusResponse {
    ok: bool,
    chains: Vec<SyncStatus>,
}

async fn handle_status(State(state): State<AppState>) -> Result<Json<StatusResponse>, ApiError> {
    // Aggregate status from all chains
    let mut all_chains = Vec::new();
    for pool in state.pools.values() {
        if let Ok(chains) = crate::service::get_all_status(pool).await {
            all_chains.extend(chains);
        }
    }

    Ok(Json(StatusResponse {
        ok: true,
        chains: all_chains,
    }))
}

#[derive(Deserialize)]
pub struct QueryParams {
    /// SQL query (SELECT only)
    sql: String,
    /// Event signature to create a CTE
    #[serde(default)]
    signature: Option<String>,
    /// Chain ID to query (uses default if not specified)
    #[serde(default, alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: Option<u64>,
    /// Enable live streaming mode (SSE)
    #[serde(default)]
    live: bool,
    /// Query timeout in milliseconds
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    /// Maximum rows to return
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_timeout() -> u64 {
    5000
}
fn default_limit() -> i64 {
    10000
}

#[derive(Serialize)]
struct QueryResponse {
    #[serde(flatten)]
    result: QueryResult,
    ok: bool,
}

async fn handle_query(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
) -> Response {
    if params.live {
        handle_query_live(state, params).await.into_response()
    } else {
        handle_query_once(state, params).await.into_response()
    }
}

async fn handle_query_once(
    state: AppState,
    params: QueryParams,
) -> Result<Json<QueryResponse>, ApiError> {
    let pool = state
        .get_pool(params.chain_id)
        .ok_or_else(|| ApiError::BadRequest(format!(
            "Unknown chain_id: {}. Available: {:?}",
            params.chain_id.unwrap_or(state.default_chain_id),
            state.pools.keys().collect::<Vec<_>>()
        )))?;

    let options = QueryOptions {
        timeout_ms: params.timeout_ms.clamp(100, 30000),
        limit: params.limit.clamp(1, 100000),
    };

    let result = crate::service::execute_query(
        pool,
        &params.sql,
        params.signature.as_deref(),
        &options,
    )
    .await
    .map_err(|e| {
        let msg = e.to_string();
        if msg.contains("timeout") {
            ApiError::Timeout
        } else if msg.contains("forbidden") || msg.contains("Only SELECT") {
            ApiError::BadRequest(msg)
        } else {
            ApiError::QueryError(msg)
        }
    })?;

    Ok(Json(QueryResponse { result, ok: true }))
}

type SseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>>;

async fn handle_query_live(
    state: AppState,
    params: QueryParams,
) -> Sse<KeepAliveStream<SseStream>> {
    let pool = match state.get_pool(params.chain_id) {
        Some(p) => p.clone(),
        None => {
            let stream: SseStream = Box::pin(async_stream::stream! {
                yield Ok(SseEvent::default()
                    .event("error")
                    .json_data(serde_json::json!({ "ok": false, "error": "Unknown chain_id" }))
                    .unwrap());
            });
            return Sse::new(stream).keep_alive(KeepAlive::default());
        }
    };

    let mut rx = state.broadcaster.subscribe();
    let sql = params.sql;
    let signature = params.signature;
    let options = QueryOptions {
        timeout_ms: params.timeout_ms.clamp(100, 30000),
        limit: params.limit.clamp(1, 100000),
    };

    let stream = async_stream::stream! {
        let mut last_block_num: u64 = 0;

        // Execute initial query
        match crate::service::execute_query(&pool, &sql, signature.as_deref(), &options).await {
            Ok(result) => {
                yield Ok(SseEvent::default()
                    .event("result")
                    .json_data(QueryResponse { result, ok: true })
                    .unwrap());
            }
            Err(e) => {
                yield Ok(SseEvent::default()
                    .event("error")
                    .json_data(serde_json::json!({ "ok": false, "error": e.to_string() }))
                    .unwrap());
                return;
            }
        }

        // Get current head block
        if let Ok(statuses) = crate::service::get_all_status(&pool).await {
            if let Some(s) = statuses.first() {
                last_block_num = s.synced_num as u64;
            }
        }

        // Stream updates on each new block
        loop {
            match rx.recv().await {
                Ok(update) => {
                    if update.block_num <= last_block_num {
                        continue;
                    }

                    let start = last_block_num + 1;
                    let end = update.block_num;

                    for block_num in start..=end {
                        let filtered_sql = inject_block_filter(&sql, block_num);
                        match crate::service::execute_query(&pool, &filtered_sql, signature.as_deref(), &options).await {
                            Ok(result) => {
                                yield Ok(SseEvent::default()
                                    .event("result")
                                    .json_data(QueryResponse { result, ok: true })
                                    .unwrap());
                            }
                            Err(e) => {
                                yield Ok(SseEvent::default()
                                    .event("error")
                                    .json_data(serde_json::json!({ "ok": false, "error": e.to_string() }))
                                    .unwrap());
                            }
                        }
                    }

                    last_block_num = end;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    yield Ok(SseEvent::default()
                        .event("lagged")
                        .json_data(serde_json::json!({ "skipped": n }))
                        .unwrap());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    let stream: SseStream = Box::pin(stream);
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Deserialize)]
pub struct LogsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    after: Option<String>,
    #[serde(default, alias = "chain_id", rename = "chainId")]
    chain_id: Option<u64>,
}

async fn handle_logs(
    State(state): State<AppState>,
    Path(signature): Path<String>,
    Query(params): Query<LogsQuery>,
) -> Result<Json<QueryResponse>, ApiError> {
    let pool = state
        .get_pool(params.chain_id)
        .ok_or_else(|| ApiError::BadRequest("Unknown chain_id".into()))?;

    let time_filter = if let Some(ref after) = params.after {
        parse_time_filter(after)?
    } else {
        "1 = 1".to_string()
    };

    let event_name = extract_event_name(&signature)?;
    
    let sql = format!(
        "SELECT * FROM \"{}\" WHERE {} ORDER BY block_timestamp DESC LIMIT {}",
        event_name,
        time_filter,
        params.limit.clamp(1, 10000)
    );

    let options = QueryOptions {
        timeout_ms: 5000,
        limit: params.limit.clamp(1, 10000),
    };

    let result = crate::service::execute_query(pool, &sql, Some(&signature), &options)
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    Ok(Json(QueryResponse { result, ok: true }))
}

fn extract_event_name(signature: &str) -> Result<String, ApiError> {
    let name = signature
        .split('(')
        .next()
        .unwrap_or("")
        .trim();
    
    if name.is_empty() {
        return Err(ApiError::BadRequest("Empty event name".into()));
    }
    
    if name.len() > 64 {
        return Err(ApiError::BadRequest("Event name too long".into()));
    }
    
    let is_valid = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    
    if !is_valid {
        return Err(ApiError::BadRequest("Invalid event name: must be alphanumeric".into()));
    }
    
    Ok(name.to_string())
}

fn parse_time_filter(after: &str) -> Result<String, ApiError> {
    if after.ends_with('h') {
        let hours: i64 = after
            .trim_end_matches('h')
            .parse()
            .map_err(|_| ApiError::BadRequest("Invalid time format".into()))?;
        if hours <= 0 || hours > 8760 {
            return Err(ApiError::BadRequest("Hours must be between 1 and 8760".into()));
        }
        Ok(format!(
            "block_timestamp > NOW() - INTERVAL '{hours} hours'"
        ))
    } else if after.ends_with('d') {
        let days: i64 = after
            .trim_end_matches('d')
            .parse()
            .map_err(|_| ApiError::BadRequest("Invalid time format".into()))?;
        if days <= 0 || days > 365 {
            return Err(ApiError::BadRequest("Days must be between 1 and 365".into()));
        }
        Ok(format!(
            "block_timestamp > NOW() - INTERVAL '{days} days'"
        ))
    } else {
        let parsed = chrono::DateTime::parse_from_rfc3339(after)
            .map_err(|_| ApiError::BadRequest("Invalid timestamp format. Use RFC3339 or relative time (e.g., '1h', '7d')".into()))?;
        Ok(format!("block_timestamp > '{}'", parsed.to_rfc3339()))
    }
}

/// Inject a block number filter into SQL query for live streaming.
/// Transforms queries to only return data for the specific block.
/// Uses 'num' for blocks table, 'block_num' for txs/logs tables.
#[doc(hidden)]
pub fn inject_block_filter(sql: &str, block_num: u64) -> String {
    let sql_upper = sql.to_uppercase();
    
    // Determine column name based on table being queried
    let col = if sql_upper.contains("FROM BLOCKS") || sql_upper.contains("FROM \"BLOCKS\"") {
        "num"
    } else {
        "block_num"
    };
    
    // Find WHERE clause position
    if let Some(where_pos) = sql_upper.find("WHERE") {
        // Insert after WHERE
        let insert_pos = where_pos + 5;
        format!(
            "{} {} = {} AND {}",
            &sql[..insert_pos],
            col,
            block_num,
            &sql[insert_pos..]
        )
    } else if let Some(order_pos) = sql_upper.find("ORDER BY") {
        // Insert WHERE before ORDER BY
        format!(
            "{} WHERE {} = {} {}",
            &sql[..order_pos],
            col,
            block_num,
            &sql[order_pos..]
        )
    } else if let Some(limit_pos) = sql_upper.find("LIMIT") {
        // Insert WHERE before LIMIT
        format!(
            "{} WHERE {} = {} {}",
            &sql[..limit_pos],
            col,
            block_num,
            &sql[limit_pos..]
        )
    } else {
        // Append WHERE at end
        format!("{sql} WHERE {col} = {block_num}")
    }
}

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Timeout,
    QueryError(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Timeout => (StatusCode::REQUEST_TIMEOUT, "Query timeout".to_string()),
            ApiError::QueryError(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = serde_json::json!({
            "ok": false,
            "error": message
        });

        (status, Json(body)).into_response()
    }
}
