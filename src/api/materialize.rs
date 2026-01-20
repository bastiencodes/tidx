use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use super::{auth::AdminAuth, ApiError, AppState};

#[derive(Deserialize)]
pub struct CreateViewRequest {
    /// Name for the materialized view (will be prefixed with "mv_")
    pub name: String,
    /// SQL SELECT query to materialize
    pub query: String,
    /// Create a unique index for CONCURRENTLY refresh (optional)
    #[serde(default)]
    pub unique_column: Option<String>,
}

#[derive(Serialize)]
pub struct CreateViewResponse {
    pub ok: bool,
    pub view_name: String,
    pub message: String,
}

#[derive(Serialize)]
pub struct ListViewsResponse {
    pub ok: bool,
    pub views: Vec<String>,
}

#[derive(Deserialize)]
pub struct DeleteViewRequest {
    pub name: String,
}

#[derive(Serialize)]
pub struct DeleteViewResponse {
    pub ok: bool,
    pub message: String,
}

pub async fn handle_create_view(
    _auth: AdminAuth,
    State(state): State<AppState>,
    Json(req): Json<CreateViewRequest>,
) -> Result<Json<CreateViewResponse>, ApiError> {
    let view_name = validate_and_build_name(&req.name)?;
    validate_query(&req.query)?;

    let pool = state
        .get_pool(None)
        .ok_or_else(|| ApiError::Internal("No database configured".into()))?;

    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    // Check if view already exists
    let exists: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM pg_matviews WHERE schemaname = 'public' AND matviewname = $1",
            &[&view_name],
        )
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .get(0);

    if exists > 0 {
        return Err(ApiError::BadRequest(format!(
            "View '{view_name}' already exists"
        )));
    }

    // Create the materialized view
    let create_sql = format!(
        "CREATE MATERIALIZED VIEW \"{}\" AS {}",
        view_name, req.query
    );

    conn.execute(&create_sql, &[])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    // Create unique index if requested (enables CONCURRENTLY refresh)
    if let Some(ref col) = req.unique_column {
        let safe_col = validate_identifier(col)?;
        let index_sql = format!(
            "CREATE UNIQUE INDEX ON \"{view_name}\" (\"{safe_col}\")"
        );
        if let Err(e) = conn.execute(&index_sql, &[]).await {
            tracing::warn!(
                view = %view_name,
                column = %col,
                error = %e,
                "Failed to create unique index"
            );
        }
    }

    Ok(Json(CreateViewResponse {
        ok: true,
        view_name: view_name.clone(),
        message: format!("Created materialized view '{view_name}'"),
    }))
}

pub async fn handle_list_views(
    _auth: AdminAuth,
    State(state): State<AppState>,
) -> Result<Json<ListViewsResponse>, ApiError> {
    let pool = state
        .get_pool(None)
        .ok_or_else(|| ApiError::Internal("No database configured".into()))?;

    let views = crate::db::list_materialized_views(pool)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(ListViewsResponse { ok: true, views }))
}

pub async fn handle_delete_view(
    _auth: AdminAuth,
    State(state): State<AppState>,
    Json(req): Json<DeleteViewRequest>,
) -> Result<Json<DeleteViewResponse>, ApiError> {
    let view_name = validate_and_build_name(&req.name)?;

    let pool = state
        .get_pool(None)
        .ok_or_else(|| ApiError::Internal("No database configured".into()))?;

    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    // Check if it's a user-created view (has mv_ prefix)
    if !view_name.starts_with("mv_") {
        return Err(ApiError::BadRequest(
            "Can only delete user-created views (mv_* prefix)".into(),
        ));
    }

    let drop_sql = format!("DROP MATERIALIZED VIEW IF EXISTS \"{view_name}\"");
    conn.execute(&drop_sql, &[])
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    Ok(Json(DeleteViewResponse {
        ok: true,
        message: format!("Deleted materialized view '{view_name}'"),
    }))
}

fn validate_and_build_name(name: &str) -> Result<String, ApiError> {
    let name = name.trim();

    if name.is_empty() {
        return Err(ApiError::BadRequest("View name cannot be empty".into()));
    }

    if name.len() > 48 {
        return Err(ApiError::BadRequest(
            "View name too long (max 48 chars)".into(),
        ));
    }

    let is_valid = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');

    if !is_valid {
        return Err(ApiError::BadRequest(
            "Invalid view name: must be alphanumeric with underscores".into(),
        ));
    }

    // Prefix with mv_ to namespace user views
    let full_name = if name.starts_with("mv_") {
        name.to_string()
    } else {
        format!("mv_{name}")
    };

    Ok(full_name)
}

fn validate_identifier(name: &str) -> Result<String, ApiError> {
    let name = name.trim();

    if name.is_empty() || name.len() > 64 {
        return Err(ApiError::BadRequest("Invalid identifier".into()));
    }

    let is_valid = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_');

    if !is_valid {
        return Err(ApiError::BadRequest(
            "Invalid identifier: must be alphanumeric".into(),
        ));
    }

    Ok(name.to_string())
}

fn validate_query(query: &str) -> Result<(), ApiError> {
    let query_upper = query.trim().to_uppercase();

    // Must be a SELECT statement
    if !query_upper.starts_with("SELECT") {
        return Err(ApiError::BadRequest(
            "Query must be a SELECT statement".into(),
        ));
    }

    // Reject dangerous keywords
    let forbidden = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "COPY",
        "EXECUTE",
        "CALL",
    ];

    for keyword in forbidden {
        if query_upper.contains(keyword) {
            return Err(ApiError::BadRequest(format!(
                "Forbidden keyword in query: {keyword}"
            )));
        }
    }

    // Reject semicolons (multiple statements)
    if query.contains(';') {
        return Err(ApiError::BadRequest(
            "Multiple statements not allowed".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name() {
        assert_eq!(
            validate_and_build_name("my_view").unwrap(),
            "mv_my_view"
        );
        assert_eq!(
            validate_and_build_name("mv_existing").unwrap(),
            "mv_existing"
        );
        assert!(validate_and_build_name("").is_err());
        assert!(validate_and_build_name("123invalid").is_err());
        assert!(validate_and_build_name("has-dash").is_err());
        assert!(validate_and_build_name("has space").is_err());
    }

    #[test]
    fn test_validate_query() {
        assert!(validate_query("SELECT * FROM blocks").is_ok());
        assert!(validate_query("  SELECT count(*) FROM txs  ").is_ok());
        assert!(validate_query("INSERT INTO blocks").is_err());
        assert!(validate_query("SELECT * FROM blocks; DROP TABLE blocks").is_err());
        assert!(validate_query("SELECT * FROM blocks WHERE x = 'DROP'").is_err()); // False positive, but safe
    }
}
