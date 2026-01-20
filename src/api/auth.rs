use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};

/// Extractor that validates admin API key from request headers.
/// Checks `Authorization: Bearer <key>` or `X-API-Key: <key>` headers.
pub struct AdminAuth;

#[derive(Debug)]
pub enum AuthError {
    MissingKey,
    InvalidKey,
    Disabled,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AuthError::MissingKey => (
                StatusCode::UNAUTHORIZED,
                "Missing API key. Use 'Authorization: Bearer <key>' or 'X-API-Key: <key>'",
            ),
            AuthError::InvalidKey => (StatusCode::FORBIDDEN, "Invalid API key"),
            AuthError::Disabled => (
                StatusCode::NOT_FOUND,
                "Admin endpoints disabled (no admin_api_key configured)",
            ),
        };

        let body = serde_json::json!({
            "ok": false,
            "error": message
        });

        (status, Json(body)).into_response()
    }
}

impl<S> FromRequestParts<S> for AdminAuth
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let expected_key = parts
            .extensions
            .get::<AdminApiKey>()
            .ok_or(AuthError::Disabled)?;

        let provided_key = extract_api_key(&parts.headers)?;

        if constant_time_eq(provided_key.as_bytes(), expected_key.0.as_bytes()) {
            Ok(AdminAuth)
        } else {
            Err(AuthError::InvalidKey)
        }
    }
}

/// Extension type to pass the expected API key to the extractor
#[derive(Clone)]
pub struct AdminApiKey(pub String);

fn extract_api_key(headers: &axum::http::HeaderMap) -> Result<String, AuthError> {
    // Try Authorization: Bearer <key>
    if let Some(auth) = headers.get(AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            if let Some(key) = auth_str.strip_prefix("Bearer ") {
                return Ok(key.trim().to_string());
            }
        }
    }

    // Try X-API-Key: <key>
    if let Some(key) = headers.get("X-API-Key") {
        if let Ok(key_str) = key.to_str() {
            return Ok(key_str.trim().to_string());
        }
    }

    Err(AuthError::MissingKey)
}

/// Constant-time string comparison to prevent timing attacks
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}
