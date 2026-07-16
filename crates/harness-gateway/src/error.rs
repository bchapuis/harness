//! The gateway's client-facing error model.
//!
//! Two failure layers stay distinct on the wire, exactly as the harness keeps
//! them apart (spec §12/§14): a *transport or durability* failure of reaching or
//! committing a command — a [`GrainError`] — becomes an HTTP status here; an
//! *application outcome* the run itself produced ([`RunOutcome`]'s `RunError`) is
//! **not** an error at this layer, it is a `200` whose body carries the outcome.
//! A future ACP adapter maps the first onto a JSON-RPC error and the second onto
//! a `session/prompt` stop reason, so keeping the split clean here keeps the
//! adapter thin.
//!
//! Every error a client sees is `{ "error": { "code": <stable>, "message": <human> } }`.
//! The `code` is a stable machine token; the `message` is the type's `Display`
//! (human-readable, never a `Debug` dump of internals).
//!
//! [`RunOutcome`]: harness::RunOutcome

use actor_core::CallError;
use axum::Json;
use axum::http::StatusCode;
use axum::http::header::WWW_AUTHENTICATE;
use axum::response::IntoResponse;
use axum::response::Response;
use harness::GrainError;
use serde_json::json;

/// A client-facing gateway error: a stable machine `code`, an HTTP `status`, and
/// a human `message`. Built from the edge's own validation ([`bad_request`],
/// [`unauthorized`]) and from a [`GrainError`] (the transport layer) — never from
/// a run's own `RunError`, which is an application outcome, not a transport
/// failure.
///
/// [`bad_request`]: GatewayError::bad_request
/// [`unauthorized`]: GatewayError::unauthorized
#[derive(Debug, Clone)]
pub struct GatewayError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl GatewayError {
    /// A gateway error with an explicit status, machine code, and message.
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> GatewayError {
        GatewayError {
            status,
            code,
            message: message.into(),
        }
    }

    /// `401 Unauthorized` — a missing or unverifiable bearer token. The response
    /// carries a `WWW-Authenticate: Bearer` challenge.
    pub fn unauthorized(message: impl Into<String>) -> GatewayError {
        GatewayError::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    /// `400 Bad Request` — a malformed request the edge itself rejects (e.g. a
    /// body that does not parse).
    pub fn bad_request(message: impl Into<String>) -> GatewayError {
        GatewayError::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    /// The HTTP status this error maps to.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// The stable machine code.
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// The `{ "code", "message" }` object, for embedding as an SSE `error` event's
    /// data (the stream analogue of the JSON body below).
    pub fn to_event_data(&self) -> String {
        json!({ "code": self.code, "message": self.message }).to_string()
    }
}

impl From<GrainError> for GatewayError {
    /// Map a transport/durability failure to an HTTP status (spec §12/§14):
    ///
    /// - `Call(Timeout)` → `504` — the deadline lapsed reaching/committing.
    /// - `Call(Unreachable | MailboxFull)`, `Unavailable`, `NotLeader` → `503` —
    ///   transient: no quorum, backpressure, or a leadership move; retryable.
    /// - anything else (`DeadLetter`, `Unhandled`, `Serialization`, `System`) →
    ///   `502` — an upstream failure the client cannot fix by retrying.
    fn from(e: GrainError) -> GatewayError {
        let (status, code) = match &e {
            GrainError::Call(CallError::Timeout) => (StatusCode::GATEWAY_TIMEOUT, "timeout"),
            GrainError::Call(CallError::Unreachable | CallError::MailboxFull)
            | GrainError::Unavailable(_)
            | GrainError::NotLeader(_) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            GrainError::Call(_) => (StatusCode::BAD_GATEWAY, "upstream"),
        };
        GatewayError::new(status, code, e.to_string())
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let body = Json(json!({ "error": { "code": self.code, "message": self.message } }));
        if self.status == StatusCode::UNAUTHORIZED {
            (self.status, [(WWW_AUTHENTICATE, "Bearer")], body).into_response()
        } else {
            (self.status, body).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grain_errors_map_to_transport_statuses() {
        assert_eq!(
            GatewayError::from(GrainError::Call(CallError::Timeout)).status(),
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            GatewayError::from(GrainError::Call(CallError::Unreachable)).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            GatewayError::from(GrainError::Call(CallError::MailboxFull)).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            GatewayError::from(GrainError::Unavailable("no quorum".into())).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        // A contract/system failure the client cannot retry away is 502.
        assert_eq!(
            GatewayError::from(GrainError::Call(CallError::Unhandled)).status(),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn event_data_is_the_stable_code_and_message() {
        let e = GatewayError::from(GrainError::Call(CallError::Timeout));
        let v: serde_json::Value = serde_json::from_str(&e.to_event_data()).unwrap();
        assert_eq!(v["code"], "timeout");
        assert!(v["message"].as_str().is_some());
    }
}
