//! The control protocol: line-delimited JSON between a REPL and a node.
//!
//! One `Request` per line, one `Response` per line, correlated by `id` so a
//! parked prompt (a run can take minutes) never blocks a tail or a cancel on
//! the same connection. Every op carries the kind: the harness client
//! addresses a session as `(kind, session)` (`Harness::session`).
//!
//! The payload types ride the harness's own serde forms — `RunOutcome`,
//! `Record`, `SeqNo` — so the wire shows exactly what the journal holds.

use harness::Record;
use harness::RunOutcome;
use harness::SeqNo;
use serde::Deserialize;
use serde::Serialize;

/// One client request, tagged for correlation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub op: Op,
}

/// The three session operations the harness client exposes (§7.4).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Op {
    /// Submit a turn and wait (bounded) for its run's terminal outcome.
    /// Re-sending the same `turn` id is the idempotent resume (H7).
    Prompt {
        kind: String,
        session: String,
        turn: String,
        content: String,
        within_secs: u64,
    },
    /// Read committed records after `from` (exclusive).
    Tail {
        kind: String,
        session: String,
        from: u64,
        limit: u32,
    },
    /// Cancel the run `turn` names; idempotent.
    Cancel {
        kind: String,
        session: String,
        turn: String,
    },
}

/// One server response, correlated to its request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub body: Reply,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    /// A run's terminal outcome: `{"Ok": …}` or `{"Err": …}` per serde.
    Outcome { outcome: RunOutcome },
    /// A page of journal records.
    Records { records: Vec<(SeqNo, Record)> },
    /// The cancel was accepted (idempotent; says nothing about the run).
    Cancelled,
    /// A transport- or protocol-level failure; the caller may retry the same
    /// turn id once the cluster converges.
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_trip_as_single_lines() {
        let request = Request {
            id: 7,
            op: Op::Prompt {
                kind: "assistant".into(),
                session: "demo".into(),
                turn: "t-1".into(),
                content: "hello".into(),
                within_secs: 600,
            },
        };
        let line = serde_json::to_string(&request).expect("encodes");
        assert!(!line.contains('\n'));
        let back: Request = serde_json::from_str(&line).expect("decodes");
        assert_eq!(back.id, 7);
        assert!(matches!(back.op, Op::Prompt { .. }));
    }
}
