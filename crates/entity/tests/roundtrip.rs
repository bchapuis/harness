//! Codec round-trip conformance for entity's wire types (V&V checklist #1).
//!
//! Every value entity puts on the wire: the entity identity `EntityName`, the
//! sequence position `Seq`, and the durability/transport error `EntityError` that
//! rides inside a command's reply (§6, §12); must survive an encode/decode round
//! trip unchanged. A silent change to any of these representations would corrupt
//! cross-node routing or replies.

use actor_core::CallError;
use actor_core::NodeId;
use entity::EntityError;
use entity::EntityName;
use entity::Seq;

fn roundtrip<T>(value: &T) -> T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let bytes = serde_json::to_vec(value).expect("encode");
    serde_json::from_slice(&bytes).expect("decode")
}

#[test]
fn entity_name_round_trips() {
    let name = EntityName::new("bank.Account", "account/42");
    let back = roundtrip(&name);
    assert_eq!(name, back);
    assert_eq!(back.entity_type(), "bank.Account");
    assert_eq!(back.key(), "account/42");
}

#[test]
fn seq_round_trips() {
    for raw in [0u64, 1, 2, 256, u64::MAX] {
        let seq = Seq::new(raw);
        assert_eq!(roundtrip(&seq), seq, "Seq({raw}) survives a round trip");
    }
    assert_eq!(roundtrip(&Seq::ZERO), Seq::ZERO);
}

/// The SQL facet's reply-carried types (spec §7.14): a handler puts query
/// results and SQL errors *inside its reply*, so they cross the wire with the
/// user codec and must survive the round trip.
#[cfg(feature = "sql")]
mod sql {
    use entity::facet::QueryResult;
    use entity::facet::SqlError;
    use entity::facet::SqlValue;

    use super::roundtrip;

    #[test]
    fn sql_value_round_trips_every_variant() {
        let cases = [
            SqlValue::Null,
            SqlValue::Integer(0),
            SqlValue::Integer(i64::MIN),
            SqlValue::Integer(i64::MAX),
            SqlValue::Real(0.0),
            SqlValue::Real(-1.5e300),
            SqlValue::Text(String::new()),
            SqlValue::Text("naïve — ünïcode".into()),
            SqlValue::Blob(Vec::new()),
            SqlValue::Blob(vec![0, 255, 1, 254]),
        ];
        for case in cases {
            assert_eq!(roundtrip(&case), case, "{case:?} survives a round trip");
        }
    }

    #[test]
    fn query_result_round_trips_columns_and_rows() {
        let result = QueryResult {
            columns: vec!["name".into(), "cents".into()],
            rows: vec![
                vec![SqlValue::Text("a".into()), SqlValue::Integer(250)],
                vec![SqlValue::Null, SqlValue::Real(0.5)],
            ],
        };
        let back = roundtrip(&result);
        assert_eq!(back, result);
        assert_eq!(
            back.columns.len(),
            back.rows[0].len(),
            "column order matches each row's value order"
        );
    }

    #[test]
    fn sql_error_round_trips() {
        let err = SqlError("no such table: entries".into());
        assert_eq!(roundtrip(&err), err);
    }
}

#[test]
fn entity_error_round_trips_every_variant() {
    // EntityError rides inside the host's reply on the wire, so every variant must
    // decode to itself (§6, §12).
    let cases = [
        EntityError::Call(CallError::Unreachable),
        EntityError::Call(CallError::Timeout),
        EntityError::Call(CallError::Serialization("boom".into())),
        EntityError::NotLeader(NodeId::new(7)),
        EntityError::Unavailable("quorum lost".into()),
        EntityError::Call(CallError::Unhandled),
    ];
    for case in cases {
        assert_eq!(roundtrip(&case), case, "{case:?} survives a round trip");
    }
}
