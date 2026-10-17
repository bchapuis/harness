//! Compile-fail conformance for invariant **E10** (spec §12): a command an entity
//! has no `EntityHandler` for must not compile, a property no runtime test can
//! express.

#[test]
fn invalid_entity_calls_do_not_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/unhandled_entity_ask.rs");
    // facets §3: a facet accessor on an entity that does not declare the facet must
    // not compile (the E10 discipline applied to storage).
    t.compile_fail("tests/compile_fail/kv_without_facet.rs");
    #[cfg(feature = "sql")]
    t.compile_fail("tests/compile_fail/sql_without_facet.rs");
}
