//! Compile-fail conformance for invariant **G10** (spec §15): a command a grain
//! has no `GrainHandler` for must not compile. A property no runtime test can
//! express — the call simply must not type-check.

#[test]
fn invalid_grain_calls_do_not_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/unhandled_grain_ask.rs");
    // §7.12: a facet accessor on a grain that does not declare the facet must
    // not compile (the G10 discipline applied to storage).
    t.compile_fail("tests/compile_fail/kv_without_facet.rs");
    #[cfg(feature = "sql")]
    t.compile_fail("tests/compile_fail/sql_without_facet.rs");
}
