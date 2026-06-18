//! Compile-fail conformance for invariant **G10** (spec §15): a command a grain
//! has no `GrainHandler` for must not compile. A property no runtime test can
//! express — the call simply must not type-check.

#[test]
fn invalid_grain_calls_do_not_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/unhandled_grain_ask.rs");
}
