//! Conformance invariant #20 (spec §3.3, §18.5, §18.6): an `ask`/`tell` of a
//! message an actor has no `Handler` for MUST NOT compile.
//!
//! This is the type-safety invariant the simulator cannot check at runtime —
//! it is asserted by the compiler. Each fixture in `tests/compile_fail/` is
//! expected to fail compilation with the recorded diagnostic (`*.stderr`).
//!
//! Regenerate the expected diagnostics after an intentional change with:
//!   `TRYBUILD=overwrite cargo test -p actor-core --test conformance_compile_fail`

#[test]
fn invalid_sends_do_not_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/unhandled_tell.rs");
    t.compile_fail("tests/compile_fail/unhandled_ask.rs");
}
