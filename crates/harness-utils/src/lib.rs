//! Shared utilities for the harness workspace.

/// Returns a friendly greeting for `name`.
pub fn greeting(name: &str) -> String {
    format!("Hello, {name}!")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greeting_includes_name() {
        assert_eq!(greeting("harness"), "Hello, harness!");
    }
}
