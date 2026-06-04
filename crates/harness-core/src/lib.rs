//! Core library for the harness workspace.

use harness_utils::greeting;

/// Produces a welcome message using [`harness_utils`].
pub fn welcome(name: &str) -> String {
    greeting(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn welcome_delegates_to_utils() {
        assert_eq!(welcome("world"), "Hello, world!");
    }
}
