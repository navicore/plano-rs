# Justfile — source of truth for build / test / lint.
# Both local dev and GitHub Actions run the same recipes via `just ci`.

default: ci

# Format all code
fmt:
    cargo fmt --all

# Verify formatting without changing files
fmt-check:
    cargo fmt --all -- --check

# Lint — opinionated clippy; warnings are errors
lint:
    cargo clippy --locked --workspace --all-targets -- \
        -D warnings \
        -W clippy::pedantic \
        -W clippy::nursery \
        -W clippy::unwrap_used \
        -W clippy::expect_used \
        -A clippy::module_name_repetitions \
        -A clippy::needless_pass_by_value

# Run all tests
test:
    cargo test --locked --workspace --all-targets

# Release build
build:
    cargo build --locked --release

# Remove build artifacts
clean:
    cargo clean

# Run all CI checks (same as GitHub Actions!)
# This is what developers should run before pushing.
ci: fmt-check lint test build
    @echo "Safe to push to GitHub - CI will pass."
