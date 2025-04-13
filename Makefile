.PHONY: clippy-fixes clippy-strict

all: lint

lint:
	cargo clippy -- -W clippy::pedantic -W clippy::nursery -W clippy::unwrap_used -W clippy::expect_used -A clippy::module_name_repetitions -A clippy::needless_pass_by_value

run:
	cargo run 

test:
	cargo test

build:
	cargo build

clean:
	cargo clean
