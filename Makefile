build:
	cargo build
	cargo build --no-default-features --features client
	cargo build --no-default-features --features broker
	cargo build --no-default-features --features backtrace,fuzzy
	cargo build --all-features

test:
	cargo test
	cargo test --no-default-features --features client
	cargo test --no-default-features --features broker
	cargo test --no-default-features --features backtrace,fuzzy
	cargo test --all-features

bench:

prepare: build test bench

clean:
	cargo clean
	rm -f check.out perf.out flamegraph.svg perf.data perf.data.old
