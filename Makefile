build:

test:

bench:

prepare: build test bench

clean:
	cargo clean
	rm -f check.out perf.out flamegraph.svg perf.data perf.data.old
