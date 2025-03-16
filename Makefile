python: venv
	VIRTUAL_ENV=.venv maturin develop --release --features python-bindings
clean:
	cargo clean
build:
	cargo build --all-features
venv:
	if [ ! -d "./.venv" ]; then python -m venv .venv;./.venv/bin/python3 -m pip install maturin; fi