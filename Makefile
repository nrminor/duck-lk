.PHONY: clean clean_all clone-refs update-refs clean-refs fmt clippy check

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=duck_lk

# Stable C API: binaries are forwards-compatible across DuckDB versions.
# The VTab and VScalar registration paths use only stable v1.2.0 API functions.
USE_UNSTABLE_C_API=0

# Minimum DuckDB C API version required by this extension.
# For C_STRUCT (stable) ABI, this field means "minimum required", not "built against".
TARGET_DUCKDB_VERSION=v1.2.0

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean

#############################################
### Development
#############################################

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all-targets -- -D warnings

check: fmt clippy

#############################################
### Reference Repositories
#############################################

clone-refs:
	@echo "Cloning reference repositories into .agents/repos/..."
	@mkdir -p .agents/repos
	@echo "Cloning duckdb-encoding (VScalar pattern reference)..."
	git clone https://github.com/onnimonni/duckdb-encoding.git .agents/repos/duckdb-encoding || echo "duckdb-encoding already exists, skipping"
	@echo "Cloning duckdb-crawler (table function + HTTP reference)..."
	git clone https://github.com/midwork-finds-jobs/duckdb-crawler.git .agents/repos/duckdb-crawler || echo "duckdb-crawler already exists, skipping"
	@echo "Cloning labkey-rs (LabKey Rust client)..."
	git clone https://github.com/nrminor/labkey-rs.git .agents/repos/labkey-rs || echo "labkey-rs already exists, skipping"
	@echo "Cloning duckdb-rs (DuckDB Rust bindings)..."
	git clone https://github.com/duckdb/duckdb-rs.git .agents/repos/duckdb-rs || echo "duckdb-rs already exists, skipping"
	@echo "Reference repositories cloned to .agents/repos/"

update-refs:
	@echo "Updating reference repositories..."
	cd .agents/repos/duckdb-encoding && git pull || true
	cd .agents/repos/duckdb-crawler && git pull || true
	cd .agents/repos/labkey-rs && git pull || true
	cd .agents/repos/duckdb-rs && git pull || true
	@echo "Reference repositories updated"

clean-refs:
	@echo "Removing reference repositories..."
	rm -rf .agents/repos
	@echo "Reference repositories removed"
