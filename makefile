# Compiler and flags
CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17 -g -O2 -Irapidjson/include -Izlib/zlib-1.2.13/ibdNinja/include
# Extra flags injected by special targets (e.g. `make asan`)
CXXFLAGS += $(EXTRA_CXXFLAGS)

LDFLAGS = -Lzlib/zlib-1.2.13/ibdNinja/lib -lz -Wl,-rpath,zlib/zlib-1.2.13/ibdNinja/lib
LDFLAGS += $(EXTRA_LDFLAGS)

# Target
TARGET = ibdNinja

# Source files, object files, and target
SRCS = main.cc ibdNinja.cc ibdUtils.cc Properties.cc Column.cc Index.cc Table.cc Record.cc JsonBinary.cc
OBJS = $(SRCS:.cc=.o)

# Default target
all: $(TARGET)

ZLIB_DIR = $(CURDIR)/zlib/zlib-1.2.13
ZLIB_LIB = $(ZLIB_DIR)/ibdNinja/lib/libz.a

check_zlib:
	@if [ ! -f $(ZLIB_LIB) ]; then \
		echo "Building zlib..."; \
		cd $(ZLIB_DIR) && ./configure --prefix=$(ZLIB_DIR)/ibdNinja && make; make install; \
	fi

# Build the target
$(TARGET): check_zlib $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDFLAGS)

-include *.d
# Compile each source file into an object file
%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -MMD -o $@

# Clean build files
clean:
	rm -f $(OBJS) $(TARGET) $(SRCS:.cc=.d)

# Test targets - run all tests
test: $(TARGET)
	@./tests/run_tests.sh
	@./tests/8.0/test_inspect_blob.sh
	@./tests/8.4/test_inspect_blob.sh
	@./tests/9.0/test_inspect_blob.sh
	@./tests/corruption/test_corruption.sh

test-verbose: $(TARGET)
	@./tests/run_tests.sh --verbose
	@./tests/8.0/test_inspect_blob.sh --verbose
	@./tests/8.4/test_inspect_blob.sh --verbose
	@./tests/9.0/test_inspect_blob.sh --verbose
	@./tests/corruption/test_corruption.sh --verbose

test-update: $(TARGET)
	@./tests/run_tests.sh --update
	@./tests/8.0/test_inspect_blob.sh --update
	@./tests/8.4/test_inspect_blob.sh --update
	@./tests/9.0/test_inspect_blob.sh --update

# Corruption/error-path tests (corrupted fixtures must produce clean errors,
# not crashes)
test-corruption: $(TARGET)
	@./tests/corruption/test_corruption.sh

# AddressSanitizer/UBSan build: rebuilds everything with sanitizers enabled.
# `make test-asan` runs the full test suite under the sanitizers.
# (Run `make clean && make` afterwards to get a normal binary back.)
ASAN_FLAGS = -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer
asan:
	@$(MAKE) clean
	@$(MAKE) all EXTRA_CXXFLAGS="$(ASAN_FLAGS)" EXTRA_LDFLAGS="$(ASAN_FLAGS)"

test-asan:
	@$(MAKE) clean
	@$(MAKE) test EXTRA_CXXFLAGS="$(ASAN_FLAGS)" EXTRA_LDFLAGS="$(ASAN_FLAGS)"

# Version-specific test targets
test-8.0: $(TARGET)
	@./tests/8.0/run_tests.sh

test-8.4: $(TARGET)
	@./tests/8.4/run_tests.sh

test-9.0: $(TARGET)
	@./tests/9.0/run_tests.sh

test-update-8.0: $(TARGET)
	@./tests/8.0/run_tests.sh --update

test-update-8.4: $(TARGET)
	@./tests/8.4/run_tests.sh --update

test-update-9.0: $(TARGET)
	@./tests/9.0/run_tests.sh --update

# Fixture generation
test-fixtures: test-fixtures-8.0

test-fixtures-8.0:
	@./tests/8.0/generate_fixtures.sh

test-fixtures-8.4:
	@./tests/8.4/generate_fixtures.sh

test-fixtures-9.0:
	@./tests/9.0/generate_fixtures.sh

test-upgrade-fixture: test-upgrade-fixture-8.0

test-upgrade-fixture-8.0:
	@./tests/8.0/generate_upgrade_fixture.sh

test-upgrade-fixture-8.4:
	@./tests/8.4/generate_upgrade_fixture.sh

test-all-fixtures: test-fixtures-8.0 test-upgrade-fixture-8.0

# JSON partial update fixture (requires special generation to preserve LOB version chains)
test-json-partial-fixture: test-json-partial-fixture-8.0

test-json-partial-fixture-8.0:
	@./tests/8.0/generate_json_partial_fixture.sh

test-json-partial-fixture-8.4:
	@./tests/8.4/generate_json_partial_fixture.sh

test-json-partial-fixture-9.0:
	@./tests/9.0/generate_json_partial_fixture.sh

# JSON partial large fixture (multi-entry LOB with version chains on different entries)
test-json-partial-large-fixture: test-json-partial-large-fixture-8.0

test-json-partial-large-fixture-8.0:
	@./tests/8.0/generate_json_partial_large_fixture.sh

test-json-partial-large-fixture-8.4:
	@./tests/8.4/generate_json_partial_large_fixture.sh

test-json-partial-large-fixture-9.0:
	@./tests/9.0/generate_json_partial_large_fixture.sh

# JSON shrink fixture (shrinking partial update; old LOB version fetch)
test-json-shrink-fixture: test-json-shrink-fixture-8.0

test-json-shrink-fixture-8.0:
	@./tests/8.0/generate_json_shrink_fixture.sh

# Collation/prefix fixture (utf8mb4_0900_bin CHAR + CHAR-prefix index)
test-collation-prefix-fixture: test-collation-prefix-fixture-8.0

test-collation-prefix-fixture-8.0:
	@./tests/8.0/generate_collation_prefix_fixture.sh

# JSON partial purged fixture (purged version chains for purge detection tests)
test-json-partial-purged-fixture: test-json-partial-purged-fixture-8.0

test-json-partial-purged-fixture-8.0:
	@./tests/8.0/generate_json_partial_purged_fixture.sh

test-json-partial-purged-fixture-8.4:
	@./tests/8.4/generate_json_partial_purged_fixture.sh

test-json-partial-purged-fixture-9.0:
	@./tests/9.0/generate_json_partial_purged_fixture.sh

# Inspect-blob test targets
test-inspect-blob: $(TARGET)
	@./tests/8.0/test_inspect_blob.sh
	@./tests/8.4/test_inspect_blob.sh
	@./tests/9.0/test_inspect_blob.sh

test-inspect-blob-verbose: $(TARGET)
	@./tests/8.0/test_inspect_blob.sh --verbose
	@./tests/8.4/test_inspect_blob.sh --verbose
	@./tests/9.0/test_inspect_blob.sh --verbose

test-inspect-blob-update: $(TARGET)
	@./tests/8.0/test_inspect_blob.sh --update
	@./tests/8.4/test_inspect_blob.sh --update
	@./tests/9.0/test_inspect_blob.sh --update

# Version-specific inspect-blob targets
test-inspect-blob-8.0: $(TARGET)
	@./tests/8.0/test_inspect_blob.sh

test-inspect-blob-8.4: $(TARGET)
	@./tests/8.4/test_inspect_blob.sh

test-inspect-blob-9.0: $(TARGET)
	@./tests/9.0/test_inspect_blob.sh

test-inspect-blob-update-8.0: $(TARGET)
	@./tests/8.0/test_inspect_blob.sh --update

test-inspect-blob-update-8.4: $(TARGET)
	@./tests/8.4/test_inspect_blob.sh --update

test-inspect-blob-update-9.0: $(TARGET)
	@./tests/9.0/test_inspect_blob.sh --update

# Phony targets
.PHONY: all clean test test-verbose test-update \
	test-corruption asan test-asan \
	test-json-shrink-fixture test-json-shrink-fixture-8.0 \
	test-collation-prefix-fixture test-collation-prefix-fixture-8.0 \
	test-8.0 test-8.4 test-9.0 test-update-8.0 test-update-8.4 test-update-9.0 \
	test-fixtures test-fixtures-8.0 test-fixtures-8.4 test-fixtures-9.0 \
	test-upgrade-fixture test-upgrade-fixture-8.0 test-upgrade-fixture-8.4 \
	test-all-fixtures \
	test-json-partial-fixture test-json-partial-fixture-8.0 \
	test-json-partial-fixture-8.4 test-json-partial-fixture-9.0 \
	test-json-partial-large-fixture test-json-partial-large-fixture-8.0 \
	test-json-partial-large-fixture-8.4 test-json-partial-large-fixture-9.0 \
	test-json-partial-purged-fixture test-json-partial-purged-fixture-8.0 \
	test-json-partial-purged-fixture-8.4 test-json-partial-purged-fixture-9.0 \
	test-inspect-blob test-inspect-blob-verbose test-inspect-blob-update \
	test-inspect-blob-8.0 test-inspect-blob-8.4 test-inspect-blob-9.0 \
	test-inspect-blob-update-8.0 test-inspect-blob-update-8.4 test-inspect-blob-update-9.0
