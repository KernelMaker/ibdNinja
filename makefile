# Compiler and flags
CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17 -g -O2 -Irapidjson/include -Izlib/zlib-1.2.13/ibdNinja/include

LDFLAGS = -Lzlib/zlib-1.2.13/ibdNinja/lib -lz -Wl,-rpath,zlib/zlib-1.2.13/ibdNinja/lib

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

# Test targets - run both versions
test: $(TARGET)
	@./tests/run_tests.sh

test-verbose: $(TARGET)
	@./tests/run_tests.sh --verbose

test-update: $(TARGET)
	@./tests/run_tests.sh --update

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
