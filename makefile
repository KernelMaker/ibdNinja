# Compiler and flags
CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17 -g -O2 -Irapidjson/include -Izlib/zlib-1.2.13/ibdNinja/include

LDFLAGS = -Lzlib/zlib-1.2.13/ibdNinja/lib -lz -Wl,-rpath,zlib/zlib-1.2.13/ibdNinja/lib

# Target
TARGET = ibdNinja

# Source files, object files, and target
SRCS = main.cc ibdNinja.cc ibdUtils.cc Properties.cc Column.cc Index.cc Table.cc Record.cc
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

# Phony targets
.PHONY: all clean test test-verbose test-update \
	test-8.0 test-8.4 test-9.0 test-update-8.0 test-update-8.4 test-update-9.0 \
	test-fixtures test-fixtures-8.0 test-fixtures-8.4 test-fixtures-9.0 \
	test-upgrade-fixture test-upgrade-fixture-8.0 test-upgrade-fixture-8.4 \
	test-all-fixtures
