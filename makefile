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

# Test targets
test: $(TARGET)
	@./tests/run_tests.sh

test-verbose: $(TARGET)
	@./tests/run_tests.sh --verbose

test-update: $(TARGET)
	@./tests/run_tests.sh --update

test-fixtures:
	@./tests/generate_fixtures.sh

test-upgrade-fixture:
	@./tests/generate_upgrade_fixture.sh

test-all-fixtures: test-fixtures test-upgrade-fixture

# Phony targets
.PHONY: all clean test test-verbose test-update test-fixtures test-upgrade-fixture test-all-fixtures
