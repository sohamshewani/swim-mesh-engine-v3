CXX = g++
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra -MMD -MP
LDFLAGS = -pthread

TARGET = swim-mesh-engine
TEST_TARGET = test_swim
BENCH_TARGET = bench_swim

SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = benchmarks

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))
DEPS = $(OBJS:.o=.d)

TEST_SRCS = $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS = $(patsubst $(TEST_DIR)/%.cpp, $(BUILD_DIR)/tests/%.o, $(TEST_SRCS))

BENCH_SRCS = $(wildcard $(BENCH_DIR)/*.cpp)
BENCH_OBJS = $(patsubst $(BENCH_DIR)/%.cpp, $(BUILD_DIR)/benchmarks/%.o, $(BENCH_SRCS))

INCLUDES = -I$(INC_DIR)

.PHONY: all build test benchmark clean

all: build

build: $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/tests/%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/benchmarks/%.o: $(BENCH_DIR)/%.cpp | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

test: $(BUILD_DIR)/$(TEST_TARGET)
	@if [ -x $< ]; then ./$<; else echo "No tests to run."; fi

$(BUILD_DIR)/$(TEST_TARGET): $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) $(TEST_OBJS) | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS)

benchmark: $(BUILD_DIR)/$(BENCH_TARGET)
	@if [ -x $< ]; then ./$<; else echo "No benchmarks to run."; fi

$(BUILD_DIR)/$(BENCH_TARGET): $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) $(BENCH_OBJS) | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)

-include $(DEPS)