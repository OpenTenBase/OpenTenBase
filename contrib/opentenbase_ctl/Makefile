PGFILEDESC = "opentenbase_ctl - Operation and maintenance tool for opentenbase instance"
PGAPPICON = win32
PROGRAM = opentenbase_ctl

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/opentenbase_ctl
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# 编译器设置
CXX = g++
CXXFLAGS = -Wall -g -std=c++17 -I/usr/local/include

# 第三方库路径
PROTOBUF_INCLUDE = /usr/local/include
GRPC_INCLUDE = /usr/local/include
CLI11_INCLUDE = /usr/local/include
INDICATORS_INCLUDE = /usr/local/include

# 库搜索路径
LDFLAGS = -L/usr/local/lib -L/usr/local/lib64

# 链接库
LIBS = -lpthread -lssh2 -lssl -lpqxx -lpq

# 项目目录结构
SRC_DIR = src
SSH_DIR = $(SRC_DIR)/ssh
CONFIG_DIR = $(SRC_DIR)/config
LOG_DIR = $(SRC_DIR)/log
NODE_DIR = $(SRC_DIR)/node
CLUSTER_DIR = $(SRC_DIR)/cluster
UTILS_DIR = $(SRC_DIR)/utils
COMMAND_DIR = $(SRC_DIR)/command
TYPES_DIR = $(SRC_DIR)/types

# 构建目录
BUILD_DIR = build
BIN_DIR = $(BUILD_DIR)/bin
LIB_DIR = $(BUILD_DIR)/lib

# 源文件
MAIN_SRC = $(SRC_DIR)/main.cpp
SSH_SRCS = $(SSH_DIR)/remote_ssh.cpp
CONFIG_SRCS = $(CONFIG_DIR)/config.cpp
LOG_SRCS = $(LOG_DIR)/log.cpp
NODE_SRCS = $(NODE_DIR)/node.cpp
CLUSTER_SRCS = $(CLUSTER_DIR)/cluster.cpp
UTILS_SRCS = $(UTILS_DIR)/utils.cpp
COMMAND_SRCS = $(COMMAND_DIR)/command.cpp
TYPES_SRCS = $(TYPES_DIR)/types.cpp

# 目标文件
MAIN_OBJ = $(MAIN_SRC:.cpp=.o)
SSH_OBJS = $(SSH_SRCS:.cpp=.o)
CONFIG_OBJS = $(CONFIG_SRCS:.cpp=.o)
LOG_OBJS = $(LOG_SRCS:.cpp=.o)
NODE_OBJS = $(NODE_SRCS:.cpp=.o)
CLUSTER_OBJS = $(CLUSTER_SRCS:.cpp=.o)
UTILS_OBJS = $(UTILS_SRCS:.cpp=.o)
COMMAND_OBJS = $(COMMAND_SRCS:.cpp=.o)
TYPES_OBJS = $(TYPES_SRCS:.cpp=.o)

# 所有目标文件
OBJS = $(MAIN_OBJ) $(SSH_OBJS) $(CONFIG_OBJS) $(LOG_OBJS) \
       $(NODE_OBJS) $(CLUSTER_OBJS) $(UTILS_OBJS) $(COMMAND_OBJS) \
       $(TYPES_OBJS)

# 包含目录
INCLUDES = -I$(SRC_DIR) \
           -I$(PROTOBUF_INCLUDE) \
           -I$(GRPC_INCLUDE) \
           -I$(CLI11_INCLUDE) \
           -I$(INDICATORS_INCLUDE)

# 测试相关变量
TEST_DIR = test
TEST_SRCS = $(TEST_DIR)/cluster_test.cpp
TEST_OBJS = $(TEST_SRCS:.cpp=.o)
TEST_TARGET = opentenbase_ctl_test

# 添加 Google Test 相关配置
GTEST_DIR = /usr/local/include
GTEST_LIB = /usr/local/lib
GTEST_FLAGS = -lgtest -lgtest_main -pthread

# 默认目标
.PHONY: all clean test run-test clean-test package check-deps install-deps

all: check-deps opentenbase_ctl

# 检查依赖
check-deps:
	@echo "Checking dependencies..."
	@if ! command -v cmake &> /dev/null; then \
		echo "cmake not found, installing..."; \
		sudo yum install -y cmake; \
	fi
	@if ! rpm -q postgresql-devel &> /dev/null; then \
		echo "postgresql-devel not found, installing..."; \
		sudo yum install -y postgresql-devel; \
	fi
	@if ! rpm -q libssh2-devel &> /dev/null; then \
		echo "libssh2-devel not found, installing..."; \
		sudo yum install -y libssh2-devel; \
	fi
	@if ! ls /usr/local/lib/libpqxx* >/dev/null 2>&1 && ! ls /usr/local/lib64/libpqxx* >/dev/null 2>&1; then \
		echo "libpqxx not found, installing from source..."; \
		if [ ! -f "lib/libpqxx-7.7.5.tar.gz" ]; then \
			echo "Error: libpqxx source package not found in lib/ directory"; \
			exit 1; \
		fi; \
                tar xf lib/libpqxx-7.7.5.tar.gz -C lib && \
		cd lib/libpqxx-7.7.5; \
		mkdir -p build && cd build; \
		cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local; \
		make -j$(nproc); \
		sudo make install; \
		cd ../../..; \
	fi

# 主程序
opentenbase_ctl: $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS) $(LIBS)

# 编译规则
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# 测试目标
test: $(TEST_TARGET)

$(TEST_TARGET): $(TEST_OBJS) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -L$(GTEST_LIB) $(GTEST_FLAGS) $(LIBS)

$(TEST_DIR)/%.o: $(TEST_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -I$(GTEST_DIR) -c $< -o $@

# 运行测试
run-test: test
	./$(TEST_TARGET)

# 清理测试文件
clean-test:
	rm -f $(TEST_OBJS) $(TEST_TARGET)

# 更新清理目标
clean: clean-test
	rm -f $(OBJS) $(TARGET)
	rm -rf $(BUILD_DIR)
	rm -rf lib/libpqxx-7.7.5

# 打包目标
package: opentenbase_ctl
	@echo "Creating build directories..."
	@mkdir -p $(BIN_DIR) $(LIB_DIR)
	@echo "Copying binary to $(BIN_DIR)..."
	@cp opentenbase_ctl $(BIN_DIR)/
	@echo "Copying dependencies to $(LIB_DIR)..."
	@cp /usr/lib64/libssh2.so* $(LIB_DIR)/ 
	@cp /usr/local/lib64/libcpprest.so* $(LIB_DIR)/ 
	@cp /usr/lib64/libssl.so* $(LIB_DIR)/ 
	@cp /usr/lib64/libcrypto.so* $(LIB_DIR)/ 
	@cp /usr/local/lib/libgrpc* $(LIB_DIR)/ 
	@cp /usr/local/lib64/libabsl* $(LIB_DIR)/ 
	@cp /usr/lib64/libprotobuf* $(LIB_DIR)/ 
	@cp /usr/local/lib64/libprotoc* $(LIB_DIR)/ 
	@cp /usr/local/lib/libupb* $(LIB_DIR)/ 
	@cp /usr/local/lib/libgpr* $(LIB_DIR)/ 
	@cp /usr/local/lib/libaddress_sorting* $(LIB_DIR)/
	@cp /usr/local/lib/libre2* $(LIB_DIR)/
	@cp /usr/lib64/libboost* $(LIB_DIR)/
	@cp /usr/lib64/libicu* $(LIB_DIR)/
	@cp /usr/lib64/libgcc_s*  $(LIB_DIR)/
	@cp /usr/lib64/libstdc*  $(LIB_DIR)/
	@cp /usr/local/lib64/libpqxx*  $(LIB_DIR)/

	@echo "Package created in $(BUILD_DIR)"
	@tar -czvf opentenbase_ctl.tar.gz $(BUILD_DIR)

installcheck:
	$(prove_installcheck)

