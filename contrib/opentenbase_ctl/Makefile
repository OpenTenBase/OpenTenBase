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
# 把 pg_config 报告的 libdir 也加进来：OpenTenBase 从源码装到自定义前缀时，
# 不加这里链接期找不到 -lpq，运行期还会因为找不到 libpq.so.5 而启动失败。
PG_LIBDIR := $(shell pg_config --libdir 2>/dev/null)
LDFLAGS = -L/usr/local/lib -L/usr/local/lib64
ifneq ($(PG_LIBDIR),)
LDFLAGS += -L$(PG_LIBDIR) -Wl,-rpath,$(PG_LIBDIR)
endif

# 链接库（libpqxx 用系统库还是就地编译，由下面的 PQXX_* 变量决定）
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

# ---------------------------------------------------------------------------
# libpqxx
#
# 优先用系统里已经装好的 libpqxx；没有的话，就用 lib/ 下随仓库携带的源码包
# 就地编到 $(BUILD_DIR)/pqxx —— 不需要 root，也不会写到 /usr/local。
#
# 旧实现在 check-deps 里直接 `sudo yum install` + `sudo make install`，问题有二：
#   1) 只认 yum/rpm，Debian/Ubuntu 上第一句就挂（issue #183 就是这类报错）；
#   2) 非交互环境（CI、容器）里 sudo 要么失败要么卡住。
# 同时旧实现没有 rpath，导致二进制装好后运行期报
#   "libpqxx.so: cannot open shared object file"。
# ---------------------------------------------------------------------------
PQXX_VERSION = 7.7.5
PQXX_TARBALL = lib/libpqxx-$(PQXX_VERSION).tar.gz
PQXX_PREFIX  = $(abspath $(BUILD_DIR)/pqxx)
# 用 stamp 文件而不是具体库文件做目标：libpqxx 7.x 的 cmake 默认产出静态库，
# 目标写成 libpqxx.so 会导致每次都判定"没编好"而重复构建。
PQXX_STAMP   = $(PQXX_PREFIX)/.installed

# 在常见库目录里找系统 libpqxx（注意：make 的 wildcard 会按逗号拆参数，
# 这里的目录列表只能用空格分隔，不能出现逗号）
PQXX_SYSTEM_DIRS = /usr/local/lib64 /usr/local/lib /usr/lib64 /usr/lib/x86_64-linux-gnu /usr/lib
PQXX_SYSTEM_DIR := $(firstword $(foreach d,$(PQXX_SYSTEM_DIRS),$(if $(wildcard $d/libpqxx.so*),$d)))

ifeq ($(strip $(PQXX_SYSTEM_DIR)),)
PQXX_CPPFLAGS = -I$(PQXX_PREFIX)/include
PQXX_LDFLAGS  = -L$(PQXX_PREFIX)/lib
PQXX_LIB_DIR  = $(PQXX_PREFIX)/lib
PQXX_DEP      = $(PQXX_STAMP)
else
PQXX_CPPFLAGS =
PQXX_LDFLAGS  = -L$(PQXX_SYSTEM_DIR) -Wl,-rpath,$(PQXX_SYSTEM_DIR)
PQXX_LIB_DIR  = $(PQXX_SYSTEM_DIR)
PQXX_DEP      =
endif

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

# 检查依赖：只检查 + 给出可执行提示，不再 sudo 安装，也不再绑定 yum/rpm。
# 缺依赖时直接说清楚该装什么，而不是让用户对着 "No such file or directory" 猜。
check-deps:
	@echo "Checking dependencies..."
	@missing=""; \
	have() { ls $$1 >/dev/null 2>&1; }; \
	command -v $(CXX) >/dev/null 2>&1 || missing="$$missing g++"; \
	if ! have /usr/include/ssh2.h && ! have /usr/include/libssh2.h && ! have /usr/local/include/ssh2.h; then \
		missing="$$missing libssh2-dev"; \
	fi; \
	if ! have /usr/include/postgresql/libpq-fe.h && ! have /usr/local/include/libpq-fe.h; then \
		pginc=$$(pg_config --includedir 2>/dev/null); \
		if [ -z "$$pginc" ] || [ ! -f "$$pginc/libpq-fe.h" ]; then missing="$$missing libpq-dev"; fi; \
	fi; \
	if ! have /usr/include/CLI/CLI.hpp && ! have /usr/local/include/CLI/CLI.hpp; then \
		missing="$$missing CLI11"; \
	fi; \
	if [ -n "$$missing" ]; then \
		echo ""; \
		echo "ERROR: missing build prerequisites:$$missing"; \
		echo "  Debian/Ubuntu: sudo apt-get install -y g++ libssh2-1-dev libssl-dev libpq-dev cmake libcli11-dev"; \
		echo "  RHEL/CentOS  : sudo dnf install -y gcc-c++ libssh2-devel openssl-devel postgresql-devel cmake"; \
		echo "  CLI11 is header-only; if your distro has no package, fetch the single header:"; \
		echo "    sudo mkdir -p /usr/local/include/CLI"; \
		echo "    sudo curl -sSL https://github.com/CLIUtils/CLI11/releases/download/v1.9.1/CLI11.hpp \\"; \
		echo "         -o /usr/local/include/CLI/CLI.hpp"; \
		exit 1; \
	fi

# 就地编译随仓库携带的 libpqxx（不需要 root，也不写 /usr/local）
$(PQXX_STAMP):
	@echo "libpqxx: no system library found, building bundled $(PQXX_VERSION) into $(PQXX_PREFIX)"
	@if [ ! -f "$(PQXX_TARBALL)" ]; then \
		echo "ERROR: $(PQXX_TARBALL) is missing."; \
		echo "       Install libpqxx (libpqxx-dev / libpqxx-devel), or restore the bundled tarball."; \
		exit 1; \
	fi
	@if ! command -v cmake >/dev/null 2>&1; then \
		echo "ERROR: cmake is required to build the bundled libpqxx."; \
		echo "  Debian/Ubuntu: sudo apt-get install -y cmake"; \
		echo "  RHEL/CentOS  : sudo dnf install -y cmake"; \
		exit 1; \
	fi
	@if ! command -v pg_config >/dev/null 2>&1; then \
		echo "ERROR: pg_config not found in PATH; libpqxx needs the PostgreSQL client headers."; \
		echo "       Add \$$OPENTENBASE_HOME/bin to PATH (it ships pg_config), or install libpq-dev / postgresql-devel."; \
		exit 1; \
	fi
	@rm -rf $(BUILD_DIR)/libpqxx-src
	@mkdir -p $(BUILD_DIR)/libpqxx-src
	@tar xf $(PQXX_TARBALL) -C $(BUILD_DIR)/libpqxx-src --strip-components=1
	@mkdir -p $(BUILD_DIR)/libpqxx-src/_build && cd $(BUILD_DIR)/libpqxx-src/_build && \
		cmake .. \
			-DCMAKE_INSTALL_PREFIX=$(PQXX_PREFIX) \
			-DCMAKE_BUILD_TYPE=Release \
			-DCMAKE_POSITION_INDEPENDENT_CODE=ON \
			-DSKIP_BUILD_EXAMPLES=ON \
			-DBUILD_TESTING=OFF \
			-DPostgreSQL_LIBRARY="$$(pg_config --libdir)/libpq.so" \
			-DPostgreSQL_INCLUDE_DIR="$$(pg_config --includedir)" >/dev/null && \
		$(MAKE) -j$(shell nproc 2>/dev/null || echo 2) >/dev/null && $(MAKE) install >/dev/null
	@touch $(PQXX_STAMP)
	@echo "libpqxx: installed into $(PQXX_PREFIX)"

# 主程序
$(OBJS): $(PQXX_DEP)

opentenbase_ctl: $(OBJS)
	$(CXX) -o $@ $(OBJS) $(LDFLAGS) $(PQXX_LDFLAGS) $(LIBS)

# 编译规则
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(PQXX_CPPFLAGS) $(INCLUDES) -c $< -o $@

# 测试目标
test: $(TEST_TARGET)

$(TEST_TARGET): $(PQXX_DEP) $(TEST_OBJS) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(TEST_OBJS) $(OBJS) $(LDFLAGS) $(PQXX_LDFLAGS) -L$(GTEST_LIB) $(GTEST_FLAGS) $(LIBS)

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
	-@cp $(PQXX_LIB_DIR)/libpqxx* $(LIB_DIR)/ 2>/dev/null || true

	@echo "Package created in $(BUILD_DIR)"
	@tar -czvf opentenbase_ctl.tar.gz $(BUILD_DIR)

installcheck:
	$(prove_installcheck)

