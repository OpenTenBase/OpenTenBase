PROGRAM = otb_aiops
CXX ?= g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
bindir ?= $(HOME)/.local/bin

TOP_DIR = ../..
LIBPQ_INC ?= $(TOP_DIR)/src/interfaces/libpq
LIBPQ_LIB ?= $(TOP_DIR)/src/interfaces/libpq

CPPFLAGS += -Isrc -I$(LIBPQ_INC) -I$(TOP_DIR)/src/include
LDFLAGS  += -L$(LIBPQ_LIB) -Wl,-rpath,$(abspath $(LIBPQ_LIB))
LIBS     += -lpq -lcurl

OBJS = src/main.o \
       src/db/pg_client.o \
       src/config/user_config.o \
       src/collector/collector.o \
       src/collector/storage_collector.o \
       src/ai/prompt_builder.o \
       src/ai/llm_client.o

.PHONY: all clean install test

all: $(PROGRAM)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@

$(PROGRAM): $(OBJS)
	$(CXX) $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LIBS) -o $@

test_otb_aiops: tests/test_otb_aiops.cpp src/db/pg_client.cpp src/ai/llm_client.cpp src/ai/prompt_builder.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $^ -Wl,--gc-sections -o $@

test_user_config: tests/test_user_config.cpp src/config/user_config.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $^ -o $@

test: test_otb_aiops test_user_config
	./test_otb_aiops
	./test_user_config

install: all
	mkdir -p "$(DESTDIR)$(bindir)"
	install -m 755 $(PROGRAM) "$(DESTDIR)$(bindir)/$(PROGRAM)"

clean:
	rm -f $(PROGRAM) test_otb_aiops test_user_config $(OBJS)
