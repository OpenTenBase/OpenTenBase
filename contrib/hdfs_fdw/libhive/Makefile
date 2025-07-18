# Makefile for libhive with JNI support

# Detect Java installation
JAVA_HOME ?= $(shell readlink -f /usr/bin/java | sed "s:bin/java::")
JNI_INCLUDES = -I$(JAVA_HOME)include -I$(JAVA_HOME)include/linux

# PostgreSQL includes (when building within OpenTenBase source tree)
PG_INCLUDES = -I../../../src/include -I../../../src/include/port/linux_x86_64 -I../../../src/interfaces/libpq

# Check if Java is installed
ifeq ($(wildcard $(JAVA_HOME)include/jni.h),)
    $(error Java development headers not found. Please install openjdk-*-jdk package or set JAVA_HOME)
endif

# Build both stub and JNI versions
STUB_SOURCES = libhive_stub.c
STUB_OBJECTS = $(STUB_SOURCES:.c=.o)

JNI_SOURCES = jdbc/data.cpp jdbc/hiveclient.cpp
JNI_OBJECTS = $(JNI_SOURCES:.cpp=.o)

CC = gcc
CXX = g++
CFLAGS = -fPIC -Wall -g
CXXFLAGS = -fPIC -Wall -g $(JNI_INCLUDES) $(PG_INCLUDES)
LDFLAGS = -shared

TARGET = libhive.so

.PHONY: all clean stub jni

all: $(TARGET)

# Default to stub version for now, can be changed to jni when ready
$(TARGET): stub

stub: $(STUB_OBJECTS)
	$(CC) $(LDFLAGS) -o $(TARGET) $^

jni: $(JNI_OBJECTS)
	$(CXX) $(LDFLAGS) -o $(TARGET) $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(STUB_OBJECTS) $(JNI_OBJECTS) $(TARGET)

install:
	cp -f $(TARGET) $(INSTALL_DIR)

