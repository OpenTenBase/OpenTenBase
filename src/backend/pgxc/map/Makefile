#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for distributed executor's shared memory queue
#
#  Copyright (c) 2019-2024 TeleDB Development Group
#  
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/backend/pgxc/map
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = map.o

include $(top_srcdir)/src/backend/common.mk

CC = g++
CFLAGS = -c -std=c++11 -Wall
LDFLAGS = -lstdc++ -lstl
INCLUDES = -I../../../include

all: map.o

map.o: map.cpp
	$(CC) $(CFLAGS) $(INCLUDES) -frtti $< -o $@

clean:
	rm -f map.o
