#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for pool
#
#  Portions Copyright (c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/backend/pgxc/pool
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = pgxcnode.o poolmgr.o poolcomm.o poolutils.o forwarder.o remoteDataAccess.o execRemote.o rda_frame_impl.o

include $(top_srcdir)/src/backend/common.mk
