# contrib/jdbc_fdw/Makefile

# these are used by the included (via contrib-global.mk) pgxs.mk
MODULE_big = jdbc_fdw
EXTENSION = jdbc_fdw
DATA = jdbc_fdw--1.0.sql jdbc_fdw--1.0--1.1.sql
DATA_built = jdbc_fdw.jar
REGRESS = postgresql/jdbc_fdw postgresql/int4 postgresql/int8 postgresql/float4 postgresql/float8 postgresql/select postgresql/insert postgresql/update postgresql/aggregates

OBJS = jdbc_fdw.o option.o deparse.o connection.o jq.o

# these are defined in pgxs.mk
PG_CPPFLAGS = -D'INSTALL_DIR=$(datadir)/$(datamoduledir)' -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

LIBDIR=/usr/lib64/

SHLIB_LINK += -L$(LIBDIR) -ljvm -L.

UNAME = $(shell uname)

jdbc_fdw.jar:
	cd java && javac *.java && cd ..
	cd java && jar cvf ../jdbc_fdw.jar *.class && cd ..

all:$(jdbc_fdw.jar)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/jdbc_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifdef REGRESS_PREFIX
REGRESS_PREFIX_SUB = $(REGRESS_PREFIX)
else
REGRESS_PREFIX_SUB = $(VERSION)
endif

REGRESS := $(addprefix $(REGRESS_PREFIX_SUB)/,$(REGRESS))
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/griddb)
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/mysql)
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/postgresql)
