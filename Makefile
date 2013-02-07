
# Copyright (c) 2012-2013 DataLab, S.L. <http://www.datalab.es>
#
# This file is part of the cluster/ida translator for GlusterFS.
#
# The cluster/ida translator for GlusterFS is free software: you can
# redistribute it and/or modify it under the terms of the GNU General
# Public License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# The cluster/ida translator for GlusterFS is distributed in the hope
# that it will be useful, but WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
# PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with the cluster/ida translator for GlusterFS. If not, see
# <http://www.gnu.org/licenses/>.

SRCDIR = ../../gluster

XLATOR = ida.so
TYPE = cluster

OBJS := ida.o
OBJS += ida-common.o
OBJS += ida-manager.o
OBJS += ida-flow.o
OBJS += ida-gf.o
OBJS += ida-rabin.o
OBJS += ida-type-iatt.o
OBJS += ida-type-inode.o
OBJS += ida-type-fd.o
OBJS += ida-type-loc.o
OBJS += ida-type-dict.o
OBJS += ida-type-str.o
OBJS += ida-type-md5.o
OBJS += ida-type-statvfs.o
OBJS += ida-type-buffer.o
OBJS += ida-type-lock.o
OBJS += ida-type-dirent.o
OBJS += ida-fop-lookup.o
OBJS += ida-fop-stat.o
OBJS += ida-fop-truncate.o
OBJS += ida-fop-access.o
OBJS += ida-fop-readlink.o
OBJS += ida-fop-mknod.o
OBJS += ida-fop-mkdir.o
OBJS += ida-fop-unlink.o
OBJS += ida-fop-rmdir.o
OBJS += ida-fop-symlink.o
OBJS += ida-fop-rename.o
OBJS += ida-fop-link.o
OBJS += ida-fop-create.o
OBJS += ida-fop-open.o
OBJS += ida-fop-readv.o
OBJS += ida-fop-writev.o
OBJS += ida-fop-flush.o
OBJS += ida-fop-fsync.o
OBJS += ida-fop-opendir.o
OBJS += ida-fop-readdir.o
OBJS += ida-fop-statfs.o
OBJS += ida-fop-xattr.o
OBJS += ida-fop-lk.o
OBJS += ida-fop-rchecksum.o
OBJS += ida-heal.o

#------------------------------------------------------------------------------

PREFIX := $(shell sed -n "s/^prefix *= *\(.*\)/\1/p" $(SRCDIR)/Makefile)
GF_HOST_OS := $(shell sed -n "s/^GF_HOST_OS *= *\(.*\)/\1/p" $(SRCDIR)/Makefile)
VERSION := $(shell sed -n "s/^VERSION *= *\(.*\)/\1/p" $(SRCDIR)/Makefile)

LIBDIR = $(PREFIX)/lib

V ?= 0

VCC = $(VCC_$(V))
VCC_0 = @echo "  CC " $@;

VLD = $(VLD_$(V))
VLD_0 = @echo "  LD " $@;

VCP = $(VCP_$(V))
VCP_0 = @echo "  CP " $^;

VMV = @

VRM = $(VRM_$(V))
VRM_0 = @echo "  CLEAN";

VLN = $(VLN_$(V))
VLN_0 = @echo "  LN " $^;

VMKDIR = @

CC = gcc
LD = gcc
MV = mv
CP = cp
RM = rm
LN = ln
MKDIR = mkdir

INCS := -I$(SRCDIR)
INCS += -I$(SRCDIR)/libglusterfs/src
INCS += -I$(SRCDIR)/contrib/uuid

LIBS := -lglusterfs

DEFS := -DHAVE_CONFIG_H
DEFS += -D_FILE_OFFSET_BITS=64
DEFS += -D_GNU_SOURCE
DEFS += -D$(GF_HOST_OS)

CFLAGS = -fPIC -Wall -O0 -g
LDFLAGS = -fPIC -g -shared -nostartfiles -L$(LIBDIR)

DEPDIR = .deps
TGTDIR = $(LIBDIR)/glusterfs/$(VERSION)/xlator/$(TYPE)

all:	$(XLATOR)

-include $(DEPDIR)/*.Po

$(XLATOR):	$(OBJS)
		$(VLD)$(LD) $(LDFLAGS) $^ $(LIBS) -o $@

install:	$(XLATOR)
		$(VCP)$(CP) $^ $(TGTDIR)/$^.0.0.0
		$(VLN)$(LN) -sf $^.0.0.0 $(TGTDIR)/$^.0
		$(VLN)$(LN) -sf $^.0.0.0 $(TGTDIR)/$^
		$(VLN)$(LN) -sf $^ $(TGTDIR)/disperse.so

clean:
		$(VRM)$(RM) -f *.o $(XLATOR)

.c.o:
		$(VMKDIR)$(MKDIR) -p $(DEPDIR)
		$(VCC)$(CC) $(CFLAGS) -MT $@ -MD -MP -MF $(DEPDIR)/$*.Tpo -c $(DEFS) $(INCS) -o $@ $<
		$(VMV)$(MV) -f $(DEPDIR)/$*.Tpo $(DEPDIR)/$*.Po

