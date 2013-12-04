
MAKEFLAGS = $(AM_MAKEFLAGS)

xlator_LTLIBRARIES := ida.la
xlatordir = $(gflibdir)/xlator/cluster

ida_la_LDFLAGS := -module -avoid-version

ida_la_SOURCES := ida.c
ida_la_SOURCES += ida-common.c
ida_la_SOURCES += ida-manager.c
ida_la_SOURCES += ida-flow.c
ida_la_SOURCES += ida-gf.c
ida_la_SOURCES += ida-rabin.c
ida_la_SOURCES += ida-type-iatt.c
ida_la_SOURCES += ida-type-inode.c
ida_la_SOURCES += ida-type-fd.c
ida_la_SOURCES += ida-type-loc.c
ida_la_SOURCES += ida-type-dict.c
ida_la_SOURCES += ida-type-str.c
ida_la_SOURCES += ida-type-md5.c
ida_la_SOURCES += ida-type-statvfs.c
ida_la_SOURCES += ida-type-buffer.c
ida_la_SOURCES += ida-type-lock.c
ida_la_SOURCES += ida-type-dirent.c
ida_la_SOURCES += ida-fop-lookup.c
ida_la_SOURCES += ida-fop-stat.c
ida_la_SOURCES += ida-fop-truncate.c
ida_la_SOURCES += ida-fop-access.c
ida_la_SOURCES += ida-fop-readlink.c
ida_la_SOURCES += ida-fop-mknod.c
ida_la_SOURCES += ida-fop-mkdir.c
ida_la_SOURCES += ida-fop-unlink.c
ida_la_SOURCES += ida-fop-rmdir.c
ida_la_SOURCES += ida-fop-symlink.c
ida_la_SOURCES += ida-fop-rename.c
ida_la_SOURCES += ida-fop-link.c
ida_la_SOURCES += ida-fop-create.c
ida_la_SOURCES += ida-fop-open.c
ida_la_SOURCES += ida-fop-readv.c
ida_la_SOURCES += ida-fop-writev.c
ida_la_SOURCES += ida-fop-flush.c
ida_la_SOURCES += ida-fop-fsync.c
ida_la_SOURCES += ida-fop-opendir.c
ida_la_SOURCES += ida-fop-readdir.c
ida_la_SOURCES += ida-fop-statfs.c
ida_la_SOURCES += ida-fop-xattr.c
ida_la_SOURCES += ida-fop-lk.c
ida_la_SOURCES += ida-fop-rchecksum.c
ida_la_SOURCES += ida-heal.c

ida_la_LIBADD = $(gfdir)/libglusterfs/src/libglusterfs.la $(gfsys)/src/libgfsys.la

