
MAKEFLAGS = $(AM_MAKEFLAGS)

xlator_LTLIBRARIES := ida.la
xlatordir = $(gflibdir)/xlator/cluster

ida_la_LDFLAGS := -module -avoid-version

ida_la_SOURCES := ida.c
ida_la_SOURCES += ida-common.c
ida_la_SOURCES += ida-manager.c
ida_la_SOURCES += ida-combine.c
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
ida_la_SOURCES += ida-type-lock.c
ida_la_SOURCES += ida-heal.c

ida_la_LIBADD = $(gfdir)/libglusterfs/src/libglusterfs.la $(gfsys)/src/libgfsys.la $(gfdfc)/lib/libgfdfc.la

uninstall-local:
	rm -f $(xlatordir)/disperse.so

install-data-hook:
	ln -sf ida.so $(xlatordir)/disperse.so
