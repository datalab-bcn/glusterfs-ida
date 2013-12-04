
AC_INIT([glusterfs-ida], [0.0.0])

AC_CONFIG_AUX_DIR([.])

AM_INIT_AUTOMAKE

m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES(yes)])

if make --help 2>&1 | grep -q no-print-directory; then
    AM_MAKEFLAGS="$AM_MAKEFLAGS --no-print-directory"
fi

if make --help 2>&1 | grep -q quiet; then
    AM_MAKEFLAGS="$AM_MAKEFLAGS --quiet"
fi

if libtool --help 2>&1 | grep -q quiet; then
    AM_LIBTOOLFLAGS="$AM_LIBTOOLFLAGS --quiet"
fi

AC_CONFIG_HEADERS([ida-config.h])

AC_CONFIG_FILES([Makefile
                 src/Makefile])

AC_CANONICAL_HOST

AC_PROG_CC
AC_DISABLE_STATIC
AC_PROG_LIBTOOL

AC_MSG_CHECKING([whether user supplied glusterfs location is valid])
AC_ARG_WITH(glusterfs,
            [AS_HELP_STRING([--with-glusterfs=DIR], [location of glusterfs])],
            [gfdir="$withval"],
            [AC_MSG_RESULT([no])
             AC_MSG_ERROR([--with-glusterfs is mandatory])])
if test -d "$gfdir"; then
    if test -f "$gfdir/config.h"; then
        AC_MSG_RESULT([yes])
    else
        AC_MSG_RESULT([no])
        AC_MSG_ERROR([glusterfs is not configured or location is not valid])
    fi
else
    AC_MSG_RESULT([no])
    AC_MSG_ERROR([invalid value for glusterfs location])
fi

AC_SUBST(gfdir)

AC_MSG_CHECKING([whether user supplied gfsys location is valid])
AC_ARG_WITH(gfsys,
            [AS_HELP_STRING([--with-gfsys=DIR], [location of gfsys])],
            [gfsys="$withval"],
            [AC_MSG_RESULT([no])
             AC_MSG_ERROR([--with-gfsys is mandatory])])
if test -d "$gfsys"; then
    if test -f "$gfsys/sys-config.h"; then
        AC_MSG_RESULT([yes])
    else
        AC_MSG_RESULT([no])
        AC_MSG_ERROR([gfsys is not configured or location is not valid])
    fi
else
    AC_MSG_RESULT([no])
    AC_MSG_ERROR([invalid value for gfsys location])
fi

AC_SUBST(gfsys)

AC_MSG_CHECKING([glusterfs library directory])
gflibdir=`make -C ${gfdir} --eval="dummy: ; @echo pkglibdir=\\\$(pkglibdir)/\\\$(PACKAGE_VERSION)" | sed -n "s/^pkglibdir=//p"`
if test -z "$gflibdir"; then
    AC_MSG_RESULT([not valid])
    AC_MSG_ERROR([cannot determine glusterfs' modules installation directory])
fi
AC_MSG_RESULT([$gflibdir])

AC_SUBST(gflibdir)

GF_HOST_OS=""
GF_CFLAGS="-Wall -O3"
GF_LDFLAGS="-rdynamic"

# check for gcc -Werror=format-security
saved_CFLAGS=$CFLAGS
CFLAGS="-Wformat -Werror=format-security"
AC_MSG_CHECKING([whether $CC accepts -Werror=format-security])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM()], [cc_werror_format_security=yes], [cc_werror_format_security=no])
echo $cc_werror_format_security
if test "x$cc_werror_format_security" = "xno"; then
    CFLAGS="$saved_CFLAGS"
else
    CFLAGS="$saved_CFLAGS $CFLAGS"
fi

# check for gcc -Werror=implicit-function-declaration
saved_CFLAGS=$CFLAGS
CFLAGS="-Werror=implicit-function-declaration"
AC_MSG_CHECKING([whether $CC accepts -Werror=implicit-function-declaration])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM()], [cc_werror_implicit=yes], [cc_werror_implicit=no])
echo $cc_werror_implicit
if test "x$cc_werror_implicit" = "xno"; then
    CFLAGS="$saved_CFLAGS"
else
    CFLAGS="$saved_CFLAGS $CFLAGS"
fi

case $host_os in
     linux*)
        GF_HOST_OS="GF_LINUX_HOST_OS"
        ;;
     solaris*)
        GF_HOST_OS="GF_SOLARIS_HOST_OS"
        GF_CFLAGS="${GF_CFLAGS} -D_REENTRANT"
        GF_CFLAGS="${GF_CFLAGS} -D_POSIX_PTHREAD_SEMANTICS"
        GF_CFLAGS="${GF_CFLAGS} -m64"
        GF_LDFLAGS=""
        ;;
     *netbsd*)
        GF_HOST_OS="GF_BSD_HOST_OS"
        GF_CFLAGS="${GF_CFLAGS} -D_INCOMPLETE_XOPEN_C063"
        GF_CFLAGS="${GF_CFLAGS} -DTHREAD_UNSAFE_BASENAME"
        GF_CFLAGS="${GF_CFLAGS} -DTHREAD_UNSAFE_DIRNAME"
        ;;
     *bsd*)
        GF_HOST_OS="GF_BSD_HOST_OS"
        GF_CFLAGS="${GF_CFLAGS} -DTHREAD_UNSAFE_BASENAME"
        GF_CFLAGS="${GF_CFLAGS} -DTHREAD_UNSAFE_DIRNAME"
        ;;
     darwin*)
        if ! test "`/usr/bin/sw_vers | grep ProductVersion: | cut -f 2 | cut -d. -f2`" -ge 5; then
            AC_MSG_ERROR([You need at least OS X 10.5 (Leopard) to build Glusterfs])
        fi
        GF_HOST_OS="GF_DARWIN_HOST_OS"
        LIBTOOL=glibtool
        GF_CFLAGS="${GF_CFLAGS} -D__DARWIN_64_BIT_INO_T"
        GF_CFLAGS="${GF_CFLAGS} -D_XOPEN_SOURCE"
        GF_CFLAGS="${GF_CFLAGS} -DTHREAD_UNSAFE_BASENAME"
        GF_CFLAGS="${GF_CFLAGS} -DTHREAD_UNSAFE_DIRNAME"
        GF_CFLAGS="${GF_CFLAGS} -bundle -undefined suppress -flat_namespace"
        ;;
esac

CFLAGS="${CFLAGS} ${GF_CFLAGS}"
LDFLAGS="${LDFLAGS} ${GF_LDFLAGS}"

CPPFLAGS="${CPPFLAGS} -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -D${GF_HOST_OS}"
CPPFLAGS="${CPPFLAGS} -I${gfdir} -I${gfdir}/libglusterfs/src"
CPPFLAGS="${CPPFLAGS} -I${gfdir}/contrib/uuid -I${gfsys}/src"

AC_SUBST(AM_MAKEFLAGS)
AC_SUBST(AM_LIBTOOLFLAGS)

AC_OUTPUT

