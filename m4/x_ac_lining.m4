##*****************************************************************************
#  AUTHOR:
#    Christopher Morrone <morrone2@llnl.gov>
#    Maksym Planeta <mplaneta@os.inf.tu-dresden.de>
#
#  SYNOPSIS:
#    X_AC_LINING()
#
#  DESCRIPTION:
#    Check the usual suspects for a liblining installation,
#    updating CPPFLAGS and LDFLAGS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************

AC_DEFUN([X_AC_LINING], [

  _x_ac_lining_dirs="/usr /opt/freeware"
  _x_ac_lining_libs="lib64 lib"

  AC_ARG_WITH(
    [lining],
    AS_HELP_STRING(--with-lining=PATH,Enable liblining and optionally specify path),
    [_x_ac_lining_dirs="$withval"
     with_lining=yes],
    [_x_ac_lining_dirs=no
     with_lining=no]
  )

  if test "x$_x_ac_lining_dirs" = xno ; then
    # user explicitly wants to disable liblining support
    LINING_CPPFLAGS=""
    LINING_LDFLAGS=""
    LINING_LIBS=""
    # TODO: would be nice to print some message here to record in the config log
  else
    # user wants liblining enabled, so let's define it in the source code
    AC_DEFINE([HAVE_LIBLINING], [1], [Define if liblining is available])

    # now let's locate the install location
    found=no

    # check for liblining in a system default location if:
    #   --with-lining or --without-lining is not specified
    #   --with-lining=yes is specified
    #   --with-lining is specified
    if test "$with_lining" = check || \
       test "x$_x_ac_lining_dirs" = xyes || \
       test "x$_x_ac_lining_dirs" = "x" ; then
      AC_CHECK_LIB([lining], [lining_init])

      # if we found it, set the build flags
      if test "$ac_cv_lib_lining_lining_init" = yes; then
        found=yes
        LINING_CPPFLAGS=""
        LINING_LDFLAGS=""
        LINING_LIBS="-llining"
      fi
    fi

    # if we have not already found it, check the lining_dirs
    if test "$found" = no; then
      AC_CACHE_CHECK(
        [for liblining installation],
        [x_ac_cv_lining_dir],
        [
          for d in $_x_ac_lining_dirs; do
            test -d "$d" || continue
            test -d "$d/include" || continue
            test -f "$d/include/lining.h" || continue
            for bit in $_x_ac_lining_libs; do
              test -d "$d/$bit" || continue

              _x_ac_lining_libs_save="$LIBS"
              LIBS="-L$d/$bit -llining $LIBS"
              AC_LINK_IFELSE(
                AC_LANG_CALL([], [lining_init]),
                AS_VAR_SET([x_ac_cv_lining_dir], [$d]))
              LIBS="$_x_ac_lining_libs_save"
              test -n "$x_ac_cv_lining_dir" && break
            done
            test -n "$x_ac_cv_lining_dir" && break
          done
      ])

      # if we found it, set the build flags
      if test -n "$x_ac_cv_lining_dir"; then
        found=yes
        LINING_CPPFLAGS="-I$x_ac_cv_lining_dir/include"
        LINING_LDFLAGS="-L$x_ac_cv_lining_dir/$bit"
        LINING_LIBS="-llining"
      fi
    fi

    # if we failed to find liblining, throw an error
    if test "$found" = no ; then
      AC_MSG_ERROR([unable to locate liblining installation])
    fi
  fi

  # propogate the build flags to our makefiles
  AC_SUBST(LINING_CPPFLAGS)
  AC_SUBST(LINING_LDFLAGS)
  AC_SUBST(LINING_LIBS)
])
