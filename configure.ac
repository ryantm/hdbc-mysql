AC_INIT([HDBC-mysql package], [0.1])

# Safety check: Ensure that we are in the correct source directory.
AC_CONFIG_SRCDIR([HDBC-mysql.cabal])

AC_PATH_PROG(mysql_config,mysql_config)
if test [ -z "$mysql_config" ]; then
    AC_MSG_ERROR([mysql_config executable not found. Make sure mysql_config is in your path])
else
    AC_MSG_CHECKING([MySQL header dir])
    MYSQLINCDIR=`${mysql_config} --include`
    AC_MSG_RESULT([$MYSQLINCDIR])
    AC_MSG_CHECKING([MySQL library dir])
    PGLIBDIR=`${mysql_config} --libs`
    AC_MSG_RESULT([$MYSQLLIBDIR])
fi

AC_SUBST([MYSQLINCDIR])
AC_SUBST([MYSQLLIBDIR])
AC_CONFIG_FILES([HDBC-mysql.buildinfo])
AC_OUTPUT
