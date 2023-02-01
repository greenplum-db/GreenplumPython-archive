Requirements
============

GreenplumPython currently requires **at least Python 3.9** to run, this is because:
    * Python 3.9 is the version we officially support and release with PL/Python3 and GPDB 6.
    * Python 3.9 is the default version in Rocky Linux 9 and is officially supported in Rocky Linux 8 (and also probably in RHEL 8).

As a dependency of GreenplumPython, psycopg2 is a source package. To install it, the client side need to have
    * libpq and its C header files,
    * C compilers, such as gcc
installed so that the C code in the psycopg2 package can be compiled. To sidestep this problem, we change
our dependency to psycopg2-binary, which is a binary package.