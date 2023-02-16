Requirements
============

GreenplumPython currently requires at least Python 3.9 to run, this is because:
    * Python 3.9 is the version we officially support and release with PL/Python3 and GPDB 6.
    * Python 3.9 is the default version in Rocky Linux 9 and is officially supported in Rocky Linux 8 (and also probably in RHEL 8 as well).

As a dependency of GreenplumPython, psycopg2 is a source package. To install it, you need the following on the client side
    * libpq and its C header files,
    * C compilers, such as gcc

To be able to compile the C code in the psycopg2 package. To work around this problem, we change our
to psycopg2-binary, which is a binary package.

Installation
============

You can install latest release of the **GreenplumPython** library with pip3:

.. code-block:: bash

    pip3 install greenplum-python

To install the latest development version, do

.. code-block:: bash

    pip3 install --user git+https://github.com/greenplum-db/GreenplumPython

NOTE: This version is considered UNSTABLE. DON'T use it in the production environment! Stable version will be released soon.

GreenplumPython requires [plpython3](https://docs.vmware.com/en/VMware-Tanzu-Greenplum/6/greenplum-database/GUID-analytics-pl_python.html) 
extension to be installed on Greenplum/Postgres.

[dill](https://github.com/uqfoundation/dill) as an optional dependency for GreenplumPython `plpython` side, 
provides convenient features like auto-importing modules in the `plpython` functions. 
For better use of greenplumPython we'd better install `dill` on the server side, and if you want to use third party packages you'll also need to install the package on the server side.

Refer to [GPDB plpython document](https://docs.vmware.com/en/VMware-Tanzu-Greenplum/6/greenplum-database/GUID-analytics-pl_python.html#pip39) 
about how to install [dill](https://github.com/uqfoundation/dill) for Greenplum.
