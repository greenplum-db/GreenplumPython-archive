Requirements
============

GreenplumPython currently requires at least Python 3.9 to run, this is because:
    * Python 3.9 is the version we officially support and release with PL/Python3 and GPDB 6.
    * Python 3.9 is the default version in Rocky Linux 9 and is officially supported in Rocky Linux 8 (and also probably in RHEL 8 as well).

Installation
============

You can install latest release of the **GreenplumPython** library with pip3:

.. code-block:: bash

    pip3 install greenplum-python

To install the latest development version, do

.. code-block:: bash

    pip3 install --user git+https://github.com/greenplum-db/GreenplumPython

GreenplumPython requires [plpython3](https://docs.vmware.com/en/VMware-Tanzu-Greenplum/6/greenplum-database/GUID-analytics-pl_python.html) 
extension to be installed on Greenplum/Postgres.

[dill](https://github.com/uqfoundation/dill) as an optional dependency for GreenplumPython `plpython` side, 
which provides convenient features like auto-importing modules in the `plpython` functions. (auto-import is available even when dill is NOT installed on server.
`dill` is require to include outside dependencies in the same file/module, like functions or classes.)

To install `dill` or any other python modules on the `plpython` side, refer to [GPDB plpython document](https://docs.vmware.com/en/VMware-Tanzu-Greenplum/6/greenplum-database/GUID-analytics-pl_python.html#pip39) for more details.
