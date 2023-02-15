Installation
============

You can install latest release of the **GreenplumPython** library with pip3:

.. code-block:: bash

    pip3 install greenplum-python

To install the latest development version, do

.. code-block:: bash

    pip3 install --user git+https://github.com/greenplum-db/GreenplumPython


NOTE: This version is considered UNSTABLE. DON'T use it in the production environment!

Stable version will be released soon.

GreenplumPython requires [plpython3](https://docs.vmware.com/en/VMware-Tanzu-Greenplum/6/greenplum-database/GUID-analytics-pl_python.html) 
extension to be installed on Greenplum/Postgres.

[dill](https://github.com/uqfoundation/dill) as an optional dependency for GreenplumPython `plpython` side, 
provides convenient features like auto-importing modules in the `plpython` functions. 
Refer to [GPDB plpython document](https://docs.vmware.com/en/VMware-Tanzu-Greenplum/6/greenplum-database/GUID-analytics-pl_python.html#pip39) 
about how to install [dill](https://github.com/uqfoundation/dill) for Greenplum.

.. code-block:: bash

    pip3 install dill

NOTE: your `PYTHONPATH` may be different from your `plpython3` path, you can use this function.

.. code-block:: bash

    CREATE FUNCTION find_python_path() 
    RETURNS text AS $$
      import sys
      return str(sys.path)
    $$ language plpython3u;
    SELECT find_python_path();

then you can copy `dill` folders to the `PYTHONPATH`. 