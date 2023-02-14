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

Since we are using `plpython3` in Greenplum/Postgres to better use GreenplumPython we also need
to install the package in the server.

If you are using Greenplum you may need to install the following package on the server(plpython3 side)

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