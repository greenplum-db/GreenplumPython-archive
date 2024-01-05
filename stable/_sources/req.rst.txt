Requirements
============

On Client
---------

On the client side, e.g., on our laptop or workstation, installing the `greenplum-python` Python package is all we need:

.. code-block:: bash

    python3 -m pip install greenplum-python

This installs the latest released version. To try the latest development version, we can install it with

.. code-block:: bash

    python3 -m pip3 install git+https://github.com/greenplum-db/GreenplumPython

Please note that the Python version needs to be at least 3.9 to install.

On Server
---------

GreenplumPython works best with Greenplum. All features will be developed and tested on Greenplum first.

We also try our best to support PostgreSQL and other PostgreSQL-derived databases, but some features might **NOT** be available when working with them.

.. _Getting Started:

Getting Started
^^^^^^^^^^^^^^^

To get started, all we need is a database that we have the permission to access.

After connecting to the database, we can create :class:`~dataframe.DataFrame` s and manipulate them like using `pandas <https://pandas.pydata.org/>`_.

.. _Creating Functions:

Creating Functions
^^^^^^^^^^^^^^^^^^

Even though we can call existing functions in database to manipulate DataFrames, sometimes they might not fit our needs and we need to create new UDFs.

To create a UDF, we need to install the PL/Python package on server and enable it in database with SQL:

.. code-block:: sql

    CREATE EXTENSION plpython3u;

There are a few points to note when working with PL/Python:

- To use the extension :code:`plpython3u`, it is required to login as a :code:`SUPERUSER`. 
  This might cause some security concerns. We will remove this limitation soon by supporting
  `PL/Container <https://github.com/greenplum-db/plcontainer>`_.
- Python 3.x is required on server. And it is recommended that the Python version on server
  is greater than or equal to the one on client. This is to ensure all Python features are available
  when writing UDFs.

With all above steup, we are ready to go through the :doc:`tutorial <./sql>` to see how GreenplumPython compares with SQL.

For other, more advanced, features, please refer to :doc:`./req_advanced`.
