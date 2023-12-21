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

After connecting to the database, we can create :class:`~dataframe.DataFrame` 's and manipulate them like using `pandas <https://pandas.pydata.org/>`_.

.. _Creating Functions:

Creating Functions
^^^^^^^^^^^^^^^^^^

Even though we can call existing functions in database to manipulate DataFrames, sometimes they might not fit our needs and we need to create new UDFs.

To create a UDF, we need to install the :code:`PL/Python` language on server and enable the extension with

.. code-block:: sql

    CREATE EXTENSION plpython3u;

There are a few points to note when working with `PL/Python`:

- To use the extension :code:`plpython3u`, it is required to login as a :code:`SUPERUSER`. This might cause some security concerns. 
  We will remove this limitation soon by supporting `PL/Container <https://github.com/greenplum-db/plcontainer>`_.
- Python 3.x is required on server. Please make sure that :code:`libpython3.x.so` is in :code:`$LD_LIBRARY_PATH`.
  
  There is no hard requirement on the minor version, but Python language features and libraries available for use in a UDF depends on it.
  Therefore, it is recommended that the Python version on server is greater than or equal to the one on client.
- Modules installed in :code:`sys.path` on server will be available for use in a UDF. It is recommended to use a dedicated virtual
  environment on server for UDFs. To achieve this, one way is to activate the environment before starting the database server.
  For example, for PostgreSQL:

  .. code-block:: bash

      python3 -m venv /path/to/venv
      source /path/to/venv/bin/activate
      pg_ctl start

  In this way, UDFs executed in the PostgreSQL server can only use packages installed in the new virutal environment. This avoids
  polluting, or being polluted by, the system environment.
- GreenplumPython will use the `dill` pickler to serialize and deserialize UDFs if it is available. 
  Using a pickler like `dill` makes UDFs easier to write and to maintain because it allows us to refer to a function or class 
  defined outside of the UDF. This means we don't need to copy it around. To use dill, we need to

  - Make sure that the Python minor version on client equals to the one on server;
  - Make sure that the dill version on server is no less than the one on client, based on 
    `dill's statement <https://github.com/uqfoundation/dill/issues/272#issuecomment-400843077>`_ on backward compatibility.

Creating and Searching Embeddings (Experimental)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Embeddings enable us to search unstructured data, e.g. texts and images, based on semantic similarity.

To create and search embeddings, we will need all in 

- the `Getting Started`_ section,
- the `Creating Functions`_ section,

plus the `sentence-transformers <https://pypi.org/project/sentence-transformers/>`_ package installed
in the server's Python environment.

Please refer to the :doc:`tutorial <./tutorial_embedding>` for a simple working example to validate your setup.

Uploading Data Files from Localhost (Experimental)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With GreenplumPython, we can upload data files of any format from localhost to server and parse them with a UDF.

This feature requires all in the the `Creating Functions`_ section to create UDFs.

Please refer to the doc of :meth:`DataFrame.from_files() <dataframe.DataFrame.from_files>` for detailed usage.

Installing Python Packages (Experimental)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With GreenplumPython, we can upload packages from localhost and install them on server.

This can greatly simplify the process when the server cannot access the PyPI service directly.

Since the installation is done by executing a UDF on server, this feature requires all in the the `Creating Functions`_ section.

Please refer to

- the doc of :meth:`Database.install_packages() <db.Database.install_packages>` for detailed usage, and
- the :doc:`tutorial <./tutorial_package>` for a simple working example.
