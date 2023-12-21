Requirements on Server for Advanced Features
============================================

Using Non-Built-in Modules in a UDF
-----------------------------------

Modules installed in :code:`sys.path` on server will be available for use in a UDF. It is recommended to use a dedicated virtual
environment on server for UDFs. To achieve this, one way is to activate the environment before starting the database server.
For example, for PostgreSQL:

  .. code-block:: bash

      python3 -m venv /path/to/venv
      source /path/to/venv/bin/activate
      pg_ctl start

In this way, UDFs executed in the PostgreSQL server can only use packages installed in the new virutal environment. This avoids
polluting, or being polluted by, the system environment.

Defining Classes and Functions Outside UDFs
-------------------------------------------

GreenplumPython will use the `dill` pickler to serialize and deserialize UDFs if it is available. 
Using a pickler like `dill` makes UDFs easier to write and to maintain because it allows us to refer to a function or class 
defined outside of the UDF. This means we don't need to copy it around. To use dill, we need to

  - Make sure that the Python minor version on client equals to the one on server;
  - Make sure that the dill version on server is no less than the one on client, based on 
    `dill's statement <https://github.com/uqfoundation/dill/issues/272#issuecomment-400843077>`_ on backward compatibility.

With all in 

- the `Using Non-Built-in Modules in a UDF`_ section and 
- the `Defining Classes and Functions Outside UDFs`_ section 

setup, we are now ready to go though the :doc:`tutorial <./abalone>` on how to do Machine Learning (ML) in database with UDFs.

Creating and Searching Embeddings (Experimental)
------------------------------------------------

Embeddings enable us to search unstructured data, e.g. texts and images, based on semantic similarity.

To create and search embeddings, we will need all in :doc:`./req`, plus the 
`sentence-transformers <https://pypi.org/project/sentence-transformers/>`_ package installed
in the server's Python environment.

Please refer to the :doc:`tutorial <./tutorial_embedding>` for a simple working example to validate your setup.

Uploading Data Files from Localhost (Experimental)
--------------------------------------------------

With GreenplumPython, we can upload data files of any format from localhost to server and parse them with a UDF.

This feature requires all in :doc:`./req` to create UDFs.

Please refer to the doc of :meth:`DataFrame.from_files() <dataframe.DataFrame.from_files>` for detailed usage.

Installing Python Packages (Experimental)
-----------------------------------------

With GreenplumPython, we can upload packages from localhost and install them on server.

This can greatly simplify the process when the server cannot access the PyPI service directly.

Since the installation is done by executing a UDF on server, this feature requires all in :doc:`./req`.

Please refer to

- the doc of :meth:`Database.install_packages() <db.Database.install_packages>` for detailed usage, and
- the :doc:`tutorial <./tutorial_package>` for a simple working example.
