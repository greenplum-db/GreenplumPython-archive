<img src="./doc/images/gppython_logo_text.svg">

GreenplumPython is a Python library that enables the user to interact with database in a Pythonic way.

GreenplumPython provides a [pandas](https://pandas.pydata.org/)-like DataFrame API that
1. looks familiar and intuitive to Python users
2. is powerful to do complex analytics, such as statistical analysis, with UDFs and UDAs
3. encapsulates common best practices and avoids common pitfalls in Greenplum, compared to writing SQL directly

## Installation

To install the latest development version, do

```bash
pip3 install --user git+https://github.com/greenplum-db/GreenplumPython
```

To install the latest released version, do

```bash
pip3 install --user greenplum-python
```

**Note:** The `--user` option in an active virtual environment will install to the local user python location.
Since a user location doesn't make sense for a virtual environment, to install the **GreenplumPython** library,
just remove `--user` from the above commands.

## Documentation

The documentation of GreenplumPython can be viewed at:

[Latest development version](https://greenplum-db.github.io/GreenplumPython/latest/)

[Latest released version](https://greenplum-db.github.io/GreenplumPython/stable/)
