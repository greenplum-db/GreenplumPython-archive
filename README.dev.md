# GreenplumPython development

## Requirements

### [tox](https://tox.wiki)

We use tox as the task runner. Tox can be installed with

```
python3 -m pip install tox
```

Install with `brew` on macOS:

```
brew install tox
```

`NodeJs` need to have version 12.X, otherwise `Pyright` can't be executed correctly. Possible scenarios:
 - Error : Cannot find module 'worker_threads'
 - Nothing happens when run `Pyright`

## Build & Test

### Lint

```
tox -e lint
```

### Test

The tests will create connection to the Greenplum cluster. So the `PGPORT` needs to be set if it is not the default `5432`:

```
export PGPORT=6000
```

Test with the default python version and a local database server:

```
tox -e test
```

To run tests against a database server in container:
```
python3 -m pip install tox-docker
tox -e test-container
```

Run a specified test case(s):

```
tox -e test -- -k <EXPRESSION>
# e.g.
tox -e test -- -k dataframe
```

### VirtualEnv

The `tox` is based on VirtualEnv. If you need to enter a special VirtualEnv, just source the `activate` file in the corresponding sub directory of `.tox`. Like:

```
source .tox/test/bin/activate
```

#### Upload to PyPI

```shell
# change the version in setup.py
pip3 install twine
python3 setup.py sdist
twine upload dist/*
```
