#!/bin/bash -l

set -exo pipefail

function _main() {
    # FIXME: The test db and extension creation should be handled by python code.
    createdb gpadmin
    psql gpadmin -c "CREATE LANGUAGE plpython3u;"

    # Run testing
    pushd /home/gpadmin/greenplumpython_src
    unset PYTHONPATH
    unset PYTHONHOME
    python3.9 -m pip install dill==0.3.6
    tox -e test_py39
    popd
}

_main
