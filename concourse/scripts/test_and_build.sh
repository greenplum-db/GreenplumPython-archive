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
    tox -e test_py39
    # build wheel
    pip3 install .
    pip3 wheel .
    mkdir ../greenplumpython_artifacts
    cp ./*.whl ../greenplumpython_artifacts
    popd
}

_main
