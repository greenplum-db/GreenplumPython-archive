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
    pip3 wheel . -w ../greenplumpython_artifacts
    popd
    tar -czf greenplumpython_artifacts/greenplumpython.tar.gz greenplumpython_artifacts/*.whl
}

_main
