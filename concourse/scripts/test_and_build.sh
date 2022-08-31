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
    name=$(find greenplumpython_artifacts -maxdepth 1 -regex "psycopg.*whl" | sed 's/....$//' |awk '{split($0, a, "-"); print a[3]"-"a[4]"-"a[5]}')
    tar -czf greenplumpython_artifacts/greenplumpython_"${name}".tar.gz greenplumpython_artifacts/*.whl
}

_main
