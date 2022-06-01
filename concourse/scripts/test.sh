#!/bin/bash -l

set -exo pipefail

function _main() {
    # FIXME: This should not be necessary. But our tests rely on this.
    createdb gpadmin

    # Run testing
    pushd /home/gpadmin/greenplumpython_src
    unset PYTHONPATH
    unset PYTHONHOME
    tox -e test_py39
    popd
}

_main
