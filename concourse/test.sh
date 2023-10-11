#!/bin/bash -l

set -exo pipefail

function install_plpython3() {
    if [[ $GP_MAJOR_VERSION == 6 ]]; then
        mkdir -p bin_plpython3/install_tmp
        pushd bin_plpython3/install_tmp
        find .. -maxdepth 1 -regex ".*-[0-9\.]*-.*\.tar\.gz" -exec tar xfv {} \;
        ./install_gpdb_component
        popd
    fi
}

function _main() {
    source "$CI_REPO_DIR/common/entry_common.sh"
    start_gpdb
    source ~/.bashrc
    install_plpython3

    # FIXME: The test db and extension creation should be handled by python code.
    createdb gpadmin
    psql gpadmin -c "CREATE LANGUAGE plpython3u;"

    # Run testing
    pushd /home/gpadmin/greenplumpython_src
    unset PYTHONPATH
    unset PYTHONHOME
    python3.9 -m pip install tox
    python3.9 -m pip install .
    tox -e test_py39
    popd
}

_main
