#!/bin/bash -l

set -o xtrace -o errexit -o nounset -o pipefail

install_plpython3() {
    if [[ $GP_MAJOR_VERSION == 6 && ! -f "$(pg_config --pkglibdir)/plpython3.so" ]]; then
        mkdir -p bin_plpython3/install_tmp
        pushd bin_plpython3/install_tmp
        find .. -maxdepth 1 -regex ".*-[0-9\.]*-.*\.tar\.gz" -exec tar xfv {} \;
        ./install_gpdb_component
        popd
    fi
}

start_gpdb_as_gpadmin() {
    if ! gpstate; then
        source "$CI_REPO_DIR/common/entry_common.sh"
        sudo passwd --delete gpadmin  # for `su gpadmin` in start_gpdb
        if [[ "$(source /etc/os-release && echo "$ID")" == "ubuntu" ]]; then
            su () {
                sudo "su" "$@"
            }
        fi
        set +o nounset
        start_gpdb
        source "$HOME/.bashrc"  # for gpdemo-env.sh
        set -o nounset
    fi
}

setup_testdb() {
    local testdb=$1
    # FIXME: The test db and extension creation should be handled by python code.
    if ! psql "$testdb" -c ''; then
        createdb "$testdb"
    fi
    psql "$testdb" -c "CREATE EXTENSION IF NOT EXISTS plpython3u;"
}

_main() {
    install_plpython3
    start_gpdb_as_gpadmin
    setup_testdb gpadmin

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
