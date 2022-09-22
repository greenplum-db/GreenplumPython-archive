#!/bin/bash

# Entry point for GPDB source & cluster related tasks.
# This script setup basic build/test environment including:
# - Create a gpadmin user
# - Copy all files from /tmp/build/xxx (concourse WORKDIR) to /home/gpadmin/ and chown to gpadmin
# - Some dependencies doesn't exist in build/test image.
# - Special setup for individual task which needs root permission.
# - At the end, call the task script with gpadmin permission.
#
# Simple rules:
# 1. Any root operations should happen in this script.
# 2. Task script only requires gpadmin permission.
# 3. Since everything has been copied to the /home/gpadmin directory, use absolute path as much as
#    as possible in the task script, it will reduce the confusion when we fly into the concourse
#    container.
# 4. Bash functions should be idempotent as much as possible to make fly hijack debugging easier.

set -eox

if [[ ! ${PWD} =~ /tmp/build/[0-9a-z]* ]]; then
    echo "This script should always be started from concourse WORKDIR."
fi

# Internal utilty functions
_determine_os() {
  local name version
  if [ -f /etc/redhat-release ]; then
    name="rhel"
    version=$(sed </etc/redhat-release 's/.*release *//' | cut -f1 -d.)
  elif [ -f /etc/SuSE-release ]; then
    name="sles"
    version=$(awk -F " *= *" '$1 == "VERSION" { print $2 }' /etc/SuSE-release)
  elif  grep -q photon /etc/os-release  ; then
    name="photon"
    version=$( awk -F " *= *" '$1 == "VERSION_ID" { print $2 }' /etc/os-release | cut -f1 -d.)
  elif grep -q ubuntu /etc/os-release ; then
    name="ubuntu"
    version=$(awk -F " *= *" '$1 == "VERSION_ID" { print $2 }' /etc/os-release | tr -d \")
  else
    echo "Could not determine operating system type" >/dev/stderr
    exit 1
  fi
  echo "${name}${version}"
}

OS_NAME=$(_determine_os)

# Global ENV defines
# /tmp/build/xxxxx. it should not be used in normal conditions. Use /home/gpadmin instead.
# Everything has been linked there.
export CONCOURSE_WORK_DIR=${PWD}

install_extra_build_dependencies() {
    case "$OS_NAME" in
    rhel7)
        yum install -y wget
        wget https://bootstrap.pypa.io/pip/get-pip.py
        ;;
    rhel8)
        ;;
    ubuntu*)
        wget https://bootstrap.pypa.io/pip/get-pip.py
        ;;
    *) ;;
    esac
}

# Dependency installers
# Ideally all dependencies should exist in the docker image. Use this script to install them only
# if it is more difficult to change it in the image side.
# Download the dependencies with concourse resources as much as possible, then we could benifit from
# concourse's resource cache system.
install_dependencies() {
    unset PYTHONPATH
    unset PYTHONHOME

    local python_bin="${GPHOME}/ext/python3.9/bin/python3.9"
    ${python_bin} -m ensurepip
    ${python_bin} -m pip install tox
}

# Create gpadmin user and chown all files in the PWD. All files will be linked to /home/gpadmin.
# All of our work should be started from there.
setup_gpadmin() {
    # If the gpadmin exist, quit
    if grep -c '^gpadmin:' /etc/passwd; then
        return
    fi

    # If the image has sshd, then we call gpdb's setup_gpadmin_user.sh to create the gpadmin user
    # and setup the ssh.
    # Otherwise, create the gpadmin user only.
    if [ -f /etc/ssh/sshd_config ]; then
        pushd "${CONCOURSE_WORK_DIR}"
        local gpdb_concourse_dir="${CONCOURSE_WORK_DIR}/gpdb_src/concourse/scripts"
        "${gpdb_concourse_dir}/setup_gpadmin_user.bash"
        popd
    else
        # Below is copied from setup_gpadmin_user.bash
        groupadd supergroup
        case "$OS_NAME" in
            rhel*)
                /usr/sbin/useradd -G supergroup,tty gpadmin
                ;;
            ubuntu*)
                /usr/sbin/useradd -G supergroup,tty gpadmin -s /bin/bash
                ;;
            sles*)
                # create a default group gpadmin, and add user gpadmin to group gapdmin, supergroup,
                # tty
                /usr/sbin/useradd -U -G supergroup,tty gpadmin
                ;;
            photon*)
                /usr/sbin/useradd -U -G supergroup,tty,root gpadmin
                ;;
            *) echo "Unknown OS: $OS_NAME"; exit 1 ;;
        esac
        echo -e "password\npassword" | passwd gpadmin
    fi
    mkdir -p /home/gpadmin
    chown gpadmin:gpadmin /home/gpadmin

    chown -R gpadmin:gpadmin /tmp/build
    ln -s "${CONCOURSE_WORK_DIR}"/* /home/gpadmin
}

function install_plpython3() {
    mkdir -p bin_plpython3/install_tmp
    pushd bin_plpython3/install_tmp
    find .. -maxdepth 1 -regex ".*-[0-9\.]*-.*\.tar\.gz" -exec tar xfv {} \;
    ./install_gpdb_component
    popd
}

# Extract gpdb binary
function install_gpdb() {
    [ ! -d /usr/local/greenplum-db-devel ] && mkdir -p /usr/local/greenplum-db-devel
    tar -xzf "${CONCOURSE_WORK_DIR}"/bin_gpdb/*.tar.gz -C /usr/local/greenplum-db-devel
    GPHOME=/usr/local/greenplum-db-devel install_plpython3

    chown -R gpadmin:gpadmin /usr/local/greenplum-db-devel
    # Start cluster
    source "/home/gpadmin/gpdb_src/concourse/scripts/common.bash"
    make_cluster
    source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
}

function setup_gpadmin_bashrc() {
    {
        echo "source /usr/local/greenplum-db-devel/greenplum_path.sh"
        echo "source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh"
        echo "export OS_NAME=${OS_NAME}"
        echo "export PATH=\$PATH:${GPHOME}/ext/python3.9/bin"
    } >> /home/gpadmin/.bashrc
}


# Setup common environment 
install_extra_build_dependencies
setup_gpadmin
install_gpdb
install_dependencies
setup_gpadmin_bashrc

# Do the special setup with root permission for the each task, then run the real task script with
# gpadmin. bashrc won't be read by 'su', it needs to be sourced explicitly.
case "$1" in
    test)

        # To make fly debug easier
        su gpadmin -c \
            "source /home/gpadmin/.bashrc &&\
            /home/gpadmin/greenplumpython_src/concourse/scripts/test.sh"
        ;;
    *)
        echo "Unknown target task $1"
        exit 1
        ;;
esac
