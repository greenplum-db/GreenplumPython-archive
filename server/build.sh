#!/bin/bash

set -o nounset -o xtrace -o errexit -o pipefail

PG_MAJOR_VERSION=$(pg_config --version | grep --only-matching --extended-regexp '[0-9]+' | head -n 1)
export DEBIAN_FRONTEND=nointeractive
apt-get update
apt-get install --no-install-recommends -y \
    postgresql-plpython3-"$PG_MAJOR_VERSION" \
    postgresql-"$PG_MAJOR_VERSION"-pgvector \
    python3-pip \
    python3-venv
apt-get autoclean

POSTGRES_USER_SITE=$(su postgres --session-command "python3 -m site --user-site")
POSTGRES_USER_BASE=$(su postgres --session-command "python3 -m site --user-base")
mkdir --parents "$POSTGRES_USER_SITE"
chown --recursive postgres "$POSTGRES_USER_BASE"

cp /tmp/initdb.sh /docker-entrypoint-initdb.d
chown postgres /docker-entrypoint-initdb.d/*

setup_venv() {
    python3 -m venv "$HOME"/venv
    # shellcheck source=/dev/null
    source "$HOME"/venv/bin/activate

    # shellcheck source=/dev/null
    source /tmp/requirements.sh
}

export -f setup_venv
su postgres --session-command 'bash -c setup_venv'
