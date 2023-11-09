#!/bin/bash

set -o nounset -o xtrace -o errexit -o pipefail

PG_MAJOR_VERSION=$(pg_config --version | grep --only-matching --extended-regexp '[0-9]+' | head -n 1)
export DEBIAN_FRONTEND=nointeractive
apt-get update
apt-get install --no-install-recommends -y \
    postgresql-plpython3-"$PG_MAJOR_VERSION" \
    postgresql-"$PG_MAJOR_VERSION"-pgvector \
    python3-pip
apt-get autoclean

POSTGRES_USER_SITE=$(su --login postgres --session-command "python3 -m site --user-site")
POSTGRES_USER_BASE=$(su --login postgres --session-command "python3 -m site --user-base")
mkdir --parents "$POSTGRES_USER_SITE"
chown --recursive postgres "$POSTGRES_USER_BASE"
