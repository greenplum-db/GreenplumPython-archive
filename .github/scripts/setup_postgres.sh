#!/bin/bash

set -o nounset -o xtrace -o errexit -o pipefail

PG_MAJOR_VERSION=$(pg_config --version | grep -oE '[0-9]+' | head -n 1)

export DEBIAN_FRONTEND=something
apt-get install -y \
    postgresql-"$PG_MAJOR_VERSION"-plpython3 \
    postgresql-"$PG_MAJOR_VERSION"-pgvector \
    python3-pip

psql "$POSTGRES_DB" -c "CREATE EXTENSION plpython3u; CREATE EXTENSION vector;"

POSTGRES_USER_SITE=$(su --login postgres --session-command "python3 -m site --user-site")
mkdir -p "$POSTGRES_USER_SITE"
chown -R postgres "$POSTGRES_USER_SITE"
