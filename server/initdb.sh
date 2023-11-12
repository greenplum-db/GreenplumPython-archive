#!/bin/bash

set -o nounset -o xtrace -o errexit -o pipefail

{
    echo "logging_collector = ON"
    echo "log_statement = 'all'"
    echo "log_destination = 'csvlog'"
} >>"$PGDATA"/postgresql.conf

python3 -m venv "$HOME"/venv
source "$HOME"/venv/bin/activate
