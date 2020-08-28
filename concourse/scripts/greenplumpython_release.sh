#!/bin/bash

#------------------------------------------------------------------------------
#
# Copyright (c) 2017-Present Pivotal Software, Inc
#
#------------------------------------------------------------------------------

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
bash ${TOP_DIR}/GreenplumPython_src/concourse/scripts/test_plcontainer_python_prepare.sh;

release_greenplumpython() {
  pushd GreenplumPython_src
  python3 -m pip install --user --upgrade setuptools wheel
  python3 setup.py sdist bdist_wheel
  popd
  mkdir -p greenplumPython-release
  cp ${TOP_DIR}/GreenplumPython_src/dist/greenplum-python-*.tar.gz greenplumPython-release/
}

release_greenplumpython