#!/bin/bash -l
set -eox pipefail;
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../

create_db (){
  echo "Create DB...."
  dropdb gppython
  createdb gppython
  psql gppython -c 'create extension plpythonu'
  psql gppython -f prepare_gppython.sql
  echo "Create DB finished"
}

# the hostfile need to be existed, and contains all hosts include both sements and master
remove_db (){
  echo "Clean DB..."
  dropdb gppython
  echo "Clean DB finished"
}

# When the number of connections is set to a high number, be care of the size of swap memory.
# Otherwise, container will not be able to start and docker may hang.
test_greenplumpython(){
  echo ${TOP_DIR}	
  pushd ${TOP_DIR}
  python3 -m pytest -s
  popd
}

#time create_db
time test_greenplumpython
#time remove_db
