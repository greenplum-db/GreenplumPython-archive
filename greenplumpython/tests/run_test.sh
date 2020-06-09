create_db (){
  echo "Create DB...."
  dropdb gppython
  createdb gppython
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
  pushd dataframe
  pytest
  popd
}

time create_db
time test_greenplumpython
time remove_db