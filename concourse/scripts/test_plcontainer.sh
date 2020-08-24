#!/bin/bash -l
set -exo pipefail

ccp_src/scripts/setup_ssh_to_cluster.sh
#############################################################
# install docker
#############################################################
plcontainer_src/concourse/scripts/docker_install.sh

#############################################################
# install plcontainer
#############################################################

scp -r bin_plcontainer mdw:/tmp/

ssh mdw "bash -c \" \
set -eox pipefail; \
export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1; \
source /usr/local/greenplum-db-devel/greenplum_path.sh; \
gppkg -i /tmp/bin_plcontainer/plcontainer-2.*.gppkg; \
\""


#############################################################
# install docker image
#############################################################
scp -r plcontainer_pyclient_docker_image/plcontainer-*.tar.gz \
    mdw:/usr/local/greenplum-db-devel/share/postgresql/plcontainer/plcontainer-python-images.tar.gz

# install dependencies
case "$platform" in
centos*)
    node=centos
    ;;
ubuntu*)
    node=ubuntu
    # install protobuf libs 
    scp -r plcontainer_gpdb_ubuntu18_build_lib/* mdw:/tmp/
    scp -r plcontainer_gpdb_ubuntu18_build_lib/* sdw1:/tmp/
    plcontainer_src/concourse/scripts/protobuf_install.sh
    ;;
*)
    echo "unknown platform: $platform"
    exit 1
    ;;
esac


scp -r GreenplumPython_src mdw:~/

ssh $node@mdw "sudo bash -c \" \
set -eox pipefail; \
bash /home/gpadmin/GreenplumPython_src/concourse/scripts/test_plcontainer_python_prepare.sh; \
\""

ssh mdw "bash -c \" \
set -eox pipefail; \
export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1; \
source /usr/local/greenplum-db-devel/greenplum_path.sh; \
plcontainer image-add -f /usr/local/greenplum-db-devel/share/postgresql/plcontainer/plcontainer-python-images.tar.gz; \
plcontainer runtime-add -r plc_python_shared -i pivotaldata/plcontainer_python3_shared:devel -l r -s use_container_logging=yes; \
gpconfig -c shared_preload_libraries -v 'plc_coordinator'; \
gpstop -arf; \
createdb gppython; \
pushd /home/gpadmin/GreenplumPython_src; \
psql gppython -U gpadmin -f greenplumpython/tests/prepare_gppython.sql; \
popd; \
\""
#############################################################
# run tests
#############################################################

ssh mdw "bash -c \" \
set -eox pipefail; \
export PGPORT=5432; \
export GPUSER=gpadmin; \
export GPDATABASE=gppython; \
pushd GreenplumPython_src; \
source /usr/local/greenplum-db-devel/greenplum_path.sh; \
bash concourse/scripts/run_test_plcontainer.sh; \
popd; \
\""
#############################################################
