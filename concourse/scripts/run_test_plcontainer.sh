#!/bin/bash -l
set -eox pipefail



function test_run() {
  cat > /home/gpadmin/test_run.sh <<-EOF
#!/bin/bash -l
set -exo pipefail
export GPRLANGUAGE=plcontainer
pushd ~/GreenplumPython_src/greenplumpython/tests/
./run_test.sh
popd
EOF

  chmod a+x /home/gpadmin/test_run.sh
  bash /home/gpadmin/test_run.sh
}

function _main() {
    time test_run
}

_main "$@"
