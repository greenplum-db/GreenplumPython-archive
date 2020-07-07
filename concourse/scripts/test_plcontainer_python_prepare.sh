function determine_os() {
    if [ -f /etc/redhat-release ] ; then
      echo "centos"
      return
    fi
    if grep -q ID=ubuntu /etc/os-release ; then
      echo "ubuntu"
      return
    fi
    echo "Could not determine operating system type" >/dev/stderr
    exit 1
}

function install_libraries() {
    TEST_OS=$(determine_os)
    case $TEST_OS in
    centos)
      yum install -y centos-release-scl
      # postgresql-devel is needed by RPostgreSQL
      yum install -y postgresql-devel python36 python36-devel python36-setuptools
      easy_install-3.6 pip
      pip3 install pytest numpy PyGreSQL SQLAlchemy pandas
      ;;
    ubuntu)
      apt update
      DEBIAN_FRONTEND=noninteractive apt install -y python3-pip libpq-dev
      pip3 install pytest numpy PyGreSQL SQLAlchemy pandas
      ;;
    *)
      echo "unknown TEST_OS = $TEST_OS"
      exit 1
      ;;
    esac
}

function _main() {
    time install_libraries
}

_main "$@"
