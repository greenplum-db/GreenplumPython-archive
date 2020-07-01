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
      yum install -y epel-release
      # postgresql-devel is needed by RPostgreSQL
      yum install -y R postgresql-devel
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
