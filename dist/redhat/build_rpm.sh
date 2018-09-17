#!/bin/bash -e

PRODUCT=scylla

. /etc/os-release
print_usage() {
    echo "build_rpm.sh -target epel-7-x86_64 --configure-user"
    echo "  --target target distribution in mock cfg name"
    exit 1
}
TARGET=
while [ $# -gt 0 ]; do
    case "$1" in
        "--target")
            TARGET=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}


if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla-tools-java dir"
    exit 1
fi

if [ -z "$TARGET" ]; then
    if [ "$ID" = "centos" -o "$ID" = "rhel" ] && [ "$VERSION_ID" = "7" ]; then
        TARGET=epel-7-$(uname -m)
    elif [ "$ID" = "fedora" ]; then
        TARGET=$ID-$VERSION_ID-$(uname -m)
    else
        echo "Please specify target"
        exit 1
    fi
fi

if [ ! -f /usr/bin/mock ]; then
    pkg_install mock
fi
if [ ! -f /usr/bin/git ]; then
    pkg_install git
fi
if [ ! -f /usr/bin/wget ]; then
    pkg_install wget
fi
if [ ! -f /usr/bin/yum-builddep ]; then
    pkg_install yum-utils
fi
if [ ! -f /usr/bin/pystache ]; then
    if is_redhat_variant; then
        sudo yum install -y python2-pystache || sudo yum install -y pystache
    elif is_debian_variant; then
        sudo apt-get install -y python-pystache
    fi
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
git archive --format=tar --prefix=$PRODUCT-tools-$SCYLLA_VERSION/ HEAD -o build/$PRODUCT-tools-$VERSION.tar
pystache dist/redhat/scylla-tools.spec.mustache "{ \"version\": \"$SCYLLA_VERSION\", \"release\": \"$SCYLLA_RELEASE\", \"product\": \"$PRODUCT\", \"$PRODUCT\": true }" > build/scylla-tools.spec

# mock generates files owned by root, fix this up
fix_ownership() {
    sudo chown "$(id -u):$(id -g)" -R "$@"
}

sudo mock --buildsrpm --root=$TARGET --resultdir=`pwd`/build/srpms --spec=build/scylla-tools.spec --sources=build/$PRODUCT-tools-$VERSION.tar
fix_ownership build/srpms
if [[ "$TARGET" =~ ^epel-7- ]]; then
    TARGET=scylla-tools-$TARGET
    RPM_OPTS="$RPM_OPTS --configdir=dist/redhat/mock"
fi
sudo mock --rebuild --root=$TARGET --resultdir=`pwd`/build/rpms $RPM_OPTS build/srpms/$PRODUCT-tools-$VERSION*.src.rpm
fix_ownership build/rpms
