#!/bin/bash -e

PRODUCT=$(cat SCYLLA-PRODUCT-FILE)

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --reloc-pkg build/scylla-tools-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}
RELOC_PKG=
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            RELOC_PKG=$2
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

if [ ! -e SCYLLA-RELOCATABLE-FILE ]; then
    echo "do not directly execute build_rpm.sh, use reloc/build_rpm.sh instead."
    exit 1
fi

if [ -z "$RELOC_PKG" ]; then
    print_usage
    exit 1
fi
if [ ! -f "$RELOC_PKG" ]; then
    echo "$RELOC_PKG is not found."
    exit 1
fi

if [ ! -f /usr/bin/rpmbuild ]; then
    pkg_install rpm-build
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

SCYLLA_VERSION=$(cat SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat SCYLLA-RELEASE-FILE)
VERSION=$SCYLLA_VERSION-$SCYLLA_RELEASE

RPMBUILD=$(readlink -f ../)
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

ln -fv $RELOC_PKG $RPMBUILD/SOURCES/
pystache dist/redhat/scylla-tools.spec.mustache "{ \"version\": \"$SCYLLA_VERSION\", \"release\": \"$SCYLLA_RELEASE\", \"product\": \"$PRODUCT\", \"$PRODUCT\": true }" > $RPMBUILD/SPECS/scylla-tools.spec

# this rpm can be install on both fedora / centos7, so drop distribution name from the file name
rpmbuild -ba --define '_binary_payload w2.xzdio' --define "_topdir $RPMBUILD" --undefine "dist" $RPM_JOBS_OPTS $RPMBUILD/SPECS/scylla-tools.spec
