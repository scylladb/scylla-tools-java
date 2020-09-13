#!/bin/bash -e

PRODUCT=$(cat scylla-tools/SCYLLA-PRODUCT-FILE)

. /etc/os-release
print_usage() {
    echo "build_deb.sh --reloc-pkg build/scylla-tools-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}
TARGET=stable
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
is_debian_variant() {
    [ -f /etc/debian_version ]
}

if [ ! -e scylla-tools/SCYLLA-RELOCATABLE-FILE ]; then
    echo "do not directly execute build_rpm.sh, use reloc/build_rpm.sh instead."
    exit 1
fi

RELOC_PKG=$(readlink -f $RELOC_PKG)

mv scylla-tools/debian debian
PKG_NAME=$(dpkg-parsechangelog --show-field Source)
# XXX: Drop revision number from version string.
#      Since it always '1', this should be okay for now.
PKG_VERSION=$(dpkg-parsechangelog --show-field Version |sed -e 's/-1$//')
ln -fv $RELOC_PKG ../"$PKG_NAME"_"$PKG_VERSION".orig.tar.gz
debuild -rfakeroot -us -uc
