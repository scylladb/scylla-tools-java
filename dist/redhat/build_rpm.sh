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

SCYLLA_VERSION=$(cat SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat SCYLLA-RELEASE-FILE)
VERSION=$SCYLLA_VERSION-$SCYLLA_RELEASE

RPMBUILD=$(readlink -f ../)
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

ln -fv $RELOC_PKG $RPMBUILD/SOURCES/

parameters=(
    -D"version $SCYLLA_VERSION"
    -D"release $SCYLLA_RELEASE"
    -D"product $PRODUCT"
)

cp dist/redhat/scylla-tools.spec $RPMBUILD/SPECS
# this rpm can be install on both fedora / centos7, so drop distribution name from the file name
rpmbuild -ba "${parameters[@]}" --define '_binary_payload w2.xzdio' --define "_topdir $RPMBUILD" --undefine "dist" $RPM_JOBS_OPTS $RPMBUILD/SPECS/scylla-tools.spec
