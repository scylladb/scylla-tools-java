#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --reloc-pkg build/scylla-tools-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}
RELOC_PKG=$(readlink -f build/scylla-tools-package.tar.gz)
OPTS=""
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            OPTS="$OPTS $1 $(readlink -f $2)"
            RELOC_PKG=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

if [[ ! $OPTS =~ --reloc-pkg ]]; then
    OPTS="$OPTS --reloc-pkg $RELOC_PKG"
fi
mkdir -p build/redhat/
tar -C build/redhat/ -xpf $RELOC_PKG scylla-tools-package/SCYLLA-RELEASE-FILE scylla-tools-package/SCYLLA-RELOCATABLE-FILE scylla-tools-package/SCYLLA-VERSION-FILE scylla-tools-package/SCYLLA-PRODUCT-FILE scylla-tools-package/dist/redhat
cd build/redhat/scylla-tools-package
exec ./dist/redhat/build_rpm.sh $OPTS
