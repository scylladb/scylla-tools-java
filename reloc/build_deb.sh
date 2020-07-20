#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh --reloc-pkg build/scylla-tools-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --builddir specify Debian package build path"
    exit 1
}

RELOC_PKG=$(readlink -f build/scylla-tools-package.tar.gz)
BUILDDIR=build/debian
OPTS=""
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            OPTS="$OPTS $1 $(readlink -f $2)"
            RELOC_PKG=$2
            shift 2
            ;;
        "--builddir")
            BUILDDIR="$2"
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
rm -rf "$BUILDDIR"
mkdir -p "$BUILDDIR"/scylla-package
tar -C "$BUILDDIR"/scylla-package -xpf $RELOC_PKG
cd "$BUILDDIR"/scylla-package
exec bash -x -e ./scylla-tools/dist/debian/build_deb.sh $OPTS
