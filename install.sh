#!/bin/bash
#
# Copyright (C) 2019 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

set -e

print_usage() {
    cat <<EOF
Usage: install.sh [options]

Options:
  --root /path/to/root     alternative install root (default /)
  --prefix /prefix         directory prefix (default /usr)
  --etcdir /etc            specify etc directory path (default /etc)
  --nonroot                install Scylla without required root priviledge
  --packaging              use install.sh for packaging
  --help                   this helpful message
EOF
    exit 1
}

root=/
etcdir=/etc
nonroot=false
packaging=false

while [ $# -gt 0 ]; do
    case "$1" in
        "--root")
            root="$2"
            shift 2
            ;;
        "--prefix")
            prefix="$2"
            shift 2
            ;;
        "--etcdir")
            etcdir="$2"
            shift 2
            ;;
        "--nonroot")
            nonroot=true
            shift 1
            ;;
        "--packaging")
            packaging=true
            shift 1
            ;;
        "--help")
            shift 1
	    print_usage
            ;;
        *)
            print_usage
            ;;
    esac
done

if ! $packaging; then
    if [ -n "$JAVA_HOME" ]; then
        for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
            if [ -x "$java" ]; then
                JAVA="$java"
                break
            fi
        done
    else
        JAVA=java
    fi

    if ! builtin command -v $JAVA > /dev/null; then
        echo "Please install openjdk-11 before running install.sh."
        exit 1
    fi
fi

if [ -z "$prefix" ]; then
    if $nonroot; then
        prefix=~/scylladb
    else
        prefix=/opt/scylladb
    fi
fi

scylla_version=$(cat SCYLLA-VERSION-FILE)
scylla_release=$(cat SCYLLA-RELEASE-FILE)

rprefix=$(realpath -m "$root/$prefix")
if ! $nonroot; then
    retc="$root/$etcdir"
    rusr="$root/usr"
else
    retc="$rprefix/$etcdir"
fi

install -d -m755 "$rprefix"/share/cassandra/bin
if ! $nonroot; then
    install -d -m755 "$rusr"/bin
fi

thunk="#!/usr/bin/env bash
b=\"\$(basename \"\$0\")\"
bindir=\"$prefix/share/cassandra/bin\"
exec -a \"\$0\" \"\$bindir/\$b\" \"\$@\""

# scylla-tools-core
install -d -m755 "$retc"/scylla/cassandra
install -d -m755 "$rprefix"/share/cassandra/lib
install -d -m755 "$rprefix"/share/cassandra/doc
install -d -m755 "$rprefix"/share/cassandra/doc/cql3
install -m644 conf/cassandra-env.sh "$retc"/scylla/cassandra
install -m644 conf/logback.xml "$retc"/scylla/cassandra
install -m644 conf/logback-tools.xml "$retc"/scylla/cassandra
install -m644 conf/jvm*.options "$retc"/scylla/cassandra
install -m644 lib/*.jar "$rprefix"/share/cassandra/lib
install -m644 doc/cql3/CQL.css doc/cql3/CQL.html "$rprefix"/share/cassandra/doc/cql3
ln -srf "$rprefix"/share/cassandra/lib/apache-cassandra-$scylla_version-$scylla_release.jar "$rprefix"/share/cassandra/lib/apache-cassandra.jar
install -m644 dist/common/cassandra.in.sh "$rprefix"/share/cassandra/bin

if $nonroot; then
    sed -i -e "s#/var/lib/scylla#$rprefix#g" "$rprefix"/share/cassandra/bin/cassandra.in.sh
    sed -i -e "s#/etc/scylla#$retc/scylla#g" "$rprefix"/share/cassandra/bin/cassandra.in.sh
    sed -i -e "s#/opt/scylladb/#$rprefix/#g" "$rprefix"/share/cassandra/bin/cassandra.in.sh
fi

install_tool_bin () {
    bin_src=$1
    if [ $# -eq 1 ]; then
        bin_dest=$(basename "$bin_src")
    else
        bin_dest=$2
    fi
    install -m755 "$bin_src" "$rprefix"/share/cassandra/bin/"$bin_dest"
    if ! $nonroot; then
        echo "$thunk" > "$rusr"/bin/"$bin_dest"
        chmod 755 "$rusr"/bin/"$bin_dest"
    fi
}

# scylla-tools
for i in bin/{sstableloader,scylla-sstableloader} tools/bin/{cassandra-stress,cassandra-stressd,sstabledump,sstablelevelreset,sstablemetadata,sstablerepairedset}; do
    install_tool_bin "$i"
done

# Don't create the thunk in $rusr/bin for nodetool, this command is now only
# available as a backup in $rprefix/share/cassandra/bin/nodetool, in case the
# native nodetool doesn't work.
install -m755 bin/nodetool "$rprefix"/share/cassandra/bin/nodetool
