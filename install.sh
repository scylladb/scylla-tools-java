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
  --help                   this helpful message
EOF
    exit 1
}

root=/
etcdir=/etc
nonroot=false

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
        "--help")
            shift 1
	    print_usage
            ;;
        *)
            print_usage
            ;;
    esac
done

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
install -m644 lib/*.zip "$rprefix"/share/cassandra/lib
install -m644 doc/cql3/CQL.css doc/cql3/CQL.html "$rprefix"/share/cassandra/doc/cql3
ln -srf "$rprefix"/share/cassandra/lib/apache-cassandra-$scylla_version-$scylla_release.jar "$rprefix"/share/cassandra/lib/apache-cassandra.jar
install -m644 dist/common/cassandra.in.sh "$rprefix"/share/cassandra/bin

if $nonroot; then
    sed -i -e "s#/var/lib/scylla#$rprefix#g" "$rprefix"/share/cassandra/bin/cassandra.in.sh
    sed -i -e "s#/etc/scylla#$retc/scylla#g" "$rprefix"/share/cassandra/bin/cassandra.in.sh
    sed -i -e "s#/opt/scylladb/#$rprefix/#g" "$rprefix"/share/cassandra/bin/cassandra.in.sh
fi

# scylla-tools
install -d -m755 "$rprefix"/share/cassandra/pylib
install -d -m755 "$retc"/bash_completion.d
install -m644 dist/common/nodetool-completion "$retc"/bash_completion.d
cp -r pylib/cqlshlib "$rprefix"/share/cassandra/pylib
for i in bin/{nodetool,sstableloader,cqlsh,cqlsh.py,scylla-sstableloader} tools/bin/{cassandra-stress,cassandra-stressd,sstabledump,sstablelevelreset,sstablemetadata,sstablerepairedset}; do
    bn=$(basename $i)
    install -m755 $i "$rprefix"/share/cassandra/bin
    if ! $nonroot; then
        echo "$thunk" > "$rusr"/bin/$bn
        chmod 755 "$rusr"/bin/$bn
    fi
done
