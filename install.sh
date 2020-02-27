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
  --packaging               use install.sh for packaging
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
    has_java=false
    has_py2=false
    has_pyyaml=false
    if [ -x /usr/bin/java ]; then
        javaver=$(/usr/bin/java -version 2>&1|head -n1|cut -f 3 -d " ")
        if [[ "$javaver" =~ ^\"1.8.0 ]]; then
            has_java=true
        fi
    fi
    for p in /usr/bin/python /usr/bin/python2; do
        if [ -x "$p" ]; then
            $p -c 'import sys; sys.exit(not (0x020700b0 < sys.hexversion < 0x03000000))' 2>/dev/null &&:
            if [ $? -eq 0 ]; then
                has_py2=true
                $p -c 'import yaml' 2>/dev/null &&:
                if [ $? -eq 0 ]; then
                    has_pyyaml=true
                fi
                break
            fi
        fi
    done
    if ! $has_java || ! $has_py2 || ! $has_pyyaml; then
        echo "Please install following package before running install.sh:"
        if ! $has_java; then
            echo "  openjdk-8"
        fi
        if ! $has_py2; then
            echo "  python2"
        fi
        if ! $has_pyyaml; then
            echo "  python2-pyyaml"
        fi
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
install -d -m755 "$rprefix"/share/cassandra/lib/licenses
install -d -m755 "$rprefix"/share/cassandra/doc
install -d -m755 "$rprefix"/share/cassandra/doc/cql3
install -m644 conf/cassandra-env.sh "$retc"/scylla/cassandra
install -m644 conf/logback.xml "$retc"/scylla/cassandra
install -m644 conf/logback-tools.xml "$retc"/scylla/cassandra
install -m644 conf/jvm.options "$retc"/scylla/cassandra
install -m644 lib/*.jar "$rprefix"/share/cassandra/lib
install -m644 lib/*.zip "$rprefix"/share/cassandra/lib
install -m644 lib/licenses/* "$rprefix"/share/cassandra/lib/licenses
install -m644 doc/cql3/CQL.css doc/cql3/CQL.html "$rprefix"/share/cassandra/doc/cql3
ln -srf "$rprefix"/share/cassandra/lib/apache-cassandra-$scylla_version-$scylla_release.jar "$rprefix"/share/cassandra/lib/apache-cassandra.jar
install -m644 dist/common/cassandra.in.sh "$rprefix"/share/cassandra/bin
for i in tools/bin/{filter_cassandra_attributes.py,cassandra_attributes.py} bin/scylla-sstableloader; do
    bn=$(basename $i)
    install -m755 $i "$rprefix"/share/cassandra/bin
    if ! $nonroot; then
        echo "$thunk" > "$rusr"/bin/$bn
        chmod 755 "$rusr"/bin/$bn
    fi
done
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
for i in bin/{nodetool,sstableloader,cqlsh,cqlsh.py} tools/bin/{cassandra-stress,cassandra-stressd,sstabledump,sstablelevelreset,sstablemetadata,sstablerepairedset}; do
    bn=$(basename $i)
    install -m755 $i "$rprefix"/share/cassandra/bin
    if ! $nonroot; then
        echo "$thunk" > "$rusr"/bin/$bn
        chmod 755 "$rusr"/bin/$bn
    fi
done
