# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "x$CASSANDRA_HOME" = "x" ]; then
    CASSANDRA_HOME="`dirname $0`/../.."
fi

# The directory where Cassandra's configs live (required)
if [ "x$CASSANDRA_CONF" = "x" ]; then
    CASSANDRA_CONF="$CASSANDRA_HOME/conf"
fi

# This can be the path to a jar file, or a directory containing the
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
cassandra_bin="$CASSANDRA_HOME/build/classes/main"
cassandra_bin="$cassandra_bin:$CASSANDRA_HOME/build/classes/stress"
cassandra_bin="$cassandra_bin:$CASSANDRA_HOME/build/classes/thrift"
#cassandra_bin="$cassandra_home/build/cassandra.jar"

# the default location for commitlogs, sstables, and saved caches
# if not set in cassandra.yaml
cassandra_storagedir="$CASSANDRA_HOME/data"

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk6

# Scylla adaption. Some one will still have to find us SCYLLA_HOME
# or place us there.
if [ "x$SCYLLA_HOME" = "x" ]; then
    SCYLLA_HOME="`dirname $0`/../.."
fi
if [ "x$SCYLLA_CONF" = "x" ]; then
    SCYLLA_CONF="$SCYLLA_HOME/conf"
fi
if [ -f "$SCYLLA_CONF/scylla.yaml" ]; then
    if [ -f "$SCYLLA_CONF/cassandra.yaml" ]; then
	CASSANDRA_CONF=$SCYLLA_CONF
    else
	# Create a temp config dir for just this execution
	TMPCONF=`mktemp -d`
	trap "rm -rf $TMPCONF" EXIT
	cp -a "$CASSANDRA_CONF"/* "$TMPCONF"
	cp -a "$SCYLLA_CONF"/* "$TMPCONF"
	# Filter out scylla specific options that make
	# cassandra options parser go boom.
	# Right now there is _one_, which is nice and easy.
	# If more and bigger ones come up, we might need
	# a proper yaml parser. Or simply replace the checker
	# in the java code. Latter is simpler.
	grep -v \
	     -e compaction_large_partition_warning_threshold_mb \
	     "$TMPCONF/scylla.yaml" > "$TMPCONF/cassandra.yaml"
	CASSANDRA_CONF=$TMPCONF
    fi
fi

# Scylla adaption. Some one will still have to find us SCYLLA_HOME
# or place us there. 
if [ "x$SCYLLA_HOME" = "x" ]; then
    SCYLLA_HOME="`dirname $0`/../.."
fi
if [ "x$SCYLLA_CONF" = "x" ]; then
    SCYLLA_CONF="$SCYLLA_HOME/conf"
fi
if [ -f "$SCYLLA_CONF/scylla.yaml" ]; then
    if [ -f "$SCYLLA_CONF/cassandra.yaml" ]; then
	CASSANDRA_CONF=$SCYLLA_CONF
    else
	# Create a temp config dir for just this execution
	TMPCONF=`mktemp -d`
	trap "rm -rf $TMPCONF" EXIT
	cp -a "$CASSANDRA_CONF"/* "$TMPCONF"
	cp -a "$SCYLLA_CONF"/* "$TMPCONF"	
	# Filter out scylla specific options that make
	# cassandra options parser go boom.
	# Also add attributes not present in scylla.yaml
	# but required by cassandra. 
	`dirname $0`/filter_cassandra_attributes.py \
		    "$CASSANDRA_CONF/cassandra.yaml" \
		    "$TMPCONF/scylla.yaml" \
		    > "$TMPCONF/cassandra.yaml"	
	CASSANDRA_CONF=$TMPCONF
    fi
fi

# The java classpath (required)
CLASSPATH="$CASSANDRA_CONF:$cassandra_bin"

for jar in "$CASSANDRA_HOME"/tools/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done
for jar in "$CASSANDRA_HOME"/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done




  
