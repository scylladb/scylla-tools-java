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

from cassandra.metadata import RegisteredTableExtension
import io
import struct

# Add a handler for scylla_encryption_options extension to at least print 
# the info when doing "desc <table>".
#
# The end result will not be cut-and-paste usable; we'd need to modify the
# driver for this. But it is something. 
class Encr(RegisteredTableExtension):
    name = 'scylla_encryption_options'

    @classmethod
    def after_table_cql(cls, table_meta, ext_key, ext_blob):
        # For scylla_encryption_options, the blob is actually 
        # a serialized unorderd_map<string, string>. 
        bytes = io.BytesIO(ext_blob)

        def read_string():
            # strings are little endian 32-bit len + bytes
            utf_length = struct.unpack('<I', bytes.read(4))[0]
            return bytes.read(utf_length)

        def read_pair():
            # each map::value_type is written as <string><string>
            key = read_string()
            val = read_string()
            return key, val

        def read_map():
            # num elements
            len = struct.unpack('<I', bytes.read(4))[0]
            res = {}
            # x value_type pairs
            for x in range(0, len):
                p = read_pair()
                res[p[0]] = p[1]
            return res

        return "%s = %s" % (ext_key, read_map())

