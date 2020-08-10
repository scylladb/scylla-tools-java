/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.scylladb.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ColumnDefinition.Kind;
import org.apache.cassandra.cql3.ColumnIdentifier;

class ColumnNamesMapping {

    private final Map<String, String> mappings;
    private final Map<String, String> reversedMappings;
    private final Map<CFMetaData, CFMetaData> cfMetaDatas = new HashMap<>();

    ColumnNamesMapping(final Map<String, String> mappings) {
        this.mappings = mappings;
        reversedMappings = new HashMap<>();
        for (Entry<String, String> mapping : mappings.entrySet()) {
            if (reversedMappings.containsKey(mapping.getValue())) {
                throw new RuntimeException("Reverse mapping is not unique, key already exists: " + mapping.getValue());
            }
            reversedMappings.put(mapping.getValue(), mapping.getKey());
        }
    }

    String getName(ColumnDefinition c) {
        final String name = c.name.toString();
        return ColumnIdentifier.getInterned(mappings.getOrDefault(name, name), true).toCQLString();
    }

    CFMetaData getMetadata(CFMetaData cfm) {
        CFMetaData result = cfMetaDatas.get(cfm);
        if (result == null) {
            result = createMetadata(cfm);
            cfMetaDatas.put(cfm, result);
        }
        return result;
    }

    private CFMetaData createMetadata(CFMetaData cfm) {
        final List<ColumnDefinition> columns = new ArrayList<>(cfm.getColumnMetadata().size());
        for (final ColumnDefinition def : cfm.allColumns()) {
            final String name = def.name.toString();
            final String newName = reversedMappings.getOrDefault(name, name);
            switch (def.kind) {
                case PARTITION_KEY:
                    columns.add(ColumnDefinition.partitionKeyDef(cfm.ksName, cfm.cfName, newName, def.type, def.position()));
                    break;
                case CLUSTERING:
                    columns.add(ColumnDefinition.clusteringDef(cfm.ksName, cfm.cfName, newName, def.type, def.position()));
                    break;
                case REGULAR:
                    columns.add(ColumnDefinition.regularDef(cfm.ksName, cfm.cfName, newName, def.type));
                    break;
                case STATIC:
                    columns.add(new ColumnDefinition(cfm.ksName, cfm.cfName, ColumnIdentifier.getInterned(newName, true),
                            def.type, ColumnDefinition.NO_POSITION, Kind.STATIC));
                    break;
            }
        }
        CFMetaData res = CFMetaData.create(cfm.ksName, cfm.cfName, cfm.cfId, cfm.isDense(),
                cfm.isCompound(), cfm.isSuper(), cfm.isCounter(), cfm.isView(), columns, cfm.partitioner);
        res.droppedColumns(cfm.getDroppedColumns());
        return res;
    }
}
