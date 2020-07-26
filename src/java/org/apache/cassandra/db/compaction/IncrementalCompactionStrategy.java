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
 * Copyright (C) 2019 ScyllaDB
 */
package org.apache.cassandra.db.compaction;

import java.util.*;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;

public class IncrementalCompactionStrategy extends SizeTieredCompactionStrategy
{
    protected IncrementalCompactionStrategyOptions sizeTieredOptions;

    public IncrementalCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.sizeTieredOptions = new IncrementalCompactionStrategyOptions(options);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = IncrementalCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("IncrementalCompactionStrategy[%s/%s]",
            cfs.getMinimumCompactionThreshold(),
            cfs.getMaximumCompactionThreshold());
    }
}
