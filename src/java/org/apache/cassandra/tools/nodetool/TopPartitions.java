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
package org.apache.cassandra.tools.nodetool;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.join;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularDataSupport;

import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TableMetrics.Sampler;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

@Command(name = "toppartitions", description = "Sample and print the most active partitions for a given column family")
public class TopPartitions extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <cfname> <duration>", description = "The keyspace, column family name, and duration in milliseconds")
    private List<String> args = new ArrayList<>();
    @Option(name = {"-d", "--duration"}, description = "Duration in milliseconds")
    private int duration = 5000;
    @Option(name = "--ks-filters", description = "Comma separated list of keyspaces to include in the query (Default: all)")
    private String keyspaceFilters = null;
    @Option(name = "--cf-filters", description = "Comma separated list of column families to include in the query in the form of 'ks:cf' (Default: all)")
    private String tableFilters = null;
    @Option(name = "-s", description = "Capacity of stream summary, closer to the actual cardinality of partitions will yield more accurate results (Default: 256)")
    private int size = 256;
    @Option(name = "-k", description = "Number of the top partitions to list (Default: 10)")
    private int topCount = 10;
    @Option(name = "-a", description = "Comma separated list of samplers to use (Default: all)")
    private String samplers = join(TableMetrics.Sampler.values(), ',');
    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(topCount < size, "TopK count (-k) option must be smaller then the summary capacity (-s)");
        // generate the list of samplers
        List<Sampler> targets = Lists.newArrayList();
        for (String s : samplers.split(","))
        {
            try
            {
                targets.add(Sampler.valueOf(s.toUpperCase()));
            } catch (Exception e)
            {
                throw new IllegalArgumentException(s + " is not a valid sampler, choose one of: " + join(Sampler.values(), ", "));
            }
        }

        if (args.size() == 3) {
            tableFilters = args.get(0) + ":" + args.get(1);
            duration = Integer.valueOf(args.get(2));
        } else if (!args.isEmpty()) {
            checkArgument(false, "toppartitions requires either a keyspace, column family name and duration or no arguments at all");
        }

        List<String> keyspaceFiltersList = null;
        List<String> tableFiltersList = null;

        if (keyspaceFilters != null)
            keyspaceFiltersList = Stream.of(keyspaceFilters.split(",")).collect(Collectors.toList());
        
        if (tableFilters != null)
            tableFiltersList = Stream.of(tableFilters.split(",")).collect(Collectors.toList());

        Map<Sampler, CompositeData> results;
        try
        {
            results = probe.getToppartitions(keyspaceFiltersList, tableFiltersList, size, duration, topCount, targets);
        } catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
        boolean first = true;
        for(Entry<Sampler, CompositeData> result : results.entrySet())
        {
            CompositeData sampling = result.getValue();
            // weird casting for http://bugs.sun.com/view_bug.do?bug_id=6548436
            List<CompositeData> topk = (List<CompositeData>) (Object) Lists.newArrayList(((TabularDataSupport) sampling.get("partitions")).values());
            Collections.sort(topk, new Ordering<CompositeData>()
            {
                public int compare(CompositeData left, CompositeData right)
                {
                    return Long.compare((long) right.get("count"), (long) left.get("count"));
                }
            });
            if(!first)
                System.out.println();
            System.out.println(result.getKey().toString()+ " Sampler:");
            System.out.printf("  Cardinality: ~%d (%d capacity)%n", sampling.get("cardinality"), size);
            System.out.printf("  Top %d partitions:%n", topCount);
            if (topk.size() == 0)
            {
                System.out.println("\tNothing recorded during sampling period...");
            } else
            {
                int offset = 0;
                for (CompositeData entry : topk)
                    offset = Math.max(offset, entry.get("string").toString().length());
                System.out.printf("\t%-" + offset + "s%10s%10s%n", "Partition", "Count", "+/-");
                for (CompositeData entry : topk)
                    System.out.printf("\t%-" + offset + "s%10d%10d%n", entry.get("string").toString(), entry.get("count"), entry.get("error"));
            }
            first = false;
        }
    }
}