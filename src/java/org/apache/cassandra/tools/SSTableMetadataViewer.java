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
package org.apache.cassandra.tools;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.metadata.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Shows the contents of sstable metadata
 */
public class SSTableMetadataViewer
{
    private static final String GCGS_KEY = "gc_grace_seconds";

    private static enum SSTableFormat {
        JB(),
        KA(),
        LA(),
        LB(),
        MA(),
        MB(),
        MC(),
        MD(),
        ME();

        public static SSTableFormat fromString(String sstableVersion) {
            if(sstableVersion.equals("jb")) {
                return JB;
            } else if (sstableVersion.equals("ka")) {
                return KA;
            } else if (sstableVersion.equals("la")) {
                return LA;
            } else if (sstableVersion.equals("lb")) {
                return LB;
            } else if (sstableVersion.equals("ma")) {
                return MA;
            } else if (sstableVersion.equals("mb")) {
                return MB;
            } else if (sstableVersion.equals("mc")) {
                return MC;
            } else if (sstableVersion.equals("md")) {
                return MD;
            } else if (sstableVersion.equals("me")) {
                return ME;
            } else {
                throw new InvalidParameterException("SSTable Format: '" + sstableVersion + "' is unsupported.");
            }
        }
    }

    /**
     * This function prints into the given stream the requested text only if
     * the current sstable format is one of the relevant formats for this field.
     * @param format - the format of the current table
     * @param relevantFormats - formats that are relevant for the printed field
     * @param ps - a PrintStream into which to do the write.
     * @param formatStr - the format streang for printf.
     * @param printParams - the parameters for printf.
     * @return true if the string was printed.
     */
    static boolean printField(SSTableFormat format, SSTableFormat[] relevantFormats, PrintStream ps,
            String formatStr,Object...printParams) {
        boolean print = false;
        for (SSTableFormat currentFormat : relevantFormats) {
            if(currentFormat == format) {
                print = true;
                break;
            }
        }
        if (print) {
            ps.printf(formatStr, printParams);
        }
        return print;
    }

    /**
     * @param args a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        Option optGcgs = new Option(null, GCGS_KEY, true, "The "+GCGS_KEY+" to use when calculating droppable tombstones");

        Options options = new Options();
        options.addOption(optGcgs);
        CommandLine cmd = null;
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e)
        {
            printHelp(options, out);
        }

        if (cmd.getArgs().length == 0)
        {
            printHelp(options, out);
        }
        int gcgs = Integer.parseInt(cmd.getOptionValue(GCGS_KEY, "0"));
        Util.initDatabaseDescriptor();

        for (String fname : cmd.getArgs())
        {
            if (!new File(fname).exists())
            {
                out.println("No such file: " + fname);
                continue;
            }
                Descriptor descriptor = Descriptor.fromFilename(fname);
                Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
                ValidationMetadata validation = (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
                StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
                CompactionMetadata compaction = (CompactionMetadata) metadata.get(MetadataType.COMPACTION);
                CompressionMetadata compression = null;
                File compressionFile = new File(descriptor.filenameFor(Component.COMPRESSION_INFO));
                if (compressionFile.exists())
                    compression = CompressionMetadata.create(fname);
                SerializationHeader.Component header = (SerializationHeader.Component) metadata.get(MetadataType.HEADER);

                // We could have just use the SSTableFormat.Values() method but here we add another layer of defence
                // that will force someone to revisit this code whenever a new format is added.
                SSTableFormat[] jklFormats = { SSTableFormat.JB, SSTableFormat.KA, SSTableFormat.LA, SSTableFormat.LB };
                SSTableFormat[] mFormats = {SSTableFormat.MA, SSTableFormat.MB, SSTableFormat.MC, SSTableFormat.MD, SSTableFormat.ME};
                SSTableFormat[] allFormats = new SSTableFormat[jklFormats.length + mFormats.length];
                System.arraycopy(jklFormats, 0, allFormats, 0, jklFormats.length);
                System.arraycopy(mFormats, 0, allFormats, jklFormats.length, mFormats.length);

                SSTableFormat sstableFormat = SSTableFormat.fromString(descriptor.version.getVersion());

                // Lets check if the sstable_type is present in what we once considered to be all of the available formats.
                boolean supportedType = false;
                for (SSTableFormat fmt : allFormats) {
                    if (fmt == sstableFormat) {
                        supportedType = true;
                        break;
                    }
                }

                if (!supportedType) {
                    throw new InvalidParameterException("SSTable Format: '" + descriptor.version.getVersion() + "' metadata printing is unsupported.");
                }

                printField(sstableFormat, allFormats, out, "SSTable: %s%n", descriptor);

                if (validation != null)
                {
                    printField(sstableFormat, allFormats, out, "Partitioner: %s%n", validation.partitioner);
                    printField(sstableFormat, allFormats, out, "Bloom Filter FP chance: %f%n", validation.bloomFilterFPChance);
                }
                if (stats != null)
                {
                    printField(sstableFormat, allFormats, out, "Minimum timestamp: %s%n", stats.minTimestamp);
                    printField(sstableFormat, allFormats, out, "Maximum timestamp: %s%n", stats.maxTimestamp);
                    printField(sstableFormat, mFormats, out, "SSTable min local deletion time: %s%n", stats.minLocalDeletionTime);
                    printField(sstableFormat, allFormats, out, "SSTable max local deletion time: %s%n", stats.maxLocalDeletionTime);
                    printField(sstableFormat, allFormats, out, "Compressor: %s%n", compression != null ? compression.compressor().getClass().getName() : "-");
                    if (compression != null)
                        printField(sstableFormat, allFormats, out, "Compression ratio: %s%n", stats.compressionRatio);
                    printField(sstableFormat, mFormats, out, "TTL min: %s%n", stats.minTTL);
                    printField(sstableFormat, mFormats, out, "TTL max: %s%n", stats.maxTTL);

                    if (validation != null) {
                        IPartitioner partitioner;
                        if (header == null) {
                            partitioner = FBUtilities.newPartitioner(validation.partitioner);
                        } else {
                            partitioner = FBUtilities.newPartitioner(descriptor);
                        }
                        printMinMaxToken(descriptor, partitioner, header, out);
                    }

                    if (header != null && header.getClusteringTypes().size() == stats.minClusteringValues.size())
                    {
                        List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
                        List<ByteBuffer> minClusteringValues = stats.minClusteringValues;
                        List<ByteBuffer> maxClusteringValues = stats.maxClusteringValues;
                        String[] minValues = new String[clusteringTypes.size()];
                        String[] maxValues = new String[clusteringTypes.size()];
                        for (int i = 0; i < clusteringTypes.size(); i++)
                        {
                            minValues[i] = clusteringTypes.get(i).getString(minClusteringValues.get(i));
                            maxValues[i] = clusteringTypes.get(i).getString(maxClusteringValues.get(i));
                        }
                        printField(sstableFormat, allFormats, out, "minClustringValues: %s%n", Arrays.toString(minValues));
                        printField(sstableFormat, allFormats, out, "maxClustringValues: %s%n", Arrays.toString(maxValues));
                    }
                    printField(sstableFormat, allFormats, out, "Estimated droppable tombstones: %s%n", stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000) - gcgs));
                    printField(sstableFormat, allFormats, out, "SSTable Level: %d%n", stats.sstableLevel);
                    printField(sstableFormat, allFormats, out, "Repaired at: %d%n", stats.repairedAt);
                    printField(sstableFormat, mFormats, out, "Replay positions covered: %s%n", stats.commitLogIntervals);
                    printField(sstableFormat, mFormats, out, "totalColumnsSet: %s%n", stats.totalColumnsSet);
                    printField(sstableFormat, mFormats, out, "totalRows: %s%n", stats.totalRows);
                    if (stats.originatingHostId != null) {
                        printField(sstableFormat, mFormats, out, "originatingHostId: %s%n", stats.originatingHostId);
                    }
                    printField(sstableFormat, allFormats, out, "Estimated tombstone drop times:");

                    for (Map.Entry<Number, long[]> entry : stats.estimatedTombstoneDropTime.getAsMap().entrySet())
                    {
                        out.printf("%-10s:%10s%n",entry.getKey().intValue(), entry.getValue()[0]);
                    }
                    printHistograms(stats, out);
                }
                if (compaction != null)
                {
                    printField(sstableFormat, allFormats, out, "Estimated cardinality: %s%n", compaction.cardinalityEstimator.cardinality());
                }
                if (header != null)
                {
                    EncodingStats encodingStats = header.getEncodingStats();
                    AbstractType<?> keyType = header.getKeyType();
                    List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
                    Map<ByteBuffer, AbstractType<?>> staticColumns = header.getStaticColumns();
                    Map<String, String> statics = staticColumns.entrySet().stream()
                                                               .collect(Collectors.toMap(
                                                                e -> UTF8Type.instance.getString(e.getKey()),
                                                                e -> e.getValue().toString()));
                    Map<ByteBuffer, AbstractType<?>> regularColumns = header.getRegularColumns();
                    Map<String, String> regulars = regularColumns.entrySet().stream()
                                                                 .collect(Collectors.toMap(
                                                                 e -> UTF8Type.instance.getString(e.getKey()),
                                                                 e -> e.getValue().toString()));

                    printField(sstableFormat, mFormats, out, "EncodingStats minTTL: %s%n", encodingStats.minTTL);
                    printField(sstableFormat, mFormats, out, "EncodingStats minLocalDeletionTime: %s%n", encodingStats.minLocalDeletionTime);
                    printField(sstableFormat, mFormats, out, "EncodingStats minTimestamp: %s%n", encodingStats.minTimestamp);
                    printField(sstableFormat, mFormats, out, "KeyType: %s%n", keyType.toString());
                    printField(sstableFormat, mFormats, out, "ClusteringTypes: %s%n", clusteringTypes.toString());
                    printField(sstableFormat, mFormats, out, "StaticColumns: {%s}%n", FBUtilities.toString(statics));
                    printField(sstableFormat, mFormats, out, "RegularColumns: {%s}%n", FBUtilities.toString(regulars));
                }
            // TODO: reindent
        }
    }

    private static void printHelp(Options options, PrintStream out)
    {
        out.println();
        new HelpFormatter().printHelp("Usage: sstablemetadata [--"+GCGS_KEY+" n] <sstable filenames>", "Dump contents of given SSTable to standard output in JSON format.", options, "");
        System.exit(1);
    }

    private static void printHistograms(StatsMetadata metadata, PrintStream out)
    {
        long[] offsets = metadata.estimatedPartitionSize.getBucketOffsets();
        long[] ersh = metadata.estimatedPartitionSize.getBuckets(false);
        long[] ecch = metadata.estimatedColumnCount.getBuckets(false);

        out.println(String.format("%-10s%18s%18s",
                                  "Count", "Row Size", "Cell Count"));

        for (int i = 0; i < offsets.length; i++)
        {
            out.println(String.format("%-10d%18s%18s",
                                      offsets[i],
                                      (i < ersh.length ? ersh[i] : ""),
                                      (i < ecch.length ? ecch[i] : "")));
        }
    }

    private static void printMinMaxToken(Descriptor descriptor, IPartitioner partitioner, SerializationHeader.Component header, PrintStream out) throws IOException
    {
        File summariesFile = new File(descriptor.filenameFor(Component.SUMMARY));
        if (!summariesFile.exists())
            return;

        try (DataInputStream iStream = new DataInputStream(new FileInputStream(summariesFile)))
        {
            Pair<DecoratedKey, DecoratedKey> firstLast = new IndexSummary.IndexSummarySerializer().deserializeFirstLastKey(iStream, partitioner, descriptor.version.hasSamplingLevel());

            if (header != null) {
                AbstractType<?> keyType = header.getKeyType();
                out.printf("First token: %s (key=%s)%n", firstLast.left.getToken(), keyType.getString(firstLast.left.getKey()));
                out.printf("Last token: %s (key=%s)%n", firstLast.right.getToken(), keyType.getString(firstLast.right.getKey()));
            } else {
                out.printf("First token: %s %n", firstLast.left.getToken());
                out.printf("Last token: %s %n", firstLast.right.getToken());
            }
        }
    }

}
