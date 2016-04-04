/*
 * Copyright 2016 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

import static org.apache.cassandra.io.sstable.SSTableReader.openForBatch;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composite.EOC;
import org.apache.cassandra.db.marshal.AbstractCompositeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic sstable -> CQL statements translator.
 * 
 * Goes through a table, in token range order if possible, and tries to generate
 * CQL delete/update statements to match the perceived sstable atom cells read.
 * 
 * This works fairly ok for most normal types, as well as frozen collections.
 * However, it breaks down completely for non-frozen lists.
 * 
 * In cassandra, a list is stored as actually a "map" of time UUID -> value.
 * Since maps in turn are just prefixed thrift column names (the key is part of
 * column name), UUID sorting makes sure the list remains in order. However,
 * actually determining what is index 0, 1, 2 etc cannot be done until the whole
 * list is materialized (reading backwards etc yadayada). Since we a.) Read
 * forwards b.) Might not be reading all relevant sstables we have no idea what
 * is actually in the CQL list. Thus we cannot generate any of the valid
 * expressions to manipulate the list in question.
 * 
 * As a "workaround", the code will instead generate map expressions, using the
 * actual time UUID keys for all list ops. This is of course bogus, and indeed
 * will result in wild errors from for example Origin getting any such
 * statements.
 * 
 * Compact storage column families are not handled yet.
 * 
 */
public class SSTableToCQL {

    /**
     * SSTable row worker.
     *
     * @author calle
     *
     */
    private static class RowBuilder {
        /** Interface for partial generating CQL statements */
        private static interface ColumnOp {
            boolean canDoInsert();

            String apply(ColumnDefinition c, List<Object> params);
        }

        private static class DeleteSetEntry implements ColumnOp {
            private final Object key;

            public DeleteSetEntry(Object key) {
                this.key = key;
            }

            @Override
            public String apply(ColumnDefinition c, List<Object> params) {
                params.add(Collections.singleton(key));
                return " = " + c.name.toString() + " - ?";
            }

            @Override
            public boolean canDoInsert() {
                return false;
            }
        }

        // CQL operations
        private static enum Op {
            NONE, UPDATE, DELETE, INSERT
        }

        private static class SetColumn implements ColumnOp {
            private final Object value;

            public SetColumn(Object value) {
                this.value = value;
            }

            @Override
            public String apply(ColumnDefinition c, List<Object> params) {
                params.add(value);
                return " = ?";
            }

            @Override
            public boolean canDoInsert() {
                return true;
            }
        }

        private static final SetColumn SET_NULL = new SetColumn(null) {
            @Override
            public boolean canDoInsert() {
                return false;
            }
        };

        private static class SetMapEntry implements ColumnOp {
            private final Object key;

            private final Object value;

            public SetMapEntry(Object key, Object value) {
                this.key = key;
                this.value = value;
            }

            @Override
            public String apply(ColumnDefinition c, List<Object> params) {
                params.add(key);
                params.add(value);
                return "[?] = ?";
            }

            @Override
            public boolean canDoInsert() {
                return false;
            }
        }

        private static class SetListEntry implements ColumnOp {
            private final Object key;
            private final Object value;

            public SetListEntry(Object key, Object value) {
                this.key = key;
                this.value = value;
            }

            @Override
            public String apply(ColumnDefinition c, List<Object> params) {
                params.add(key);
                params.add(value);
                return "[SCYLLA_TIMEUUID_LIST_INDEX(?)] = ?";
            }

            @Override
            public boolean canDoInsert() {
                return false;
            }
        }

        private static class SetSetEntry implements ColumnOp {
            private final Object key;

            public SetSetEntry(Object key) {
                this.key = key;
            }

            @Override
            public String apply(ColumnDefinition c, List<Object> params) {
                params.add(Collections.singleton(key));
                return " = " + c.name.toString() + " + ?";
            }

            @Override
            public boolean canDoInsert() {
                return false;
            }
        }

        private static final long invalidTimestamp = Long.MIN_VALUE;
        private static final int invalidTTL = Integer.MIN_VALUE;

        private final Client client;

        Op op;
        Object callback;
        CFMetaData cfMetaData;
        DecoratedKey key;
        long timestamp;
        int ttl;
        Map<ColumnDefinition, ColumnOp> values = new HashMap<>();
        Map<ColumnDefinition, Object> where = new HashMap<>();
        Map<ColumnDefinition, Object> tmp = new HashMap<>();
        TreeSet<OnDiskAtom> sortedAtoms = new TreeSet<>(new Comparator<OnDiskAtom>() {
            @Override
            public int compare(OnDiskAtom o1, OnDiskAtom o2) {
                int diff = (int) (o1.timestamp() - o2.timestamp());
                if (diff == 0) {
                    diff = o1.name().toByteBuffer().compareTo(o2.name().toByteBuffer());
                }
                return diff;
            }
        });

        public RowBuilder(Client client) {
            this.client = client;
        }

        /**
         * Figure out the "WHERE" clauses (except for PK) for a column name
         *
         * @param composite
         *            Thrift/cassandra name composite
         * @param timestamp
         * @param ttl
         */
        private void addWhere(Composite composite, long timestamp, int ttl) {
            updateTimestamp(timestamp);
            updateTTL(ttl);

            if (composite.isStatic()) {
                return;
            }
            tmp.clear();
            for (int i = 0;;) {
                for (ColumnDefinition c : cfMetaData.clusteringColumns()) {
                    if (i == composite.size()) {
                        break;
                    }
                    Object value = c.type.compose(composite.get(i));
                    tmp.put(c, value);
                    ++i;
                }
                break;
            }
            // If we get clustering that differ from already
            // existing data, we need to flush the row
            if (!where.isEmpty() && !where.equals(tmp)) {
                finish();
            }
            Map<ColumnDefinition, Object> m = where;
            where = tmp;
            tmp = m;
            tmp.clear();
        }

        // Begin a new partition (cassandra "Row")
        private void begin(Object callback, DecoratedKey key, CFMetaData cfMetaData) {
            this.callback = callback;
            this.key = key;
            this.cfMetaData = cfMetaData;
            sortedAtoms.clear();
            clear();
        }

        private void clear() {
            op = Op.NONE;
            values.clear();
            where.clear();
            timestamp = invalidTimestamp;
            ttl = invalidTTL;
        }

        // Delete a whole cql colummn
        private void deleteColumn(Composite composite, ColumnDefinition c, ColumnOp object, long timestamp) {
            if (values.containsKey(c)) {
                finish();
            }
            if (c.type.isCollection() && !c.type.isMultiCell() && object != null) {
                updateColumn(composite, c, object, timestamp, invalidTTL);
            } else {
                setOp(Op.DELETE, timestamp);
                values.put(c, null);
            }
        }

        // Delete the whole cql row
        private void deleteCqlRow(Composite max, long timestamp) {
            if (!values.isEmpty()) {
                finish();
            }
            setOp(Op.DELETE, timestamp);
            addWhere(max, timestamp, invalidTTL);
        }

        // Delete the whole partition
        private void deletePartition(DecoratedKey key, DeletionTime topLevelDeletion) {
            setOp(Op.DELETE, topLevelDeletion.markedForDeleteAt);
            finish();
        };

        // Genenerate the CQL query for this CQL row
        private void finish() {
            // Nothing?
            if (op == Op.NONE) {
                clear();
                return;
            }

            List<Object> params = new ArrayList<>();
            StringBuilder buf = new StringBuilder();

            buf.append(op.toString());

            if (op == Op.UPDATE) {
                writeColumnFamily(buf);
                // Timestamps can be sent using statement options.
                // TTL cannot. But just to be extra funny, at least
                // origin does not seem to respect the timestamp
                // in statement, so we'll add them to the CQL string as well.
                writeUsingTimestamp(buf);
                writeUsingTTL(buf);
                buf.append(" SET ");
            }

            if (op == Op.INSERT) {
                buf.append(" INTO");
                writeColumnFamily(buf);
            }

            int i = 0;
            for (Map.Entry<ColumnDefinition, ColumnOp> e : values.entrySet()) {
                ColumnDefinition c = e.getKey();
                ColumnOp o = e.getValue();
                String s = o != null ? o.apply(c, params) : null;

                if (op != Op.INSERT) {
                    if (i++ > 0) {
                        buf.append(", ");
                    }
                    ensureWhitespace(buf);
                    buf.append(c.name.toString());
                    if (s != null) {
                        buf.append(s);
                    }
                }
            }

            if (op == Op.DELETE) {
                buf.append(" FROM");
                writeColumnFamily(buf);
                writeUsingTimestamp(buf);
            }

            if (op != Op.INSERT) {
                buf.append(" WHERE ");
            }

            // Add "WHERE pk1 = , pk2 = "
            List<ColumnDefinition> pk = cfMetaData.partitionKeyColumns();
            ByteBuffer bufs[];
            AbstractType<?> type = cfMetaData.getKeyValidator();
            if (type instanceof AbstractCompositeType) {
                bufs = ((AbstractCompositeType) type).split(key.getKey());
            } else {
                bufs = new ByteBuffer[] { key.getKey() };
            }
            int k = 0;
            for (ColumnDefinition c : pk) {
                where.put(c, c.type.compose(bufs[k++]));
            }

            params.addAll(where.values());

            i = 0;
            if (op == Op.INSERT) {
                buf.append('(');
                for (ColumnDefinition c : values.keySet()) {
                    if (i++ > 0) {
                        buf.append(',');
                    }
                    buf.append(c.name.toString());
                }
                for (ColumnDefinition c : where.keySet()) {
                    if (i++ > 0) {
                        buf.append(',');
                    }
                    buf.append(c.name.toString());
                }
                buf.append(") values (");
                for (i = 0; i < values.size() + where.size(); ++i) {
                    if (i > 0) {
                        buf.append(',');
                    }
                    buf.append('?');
                }
                buf.append(')');
                writeUsingTimestamp(buf);
                writeUsingTTL(buf);
            } else {
                for (Map.Entry<ColumnDefinition, Object> e : where.entrySet()) {
                    if (i++ > 0) {
                        buf.append(" AND ");
                    }
                    buf.append(e.getKey().name.toString());
                    buf.append(" = ?");
                }
            }
            buf.append(';');

            makeStatement(key, timestamp, buf.toString(), params);
            clear();
        }

        private void writeUsingTTL(StringBuilder buf) {
            if (ttl != invalidTTL) {
                ensureWhitespace(buf);
                if (timestamp == invalidTimestamp) {
                    buf.append("USING ");
                } else {
                    buf.append("AND ");
                }
                buf.append(" TTL " + ttl);
            }
        }

        private void ensureWhitespace(StringBuilder buf) {
            if (buf.length() > 0 && !Character.isWhitespace(buf.charAt(buf.length() - 1))) {
                buf.append(' ');
            }
        }

        private void writeUsingTimestamp(StringBuilder buf) {
            if (timestamp != invalidTimestamp) {
                ensureWhitespace(buf);
                buf.append("USING TIMESTAMP " + timestamp);
            }
        }

        // Dispatch the CQL
        private void makeStatement(DecoratedKey key, long timestamp, String what, List<Object> objects) {
            client.processStatment(callback, key, timestamp, what, objects);
        }

        // process an actual cell (data or tombstone)
        private void process(Cell cell) {
            CellNameType comparator = cfMetaData.comparator;
            CellName name = cell.name();
            ColumnDefinition c = cfMetaData.getColumnDefinition(name);

            if (c == null) {
                ColumnIdentifier id = name.cql3ColumnName(cfMetaData);
                if (id != null && name.size() > 1 && id.bytes.hasRemaining()) {
                    // not cql column marker. (?)
                    logger.warn("No column found: {}", comparator.getString(name));
                }
                if (cfMetaData.clusteringColumns().isEmpty()) {
                    return;
                }
                setOp(Op.INSERT, cell.timestamp());
                addWhere(cell.name(), cell.timestamp(), ttlFor(cell));
                return;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Processing {}", comparator.getString(name));
            }
            AbstractType<?> type = c.type;

            ColumnOp cop = null;

            try {
                if (cell.name().isCollectionCell()) {
                    CollectionType<?> ctype = (CollectionType<?>) type;

                    Object key = ctype.nameComparator().compose(cell.name().collectionElement());
                    Object val = cell.isLive() ? ctype.valueComparator().compose(cell.value()) : null;

                    switch (ctype.kind) {
                    case MAP:
                        cop = new SetMapEntry(key, val);
                        break;
                    case LIST:
                        cop = new SetListEntry(key, val);
                        break;
                    case SET:
                        cop = cell.isLive() ? new SetSetEntry(key) : new DeleteSetEntry(key);
                        break;
                    }
                } else if (cell.isLive()) {
                    cop = new SetColumn(type.compose(cell.value()));
                } else {
                    cop = SET_NULL;
                }

            } catch (Exception e) {
                logger.error("Could not compose value for " + comparator.getString(name), e);
                throw e;
            }

            updateColumn(cell.name(), c, cop, cell.timestamp(), ttlFor(cell));
        }

        private int ttlFor(Cell cell) {
            int ttl = invalidTTL;
            if (cell instanceof ExpiringCell) {
                ttl = ((ExpiringCell) cell).getTimeToLive();
            }
            return ttl;
        }

        // Process an SSTable row (partial partition)
        private void process(Object callback, OnDiskAtomIterator row) {
            ColumnFamily columnFamily = row.getColumnFamily();
            CFMetaData cfMetaData = columnFamily.metadata();
            DeletionInfo deletionInfo = columnFamily.deletionInfo();
            DecoratedKey key = row.getKey();

            begin(callback, key, cfMetaData);

            if (!deletionInfo.isLive()) {
                deletePartition(key, deletionInfo.getTopLevelDeletion());
                return;
            }

            Cell prev = null;

            while (row.hasNext()) {
                OnDiskAtom atom = row.next();
                if (atom instanceof Cell) {
                    Cell cell = (Cell) atom;
                    if (prev != null && !cell.name().isSameCQL3RowAs(cfMetaData.comparator, prev.name())) {
                        processAtoms();
                    }
                }
                sortedAtoms.add(atom);
            }
            processAtoms();
        }

        private void processAtoms() {
            for (OnDiskAtom atom : sortedAtoms) {
                if (atom instanceof Cell) {
                    process((Cell) atom);
                } else {
                    process((RangeTombstone) atom);
                }
            }
            sortedAtoms.clear();
            finish();
        }

        private void process(RangeTombstone r) {
            Composite max = r.max;
            Composite min = r.min;
            EOC maxEoc = max.eoc();
            EOC minEoc = min.eoc();
            int maxSize = max.size();
            int minSize = min.size();

            if (maxEoc == EOC.END && minEoc == EOC.START && maxSize == minSize) {
                // simple delete row/column
                int cn = cfMetaData.clusteringColumns().size();

                if (cn <= max.size()) {
                    deleteCqlRow(max, r.timestamp());
                } else {
                    ColumnDefinition c = cfMetaData.getColumnDefinition(max.get(max.size() - 1));
                    deleteColumn(max, c, null, r.timestamp());
                }
                return;
            }
            if (maxEoc == minEoc && maxSize != minSize) {
                // Cassandra creates weird range tombstones for delete on rows
                // matching
                // sub-set of clustering columns.
                // But the pattern seems to be to match clustering on smallest
                // prefix,
                // and start/end combo tombstone
                Composite match = maxSize < minSize ? max : min;
                deleteCqlRow(match, r.timestamp());
                return;
            }

            logger.warn("Could not parse RangeTombstone {},{}", cfMetaData.comparator.getString(min),
                    cfMetaData.comparator.getString(max));
        }

        // update the CQL operation. If we change, we need
        // to send the old query.
        private void setOp(Op op, long timestamp) {
            if (this.op != op) {
                finish();
                assert this.op == Op.NONE;
            }
            updateTimestamp(timestamp);
            this.op = op;
        }

        // add a column value to update. If we already have one for this column,
        // flush. (Should never happen though, as long as CQL row detection is
        // valid)
        private void updateColumn(Composite composite, ColumnDefinition c, ColumnOp object, long timestamp, int ttl) {
            if (values.containsKey(c)) {
                finish();
            }
            if (object != null && object.canDoInsert() && this.op != Op.UPDATE) {
                setOp(Op.INSERT, timestamp);
            } else {
                setOp(Op.UPDATE, timestamp);
            }
            addWhere(composite, timestamp, ttl);
            values.put(c, object);
        }

        // Since each CQL query can only have a single
        // timestamp, we must send old query once we
        // set a new timestamp
        private void updateTimestamp(long timestamp) {
            if (this.timestamp != invalidTimestamp && this.timestamp != timestamp) {
                finish();
            }
            this.timestamp = timestamp;
        }

        private void updateTTL(int ttl) {
            if (this.ttl != invalidTTL && this.ttl != ttl) {
                finish();
            }
            this.ttl = ttl;
        }

        protected void writeColumnFamily(StringBuilder buf) {
            buf.append(' ');
            buf.append(cfMetaData.ksName);
            buf.append('.');
            buf.append(cfMetaData.cfName);
            buf.append(' ');
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(SSTableToCQL.class);

    private final Client client;

    private final String keyspace;

    public SSTableToCQL(String keyspace, Client client) {
        this.client = client;
        this.keyspace = keyspace;
    }

    private CFMetaData getCFMetaData(String keyspace, String cfName) {
        return client.getCFMetaData(keyspace, cfName);
    }

    private IPartitioner getPartitioner() {
        return client.getPartitioner();
    }

    protected Collection<SSTableReader> openSSTables(File directoryOrSStable) {
        logger.info("Opening sstables and calculating sections to stream");

        final List<SSTableReader> sstables = new ArrayList<>();

        if (!directoryOrSStable.isDirectory()) {
            addFile(sstables, directoryOrSStable.getParentFile(), directoryOrSStable.getName());
        } else {
            directoryOrSStable.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (new File(dir, name).isDirectory()) {
                        return false;
                    }
                    addFile(sstables, dir, name);
                    return false;
                }

            });
        }

        return sstables;
    }

    private void addFile(final List<SSTableReader> sstables, File dir, String name) {
        Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(dir, name);
        Descriptor desc = p == null ? null : p.left;
        if (p == null || !p.right.equals(Component.DATA) || desc.type.isTemporary) {
            return;
        }

        if (!new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists()) {
            logger.info("Skipping file {} because index is missing", name);
            return;
        }

        CFMetaData metadata = getCFMetaData(keyspace, desc.cfname);
        if (metadata == null) {
            logger.info("Skipping file {}: column family {}.{} doesn't exist", name, keyspace, desc.cfname);
            return;
        }

        Set<Component> components = new HashSet<>();
        components.add(Component.DATA);
        components.add(Component.PRIMARY_INDEX);
        if (new File(desc.filenameFor(Component.SUMMARY)).exists()) {
            components.add(Component.SUMMARY);
        }
        if (new File(desc.filenameFor(Component.COMPRESSION_INFO)).exists()) {
            components.add(Component.COMPRESSION_INFO);
        }
        if (new File(desc.filenameFor(Component.STATS)).exists()) {
            components.add(Component.STATS);
        }

        try {
            // To conserve memory, open SSTableReaders without bloom
            // filters and discard
            // the index summary after calculating the file sections to
            // stream and the estimated
            // number of keys for each endpoint. See CASSANDRA-5555 for
            // details.
            SSTableReader sstable = openForBatch(desc, components, metadata, getPartitioner());
            sstables.add(sstable);
        } catch (IOException e) {
            logger.warn("Skipping file {}, error opening it: {}", name, e.getMessage());
        }
    }

    protected void process(RowBuilder builder, InetAddress address, ISSTableScanner scanner) {
        // collecting keys to export
        while (scanner.hasNext()) {
            OnDiskAtomIterator row = scanner.next();
            builder.process(address, row);
        }
    }

    public void stream(File directoryOrSStable) throws IOException, ConfigurationException {
        RowBuilder builder = new RowBuilder(client);

        logger.info("Opening sstables and calculating sections to stream");

        Map<InetAddress, Collection<Range<Token>>> ranges = client.getEndpointRanges();
        Collection<SSTableReader> sstables = openSSTables(directoryOrSStable);

        // Hack. Must do because Range mangling code in cassandra is
        // broken, and does not preserve input range objects internal
        // "partitioner" field.
        DatabaseDescriptor.setPartitioner(client.getPartitioner());
        try {
            for (SSTableReader reader : sstables) {
                if (ranges == null || ranges.isEmpty()) {
                    ISSTableScanner scanner = reader.getScanner();
                    try {
                        process(builder, null, scanner);
                    } finally {
                        scanner.close();
                    }
                } else {
                    for (Map.Entry<InetAddress, Collection<Range<Token>>> e : ranges.entrySet()) {
                        ISSTableScanner scanner = reader.getScanner(e.getValue(), null);
                        try {
                            process(builder, e.getKey(), scanner);
                        } finally {
                            scanner.close();
                        }
                    }
                }
            }

        } finally {
            client.finish();
        }
    }

}
