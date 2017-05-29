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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.io.sstable.format.SSTableReader.openForBatch;
import static org.apache.cassandra.utils.UUIDGen.getUUID;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone.Bound;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractCompositeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Row.Deletion;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

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
                return " = " + c.name.toCQLString() + " - ?";
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
                return " = " + c.name.toCQLString() + " + ?";
            }

            @Override
            public boolean canDoInsert() {
                return false;
            }
        }

        private static class SetCounterEntry implements ColumnOp {
            private final AbstractType<?> type;
            private final ByteBuffer value;

            SetCounterEntry(AbstractType<?> type, ByteBuffer value) {
                this.type = type;
                this.value = value;
            }

            @Override
            public boolean canDoInsert() {
                return false;
            }

            @Override
            public String apply(ColumnDefinition c, List<Object> params) {
                CounterContext.ContextState state = CounterContext.ContextState.wrap(value);
                StringBuilder buf = new StringBuilder();
                while (state.hasRemaining()) {
                    if (buf.length() != 0) {
                        buf.append(", ");
                    }
                    int type = 'r';
                    if (state.isGlobal()) {
                        type = 'g';
                    }
                    if (state.isLocal()) {
                        type = 'l';
                    }
                    buf.append('(').append(type).append(',').append(getUUID(state.getCounterId().bytes())).append(',')
                            .append(state.getClock()).append(',').append(state.getCount()).append(')');
                    state.moveToNext();
                }
                return " = SCYLLA_COUNTER_SHARD_LIST([" + buf.toString() + "])";
            }
        }

        private static final long invalidTimestamp = LivenessInfo.NO_TIMESTAMP;
        private static final int invalidTTL = LivenessInfo.NO_TTL;

        private final Client client;

        Op op;
        Object callback;
        CFMetaData cfMetaData;
        DecoratedKey key;
        Row row;
        boolean rowDelete;
        long timestamp;
        int ttl;
        Multimap<ColumnDefinition, ColumnOp> values = MultimapBuilder.treeKeys().arrayListValues(1).build();
        Multimap<ColumnDefinition, Pair<Comp, Object>> where = MultimapBuilder.treeKeys().arrayListValues(2).build();
        
        enum Comp {
            Equal("="),
            GreaterEqual(">="),
            Greater(">"),
            LessEqual("<="),
            Less("<"),        
            ;
            
            private String s;
            private Comp(String s) {
                this.s = s;
            }
            public String toString() {
                return s;
            }            
        }
        // sorted atoms?
        
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
        private void setWhere(Slice.Bound start, Slice.Bound end) {
            assert where.isEmpty();
            
            ClusteringPrefix spfx = start.clustering();
            ClusteringPrefix epfx = end.clustering();
            
            List<ColumnDefinition> clusteringColumns = cfMetaData.clusteringColumns();
            for (int i = 0; i < clusteringColumns.size(); i++) {
                ColumnDefinition column = clusteringColumns.get(i);                
                
                Object sval = i < spfx.size() ? column.cellValueType().compose(spfx.get(i)) : null;
                Object eval = spfx != epfx ? (i < epfx.size() ? column.cellValueType().compose(epfx.get(i)) : null) : sval;
                
                if (sval == null && eval == null) {
                    // nothing. 
                } else if (sval != null && (sval == eval || sval.equals(eval))) {
                    assert start.isInclusive();
                    where.put(column, Pair.create(Comp.Equal, sval));                                                              
                } else {
                    if (column.isPrimaryKeyColumn()) {
                        // cannot generate <> for pk columns
                        throw new IllegalStateException("Cannot generate <> comparison for primary key colum " + column);
                    }
                    if (sval != null) {
                        where.put(column, 
                                Pair.create( 
                                        start.isInclusive() ? Comp.GreaterEqual : Comp.Greater, sval));
                    }
                    if (eval != null) {
                        where.put(column, 
                            Pair.create( 
                            end.isInclusive() ? Comp.LessEqual : Comp.Less, eval));
                    }
                }
            } 
        }

        // Begin a new partition (cassandra "Row")
        private void begin(Object callback, DecoratedKey key, CFMetaData cfMetaData) {
            this.callback = callback;
            this.key = key;
            this.cfMetaData = cfMetaData;
            clear();
        }

        private void beginRow(Row row) {
            where.clear();
            this.row = row;
        }
        private void endRow() {
            this.row = null;
            this.rowDelete = false;
        }
        
        private void clear() {
            op = Op.NONE;
            values.clear();
            where.clear();
            timestamp = invalidTimestamp;
            ttl = invalidTTL;
        }

        // Delete the whole cql row
        void deleteCqlRow(Bound start, Bound end, long timestamp) {
            if (!values.isEmpty()) {
                finish();
            }            
            setOp(Op.DELETE, timestamp, invalidTTL);                        
            setWhere(start, end);
            finish();
        }

        // Delete the whole partition
        private void deletePartition(DecoratedKey key, DeletionTime topLevelDeletion) {
            setOp(Op.DELETE, topLevelDeletion.markedForDeleteAt(), invalidTTL);
            finish();
        };

        // Genenerate the CQL query for this CQL row
        private void finish() {
            // Nothing?
            if (op == Op.NONE) {
                clear();
                return;
            }


            checkRowClustering();

            List<Object> params = new ArrayList<>();
            StringBuilder buf = new StringBuilder();

            buf.append(op.toString());
            
            if (op == Op.UPDATE) {
                writeColumnFamily(buf);
                // Timestamps can be sent using statement options.
                // TTL cannot. But just to be extra funny, at least
                // origin does not seem to respect the timestamp
                // in statement, so we'll add them to the CQL string as well.
                writeUsingTimestamp(buf, params);
                writeUsingTTL(buf, params);
                buf.append(" SET ");
            }

            if (op == Op.INSERT) {
                buf.append(" INTO");
                writeColumnFamily(buf);
            }

            int i = 0;
            for (Map.Entry<ColumnDefinition, ColumnOp> e : values.entries()) {
                ColumnDefinition c = e.getKey();
                ColumnOp o = e.getValue();
                String s = o != null ? o.apply(c, params) : null;

                if (op != Op.INSERT) {
                    if (i++ > 0) {
                        buf.append(", ");
                    }
                    ensureWhitespace(buf);
                    buf.append(c.name.toCQLString());
                    if (s != null) {
                        buf.append(s);
                    }
                }
            }

            if (op == Op.DELETE) {
                buf.append(" FROM");
                writeColumnFamily(buf);
                writeUsingTimestamp(buf, params);
            }

            if (op != Op.INSERT) {
                buf.append(" WHERE ");
            }

            // Add "WHERE pk1 = , pk2 = "
            
            List<ColumnDefinition> pk = cfMetaData.partitionKeyColumns();
            AbstractType<?> type = cfMetaData.getKeyValidator();
            ByteBuffer bufs[];
            if (type instanceof AbstractCompositeType) {
                bufs = ((AbstractCompositeType) type).split(key.getKey());
            } else {
                bufs = new ByteBuffer[] { key.getKey() };
            }
            int k = 0;
            for (ColumnDefinition c : pk) {
                where.put(c, Pair.create(Comp.Equal, c.type.compose(bufs[k++])));
            }

            for (Pair<Comp, Object> p : where.values()) {
                params.add(p.right);                
            }

            i = 0;
            if (op == Op.INSERT) {
                buf.append('(');
                for (ColumnDefinition c : values.keySet()) {
                    if (i++ > 0) {
                        buf.append(',');
                    }
                    buf.append(c.name.toCQLString());
                }
                for (ColumnDefinition c : where.keySet()) {
                    if (i++ > 0) {
                        buf.append(',');
                    }
                    buf.append(c.name.toCQLString());
                }
                buf.append(") values (");
                for (i = 0; i < values.size() + where.size(); ++i) {
                    if (i > 0) {
                        buf.append(',');
                    }
                    buf.append('?');
                }
                buf.append(')');
                writeUsingTimestamp(buf, params);
                writeUsingTTL(buf, params);
            } else {
                for (Map.Entry<ColumnDefinition, Pair<Comp, Object>> e : where.entries()) {
                    if (i++ > 0) {
                        buf.append(" AND ");
                    }
                    buf.append(e.getKey().name.toCQLString());
                    buf.append(' ');
                    buf.append(e.getValue().left.toString());
                    buf.append(" ?");
                }
            }
            buf.append(';');

            makeStatement(key, timestamp, buf.toString(), params);
            clear();
        }

        private void writeUsingTTL(StringBuilder buf, List<Object> params) {
            if (ttl != invalidTTL) {
                ensureWhitespace(buf);

                int adjustedTTL = ttl;
                
                if (timestamp == invalidTimestamp) {
                    buf.append("USING ");
                } else {
                    buf.append("AND ");
                    
                    long exp = SECONDS.convert(timestamp, MICROSECONDS) + ttl; 
                    long now = SECONDS.convert(System.currentTimeMillis(), MILLISECONDS); 

                    if (exp < now) {
                        adjustedTTL = 1; // 0 -> no ttl. 1 should disappear fast enoug
                    } else {
                        adjustedTTL = (int)Math.min(ttl, exp - now);
                    }                    
                }
                
                
                buf.append(" TTL ?");
                params.add(adjustedTTL);
            }
        }

        private void ensureWhitespace(StringBuilder buf) {
            if (buf.length() > 0 && !Character.isWhitespace(buf.charAt(buf.length() - 1))) {
                buf.append(' ');
            }
        }

        private void writeUsingTimestamp(StringBuilder buf, List<Object> params) {
            if (timestamp != invalidTimestamp) {
                ensureWhitespace(buf);
                buf.append("USING TIMESTAMP ?");
                params.add(timestamp);
            }
        }

        // Dispatch the CQL
        private void makeStatement(DecoratedKey key, long timestamp, String what, List<Object> objects) {
            client.processStatment(callback, key, timestamp, what, objects);
        }

        private void process(Row row) {
            beginRow(row);

            try {
                LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
                Deletion d = row.deletion();

                updateTimestamp(liveInfo.timestamp());
                updateTTL(liveInfo.ttl());

                for (ColumnData cd : row) {
                    if (cd.column().isSimple()) {
                        process((Cell) cd, liveInfo, null);
                    } else {
                        ComplexColumnData complexData = (ComplexColumnData) cd;

                        for (Cell cell : complexData) {
                            process(cell, liveInfo, null);
                        }
                    }
                }

                if (!d.isLive() && d.deletes(liveInfo)) {
                    rowDelete = true;
                }

                if (rowDelete) {
                    setOp(Op.DELETE, d.time().markedForDeleteAt(), invalidTTL);
                }
                if (row.size() == 0 && !rowDelete && !row.isStatic()) {
                    op = Op.INSERT;
                }

                finish();
            } finally {
                endRow();
            }
        }
        
        private void checkRowClustering() {
            if (row == null) {
                return;
            }
            if (!row.isStatic()) {
                Slice.Bound b = Slice.Bound.inclusiveStartOf(row.clustering().clustering());
                Slice.Bound e = Slice.Bound.inclusiveEndOf(row.clustering().clustering());
                setWhere(b, e);

                if (rowDelete && !tombstoneMarkers.isEmpty()) {
                    RangeTombstoneMarker last = tombstoneMarkers.getLast();
                    Bound start = last.openBound(false);
                    // If we're doing a cql row delete while processing a ranged
                    // tombstone
                    // chain, we're probably dealing with (old, horrble)
                    // sstables with
                    // overlapping tombstones. Since then this row delete was
                    // also a link
                    // in that tombstone chain, add a marker to the current list
                    // being processed.
                    if (start != null && this.cfMetaData.comparator.compare(start, b) < 0) {
                        tombstoneMarkers.add(new RangeTombstoneBoundMarker(e, row.deletion().time()));
                    }
                }

            }            
        }
        // process an actual cell (data or tombstone)
        private void process(Cell cell, LivenessInfo liveInfo, DeletionTime d) {
            ColumnDefinition c = cell.column();
            
            if (logger.isTraceEnabled()) {
                logger.trace("Processing {}", c.name);
            }
            
            AbstractType<?> type = c.type;
            ColumnOp cop = null;

            boolean live = !cell.isTombstone() && (d == null || d.isLive());
            
            try {
                if (cell.path() != null && cell.path().size() > 0) {
                    CollectionType<?> ctype = (CollectionType<?>) type;
                    
                    Object key = ctype.nameComparator().compose(cell.path().get(0));
                    Object val = live ? cell.column().cellValueType().compose(cell.value()) : null;

                    switch (ctype.kind) {
                    case MAP:
                        cop = new SetMapEntry(key, val);
                        break;
                    case LIST:
                        cop = new SetListEntry(key, val);
                        break;
                    case SET:
                        cop = cell.isTombstone() ?  new DeleteSetEntry(key) : new SetSetEntry(key);
                        break;
                    }
                } else if (live && type.isCounter()) {
                    cop = new SetCounterEntry(type, cell.value());
                } else if (live) {
                    cop = new SetColumn(type.compose(cell.value()));
                } else {
                    cop = SET_NULL;
                }

            } catch (Exception e) {
                logger.error("Could not compose value for " + c.name, e);
                throw e;
            }

            updateColumn(c, cop, cell.timestamp(), cell.ttl());
        }
        
        // Process an SSTable row (partial partition)
        private void process(Object callback, UnfilteredRowIterator rows) {
            CFMetaData cfMetaData = rows.metadata();
            DeletionTime deletionTime = rows.partitionLevelDeletion();
            DecoratedKey key = rows.partitionKey();

            begin(callback, key, cfMetaData);

            if (!deletionTime.isLive()) {
                deletePartition(key, deletionTime);
                return;
            }

            Row sr = rows.staticRow();
            if (sr != null) {
                process(sr);
            }
            
            while (rows.hasNext()) {
                Unfiltered f = rows.next();
                switch (f.kind()) {
                case RANGE_TOMBSTONE_MARKER:
                    process((RangeTombstoneMarker) f);
                    break;
                case ROW:
                    process((Row)f);
                    break;
                default:
                    break;                
                }
            }
        }

        private Deque<RangeTombstoneMarker> tombstoneMarkers = new ArrayDeque<>();

        private void process(RangeTombstoneMarker tombstone) {
            Bound end = tombstone.closeBound(false);

            if (end != null && tombstoneMarkers.isEmpty()) {
                throw new IllegalStateException("Unexpected tombstone: " + tombstone);
            }

            if (end != null && !tombstoneMarkers.isEmpty()) {
                Bound last = tombstoneMarkers.getLast().closeBound(false);

                // This can happen if we're adding a tombstone marker but had a
                // row delete in between. In that case (overlapping tombstones),
                // we should (I hope) assume that he was really the high
                // watermark for the chain, and should also be followed by a new 
                // tombstone range.
                if (last != null && this.cfMetaData.comparator.compare(end, last) < 0) {
                    return;
                }

                Bound start = tombstoneMarkers.getFirst().openBound(false);
                assert start != null;
                deleteCqlRow(start, end, tombstoneMarkers.getFirst().openDeletionTime(false).markedForDeleteAt());
                tombstoneMarkers.clear();
                return;
            }

            Bound start = tombstone.openBound(false);
            if (start != null && !tombstoneMarkers.isEmpty()) {
                RangeTombstoneMarker last = tombstoneMarkers.getLast();
                Bound stop = last.closeBound(false);

                if (stop == null) {
                    throw new IllegalStateException("Unexpected tombstone: " + tombstone);
                }
                if (this.cfMetaData.comparator.compare(start, stop) != 0) {
                    deleteCqlRow(tombstoneMarkers.getFirst().openBound(false), stop,
                            tombstoneMarkers.getFirst().openDeletionTime(false).markedForDeleteAt());
                    tombstoneMarkers.clear();
                }
            }
            if (start != null) {
                tombstoneMarkers.add(tombstone);
            }
        }

        // update the CQL operation. If we change, we need
        // to send the old query.
        private void setOp(Op op, long timestamp, int ttl) {
            if (this.op != op) {
                finish();
                assert this.op == Op.NONE;
            }
            updateTimestamp(timestamp);
            updateTTL(ttl);
            this.op = op;
        }

        private boolean canDoInsert() {
            if (this.op == Op.UPDATE) {
                return false;
            }
            if (this.row != null && this.row.primaryKeyLivenessInfo().timestamp() != timestamp) {
                return false;
            }
            return true;
        }

        // add a column value to update. If we already have one for this column,
        // flush. (Should never happen though, as long as CQL row detection is
        // valid)
        private void updateColumn(ColumnDefinition c, ColumnOp object, long timestamp, int ttl) {
            if (object != null && object.canDoInsert() && canDoInsert()) {
                setOp(Op.INSERT, timestamp, ttl);
            } else {
                setOp(Op.UPDATE, timestamp, ttl);
            }
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
        if (p == null || !p.right.equals(Component.DATA)) {
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
            SSTableReader sstable = openForBatch(desc, components, metadata);
            sstables.add(sstable);
        } catch (IOException e) {
            logger.warn("Skipping file {}, error opening it: {}", name, e.getMessage());
        }
    }

    protected void process(RowBuilder builder, InetAddress address, ISSTableScanner scanner) {
        // collecting keys to export
        while (scanner.hasNext()) {
            UnfilteredRowIterator ri = scanner.next();
            builder.process(address, ri);
        }
    }

    public void stream(File directoryOrSStable) throws IOException, ConfigurationException {
        RowBuilder builder = new RowBuilder(client);

        logger.info("Opening sstables and calculating sections to stream");

        // Hack. Must do because Range mangling code in cassandra is
        // broken, and does not preserve input range objects internal
        // "partitioner" field.
        DatabaseDescriptor.setPartitionerUnsafe(client.getPartitioner());        
        
        Map<InetAddress, Collection<Range<Token>>> ranges = client.getEndpointRanges();
        Collection<SSTableReader> sstables = openSSTables(directoryOrSStable);

        try {
            for (SSTableReader reader : sstables) {
                logger.info("Processing {}", reader.getFilename());                
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
