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

import static com.datastax.driver.core.Cluster.builder;
import static java.lang.Thread.currentThread;
import static org.apache.cassandra.schema.CQLTypeParser.parse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.CFMetaData.DroppedColumn;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.CustomType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

public class BulkLoader {
    public static class InfiniteRetryPolicy implements RetryPolicy {
        public InfiniteRetryPolicy() {
        }

        @Override
        public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl, WriteType wt, int requiredResponses, int receivedResponses, int wTime) {
            return RetryDecision.retry(cl);
        }

        @Override
        public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, int uTime) {
            return RetryDecision.retry(cl);
        }

        @Override
        public RetryPolicy.RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            return RetryDecision.retry(cl);
        }

        @Override
        public RetryPolicy.RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
            return RetryDecision.retry(cl);
        }
        @Override
        public void close() {
        }

        @Override
        public void init(Cluster c) {
        }
    }
    public static class CmdLineOptions extends Options {
        /**
         * Add option without argument
         *
         * @param opt
         *            shortcut for option name
         * @param longOpt
         *            complete option name
         * @param description
         *            description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String description) {
            return addOption(new Option(opt, longOpt, false, description));
        }

        /**
         * Add option with argument and argument name
         *
         * @param opt
         *            shortcut for option name
         * @param longOpt
         *            complete option name
         * @param argName
         *            argument name
         * @param description
         *            description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String argName, String description) {
            Option option = new Option(opt, longOpt, true, description);
            option.setArgName(argName);

            return addOption(option);
        }
    }

    static class CQLClient implements Client {
        private static final ProtocolVersion PROTOCOL_VERSION = ProtocolVersion.V3;

        private final Cluster cluster;
        private final Session session;
        private final Metadata metadata;
        private final KeyspaceMetadata keyspaceMetadata;
        private final IPartitioner partitioner;
        private final boolean simulate;
        private final boolean verbose;
        private BatchStatement batchStatement;
        private int batchSize;
        private DecoratedKey key;

        private RateLimiter rateLimiter;
        private int bytes;

        private final boolean batch;
        private final Map<String, ListenableFuture<PreparedStatement>> preparedStatements;
        private final Set<String> ignoreColumns;
        private final CodecRegistry codecRegistry = new CodecRegistry();
        private final TypeCodec<ByteBuffer> blob = codecRegistry.codecFor(ByteBuffer.allocate(1));

        public CQLClient(LoaderOptions options, String keyspace)
                throws NoSuchAlgorithmException, FileNotFoundException, IOException, KeyStoreException,
                CertificateException, UnrecoverableKeyException, KeyManagementException, ConfigurationException {

            // System.setProperty("com.datastax.driver.NON_BLOCKING_EXECUTOR_SIZE",
            // "64");

            PoolingOptions poolingOptions = new PoolingOptions();

            int connections = options.connectionsPerHost;
            if (connections == 0) {
                connections = 8;
            }
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, Math.max(1, connections / 2));
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, Math.max(1, connections / 4));
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, connections);
            poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, Math.max(1, connections / 2));
            poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 32768);
            poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);

            this.simulate = options.simulate;
            this.verbose = options.verbose;
            Cluster.Builder builder = builder().addContactPoints(options.hosts).withProtocolVersion(PROTOCOL_VERSION)
                    .withCompression(Compression.LZ4).withPoolingOptions(poolingOptions)
                    .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                    .withCodecRegistry(codecRegistry);

            if (options.infiniteRetry) {
                builder = builder.withRetryPolicy(new InfiniteRetryPolicy());
            }

            if (options.user != null && options.passwd != null) {
                builder = builder.withCredentials(options.user, options.passwd);
            }
            if (options.ssl) {
                EncryptionOptions enco = options.encOptions;
                SSLContext ctx = SSLContext.getInstance(options.encOptions.protocol);

                try (FileInputStream tsf = new FileInputStream(enco.truststore);
                        FileInputStream ksf = new FileInputStream(enco.keystore)) {
                    KeyStore ts = KeyStore.getInstance(enco.store_type);
                    ts.load(tsf, enco.truststore_password.toCharArray());
                    TrustManagerFactory tmf = TrustManagerFactory
                            .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(ts);

                    KeyStore ks = KeyStore.getInstance("JKS");
                    ks.load(ksf, enco.keystore_password.toCharArray());
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(ks, enco.keystore_password.toCharArray());
                    ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
                }
                SSLOptions sslOptions = JdkSSLOptions.builder().withSSLContext(ctx).withCipherSuites(enco.cipher_suites)
                        .build();
                builder = builder.withSSL(sslOptions);
            }

            cluster = builder.build();
            session = cluster.connect(keyspace);
            metadata = cluster.getMetadata();
            keyspaceMetadata = metadata.getKeyspace(keyspace);
            org.apache.cassandra.schema.KeyspaceMetadata ksMetaData = org.apache.cassandra.schema.KeyspaceMetadata
                    .create(keyspaceMetadata.getName(), KeyspaceParams.create(keyspaceMetadata.isDurableWrites(),
                            keyspaceMetadata.getReplication()));
            Schema.instance.load(ksMetaData);

            loadUserTypes(keyspaceMetadata.getUserTypes(), keyspace);

            partitioner = FBUtilities.newPartitioner(metadata.getPartitioner());
            if (options.throttle != 0) {
                rateLimiter = RateLimiter.create(options.throttle * 1000 * 1000 / 8);
            }

            this.batch = options.batch;
            this.maxBatchSize = options.maxBatchSize;
            this.preparedStatements = options.prepare ? new ConcurrentHashMap<>() : null;
            this.ignoreColumns = options.ignoreColumns;
        }

        private CQLClient(CQLClient other) {
            simulate = other.simulate;
            verbose = other.verbose;
            cluster = other.cluster;
            session = other.session;
            metadata = other.metadata;
            keyspaceMetadata = other.keyspaceMetadata;
            partitioner = other.partitioner;
            rateLimiter = other.rateLimiter;
            batch = other.batch;
            preparedStatements = other.preparedStatements;
            ignoreColumns = other.ignoreColumns;
            maxBatchSize = other.maxBatchSize;
        }

        @Override
        public Client copy() {
            return new CQLClient(this);
        }

        public Cluster getCluster( ) {
            return cluster;
        }

        // Load user defined types. Since loading a UDT entails validation
        // of the field types against known types, we may fail to load a UDT if
        // it references a UDT that has not yet been loaded. So we run a
        // fixed-point algorithm until we either load all UDTs or fail to make
        // forward progress.
        private void loadUserTypes(Collection<UserType> udts, String ksname) {
            List<UserType> notLoaded = new ArrayList<>(udts);

            while (!notLoaded.isEmpty()) {
                Iterator<UserType> i = notLoaded.iterator();

                int n = 0;
                Types.Builder types = Types.builder();

                while (i.hasNext()) {
                    try {
                        UserType ut = i.next();
                        ArrayList<ByteBuffer> fieldNames = new ArrayList<ByteBuffer>(ut.getFieldNames().size());
                        ArrayList<AbstractType<?>> fieldTypes = new ArrayList<AbstractType<?>>();
                        for (UserType.Field f : ut) {
                            fieldNames.add(ByteBufferUtil.bytes(f.getName()));
                            fieldTypes.add(getCql3Type(f.getType()).prepare(ksname).getType());
                        }
                        types = types.add(new org.apache.cassandra.db.marshal.UserType(ksname,
                                ByteBufferUtil.bytes(ut.getTypeName()), fieldNames, fieldTypes));
                        i.remove();
                        ++n;
                    } catch (Exception e) {
                        // try again.
                    }
                }

                if (n == 0) {
                    throw new RuntimeException("Unable to load user types " + notLoaded);
                }

                types.build().forEach(Schema.instance::addType);
            }
            
            udts.forEach(this::bindUserTypeCodec);
        }
        
        private void bindUserTypeCodec(UserType t) {
            // Cassandra drivers assume incoming bound data for UDT is 
            // an actual object looking like the type. In our case, we will 
            // not have neither time to unmarchal/marshal, or even the classes
            // in classpath to do so. 
            // We instead get UDT data as its frozen representation in sstable. Which 
            // is how the wire should look as well. 
            // When using non-prepared statements, this works perfectly, because 
            // there the actual CQL column type is ignored, and only the java type
            // is user for serialization. For prepared statements however, it gets
            // stricter, and bytebuffer data will not fly. 
            // To get around this, we simply register a codec for every user type
            // that treats the type as blobs. 
            // 
            // BUT that does not solve everything. Just to make things super fun, 
            // the types we get from the cluster query and use in loadUserTypes
            // are not always 100% equal to those that prepared statements will 
            // infer for columns. So we might need to register additional types 
            // on the fly as we generate statements. See sendPrepared
            codecRegistry.register(new TypeCodec<ByteBuffer>(t, ByteBuffer.class) {
                @Override
                public ByteBuffer serialize(ByteBuffer value, ProtocolVersion protocolVersion)
                        throws InvalidTypeException {
                    return blob.serialize(value, protocolVersion);
                }
                @Override
                public ByteBuffer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
                        throws InvalidTypeException {
                    return blob.deserialize(bytes,  protocolVersion);
                }
                @Override
                public ByteBuffer parse(String value) throws InvalidTypeException {
                    return blob.parse(value);
                }

                @Override
                public String format(ByteBuffer value) throws InvalidTypeException {
                    return blob.format(value);
                }
            });
        }

        /**
         * Creates a codec for a Cassandra {@link AbstractType} instance
         * We get these for certain types (date) via a {@link CustomType}
         * through the drivers statement prepare. Annoying, but easy to bind... 
         */
        private <T> void bindCustomType(DataType.CustomType ct, final AbstractType<T> atype) {
            codecRegistry.register(new TypeCodec<T>(ct, atype.getSerializer().getType()) {
                @Override
                public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
                    return atype.decompose(value);
                }

                @Override
                public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
                    return atype.compose(bytes);
                }

                @Override
                public T parse(String value) throws InvalidTypeException {
                    return atype.compose(atype.fromString(value));
                }

                @Override
                public String format(T value) throws InvalidTypeException {
                    return atype.getString(atype.decompose(value));
                }
            });
        }

        private static CQL3Type.Raw getCql3Type(DataType dt) throws Exception {
            CQL3Type.Raw type;
            switch (dt.getName()) {
            case LIST:
                type = CQL3Type.Raw.list(getCql3Type(dt.getTypeArguments().get(0)));
                break;
            case MAP:
                type = CQL3Type.Raw.map(getCql3Type(dt.getTypeArguments().get(0)),
                        getCql3Type(dt.getTypeArguments().get(1)));
                break;
            case SET:
                type = CQL3Type.Raw.set(getCql3Type(dt.getTypeArguments().get(0)));
                break;
            case TUPLE:
                ArrayList<CQL3Type.Raw> tupleTypes = new ArrayList<CQL3Type.Raw>();
                for (DataType arg : ((TupleType) dt).getComponentTypes()) {
                    tupleTypes.add(getCql3Type(arg));
                }
                type = CQL3Type.Raw.tuple(tupleTypes);
                break;
            case UDT: // Requires this UDT to already be loaded
                UserType udt = (UserType) dt;
                type = CQL3Type.Raw.userType(new UTName(new ColumnIdentifier(udt.getKeyspace(), true),
                        new ColumnIdentifier(udt.getTypeName(), true)));
                break;
            default:
                type = CQL3Type.Raw.from(
                        Enum.<CQL3Type.Native> valueOf(CQL3Type.Native.class, dt.getName().toString().toUpperCase()));
                break;
            }
            if (dt.isFrozen()) {
                type = CQL3Type.Raw.frozen(type);
            }
            return type;
        }

        private static final int maxStatements = 256;
        private final int maxBatchSize;
        private final Semaphore semaphore = new Semaphore(maxStatements);
        private final Semaphore preparations = new Semaphore(maxStatements);

        @Override
        public void close() {
            try {
                preparations.acquire(maxStatements);
            } catch (InterruptedException e) {
            }
            synchronized (this) {
                if (batchStatement != null && !batchStatement.getStatements().isEmpty()) {
                    send(batchStatement);
                    batchStatement = null;
                    batchSize = 0;
                }
            }
            try {
                semaphore.acquire(maxStatements);
            } catch (InterruptedException e) {
            }
        }

        private void send(Statement s) {
            if (simulate) {
                return;
            }
            if (rateLimiter != null) {
                rateLimiter.acquire(s.requestSizeInBytes(PROTOCOL_VERSION, codecRegistry));
            }

            try {
                semaphore.acquire();
                try {
                    ResultSetFuture future = session.executeAsync(s);
                    Futures.addCallback(future, new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet result) {
                            semaphore.release();
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            semaphore.release();
                            System.err.println(t);
                        }
                    }, MoreExecutors.directExecutor());
                } finally {
                }
            } catch (InterruptedException e) {
            }
        }

        // This method has to be synchronized because it can be called from different threads.
        // Usually it's called just from a single thread - the one that owns the client but for
        // prepared statements it's called from callbacks that have their own thread pool which runs them.
        private synchronized void send(DecoratedKey key, Statement s) {
            if (batch && batchStatement != null && batchSize < maxBatchSize
                    && this.key.equals(key)) {
                batchStatement.add(s);
                batchSize += s.requestSizeInBytes(PROTOCOL_VERSION, codecRegistry);
                return;
            }
            if (batchStatement != null && batchStatement.size() != 0) {
                send(batchStatement);
                batchStatement = null;
                batchSize = 0;
            }
            if (batch) {
                batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                batchStatement.add(s);
                batchSize = batchStatement.requestSizeInBytes(PROTOCOL_VERSION, codecRegistry);
                this.key = key;
            } else {
                send(s);
            }
        }

        private final Map<Pair<String, String>, CFMetaData> cfMetaDatas = new HashMap<>();

        private static final String SELECT_DROPPED_COLUMNS = "SELECT * FROM system_schema.dropped_columns";

        @SuppressWarnings("serial")
        @Override
        public CFMetaData getCFMetaData(String keyspace, String cfName) {
            Pair<String, String> key = Pair.create(keyspace, cfName);
            CFMetaData cfm = cfMetaDatas.get(key);
            if (cfm == null) {
                KeyspaceMetadata ks = metadata.getKeyspace(keyspace);
                TableMetadata cf = ks.getTable(cfName);
                CFStatement parsed = (CFStatement) QueryProcessor.parseStatement(cf.asCQLQuery());
                org.apache.cassandra.schema.KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);
                CreateTableStatement statement = (CreateTableStatement) ((CreateTableStatement.RawStatement) parsed)
                        .prepare(ksm != null ? ksm.types : Types.none()).statement;
                statement.validate(ClientState.forInternalCalls());
                cfm = statement.getCFMetaData();

                final Map<ByteBuffer, DroppedColumn> map = new HashMap<>();
                Map<ByteBuffer, DroppedColumn> droppedColumns = map;

                try {
                    ResultSet r = session.execute(SELECT_DROPPED_COLUMNS + " WHERE keyspace_name = '" + keyspace
                            + "' AND table_name = '" + cfName + '\'');

                    for (Row row : r) {
                        String name = row.getString("column_name");
                        long droppedTime = row.getTimestamp("dropped_time").getTime();
                        AbstractType<?> type = parse(keyspace, row.getString("type"),
                                org.apache.cassandra.schema.Types.none());
                        droppedColumns.put(UTF8Type.instance.decompose(name),
                                new CFMetaData.DroppedColumn(name, type, droppedTime * 1000));
                    }
                } catch (DriverException e) {
                    // ignore. Assume we're asking a v2 schema source.
                }
                if (!ignoreColumns.isEmpty()) {
                    droppedColumns = new HashMap<ByteBuffer, DroppedColumn>(droppedColumns) {
                        @Override
                        public DroppedColumn get(Object key) {
                            DroppedColumn c = super.get(key);
                            if (c == null) {
                                String name = UTF8Type.instance.compose((ByteBuffer) key);
                                if (ignoreColumns.contains(name)) {
                                    c = new DroppedColumn(name, BytesType.instance, FBUtilities.timestampMicros());
                                    put((ByteBuffer) key, c);
                                }
                            }
                            return c;
                        }
                    };
                }
                cfm.droppedColumns(droppedColumns);
                cfMetaDatas.put(key, cfm);
            }
            return cfm;
        }

        @Override
        public Map<InetAddress, Collection<Range<Token>>> getEndpointRanges() {
            HashMap<InetAddress, Collection<Range<Token>>> map = new HashMap<>();
            for (TokenRange range : metadata.getTokenRanges()) {
                Range<Token> tr = new Range<Token>(getToken(range.getStart()), getToken(range.getEnd()));
                for (Host host : metadata.getReplicas(getKeyspace(), range)) {
                    Collection<Range<Token>> c = map.get(host.getAddress());
                    if (c == null) {
                        c = new ArrayList<>();
                        map.put(host.getAddress(), c);
                    }
                    c.add(tr);
                }
            }
            return map;
        }

        private String getKeyspace() {
            return keyspaceMetadata.getName();
        }

        @Override
        public IPartitioner getPartitioner() {
            return partitioner;
        }

        private Token getToken(com.datastax.driver.core.Token t) {
            return getPartitioner().getTokenFactory().fromByteArray(t.serialize(PROTOCOL_VERSION));
        }

        @Override
        public void processStatment(DecoratedKey key, long timestamp, String what,
                List<Object> objects) {
            if (verbose) {
                System.out.print("CQL: '");
                System.out.print(what);
                System.out.print("'");
                if (!objects.isEmpty()) {
                    System.out.print(" ");
                    System.out.print(objects);
                }
                System.out.println();
            }

            if (preparedStatements != null) {
                sendPrepared(key, timestamp, what, objects);
            } else {
                send(key, timestamp, what, objects);
            }
        }

        private void send(DecoratedKey key, long timestamp, String what, List<Object> objects) {
            SimpleStatement s = new SimpleStatement(what, objects.toArray());
            s.setDefaultTimestamp(timestamp);
            s.setKeyspace(getKeyspace());
            s.setRoutingKey(key.getKey());

            send(key, s);
        }

        private void sendPrepared(final DecoratedKey key, final long timestamp, String what,
                final List<Object> objects) {
            ListenableFuture<PreparedStatement> f = preparedStatements.get(what);
            if (f == null) {
                if (verbose) {
                    System.out.println("Preparing: " + what);
                }
                f = session.prepareAsync(what);
                preparedStatements.put(what, f);
            }

            try {
                preparations.acquire();

                Futures.addCallback(f, new FutureCallback<PreparedStatement>() {
                    @Override
                    public void onSuccess(PreparedStatement p) {
                        BoundStatement s;
                        
                        for (;;) {
                            try {
                                s = p.bind(objects.toArray(new Object[objects.size()]));  
                                break;
                            } catch (CodecNotFoundException e) {
                                // If we get here with a user type, it means we have
                                // a subtle difference in the types we got from the cluster, and
                                // what the statement parser created. (text instead of varchar etc).
                                // Register a UDT codec for the "new" type and try again. 
                                // Eventually we will run out of UDT:s...
                                //
                                // We do this to avoid missing something by trying to recurse 
                                // types in the column data...
                                DataType t = e.getCqlType();
                                if (t instanceof UserType) {
                                    bindUserTypeCodec((UserType)t);
                                    continue;
                                } else if (t instanceof DataType.CustomType) {
                                    /**
                                     * #59. Some types, like SimpleDate, show up as "custom" type
                                     * in prepared statement types. These cannot bind their data (booh!)
                                     * but we can handle this by trying to find the type 
                                     * referenced (in classname) and binding it into a codec, then retry this 
                                     * whole thing. 
                                     */
                                    DataType.CustomType ct = (DataType.CustomType) t;
                                    String name = ct.getCustomTypeClassName();
                                    try {
                                        Class<?> c = Class.forName(name, true, currentThread().getContextClassLoader());
                                        @SuppressWarnings("rawtypes")
                                        Class<? extends AbstractType> ac = c.asSubclass(AbstractType.class);
                                        Field f = ac.getField("instance");
                                        AbstractType<?> atype = (AbstractType<?>) f.get(null);
                                        bindCustomType(ct, atype);
                                        continue;
                                    } catch (ClassNotFoundException | IllegalAccessException | NoSuchFieldException
                                            | SecurityException ce) {
                                        throw e;
                                    }
                                }
                                throw e;
                            }
                        }
                                                
                        s.setRoutingKey(key.getKey());
                        s.setDefaultTimestamp(timestamp);
                        send(key, s);
                        preparations.release();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        preparations.release();
                        System.err.println(t);
                    }
                }, MoreExecutors.directExecutor());
            } catch (InterruptedException e) {
            }
        }
    }

    static class LoaderOptions {
        private static void errorMsg(String msg, CmdLineOptions options) {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }

        private static CmdLineOptions getCmdLineOptions() {
            CmdLineOptions options = new CmdLineOptions();
            options.addOption("v", VERBOSE_OPTION, "verbose output");
            options.addOption("sim", SIMULATE, "simulate. Only print CQL generated");
            options.addOption("h", HELP_OPTION, "display this help message");
            options.addOption(null, NOPROGRESS_OPTION, "don't display progress");
            options.addOption("i", IGNORE_NODES_OPTION, "NODES",
                    "don't stream to this (comma separated) list of nodes");
            options.addOption("d", INITIAL_HOST_ADDRESS_OPTION, "initial hosts",
                    "Required. try to connect to these hosts (comma separated) initially for ring information");
            options.addOption("p", PORT_OPTION, "port", "port used for connections (default 9042)");
            options.addOption("t", THROTTLE_MBITS, "throttle", "throttle speed in Mbits (default unlimited)");
            options.addOption("u", USER_OPTION, "username", "username for cassandra authentication");
            options.addOption("pw", PASSWD_OPTION, "password", "password for cassandra authentication");
            options.addOption("cph", CONNECTIONS_PER_HOST, "connectionsPerHost",
                    "number of concurrent connections-per-host.");
            // ssl connection-related options
            options.addOption("s", SSL, "SSL", "Use SSL connection(s)");
            options.addOption("ts", SSL_TRUSTSTORE, "TRUSTSTORE", "Client SSL: full path to truststore");
            options.addOption("tspw", SSL_TRUSTSTORE_PW, "TRUSTSTORE-PASSWORD",
                    "Client SSL: password of the truststore");
            options.addOption("ks", SSL_KEYSTORE, "KEYSTORE", "Client SSL: full path to keystore");
            options.addOption("kspw", SSL_KEYSTORE_PW, "KEYSTORE-PASSWORD", "Client SSL: password of the keystore");
            options.addOption("prtcl", SSL_PROTOCOL, "PROTOCOL",
                    "Client SSL: connections protocol to use (default: TLS)");
            options.addOption("alg", SSL_ALGORITHM, "ALGORITHM", "Client SSL: algorithm (default: SunX509)");
            options.addOption("st", SSL_STORE_TYPE, "STORE-TYPE", "Client SSL: type of store");
            options.addOption("ciphers", SSL_CIPHER_SUITES, "CIPHER-SUITES",
                    "Client SSL: comma-separated list of encryption suites to use");
            options.addOption("f", CONFIG_PATH, "path to config file",
                    "cassandra.yaml file path for streaming throughput and client/server SSL.");
            options.addOption("nb", NO_BATCH, "Do not use batch statements updates for same partition key.");
            options.addOption("nx", NO_PREPARED, "Do not use prepared statements");
            options.addOption("g", IGNORE_MISSING_COLUMNS, "COLUMN NAMES...", "ignore named missing columns in tables");
            options.addOption("ir", NO_INFINITE_RETRY_OPTION, "Disable infinite retry policy");
            options.addOption("j", THREADS_COUNT_OPTION, "Number of threads to execute tasks", "Run tasks in parallel");
            options.addOption("bs", BATCH_SIZE_OPTION, "Number of bytes above which batch is being sent out", "Does not work with -nb");
            options.addOption("translate", TRANSLATE_OPTION, "mapping list", "comma-separated list of column name mappings");

            return options;
        }

        public static LoaderOptions parseArgs(String cmdArgs[]) {
            CommandLineParser parser = new GnuParser();
            CmdLineOptions options = getCmdLineOptions();
            try {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION)) {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length == 0) {
                    System.err.println("Missing sstable directory argument");
                    printUsage(options);
                    System.exit(1);
                }

                if (args.length > 1) {
                    System.err.println("Too many arguments");
                    printUsage(options);
                    System.exit(1);
                }

                String dirname = args[0];
                File dir = new File(dirname);

                if (!dir.exists()) {
                    errorMsg("Unknown file/directory: " + dirname, options);
                }

                LoaderOptions opts = new LoaderOptions(dir);

                opts.verbose = cmd.hasOption(VERBOSE_OPTION);
                opts.simulate = cmd.hasOption(SIMULATE);
                opts.noProgress = cmd.hasOption(NOPROGRESS_OPTION);
                opts.infiniteRetry = !cmd.hasOption(NO_INFINITE_RETRY_OPTION);

                if (cmd.hasOption(PORT_OPTION)) {
                    opts.port = Integer.parseInt(cmd.getOptionValue(PORT_OPTION));
                }

                if (cmd.hasOption(THREADS_COUNT_OPTION)) {
                    opts.threadCount = Integer.parseInt(cmd.getOptionValue(THREADS_COUNT_OPTION));
                }

                if (cmd.hasOption(BATCH_SIZE_OPTION) && !cmd.hasOption(NO_BATCH)) {
                    opts.maxBatchSize = Integer.parseInt(cmd.getOptionValue(BATCH_SIZE_OPTION));
                }

                if (cmd.hasOption(TRANSLATE_OPTION)) {
                    for (String mapping : cmd.getOptionValue(TRANSLATE_OPTION).split(",")) {
                        String[] parts = mapping.split(":");
                        if (parts.length != 2) {
                            errorMsg("Invalid column name mapping: " + mapping, options);
                        }
                        String sourceName = parts[0].trim();
                        String targetName = parts[1].trim();
                        if (sourceName.isEmpty() || targetName.isEmpty()) {
                            errorMsg("Invalid column name mapping: " + mapping, options);
                        }
                        if (opts.columnNamesMappings.containsKey(sourceName)) {
                            throw new RuntimeException("Mapping is not unique, key already exists: " + sourceName);
                        }
                        opts.columnNamesMappings.put(sourceName, targetName);
                    }
                }

                if (cmd.hasOption(USER_OPTION)) {
                    opts.user = cmd.getOptionValue(USER_OPTION);
                }

                if (cmd.hasOption(PASSWD_OPTION)) {
                    opts.passwd = cmd.getOptionValue(PASSWD_OPTION);
                }

                if (cmd.hasOption(INITIAL_HOST_ADDRESS_OPTION)) {
                    String[] nodes = cmd.getOptionValue(INITIAL_HOST_ADDRESS_OPTION).split(",");
                    try {
                        for (String node : nodes) {
                            opts.hosts.add(InetAddress.getByName(node.trim()));
                        }
                    } catch (UnknownHostException e) {
                        errorMsg("Unknown host: " + e.getMessage(), options);
                    }

                } else {
                    System.err.println("Initial hosts must be specified (-d)");
                    printUsage(options);
                    System.exit(1);
                }

                if (cmd.hasOption(IGNORE_NODES_OPTION)) {
                    String[] nodes = cmd.getOptionValue(IGNORE_NODES_OPTION).split(",");
                    try {
                        for (String node : nodes) {
                            opts.ignores.add(InetAddress.getByName(node.trim()));
                        }
                    } catch (UnknownHostException e) {
                        errorMsg("Unknown host: " + e.getMessage(), options);
                    }
                }

                if (cmd.hasOption(CONNECTIONS_PER_HOST)) {
                    opts.connectionsPerHost = Integer.parseInt(cmd.getOptionValue(CONNECTIONS_PER_HOST));
                }

                // try to load config file first, so that values can be
                // rewritten with other option values.
                // otherwise use default config.
                Config config;
                if (cmd.hasOption(CONFIG_PATH)) {
                    File configFile = new File(cmd.getOptionValue(CONFIG_PATH));
                    if (!configFile.exists()) {
                        errorMsg("Config file not found", options);
                    }
                    config = new YamlConfigurationLoader().loadConfig(configFile.toURI().toURL());
                } else {
                    config = new Config();
                }
                opts.port = config.native_transport_port;
                opts.throttle = config.stream_throughput_outbound_megabits_per_sec;
                opts.encOptions = config.client_encryption_options;

                if (cmd.hasOption(THROTTLE_MBITS)) {
                    opts.throttle = Integer.parseInt(cmd.getOptionValue(THROTTLE_MBITS));
                }

                if (cmd.hasOption(SSL)) {
                    opts.ssl = true;
                }
                if (cmd.hasOption(SSL_TRUSTSTORE)) {
                    opts.encOptions.truststore = cmd.getOptionValue(SSL_TRUSTSTORE);
                }

                if (cmd.hasOption(SSL_TRUSTSTORE_PW)) {
                    opts.encOptions.truststore_password = cmd.getOptionValue(SSL_TRUSTSTORE_PW);
                }

                if (cmd.hasOption(SSL_KEYSTORE)) {
                    opts.encOptions.keystore = cmd.getOptionValue(SSL_KEYSTORE);
                    // if a keystore was provided, lets assume we'll need to use
                    // it
                    opts.encOptions.require_client_auth = true;
                }

                if (cmd.hasOption(SSL_KEYSTORE_PW)) {
                    opts.encOptions.keystore_password = cmd.getOptionValue(SSL_KEYSTORE_PW);
                }

                if (cmd.hasOption(SSL_PROTOCOL)) {
                    opts.encOptions.protocol = cmd.getOptionValue(SSL_PROTOCOL);
                }

                if (cmd.hasOption(SSL_ALGORITHM)) {
                    opts.encOptions.algorithm = cmd.getOptionValue(SSL_ALGORITHM);
                }

                if (cmd.hasOption(SSL_STORE_TYPE)) {
                    opts.encOptions.store_type = cmd.getOptionValue(SSL_STORE_TYPE);
                }

                if (cmd.hasOption(SSL_CIPHER_SUITES)) {
                    opts.encOptions.cipher_suites = cmd.getOptionValue(SSL_CIPHER_SUITES).split(",");
                }

                if (!cmd.hasOption(NO_PREPARED)) {
                    opts.prepare = true;
                }
                if (!cmd.hasOption(NO_BATCH)) {
                    opts.batch = true;
                }
                if (cmd.hasOption(IGNORE_MISSING_COLUMNS)) {
                    opts.ignoreColumns.addAll(Arrays.asList(cmd.getOptionValues(IGNORE_MISSING_COLUMNS)));
                }

                return opts;
            } catch (ParseException | ConfigurationException | MalformedURLException e) {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        public static void printUsage(Options options) {
            String usage = String.format("%s [options] <dir_path>", TOOL_NAME);
            String header = System.lineSeparator()
                    + "Bulk load the sstables found in the directory <dir_path> to the configured cluster."
                    + "The parent directories of <dir_path> are used as the target keyspace/table name. "
                    + "So for instance, to load an sstable named Standard1-g-1-Data.db into Keyspace1/Standard1, "
                    + "you will need to have the files Standard1-g-1-Data.db and Standard1-g-1-Index.db into a directory /path/to/Keyspace1/Standard1/.";
            String footer = System.lineSeparator()
                    + "You can provide cassandra.yaml file with -f command line option to set up streaming throughput, client and server encryption options. "
                    + "Only stream_throughput_outbound_megabits_per_sec, server_encryption_options and client_encryption_options are read from yaml. "
                    + "You can override options read from cassandra.yaml with corresponding command line options.";
            new HelpFormatter().printHelp(usage, header, options, footer);
        }

        public final File directory;
        public boolean ssl;
        public boolean debug;
        public boolean verbose;
        public boolean simulate;
        public boolean noProgress;
        public int port = 9042;
        public String user;
        public boolean infiniteRetry;
        public int threadCount = 16;
        public int maxBatchSize = 100 * 1024; // 100 kB

        public String passwd;
        public int throttle = 0;

        public boolean batch;
        public boolean prepare;
        
        public Map<String, String> columnNamesMappings = new HashMap<>();

        public EncryptionOptions encOptions = new EncryptionOptions.ClientEncryptionOptions();

        public int connectionsPerHost = 1;

        public final Set<InetAddress> hosts = new HashSet<>();

        public final Set<InetAddress> ignores = new HashSet<>();

        public final Set<String> ignoreColumns = new HashSet<>();

        LoaderOptions(File directory) {
            this.directory = directory;
        }
    }

    private static final String TOOL_NAME = "sstableloader";
    private static final String SIMULATE = "simulate";
    private static final String IGNORE_MISSING_COLUMNS = "ignore-missing-columns";
    private static final String VERBOSE_OPTION = "verbose";
    private static final String HELP_OPTION = "help";
    private static final String NOPROGRESS_OPTION = "no-progress";
    private static final String IGNORE_NODES_OPTION = "ignore";
    private static final String INITIAL_HOST_ADDRESS_OPTION = "nodes";
    private static final String PORT_OPTION = "port";
    private static final String NO_INFINITE_RETRY_OPTION = "no-infinite-retry";
    private static final String THREADS_COUNT_OPTION = "threads-count";
    private static final String BATCH_SIZE_OPTION = "batch-size";
    private static final String TRANSLATE_OPTION = "translate";

    private static final String USER_OPTION = "username";
    private static final String PASSWD_OPTION = "password";
    private static final String THROTTLE_MBITS = "throttle";
    /* client encryption options */
    private static final String SSL = "ssl";
    private static final String SSL_TRUSTSTORE = "truststore";
    private static final String SSL_TRUSTSTORE_PW = "truststore-password";
    private static final String SSL_KEYSTORE = "keystore";
    private static final String SSL_KEYSTORE_PW = "keystore-password";
    private static final String SSL_PROTOCOL = "ssl-protocol";
    private static final String SSL_ALGORITHM = "ssl-alg";
    private static final String SSL_STORE_TYPE = "store-type";

    private static final String SSL_CIPHER_SUITES = "ssl-ciphers";

    private static final String CONNECTIONS_PER_HOST = "connections-per-host";

    private static final String CONFIG_PATH = "conf-path";
    private static final String NO_BATCH = "no-batch";
    private static final String NO_PREPARED = "no-prepared";

    public static void main(String args[]) {
        Config.setClientMode(true);
        LoaderOptions options = LoaderOptions.parseArgs(args);

        CQLClient client = null;
        try {
            File dir = options.directory;
            if (dir.isFile()) {
                dir = dir.getParentFile();
            }

            // Backups are keyspace/table/backups
            // snapshots are in keyspace/table/snapshots/snapshotID
            if (dir.getName().equals("backups")) {
                dir = dir.getParentFile();
            } else if (dir.getParentFile().getName().equals("snapshots")) {
                dir = dir.getParentFile().getParentFile();
            }
            String keyspace = dir.getParentFile().getName();

            client = new CQLClient(options, keyspace);
            SSTableToCQL ssTableToCQL = new SSTableToCQL(keyspace, client, new ColumnNamesMapping(options.columnNamesMappings), options.threadCount);
            ssTableToCQL.stream(options.directory);
            System.exit(0);
        } catch (Throwable t) {
            if (client != null) {
                if (client.getCluster() != null) {
                    client.getCluster().close();
                }
            }
            t.printStackTrace();
            System.exit(1);
        }
    }
}
