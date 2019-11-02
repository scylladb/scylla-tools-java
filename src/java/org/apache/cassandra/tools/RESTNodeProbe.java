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

import com.google.common.collect.Multimap;
import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.api.APIConfig;
import com.scylladb.jmx.utils.FileUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TableMetrics.Sampler;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.streaming.StreamState;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.logging.Logger;

/**
 * REST client operations for Scylla.
 */
public class RESTNodeProbe extends NodeProbe
{

    private static APIConfig config;
    protected final APIClient client;

    /**
     * Creates a NodeProbe using the specified JMX host, port, username, and password.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    public RESTNodeProbe(String host, int port, int rport, String username, String password) throws IOException
    {
        super(host,port,username,password);
        System.setProperty("apiaddress", host);
        System.getProperty("apiport", String.valueOf(rport) );
        //TODO add username and password support - first in scylla-apiclient, then here
        config = new APIConfig();
        client = new APIClient(config);
    }

    /**
     * Creates a NodeProbe using the specified JMX host and port.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    public RESTNodeProbe(String host, int port, int rport) throws IOException
    {
        super(host,port);
        System.setProperty("apiaddress", host);
        System.getProperty("apiport", String.valueOf(rport) );
        config = new APIConfig();
        client = new APIClient(config);
    }

    /**
     * Creates a NodeProbe using the specified JMX host and default port.
     *
     * @param host hostname or IP address of the JMX agent
     * @throws IOException on connection failures
     */
    public RESTNodeProbe(String host) throws IOException
    {
        super(host);
        System.setProperty("apiaddress", host);
        config = new APIConfig();
        client = new APIClient(config);
    }

    public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return super.forceKeyspaceCleanup(jobs, keyspaceName, tables);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        return super.scrub(disableSnapshot, skipCorrupted, checkData, reinsertOverflowedTTL, jobs, keyspaceName, tables);
    }

    public int verify(boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return super.verify(extendedVerify, keyspaceName, tableNames);
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return super.upgradeSSTables(keyspaceName, excludeCurrentVersion, jobs, tableNames);
    }

    public int garbageCollect(String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        return super.garbageCollect(tombstoneOption, jobs, keyspaceName, tableNames);
    }

    public void forceKeyspaceCleanup(PrintStream out, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
       super.forceKeyspaceCleanup(out,jobs,keyspaceName,tableNames);
    }

    public void scrub(PrintStream out, boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException
    {
        super.scrub(out,disableSnapshot,skipCorrupted,checkData,reinsertOverflowedTTL,jobs,keyspaceName,tables);
    }

    public void verify(PrintStream out, boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        super.verify(out,extendedVerify,keyspaceName,tableNames);
    }


    public void upgradeSSTables(PrintStream out, String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        super.upgradeSSTables(out,keyspaceName,excludeCurrentVersion,jobs,tableNames);
    }

    public void garbageCollect(PrintStream out, String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        super.garbageCollect(out,tombstoneOption,jobs,keyspaceName,tableNames);
    }

    public void forceUserDefinedCompaction(String datafiles) throws IOException, ExecutionException, InterruptedException
    {
        super.forceUserDefinedCompaction(datafiles);
    }

    public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        super.forceKeyspaceCompaction(splitOutput, keyspaceName, tableNames);
    }

    public void relocateSSTables(int jobs, String keyspace, String[] cfnames) throws IOException, ExecutionException, InterruptedException
    {
        super.relocateSSTables(jobs, keyspace, cfnames);
    }

    public void forceKeyspaceCompactionForTokenRange(String keyspaceName, final String startToken, final String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        super.forceKeyspaceCompactionForTokenRange(keyspaceName, startToken, endToken, tableNames);
    }

    public void forceKeyspaceFlush(String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException
    {
        super.forceKeyspaceFlush(keyspaceName, tableNames);
    }

    public void repairAsync(final PrintStream out, final String keyspace, Map<String, String> options) throws IOException
    {
        super.repairAsync(out,keyspace,options);
    }

    public Map<Sampler, CompositeData> getPartitionSample(String ks, String cf, int capacity, int duration, int count, List<Sampler> samplers) throws OpenDataException
    {
        return super.getPartitionSample(ks, cf, capacity, duration, count, samplers);
    }

    public void invalidateCounterCache()
    {
        super.invalidateCounterCache();
    }

    public void invalidateKeyCache()
    {
        super.invalidateKeyCache();
    }

    public void invalidateRowCache()
    {
        super.invalidateRowCache();
    }

    public void drain() throws IOException, InterruptedException, ExecutionException
    {
        super.drain();
    }

    public Map<String, String> getTokenToEndpointMap()
    {
        log(" getTokenToEndpointMap()");
        return client.getMapStrValue("/storage_service/tokens_endpoint");
    }

    public List<String> getLiveNodes()
    {
        log(" getLiveNodes()");
        return client.getListStrValue("/gossiper/endpoint/live");
    }

    public List<String> getJoiningNodes()
    {
        log(" getJoiningNodes()");
        return client.getListStrValue("/storage_service/nodes/joining");
    }

    public List<String> getLeavingNodes()
    {
        log(" getLeavingNodes()");
        return client.getListStrValue("/storage_service/nodes/leaving");
    }

    public List<String> getMovingNodes()
    {
        log(" getMovingNodes()");
        return client.getListStrValue("/storage_service/nodes/moving");
    }

    public List<String> getUnreachableNodes()
    {
        log(" getUnreachableNodes()");
        return client.getListStrValue("/gossiper/endpoint/down");
    }

    public Map<String, String> getLoadMap()
    {
        log(" getLoadMap()");
        Map<String, Double> load = getLoadMapAsDouble();
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, Double> entry : load.entrySet()) {
            map.put(entry.getKey(), FileUtils.stringifyFileSize(entry.getValue()));
        }
        return map;
    }

    public Map<String, Double> getLoadMapAsDouble() {
        log(" getLoadMapAsDouble()");
        return client.getMapStringDouble("/storage_service/load_map");
    }


    public Map<InetAddress, Float> getOwnership()
    {
        log(" getOwnership()");
        return client.getMapInetAddressFloatValue("/storage_service/ownership/");
    }

    public Map<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException
    {
        log(" effectiveOwnership(String keyspace) throws IllegalStateException");
        try {
            return client.getMapInetAddressFloatValue("/storage_service/ownership/" + keyspace);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
        }
    }

    public CacheServiceMBean getCacheServiceMBean()
    {
        return super.getCacheServiceMBean();
    }

    public double[] getAndResetGCStats()
    {
        return super.getAndResetGCStats();
    }

    public Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies()
    {
        return super.getColumnFamilyStoreMBeanProxies();
    }

    public Iterator<Map.Entry<String, String>> getColumnFamilyStoreMap()
    {
        JsonArray tables = client.getJsonArray("/column_family/name"); // format keyspace:table

        List<Map.Entry<String, String>> cfMbeans = new ArrayList<>(tables.size());
        for (JsonString record : tables.getValuesAs(JsonString.class ) ) {
            String srecord=record.getString();
            String[] sarray=srecord.split(":");
            String keyspaceName=sarray[0];
            String tableName=null;
            if (sarray.length>1) {
                tableName=sarray[1];}
            cfMbeans.add( new AbstractMap.SimpleImmutableEntry<>(keyspaceName, tableName) );
        }
        return cfMbeans.iterator();
    }

    public CompactionManagerMBean getCompactionManagerProxy()
    {
        return super.getCompactionManagerProxy();
    }

    @Override
    public List<String> getTokens()
    {
//        return super.getTokens();
        log(" getTokens()");
        return getTokens(getLocalBroadCastingAddress());
    }

    public String getLocalBroadCastingAddress() {
        // FIXME:
        // There is no straight API to get the broadcasting
        // address, instead of trying to figure it out from the configuration
        // we will use the getHostIdToAddressMap with the hostid
        return getHostIdToAddressMap().get(getLocalHostId());
    }

    /** Retrieve the mapping of endpoint to host ID */
    public Map<String, String> getHostIdToAddressMap() {
        log(" getHostIdToAddressMap()");
        return client.getReverseMapStrValue("/storage_service/host_id");
    }

    @Override
    public List<String> getTokens(String endpoint)
    {
//            return super.getTokens(endpoint);
        log(" getTokens(String endpoint) throws UnknownHostException");
        return client.getListStrValue("/storage_service/tokens/" + endpoint);
    }

    @Override
    public String getLocalHostId()
    {
        log(" getLocalHostId()");
        return client.getStringValue("/storage_service/hostid/local");
    }

    public Map<String, String> getHostIdMap()
    {
        log(" getHostIdMap()");
        return client.getMapStrValue("/storage_service/host_id");
    }

    public String getLoadString()
    {
        log(" getLoadString()");
        return FileUtils.stringifyFileSize(getLoad());
    }

    /**
     * Numeric load value.
     *
     * @see org.apache.cassandra.metrics.StorageMetrics#load
     */
    @Deprecated
    private double getLoad() {
        log(" getLoad()");
        return client.getDoubleValue("/storage_service/load");
    }

    public String getReleaseVersion()
    {
        return super.getReleaseVersion();
    }

    @Override
    public int getCurrentGenerationNumber()
    {
        log(" getCurrentGenerationNumber()");
        return client.getIntValue("/storage_service/generation_number");
    }

    @Override
    public long getUptime()
    {
        log(" getUptime()");
        return client.getLongValue("/system/uptime_ms");
    }

    @Override
    public MemoryUsage getHeapMemoryUsage()
    {
        //TODO FIX this to get uptime from scylla server NOT from JMX mxbean!
        return new MemoryUsage(0,0,0,0);
    }

    /**
     * Take a snapshot of all the keyspaces, optionally specifying only a specific column family.
     *
     * @param snapshotName the name of the snapshot.
     * @param table the table to snapshot or all on null
     * @param options Options (skipFlush for now)
     * @param keyspaces the keyspaces to snapshot
     */
    public void takeSnapshot(String snapshotName, String table, Map<String, String> options, String... keyspaces) throws IOException
    {
        super.takeSnapshot(snapshotName, table, options,keyspaces);
    }

    /**
     * Take a snapshot of all column family from different keyspaces.
     *
     * @param snapshotName
     *            the name of the snapshot.
     * @param options
     *            Options (skipFlush for now)
     * @param tableList
     *            list of columnfamily from different keyspace in the form of ks1.cf1 ks2.cf2
     */
    public void takeMultipleTableSnapshot(String snapshotName, Map<String, String> options, String... tableList)
            throws IOException
    {
        super.takeMultipleTableSnapshot(snapshotName,options,tableList);
    }

    /**
     * Remove all the existing snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaces) throws IOException
    {
        super.clearSnapshot(tag, keyspaces);
    }

    public Map<String, TabularData> getSnapshotDetails()
    {
        return super.getSnapshotDetails();
    }

    public long trueSnapshotsSize()
    {
        return super.trueSnapshotsSize();
    }

    @Override
    public boolean isJoined()
    {
        log(" isJoined()");
        return client.getBooleanValue("/storage_service/join_ring");
    }

    public boolean isDrained()
    {
        return super.isDrained();
    }

    public boolean isDraining()
    {
        return super.isDraining();
    }

    public void joinRing() throws IOException
    {
        super.joinRing();
    }

    public void decommission() throws InterruptedException
    {
        super.decommission();
    }

    public void move(String newToken) throws IOException
    {
        super.move(newToken);
    }

    public void removeNode(String token)
    {
        super.removeNode(token);
    }

    public String getRemovalStatus()
    {
        return super.getRemovalStatus();
    }

    public void forceRemoveCompletion()
    {
        super.forceRemoveCompletion();
    }

    public void assassinateEndpoint(String address) throws UnknownHostException
    {
        super.assassinateEndpoint(address);
    }

    /**
     * Set the compaction threshold
     *
     * @param minimumCompactionThreshold minimum compaction threshold
     * @param maximumCompactionThreshold maximum compaction threshold
     */
    public void setCompactionThreshold(String ks, String cf, int minimumCompactionThreshold, int maximumCompactionThreshold)
    {
        super.setCompactionThreshold(ks, cf, minimumCompactionThreshold, maximumCompactionThreshold);
    }

    public void disableAutoCompaction(String ks, String ... tables) throws IOException
    {
        super.disableAutoCompaction(ks, tables);
    }

    public void enableAutoCompaction(String ks, String ... tableNames) throws IOException
    {
        super.enableAutoCompaction(ks, tableNames);
    }

    public void setIncrementalBackupsEnabled(boolean enabled)
    {
        super.setIncrementalBackupsEnabled(enabled);
    }

    public boolean isIncrementalBackupsEnabled()
    {
        return super.isIncrementalBackupsEnabled();
    }

    public void setCacheCapacities(int keyCacheCapacity, int rowCacheCapacity, int counterCacheCapacity)
    {
        super.setCacheCapacities(keyCacheCapacity,rowCacheCapacity,counterCacheCapacity);
    }

    public void setCacheKeysToSave(int keyCacheKeysToSave, int rowCacheKeysToSave, int counterCacheKeysToSave)
    {
        super.setCacheKeysToSave(keyCacheKeysToSave, rowCacheKeysToSave, counterCacheKeysToSave);
    }

    public void setHintedHandoffThrottleInKB(int throttleInKB)
    {
        super.setHintedHandoffThrottleInKB(throttleInKB);
    }

    public List<InetAddress> getEndpoints(String keyspace, String cf, String key)
    {
        return super.getEndpoints(keyspace, cf, key);
    }

    public List<String> getSSTables(String keyspace, String cf, String key, boolean hexFormat)
    {
        return super.getSSTables(keyspace, cf, key, hexFormat);
    }

    public Set<StreamState> getStreamStatus()
    {
        return super.getStreamStatus();
    }

    public String getOperationMode()
    {
        return super.getOperationMode();
    }

    public boolean isStarting()
    {
        return super.isStarting();
    }

    public void truncate(String keyspaceName, String tableName)
    {
            super.truncate(keyspaceName, tableName);
    }

    public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy()
    {
        return super.getEndpointSnitchInfoProxy();
    }

    public DynamicEndpointSnitchMBean getDynamicEndpointSnitchInfoProxy()
    {
        return super.getDynamicEndpointSnitchInfoProxy();
    }

    public ColumnFamilyStoreMBean getCfsProxy(String ks, String cf)
    {
        return super.getCfsProxy(ks, cf);
    }

    public StorageProxyMBean getSpProxy()
    {
        return super.getSpProxy();
    }

    public String getEndpoint()
    {
        return super.getEndpoint();
    }

    @Override
    public String getDataCenter()
    {
        return client.getStringValue("/snitch/datacenter", null, 10000);
//        log(" getDatacenter(String host) throws UnknownHostException");
//        MultivaluedMap<String, String> queryParams = null;
//        try {
//            queryParams = host != null ? new MultivaluedHashMap<String, String>(
//                    singletonMap("host", InetAddress.getByName(host).getHostAddress())) : null;
//        } catch (UnknownHostException e) {
//            e.printStackTrace(); //TODO fix DNS name lookup error
//        }
//        return client.getStringValue("/snitch/datacenter", queryParams, 10000);
    }

    @Override
    public String getRack()
    {
        return client.getStringValue("/snitch/rack", null, 10000);
    }

    public List<String> getKeyspaces()
    {
        return super.getKeyspaces();
    }

    public List<String> getNonSystemKeyspaces()
    {
        return super.getNonSystemKeyspaces();
    }

    public List<String> getNonLocalStrategyKeyspaces()
    {
        return super.getNonLocalStrategyKeyspaces();
    }

    public String getClusterName()
    {
        return super.getClusterName();
    }

    public String getPartitioner()
    {
        return super.getPartitioner();
    }

    public void disableHintedHandoff()
    {
        super.disableHintedHandoff();
    }

    public void enableHintedHandoff()
    {
        super.enableHintedHandoff();
    }

    public boolean isHandoffEnabled()
    {
        return super.isHandoffEnabled();
    }

    public void enableHintsForDC(String dc)
    {
        super.enableHintsForDC(dc);
    }

    public void disableHintsForDC(String dc)
    {
        super.disableHintsForDC(dc);
    }

    public Set<String> getHintedHandoffDisabledDCs()
    {
        return super.getHintedHandoffDisabledDCs();
    }

    public Map<String, String> getViewBuildStatuses(String keyspace, String view)
    {
        return super.getViewBuildStatuses(keyspace, view);
    }

    public void pauseHintsDelivery()
    {
        super.pauseHintsDelivery();
    }

    public void resumeHintsDelivery()
    {
        super.pauseHintsDelivery();
    }

    public void truncateHints(final String host)
    {
        super.truncateHints(host);
    }

    public void truncateHints()
    {
            super.truncateHints();
    }

    public void refreshSizeEstimates()
    {
            super.refreshSizeEstimates();
    }

    public void stopNativeTransport()
    {
        super.stopNativeTransport();
    }

    public void startNativeTransport()
    {
        super.startNativeTransport();
    }

    public boolean isNativeTransportRunning()
    {
        return super.isNativeTransportRunning();
    }

    public void stopGossiping()
    {
        super.stopGossiping();
    }

    public void startGossiping()
    {
        super.startGossiping();
    }

    @Override
    public boolean isGossipRunning()
    {
        log(" isGossipRunning()");
        return client.getBooleanValue("/storage_service/gossiping");
    }

    public void stopThriftServer()
    {
        super.stopThriftServer();
    }

    public void startThriftServer()
    {
        super.startThriftServer();
    }

    @Override
    public boolean isThriftServerRunning()
    {
        log(" isRPCServerRunning()");
        return client.getBooleanValue("/storage_service/rpc_server");
    }

    public void stopCassandraDaemon()
    {
        super.stopCassandraDaemon();
    }

    public boolean isInitialized()
    {
        return super.isInitialized();
    }

    public void setCompactionThroughput(int value)
    {
        super.setCompactionThroughput(value);
    }

    public int getCompactionThroughput()
    {
        return super.getCompactionThroughput();
    }

    public void setConcurrentCompactors(int value)
    {
        super.setConcurrentCompactors(value);
    }

    public int getConcurrentCompactors()
    {
        return super.getConcurrentCompactors();
    }

    public long getTimeout(String type)
    {
        return super.getTimeout(type);
    }

    public int getStreamThroughput()
    {
        return super.getStreamThroughput();
    }

    public int getInterDCStreamThroughput()
    {
        return super.getInterDCStreamThroughput();
    }

    public double getTraceProbability()
    {
        return super.getTraceProbability();
    }

    public int getExceptionCount()
    {
        return (int)StorageMetrics.exceptions.getCount();
    }

    public Map<String, Integer> getDroppedMessages()
    {
        return msProxy.getDroppedMessages();
    }

    public void loadNewSSTables(String ksName, String cfName)
    {
        super.loadNewSSTables(ksName, cfName);
    }

    public void rebuildIndex(String ksName, String cfName, String... idxNames)
    {
        super.rebuildIndex(ksName, cfName, idxNames);
    }

    public String getGossipInfo()
    {
        return super.getGossipInfo();
    }

    public void stop(String string)
    {
        super.stop(string);
    }

    public void setTimeout(String type, long value)
    {
        super.setTimeout(type, value);
    }

    public void stopById(String compactionId)
    {
        super.stopById(compactionId);
    }

    public void setStreamThroughput(int value)
    {
        super.setStreamThroughput(value);
    }

    public void setInterDCStreamThroughput(int value)
    {
        super.setInterDCStreamThroughput(value);
    }

    public void setTraceProbability(double value)
    {
        super.setTraceProbability(value);
    }

    public String getSchemaVersion()
    {
        return super.getSchemaVersion();
    }

    public List<String> describeRing(String keyspaceName) throws IOException
    {
        return super.describeRing(keyspaceName);
    }

    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources)
    {
        super.rebuild(sourceDc, keyspace, tokens, specificSources);
    }

    public List<String> sampleKeyRange()
    {
        return super.sampleKeyRange();
    }

    public void resetLocalSchema() throws IOException
    {
        super.resetLocalSchema();
    }

    public void reloadLocalSchema()
    {
        super.reloadLocalSchema();
    }

    public boolean isFailed()
    {
        return super.isFailed();
    }

    public long getReadRepairAttempted()
    {
        return super.getReadRepairAttempted();
    }

    public long getReadRepairRepairedBlocking()
    {
        return super.getReadRepairRepairedBlocking();
    }

    public long getReadRepairRepairedBackground()
    {
        return super.getReadRepairRepairedBackground();
    }


    static Map<String, String> uriCacheRegistry = new HashMap<>();
    static {
        uriCacheRegistry.put("Capacity", "capacity"); //Long.class
        uriCacheRegistry.put("Hits", "hits_moving_avrage");
        uriCacheRegistry.put("Requests", "requests_moving_avrage");
        uriCacheRegistry.put("HitRate", "hit_rate"); //Double.class
        uriCacheRegistry.put("Size", "size");
        uriCacheRegistry.put("Entries", "entries"); //Integer.class
    }

    static Map<String, String> uriCacheTypeRegistry = new HashMap<>();
    static {
        uriCacheTypeRegistry.put("RowCache", "row");
        uriCacheTypeRegistry.put("KeyCache", "key");
        uriCacheTypeRegistry.put("CounterCache", "counter");
        uriCacheTypeRegistry.put("ChunkCache", "");

    }

    // JMX getters for the o.a.c.metrics API below.
    /**
     * Retrieve cache metrics based on the cache type (KeyCache, RowCache, or CounterCache)
     * @param cacheType KeyCach, RowCache, or CounterCache
     * @param metricName Capacity, Entries, HitRate, Size, Requests or Hits.
     */
    public Object getCacheMetric(String cacheType, String metricName)
    {

        if (cacheType=="ChunkCache") {
            if (metricName=="MissLatencyUnit") {
                return TimeUnit.MICROSECONDS;
            } else {
                if (metricName=="Entries") {
                    return 0;
                } else if (metricName=="HitRate") {
                    return 0D;
                } else {
                    return 0L;
                }
            }
        }
        String url = "/cache_service/metrics/" + uriCacheTypeRegistry.get(cacheType) + "/" + uriCacheRegistry.get(metricName);

//        return super.getCacheMetric(cacheType, metricName);
//        try
//        {

            /*
            @Override
        public void update(JsonObject obj) {
            meter = new Meter(obj);
        }

        - get above for different types - gauge, meter ,histogram  in scylla-jmx MetricsRegistry !!!!!!!

        - from         public Histogram(JsonObject obj) {
or        public Meter(JsonObject obj) {
or         public EstimatedHistogram(JsonObject obj) {

             */
            switch(metricName)
            {
                case "Capacity":
                case "Size":
                    return client.getLongValue(url); //TODO fix for proper types using getReader(xxx)
                case "Entries":
                    return client.getIntValue(url);
                case "HitRate":
                    return client.getDoubleValue(url);
//                    return JMX.newMBeanProxy(mbeanServerConn,
//                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName),
//                            CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                case "Requests":
                case "Hits":
                case "Misses": {
                    JsonObject obj = client.getJsonObj(url,null);
//                    JsonArray rates = obj.getJsonArray("rates");
//                    Double oneMinuteRate = rates.getJsonNumber(0).doubleValue();
//                    Double fiveMinuteRate = rates.getJsonNumber(1).doubleValue();
//                    Double fifteenMinuteRate = rates.getJsonNumber(2).doubleValue();
//                    Double meanRate = obj.getJsonNumber("mean_rate").doubleValue();
                    Long count = obj.getJsonNumber("count").longValue();
                    return count;
                }
//                    return JMX.newMBeanProxy(mbeanServerConn,
//                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName),
//                            CassandraMetricsRegistry.JmxMeterMBean.class).getCount();
                case "MissLatency":
                    return 0D; //TODO implement call on server side?
//                    return JMX.newMBeanProxy(mbeanServerConn,
//                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=" + metricName),
//                            CassandraMetricsRegistry.JmxTimerMBean.class).getMean();
                case "MissLatencyUnit":
                    return TimeUnit.MICROSECONDS.toString();
//                    return JMX.newMBeanProxy(mbeanServerConn,
//                            new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=" + cacheType + ",name=MissLatency"),
//                            CassandraMetricsRegistry.JmxTimerMBean.class).getDurationUnit();
                default:
                    throw new RuntimeException("Unknown cache metric name.");

            }
//        }
//        catch (MalformedObjectNameException e)
//        {
//            throw new RuntimeException(e);
//        }
    }

    public static <T> BiFunction<APIClient, String, T> getReader(Class<T> type) {
        if (type == String.class) {
            return (c, s) -> type.cast(c.getRawValue(s));
        } else if (type == Integer.class) {
            return (c, s) -> type.cast(c.getIntValue(s));
        } else if (type == Double.class) {
            return (c, s) -> type.cast(c.getDoubleValue(s));
        } else if (type == Long.class) {
            return (c, s) -> type.cast(c.getLongValue(s));
        }
        throw new IllegalArgumentException(type.getName());
    }

    public Object getThreadPoolMetric(String pathName, String poolName, String metricName)
    {
        return super.getThreadPoolMetric(pathName, poolName, metricName);
    }

    /**
     * Retrieve threadpool paths and names for threadpools with metrics.
     * @return Multimap from path (internal, request, etc.) to name
     */
    public Multimap<String, String> getThreadPools()
    {
        return super.getThreadPools();
    }

    public int getNumberOfTables()
    {
        return super.getNumberOfTables();
    }

    static Map<String, String> uriCFMetricRegistry = new HashMap<>();
    static {
//registerCommon
        uriCFMetricRegistry.put("MemtableColumnsCount", "memtable_columns_count");
        uriCFMetricRegistry.put("MemtableOnHeapSize", "memtable_on_heap_size");
        uriCFMetricRegistry.put("MemtableOffHeapSize", "memtable_off_heap_size");
        uriCFMetricRegistry.put("MemtableLiveDataSize", "memtable_live_data_size");
        uriCFMetricRegistry.put("AllMemtablesHeapSize", "all_memtables_on_heap_size");
        uriCFMetricRegistry.put("AllMemtablesOffHeapSize", "all_memtables_off_heap_size");
        uriCFMetricRegistry.put("AllMemtablesLiveDataSize", "all_memtables_live_data_size");

        uriCFMetricRegistry.put("MemtableSwitchCount", "memtable_switch_count");

        uriCFMetricRegistry.put("SSTablesPerReadHistogram", "sstables_per_read_histogram");
        uriCFMetricRegistry.put("CompressionRatio", "compression_ratio");

        uriCFMetricRegistry.put("PendingFlushes", "pending_flushes");

        uriCFMetricRegistry.put("PendingCompactions", "pending_compactions");
        uriCFMetricRegistry.put("LiveSSTableCount", "live_ss_table_count");

        uriCFMetricRegistry.put("LiveDiskSpaceUsed", "live_disk_space_used");
        uriCFMetricRegistry.put("TotalDiskSpaceUsed", "total_disk_space_used");
        uriCFMetricRegistry.put("MinPartitionSize", "min_row_size");
        uriCFMetricRegistry.put("MaxPartitionSize", "max_row_size");
        uriCFMetricRegistry.put("MeanPartitionSize", "mean_row_size");

        uriCFMetricRegistry.put("BloomFilterFalsePositives", "bloom_filter_false_positives");
        uriCFMetricRegistry.put("RecentBloomFilterFalsePositives", "recent_bloom_filter_false_positives");
        uriCFMetricRegistry.put("BloomFilterFalseRatio", "bloom_filter_false_ratio");
        uriCFMetricRegistry.put("RecentBloomFilterFalseRatio", "recent_bloom_filter_false_ratio");

        uriCFMetricRegistry.put("BloomFilterDiskSpaceUsed", "bloom_filter_disk_space_used");
        uriCFMetricRegistry.put("BloomFilterOffHeapMemoryUsed", "bloom_filter_off_heap_memory_used");
        uriCFMetricRegistry.put("IndexSummaryOffHeapMemoryUsed", "index_summary_off_heap_memory_used");
        uriCFMetricRegistry.put("CompressionMetadataOffHeapMemoryUsed", "compression_metadata_off_heap_memory_used");
        uriCFMetricRegistry.put("SpeculativeRetries", "speculative_retries");

        uriCFMetricRegistry.put("TombstoneScannedHistogram", "tombstone_scanned_histogram");
        uriCFMetricRegistry.put("LiveScannedHistogram", "live_scanned_histogram");
        uriCFMetricRegistry.put("ColUpdateTimeDeltaHistogram", "col_update_time_delta_histogram");

    // We do not want to capture view mutation specific metrics for a view
    // They only makes sense to capture on the base table
    // TODO: views
    // if (!cfs.metadata.isView())
    // {
    // viewLockAcquireTime = createTableTimer("ViewLockAcquireTime",
    // cfs.keyspace.metric.viewLockAcquireTime);
    // viewReadTime = createTableTimer("ViewReadTime",
    // cfs.keyspace.metric.viewReadTime);
    // }

        uriCFMetricRegistry.put("SnapshotsSize", "snapshots_size");
        uriCFMetricRegistry.put("RowCacheHitOutOfRange", "row_cache_hit_out_of_range");
        uriCFMetricRegistry.put("RowCacheHit", "row_cache_hit");
        uriCFMetricRegistry.put("RowCacheMiss", "row_cache_miss");
        // TODO: implement
//        uriCFMetricRegistry.put("PercentRepaired","");

        //TODO globalLatency from TableMetrics

        //registerLocal
        uriCFMetricRegistry.put("EstimatedPartitionSizeHistogram","estimated_row_size_histogram"); //"EstimatedRowSizeHistogram"
        uriCFMetricRegistry.put("EstimatedPartitionCount", "estimated_row_count"); //"EstimatedRowCount"
        uriCFMetricRegistry.put("EstimatedColumnCountHistogram", "estimated_column_count_histogram");
        uriCFMetricRegistry.put("KeyCacheHitRate", "key_cache_hit_rate");

        uriCFMetricRegistry.put("CoordinatorReadLatency", "coordinator/read");
        uriCFMetricRegistry.put("CoordinatorScanLatency", "coordinator/scan");
        uriCFMetricRegistry.put("WaitingOnFreeMemtableSpace", "waiting_on_free_memtable");


        uriCFMetricRegistry.put("WriteLatency", "write_latency/estimated_recent_histogram");
        uriCFMetricRegistry.put("ReadLatency", "read_latency/estimated_recent_histogram");

        uriCFMetricRegistry.put("WriteTotalLatency", "write_latency/estimated_histogram");
        uriCFMetricRegistry.put("ReadTotalLatency", "read_latency/estimated_histogram");



        //TODO latencyMetrics fromTableMetrics

        // TODO: implement
        //registry.createDummyTableCounter("DroppedMutations");


    }

    //custom for RESTInfo class to avoid counting metrics together
    public Long getAggrColumnFamilyMetric(String metricName)
    {
        return client.getLongValue("/column_family/metrics/"+ uriCFMetricRegistry.get(metricName));
    }

    /**
     * Retrieve ColumnFamily metrics
     * @param ks Keyspace for which stats are to be displayed or null for the global value
     * @param cf ColumnFamily for which stats are to be displayed or null for the keyspace value (if ks supplied)
     * @param metricName View {@link TableMetrics}.
     */
    @Override
    public Object getColumnFamilyMetric(String ks, String cf, String metricName)
    {
//        return super.getColumnFamilyMetric(ks, cf, metricName);
        String post="";
        if (ks!=null && cf!=null) {
            post="/" + ks + ":" + cf;
        }
            switch(metricName)
            {
                case "BloomFilterDiskSpaceUsed":
                case "BloomFilterFalsePositives":
                case "BloomFilterOffHeapMemoryUsed":
                case "IndexSummaryOffHeapMemoryUsed":
                case "CompressionMetadataOffHeapMemoryUsed":
                case "EstimatedPartitionCount":
                case "MaxPartitionSize":
                case "MeanPartitionSize":
                case "MemtableColumnsCount":
                case "MemtableLiveDataSize":
                case "MemtableOffHeapSize":
                case "MinPartitionSize":
                case "RecentBloomFilterFalsePositives":
                case "SnapshotsSize": {
                    return client.getLongValue("/column_family/metrics/" + uriCFMetricRegistry.get(metricName) + post);
//                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                }
                case "EstimatedPartitionSizeHistogram":
                case "EstimatedColumnCountHistogram": {
                    return client.getEstimatedHistogramAsLongArrValue("/column_family/metrics/"+ uriCFMetricRegistry.get(metricName)+post); //TODO fix conversion
                    //TODO
                    // APIClient::getEstimatedHistogramAsLongArrValue
                }
                case "LiveSSTableCount": //Integer
                case "PendingCompactions": {
                    if (cf==null) {
                        post="/" + ks;
                        return client.getLongValue("/column_family/metrics/" + uriCFMetricRegistry.get(metricName) + post);
                    }
                    return client.getIntValue("/column_family/metrics/" + uriCFMetricRegistry.get(metricName) + post);
//                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                }
                case "KeyCacheHitRate":
                case "BloomFilterFalseRatio": //Double
                case "CompressionRatio":
                case "RecentBloomFilterFalseRatio":
                {

                    return client.getDoubleValue("/column_family/metrics/" + uriCFMetricRegistry.get(metricName) + post);
//                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                }
                case "PercentRepaired": //Double //TODO types dedup - similar as with getReader on getCacheMetric !!!
                { //TODO - this needs server implementation !!!!
                    return 0D;
//                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
                }
                case "LiveDiskSpaceUsed":
                case "MemtableSwitchCount":
                case "SpeculativeRetries":
                case "TotalDiskSpaceUsed":
                case "WriteTotalLatency":
                case "ReadTotalLatency":
                case "PendingFlushes":
                case "DroppedMutations":
                    return client.getLongValue("/column_family/metrics/"+ uriCFMetricRegistry.get(metricName)+ post);
//                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxCounterMBean.class).getCount();
                case "CoordinatorReadLatency":
                case "CoordinatorScanLatency":
                case "ReadLatency":
                case "WriteLatency": //TODO return histogram???
                {
                    // TODO: this is not atomic.
//                    super.update(obj.getJsonObject("meter"));
//                    histogram = new Histogram(obj.getJsonObject("hist"));

                    JsonObject obj = client.getJsonObj("/column_family/metrics/"+ uriCFMetricRegistry.get(metricName)+ post,null);
//                    JsonArray rates = obj.getJsonArray("rates");
//                    Double oneMinuteRate = rates.getJsonNumber(0).doubleValue();
//                    Double fiveMinuteRate = rates.getJsonNumber(1).doubleValue();
//                    Double fifteenMinuteRate = rates.getJsonNumber(2).doubleValue();
//                    Double meanRate = obj.getJsonNumber("mean_rate").doubleValue();
                    Long count = obj.getJsonNumber("count").longValue();
                    return count;
                }
//                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxTimerMBean.class);
                case "LiveScannedHistogram":
                case "SSTablesPerReadHistogram":
                case "TombstoneScannedHistogram": //TODO return histogram???
                    return client.getEstimatedHistogramAsLongArrValue("/column_family/metrics/"+ uriCFMetricRegistry.get(metricName)+"/"+ks+":"+cf); //TODO fix conversion
//                    return JMX.newMBeanProxy(mbeanServerConn, oName, CassandraMetricsRegistry.JmxHistogramMBean.class);
                default:
                    throw new RuntimeException("Unknown table metric " + metricName);
            }
    }

    private static <T> BiFunction<APIClient, String, T> getDummy(Class<T> type) {
        if (type == String.class) {
            return (c, s) -> type.cast("");
        } else if (type == Integer.class) {
            return (c, s) -> type.cast(0);
        } else if (type == Double.class) {
            return (c, s) -> type.cast(0.0);
        } else if (type == Long.class) {
            return (c, s) -> type.cast(0L);
        }
        throw new IllegalArgumentException(type.getName());
    }

    /**
     * Retrieve Proxy metrics
     * @param scope RangeSlice, Read or Write
     */
    public CassandraMetricsRegistry.JmxTimerMBean getProxyMetric(String scope)
    {
        return super.getProxyMetric(scope);
    }

    /**
     * Retrieve Proxy metrics
     * @param metricName CompletedTasks, PendingTasks, BytesCompacted or TotalCompactionsCompleted.
     */
    public Object getCompactionMetric(String metricName)
    {
        return super.getCompactionMetric(metricName);
    }

    static Map<String, String> uriStorageRegistry = new HashMap<>();
    static {
        uriStorageRegistry.put("Load", "/storage_service/metrics/load");
        uriStorageRegistry.put("Exceptions", "/storage_service/metrics/exceptions");
        uriStorageRegistry.put("TotalHintsInProgress", "/storage_service/metrics/hints_in_progress");
        uriStorageRegistry.put("TotalHints", "/storage_service/metrics/total_hints");
    }
    /**
     * Retrieve Proxy metrics
     * @param metricName Exceptions, Load, TotalHints or TotalHintsInProgress.
     */
    @Override
    public long getStorageMetric(String metricName)
    {
        return client.getLongValue(uriStorageRegistry.get(metricName));
    }

    public double[] metricPercentilesAsArray(CassandraMetricsRegistry.JmxHistogramMBean metric)
    {
        return new double[]{ metric.get50thPercentile(),
                metric.get75thPercentile(),
                metric.get95thPercentile(),
                metric.get98thPercentile(),
                metric.get99thPercentile(),
                metric.getMin(),
                metric.getMax()};
    }

    public double[] metricPercentilesAsArray(CassandraMetricsRegistry.JmxTimerMBean metric)
    {
        return new double[]{ metric.get50thPercentile(),
                metric.get75thPercentile(),
                metric.get95thPercentile(),
                metric.get98thPercentile(),
                metric.get99thPercentile(),
                metric.getMin(),
                metric.getMax()};
    }

    public TabularData getCompactionHistory()
    {
        return super.getCompactionHistory();
    }

    public void reloadTriggers()
    {
        super.reloadTriggers();
    }

    public void setLoggingLevel(String classQualifier, String level)
    {
            super.setLoggingLevel(classQualifier, level);
    }

    public Map<String, String> getLoggingLevels()
    {
        return super.getLoggingLevels();
    }

    public void resumeBootstrap(PrintStream out) throws IOException
    {
        super.resumeBootstrap(out);
    }

    public void replayBatchlog() throws IOException
    {
        super.replayBatchlog();
    }

    public TabularData getFailureDetectorPhilValues()
    {
            return super.getFailureDetectorPhilValues();
    }


    private static final Logger logger = Logger.getLogger(RESTNodeProbe.class.getName());
    public void log(String str) {
        logger.finest(str);
    }

    public int getKeyCacheSavePeriodInSeconds() {
        log(" getKeyCacheSavePeriodInSeconds()");
        return client.getIntValue("cache_service/key_cache_save_period");
    }

    public int getRowCacheSavePeriodInSeconds() {
        log(" getRowCacheSavePeriodInSeconds()");
        return client.getIntValue("cache_service/row_cache_save_period");
    }

    public int getCounterCacheSavePeriodInSeconds() {
        log(" getCounterCacheSavePeriodInSeconds()");
        return client.getIntValue("cache_service/counter_cache_save_period");
    }

}

//TODO below is unused, was a PoC for mocking mbeans to avoid changing Info.class (but then I went for RESTInfo anyways
class RESTColumnFamilyStoreMBeanIterator implements Iterator<Map.Entry<String, ColumnFamilyStoreMBean>>
{
    private MBeanServerConnection mbeanServerConn;
    Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> mbeans;

    public RESTColumnFamilyStoreMBeanIterator(APIClient client)
            throws MalformedObjectNameException, NullPointerException, IOException
    {

        JsonArray tables = client.getJsonArray("/column_family/name"); // format keyspace:table

        List<Map.Entry<String, ColumnFamilyStoreMBean>> cfMbeans = new ArrayList<Map.Entry<String, ColumnFamilyStoreMBean>>(tables.size());
        for (JsonString record : tables.getValuesAs(JsonString.class ) ) {
            String srecord=record.getString();
            String[] sarray=srecord.split(":");
            String keyspaceName=sarray[0];
            String tableName=null;
            if (sarray.length>1) {
                tableName=sarray[1];}
            CFMetaData cfmd = CFMetaData.Builder.create(keyspaceName, tableName, false, false, false)
                    .addPartitionKey("pkey", AsciiType.instance)
                    .addClusteringColumn("name", AsciiType.instance)
                    .addRegularColumn("val", AsciiType.instance)
                    .build();
            ColumnFamilyStoreMBean cfs = new ColumnFamilyStore(Keyspace.openWithoutSSTables(keyspaceName),srecord.replaceFirst(":","."),0,cfmd,new Directories(cfmd),false,false,false);
            cfMbeans.add( new AbstractMap.SimpleImmutableEntry<String, ColumnFamilyStoreMBean>(keyspaceName, cfs) );
        }
//                getCFSMBeans(mbeanServerConn, "ColumnFamilies");
//        cfMbeans.addAll(getCFSMBeans(mbeanServerConn, "IndexColumnFamilies"));
        Collections.sort(cfMbeans, new Comparator<Map.Entry<String, ColumnFamilyStoreMBean>>()
        {
            public int compare(Map.Entry<String, ColumnFamilyStoreMBean> e1, Map.Entry<String, ColumnFamilyStoreMBean> e2)
            {
                //compare keyspace, then CF name, then normal vs. index
                int keyspaceNameCmp = e1.getKey().compareTo(e2.getKey());
                if(keyspaceNameCmp != 0)
                    return keyspaceNameCmp;

                // get CF name and split it for index name
                String e1CF[] = e1.getValue().getColumnFamilyName().split("\\.");
                String e2CF[] = e2.getValue().getColumnFamilyName().split("\\.");
                assert e1CF.length <= 2 && e2CF.length <= 2 : "unexpected split count for table name";

                //if neither are indexes, just compare CF names
                if(e1CF.length == 1 && e2CF.length == 1)
                    return e1CF[0].compareTo(e2CF[0]);

                //check if it's the same CF
                int cfNameCmp = e1CF[0].compareTo(e2CF[0]);
                if(cfNameCmp != 0)
                    return cfNameCmp;

                // if both are indexes (for the same CF), compare them
                if(e1CF.length == 2 && e2CF.length == 2)
                    return e1CF[1].compareTo(e2CF[1]);

                //if length of e1CF is 1, it's not an index, so sort it higher
                return e1CF.length == 1 ? 1 : -1;
            }
        });
        mbeans = cfMbeans.iterator();
    }

    //TODO delete if you are sure we get both IndexColumnFamilies and ColumnFamilies
    private List<Map.Entry<String, ColumnFamilyStoreMBean>> getCFSMBeans(MBeanServerConnection mbeanServerConn, String type)
            throws MalformedObjectNameException, IOException
    {
        ObjectName query = new ObjectName("org.apache.cassandra.db:type=" + type +",*");
        Set<ObjectName> cfObjects = mbeanServerConn.queryNames(query, null);
        List<Map.Entry<String, ColumnFamilyStoreMBean>> mbeans = new ArrayList<Map.Entry<String, ColumnFamilyStoreMBean>>(cfObjects.size());
        for(ObjectName n : cfObjects)
        {
            String keyspaceName = n.getKeyProperty("keyspace");
            ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(mbeanServerConn, n, ColumnFamilyStoreMBean.class);
            mbeans.add(new AbstractMap.SimpleImmutableEntry<String, ColumnFamilyStoreMBean>(keyspaceName, cfsProxy));
        }
        return mbeans;
    }

    public boolean hasNext()
    {
        return mbeans.hasNext();
    }

    public Map.Entry<String, ColumnFamilyStoreMBean> next()
    {
        return mbeans.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}