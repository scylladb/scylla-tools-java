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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
package org.apache.cassandra.service;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.TabularData;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.cassandra.repair.RepairParallelism;

import com.google.common.base.Joiner;
import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.utils.FileUtils;

/**
 * This abstraction contains the token/identifier of this node on the identifier
 * space. This token gets gossiped around. This class will also maintain
 * histograms of the load information of other nodes in the cluster.
 */
public class ScyllaStorageService implements StorageServiceMBean {
    private static final Logger logger = Logger.getLogger(StorageService.class.getName());
    private static final Timer timer = new Timer("Storage Service Repair", true);

    private final NotificationBroadcasterSupport notificationBroadcasterSupport = new NotificationBroadcasterSupport();

    @Override
    public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) {
        notificationBroadcasterSupport.addNotificationListener(listener, filter, handback);
    }

    @Override
    public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
        notificationBroadcasterSupport.removeNotificationListener(listener);
    }

    @Override
    public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
            throws ListenerNotFoundException {
        notificationBroadcasterSupport.removeNotificationListener(listener, filter, handback);
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        return notificationBroadcasterSupport.getNotificationInfo();
    }

    public void sendNotification(Notification notification) {
        notificationBroadcasterSupport.sendNotification(notification);
    }

    public static enum RepairStatus {
        STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED
    }

    /* JMX notification serial number counter */
    private final AtomicLong notificationSerialNumber = new AtomicLong();
    protected final APIClient client;

    public ScyllaStorageService(APIClient client) {
        this.client=client;
//        super("org.apache.cassandra.db:type=StorageService", client, new StorageMetrics());

    }

    public void log(String str) {
        logger.finest(str);
    }

    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried.
     *
     * @return set of IP addresses, as Strings
     */
    @Override
    public List<String> getLiveNodes() {
        log(" getLiveNodes()");
        return client.getListStrValue("/gossiper/endpoint/live");
    }

    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined by
     * this node's failure detector.
     *
     * @return set of IP addresses, as Strings
     */
    @Override
    public List<String> getUnreachableNodes() {
        log(" getUnreachableNodes()");
        return client.getListStrValue("/gossiper/endpoint/down");
    }

    /**
     * Retrieve the list of nodes currently bootstrapping into the ring.
     *
     * @return set of IP addresses, as Strings
     */
    @Override
    public List<String> getJoiningNodes() {
        log(" getJoiningNodes()");
        return client.getListStrValue("/storage_service/nodes/joining");
    }

    /**
     * Retrieve the list of nodes currently leaving the ring.
     *
     * @return set of IP addresses, as Strings
     */
    @Override
    public List<String> getLeavingNodes() {
        log(" getLeavingNodes()");
        return client.getListStrValue("/storage_service/nodes/leaving");
    }

    /**
     * Retrieve the list of nodes currently moving in the ring.
     *
     * @return set of IP addresses, as Strings
     */
    @Override
    public List<String> getMovingNodes() {
        log(" getMovingNodes()");
        return client.getListStrValue("/storage_service/nodes/moving");
    }

    /**
     * Fetch string representations of the tokens for this node.
     *
     * @return a collection of tokens formatted as strings
     */
    @Override
    public List<String> getTokens() {
        log(" getTokens()");
        try {
            return getTokens(getLocalBroadCastingAddress());
        } catch (UnknownHostException e) {
            // We should never reach here,
            // but it makes the compiler happy
            return null;
        }
    }

    /**
     * Fetch string representations of the tokens for a specified node.
     *
     * @param endpoint
     *            string representation of an node
     * @return a collection of tokens formatted as strings
     */
    @Override
    public List<String> getTokens(String endpoint) throws UnknownHostException {
        log(" getTokens(String endpoint) throws UnknownHostException");
        return client.getListStrValue("/storage_service/tokens/" + endpoint);
    }

    /**
     * Fetch a string representation of the Cassandra version.
     *
     * @return A string representation of the Cassandra version.
     */
    @Override
    public String getReleaseVersion() {
        log(" getReleaseVersion()");
        return client.getStringValue("/storage_service/release_version");
    }

    /**
     * Fetch a string representation of the current Schema version.
     *
     * @return A string representation of the Schema version.
     */
    @Override
    public String getSchemaVersion() {
        log(" getSchemaVersion()");
        return client.getStringValue("/storage_service/schema_version");
    }

    /**
     * Get the list of all data file locations from conf
     *
     * @return String array of all locations
     */
    @Override
    public String[] getAllDataFileLocations() {
        log(" getAllDataFileLocations()");
        return client.getStringArrValue("/storage_service/data_file/locations");
    }

    /**
     * Get location of the commit log
     *
     * @return a string path
     */
    @Override
    public String getCommitLogLocation() {
        log(" getCommitLogLocation()");
        return client.getStringValue("/storage_service/commitlog");
    }

    /**
     * Get location of the saved caches dir
     *
     * @return a string path
     */
    @Override
    public String getSavedCachesLocation() {
        log(" getSavedCachesLocation()");
        return client.getStringValue("/storage_service/saved_caches/location");
    }

    /**
     * Retrieve a map of range to end points that describe the ring topology of
     * a Cassandra cluster.
     *
     * @return mapping of ranges to end points
     */
    @Override
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) {
        log(" getRangeToEndpointMap(String keyspace)");
        return client.getMapListStrValue("/storage_service/range/" + keyspace);
    }

    /**
     * Retrieve a map of range to rpc addresses that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to rpc addresses
     */
    @Override
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace) {
        log(" getRangeToRpcaddressMap(String keyspace)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("rpc", "true");
        return client.getMapListStrValue("/storage_service/range/" + keyspace, queryParams);
    }

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the
     * String for JMX compatibility
     *
     * @param keyspace
     *            The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given
     *         keyspace
     */
    @Override
    public List<String> describeRingJMX(String keyspace) throws IOException {
        log(" describeRingJMX(String keyspace) throws IOException");
        JsonArray arr = client.getJsonArray("/storage_service/describe_ring/" + keyspace);
        List<String> res = new ArrayList<String>();

        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            StringBuilder sb = new StringBuilder();
            sb.append("TokenRange(");
            sb.append("start_token:");
            sb.append(obj.getString("start_token"));
            sb.append(", end_token:");
            sb.append(obj.getString("end_token"));
            sb.append(", endpoints:[");
            JsonArray endpoints = obj.getJsonArray("endpoints");
            for (int j = 0; j < endpoints.size(); j++) {
                if (j > 0) {
                    sb.append(", ");
                }
                sb.append(endpoints.getString(j));
            }
            sb.append("], rpc_endpoints:[");
            JsonArray rpc_endpoints = obj.getJsonArray("rpc_endpoints");
            for (int j = 0; j < rpc_endpoints.size(); j++) {
                if (j > 0) {
                    sb.append(", ");
                }
                sb.append(rpc_endpoints.getString(j));
            }

            sb.append("], endpoint_details:[");
            JsonArray endpoint_details = obj.getJsonArray("endpoint_details");
            for (int j = 0; j < endpoint_details.size(); j++) {
                JsonObject detail = endpoint_details.getJsonObject(j);
                if (j > 0) {
                    sb.append(", ");
                }
                sb.append("EndpointDetails(");
                sb.append("host:");
                sb.append(detail.getString("host"));
                sb.append(", datacenter:");
                sb.append(detail.getString("datacenter"));
                sb.append(", rack:");
                sb.append(detail.getString("rack"));
                sb.append(')');
            }
            sb.append("])");
            res.add(sb.toString());
        }
        return res;
    }

    /**
     * Retrieve a map of pending ranges to endpoints that describe the ring
     * topology
     *
     * @param keyspace
     *            the keyspace to get the pending range map for.
     * @return a map of pending ranges to endpoints
     */
    @Override
    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace) {
        log(" getPendingRangeToEndpointMap(String keyspace)");
        return client.getMapListStrValue("/storage_service/pending_range/" + keyspace);
    }

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    @Override
    public Map<String, String> getTokenToEndpointMap() {
        log(" getTokenToEndpointMap()");
        return client.getMapStrValue("/storage_service/tokens_endpoint");
    }

    /** Retrieve this hosts unique ID */
    @Override
    public String getLocalHostId() {
        log(" getLocalHostId()");
        return client.getStringValue("/storage_service/hostid/local");
    }

    public String getLocalBroadCastingAddress() {
        // FIXME:
        // There is no straight API to get the broadcasting
        // address, instead of trying to figure it out from the configuration
        // we will use the getHostIdToAddressMap with the hostid
        return getHostIdToAddressMap().get(getLocalHostId());
    }

    /** Retrieve the mapping of endpoint to host ID */
    @Override
    public Map<String, String> getHostIdMap() {
        log(" getHostIdMap()");
        return client.getMapStrValue("/storage_service/host_id");
    }

    /** Retrieve the mapping of endpoint to host ID */
    public Map<String, String> getHostIdToAddressMap() {
        log(" getHostIdToAddressMap()");
        return client.getReverseMapStrValue("/storage_service/host_id");
    }

    /**
     * Numeric load value.
     *
     * @see org.apache.cassandra.metrics.StorageMetrics#load
     */
    @Deprecated
    public double getLoad() {
        log(" getLoad()");
        return client.getDoubleValue("/storage_service/load");
    }

    /** Human-readable load value */
    @Override
    public String getLoadString() {
        log(" getLoadString()");
        return FileUtils.stringifyFileSize(getLoad());
    }

    /** Human-readable load value. Keys are IP addresses. */
    @Override
    public Map<String, String> getLoadMap() {
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

    /**
     * Return the generation value for this node.
     *
     * @return generation number
     */
    @Override
    public int getCurrentGenerationNumber() {
        log(" getCurrentGenerationNumber()");
        return client.getIntValue("/storage_service/generation_number");
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName
     *            keyspace name
     * @param cf
     *            Column family name
     * @param key
     *            - key for which we need to find the endpoint return value -
     *            the endpoint responsible for this key
     */
    @Override
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key) {
        log(" getNaturalEndpoints(String keyspaceName, String cf, String key)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("cf", cf);
        queryParams.add("key", key);
        return client.getListInetAddressValue("/storage_service/natural_endpoints/" + keyspaceName, queryParams);
    }

    @Override
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key) {
        log(" getNaturalEndpoints(String keyspaceName, ByteBuffer key)");
        return client.getListInetAddressValue("");
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be
     * specified.
     *
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames
     *            the name of the keyspaces to snapshot; empty means "all."
     */
    @Override
    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException {
        takeSnapshot(tag, null, keyspaceNames);
    }

    @Override
    public void takeTableSnapshot(String keyspaceName, String tableName, String tag) throws IOException {

    }

    @Override
    public void takeMultipleTableSnapshot(String tag, String... tableList) throws IOException {

    }

    @Override
    public void takeSnapshot(String tag, Map<String, String> options, String... keyspaceNames) throws IOException {
        log(" takeSnapshot(String tag, String... keyspaceNames) throws IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "tag", tag);

        if (keyspaceNames.length == 1 && keyspaceNames[0].indexOf('.') != -1) {
            String[] parts = keyspaceNames[0].split("\\.");
            keyspaceNames = new String[] { parts[0] };
            APIClient.set_query_param(queryParams, "cf", parts[1]);
        }
        APIClient.set_query_param(queryParams, "kn", APIClient.join(keyspaceNames));
        // TODO: origin has one recognized option: skip flush. We don't.
        client.post("/storage_service/snapshots", queryParams);
    }

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be
     * specified.
     *
     * @param keyspaceName
     *            the keyspace which holds the specified column family
     * @param columnFamilyName
     *            the column family to snapshot
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     */
//    @Override
    public void takeColumnFamilySnapshot(String keyspaceName, String columnFamilyName, String tag) throws IOException {
        log(" takeColumnFamilySnapshot(String keyspaceName, String columnFamilyName, String tag) throws IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        if (keyspaceName == null) {
            throw new IOException("You must supply a keyspace name");
        }
        if (columnFamilyName == null) {
            throw new IOException("You must supply a table name");
        }
        if (tag == null || tag.equals("")) {
            throw new IOException("You must supply a snapshot name.");
        }
        queryParams.add("tag", tag);
        queryParams.add("kn", keyspaceName);
        queryParams.add("cf", columnFamilyName);
        client.post("/storage_service/snapshots", queryParams);
    }

    /**
     * Remove the snapshot with the given name from the given keyspaces. If no
     * tag is specified we will remove all snapshots.
     */
    @Override
    public void clearSnapshot(String tag, String... keyspaceNames) throws IOException {
        log(" clearSnapshot(String tag, String... keyspaceNames) throws IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "tag", tag);
        APIClient.set_query_param(queryParams, "kn", APIClient.join(keyspaceNames));
        client.delete("/storage_service/snapshots", queryParams);
    }

    /**
     * Get the details of all the snapshot
     *
     * @return A map of snapshotName to all its details in Tabular form.
     */
    @Override
    public Map<String, TabularData> getSnapshotDetails() {
        log(" getSnapshotDetails()");
        return client.getMapStringSnapshotTabularDataValue("/storage_service/snapshots", null);
    }

    public Map<String, Map<String, Set<String>>> getSnapshotKeyspaceColumnFamily() {
        JsonArray arr = client.getJsonArray("/storage_service/snapshots");
        Map<String, Map<String, Set<String>>> res = new HashMap<String, Map<String, Set<String>>>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            Map<String, Set<String>> kscf = new HashMap<String, Set<String>>();
            JsonArray snapshots = obj.getJsonArray("value");
            for (int j = 0; j < snapshots.size(); j++) {
                JsonObject s = snapshots.getJsonObject(j);
                String ks = s.getString("ks");
                String cf = s.getString("cf");
                if (!kscf.containsKey(ks)) {
                    kscf.put(ks, new HashSet<String>());
                }
                kscf.get(ks).add(cf);
            }
            res.put(obj.getString("key"), kscf);
        }
        return res;
    }

    /**
     * Get the true size taken by all snapshots across all keyspaces.
     *
     * @return True size taken by all the snapshots.
     */
    @Override
    public long trueSnapshotsSize() {
        log(" trueSnapshotsSize()");
        return client.getLongValue("/storage_service/snapshots/size/true");
    }

    /**
     * Forces major compaction of a single keyspace
     */
//    @Override
    public void forceKeyspaceCompaction(String keyspaceName, String... columnFamilies)
            throws IOException, ExecutionException, InterruptedException {
        log(" forceKeyspaceCompaction(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "cf", APIClient.join(columnFamilies));
        client.post("/storage_service/keyspace_compaction/" + keyspaceName, queryParams);
    }

    @Override
    public void forceKeyspaceCompactionForTokenRange(String keyspaceName, String startToken, String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException {
        // TODO: actually handle token ranges.
        forceKeyspaceCompaction(keyspaceName, tableNames);
    }

    /**
     * Trigger a cleanup of keys on a single keyspace
     */
    @Override
    public int forceKeyspaceCleanup(String keyspaceName, String... columnFamilies)
            throws IOException, ExecutionException, InterruptedException {
        log(" forceKeyspaceCleanup(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "cf", APIClient.join(columnFamilies));
        return client.postInt("/storage_service/keyspace_cleanup/" + keyspaceName, queryParams);
    }

    /**
     * Scrub (deserialize + reserialize at the latest version, skipping bad rows
     * if any) the given keyspace. If columnFamilies array is empty, all CFs are
     * scrubbed.
     *
     * Scrubbed CFs will be snapshotted first, if disableSnapshot is false
     */
    @Override
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... columnFamilies)
            throws IOException, ExecutionException, InterruptedException {
        log(" scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        return scrub(disableSnapshot, skipCorrupted, true, keyspaceName, columnFamilies);
    }

    @Override
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, String keyspaceName,
                     String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
        log(" scrub(boolean disableSnapshot, boolean skipCorrupted, bool checkData, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_bool_query_param(queryParams, "disable_snapshot", disableSnapshot);
        APIClient.set_bool_query_param(queryParams, "skip_corrupted", skipCorrupted);
        APIClient.set_query_param(queryParams, "cf", APIClient.join(columnFamilies));
        return client.getIntValue("/storage_service/keyspace_scrub/" + keyspaceName);
    }

    /**
     * Rewrite all sstables to the latest version. Unlike scrub, it doesn't skip
     * bad rows and do not snapshot sstables first.
     */
    @Override
    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... columnFamilies)
            throws IOException, ExecutionException, InterruptedException {
        log(" upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_bool_query_param(queryParams, "exclude_current_version", excludeCurrentVersion);
        APIClient.set_query_param(queryParams, "cf", APIClient.join(columnFamilies));
        return client.getIntValue("/storage_service/keyspace_upgrade_sstables/" + keyspaceName, queryParams);
    }

    /**
     * Flush all memtables for the given column families, or all columnfamilies
     * for the given keyspace if none are explicitly listed.
     *
     * @param keyspaceName
     * @param columnFamilies
     * @throws IOException
     */
    @Override
    public void forceKeyspaceFlush(String keyspaceName, String... columnFamilies)
            throws IOException, ExecutionException, InterruptedException {
        log(" forceKeyspaceFlush(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "cf", APIClient.join(columnFamilies));
        client.post("/storage_service/keyspace_flush/" + keyspaceName, queryParams);
    }

    private class CheckRepair extends TimerTask {
        @SuppressWarnings("unused")
        private int id;
        private String keyspace;
        private String message;
        private MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        private int cmd;
        private final boolean legacy;

        public CheckRepair(int id, String keyspace, boolean legacy) {
            this.id = id;
            this.keyspace = keyspace;
            this.legacy = legacy;
            APIClient.set_query_param(queryParams, "id", Integer.toString(id));
            message = String.format("Repair session %d ", id);
            // The returned id is the command number
            this.cmd = id;
        }

        @Override
        public void run() {
            String status = client.getStringValue("/storage_service/repair_async/" + keyspace, queryParams);
            if (!status.equals("RUNNING")) {
                cancel();
                if (status.equals("SUCCESSFUL")) {
                    sendMessage(cmd, RepairStatus.SESSION_SUCCESS, message, legacy);
                } else {
                    sendMessage(cmd, RepairStatus.SESSION_FAILED, message + "failed", legacy);
                }
                sendMessage(cmd, RepairStatus.FINISHED, message + "finished", legacy);
            }
        }

    }


    public String getRepairMessage(final int cmd, final String keyspace, final int ranges_size,
                                   final RepairParallelism parallelismDegree, final boolean fullRepair) {
        return String.format(
                "Starting repair command #%d, repairing %d ranges for keyspace %s (parallelism=%s, full=%b)", cmd,
                ranges_size, keyspace, parallelismDegree, fullRepair);
    }

    /**
     *
     */
    private int waitAndNotifyRepair(int cmd, String keyspace, String message, boolean legacy) {
        logger.finest(message);

        sendMessage(cmd, RepairStatus.STARTED, message, legacy);

        TimerTask taskToExecute = new CheckRepair(cmd, keyspace, legacy);
        timer.schedule(taskToExecute, 100, 1000);
        return cmd;
    }

    // See org.apache.cassandra.utils.progress.ProgressEventType
    private static enum ProgressEventType {
        START, PROGRESS, ERROR, ABORT, SUCCESS, COMPLETE, NOTIFICATION
    }

    private void sendMessage(int cmd, RepairStatus status, String message, boolean legacy) {
        String tag = "repair:" + cmd;

        ProgressEventType type = ProgressEventType.ERROR;
        int total = 100;
        int count = 0;
        switch (status) {
            case STARTED:
                type = ProgressEventType.START;
                break;
            case FINISHED:
                type = ProgressEventType.COMPLETE;
                count = 100;
                break;
            case SESSION_SUCCESS:
                type = ProgressEventType.SUCCESS;
                count = 100;
                break;
            default:
                break;
        }

        Notification jmxNotification = new Notification("progress", tag, notificationSerialNumber.incrementAndGet(),
                message);
        Map<String, Integer> userData = new HashMap<>();
        userData.put("type", type.ordinal());
        userData.put("progressCount", count);
        userData.put("total", total);
        jmxNotification.setUserData(userData);
        sendNotification(jmxNotification);

        if (legacy) {
//            sendNotification("repair", message, new int[] { cmd, status.ordinal() });
        }
    }

    /**
     * Invoke repair asynchronously. You can track repair progress by
     * subscribing JMX notification sent from this StorageServiceMBean.
     * Notification format is: type: "repair" userObject: int array of length 2,
     * [0]=command number, [1]=ordinal of AntiEntropyService.Status
     *
     * @param keyspace
     *            Keyspace name to repair. Should not be null.
     * @param options
     *            repair option.
     * @return Repair command number, or 0 if nothing to repair
     */
    @Override
    public int repairAsync(String keyspace, Map<String, String> options) {
        return repairAsync(keyspace, options, false);
    }

    @SuppressWarnings("unused")
    private static final String PARALLELISM_KEY = "parallelism";
    private static final String PRIMARY_RANGE_KEY = "primaryRange";
    @SuppressWarnings("unused")
    private static final String INCREMENTAL_KEY = "incremental";
    @SuppressWarnings("unused")
    private static final String JOB_THREADS_KEY = "jobThreads";
    private static final String RANGES_KEY = "ranges";
    private static final String COLUMNFAMILIES_KEY = "columnFamilies";
    private static final String DATACENTERS_KEY = "dataCenters";
    private static final String HOSTS_KEY = "hosts";
    @SuppressWarnings("unused")
    private static final String TRACE_KEY = "trace";

    private int repairAsync(String keyspace, Map<String, String> options, boolean legacy) {
        log(" repairAsync(String keyspace, Map<String, String> options)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        for (String op : options.keySet()) {
            APIClient.set_query_param(queryParams, op, options.get(op));
        }

        int cmd = client.postInt("/storage_service/repair_async/" + keyspace, queryParams);
        waitAndNotifyRepair(cmd, keyspace, getRepairMessage(cmd, keyspace, 1, RepairParallelism.SEQUENTIAL, true),
                legacy);
        return cmd;
    }

    private static String commaSeparated(Collection<?> c) {
        String s = c.toString();
        return s.substring(1, s.length() - 1);
    }

    private int repairRangeAsync(String beginToken, String endToken, String keyspaceName, Boolean isSequential,
                                 Collection<String> dataCenters, Collection<String> hosts, Boolean primaryRange, Boolean repairedAt,
                                 String... columnFamilies) {
        log(" forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts, boolean repairedAt, String... columnFamilies) throws IOException");

        Map<String, String> options = new HashMap<String, String>();
        if (beginToken != null && endToken != null) {
            options.put(RANGES_KEY, beginToken + ":" + endToken);
        }
        if (dataCenters != null) {
            options.put(DATACENTERS_KEY, commaSeparated(dataCenters));
        }
        if (hosts != null) {
            options.put(HOSTS_KEY, commaSeparated(hosts));
        }
        if (columnFamilies != null && columnFamilies.length != 0) {
            options.put(COLUMNFAMILIES_KEY, commaSeparated(asList(columnFamilies)));
        }
        if (primaryRange != null) {
            options.put(PRIMARY_RANGE_KEY, primaryRange.toString());
        }

        return repairAsync(keyspaceName, options, true);
    }

    @Override
    @Deprecated
    public int forceRepairAsync(String keyspace, boolean isSequential, Collection<String> dataCenters,
                                Collection<String> hosts, boolean primaryRange, boolean repairedAt, String... columnFamilies)
            throws IOException {
        log(" forceRepairAsync(String keyspace, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts,  boolean primaryRange, boolean repairedAt, String... columnFamilies) throws IOException");
        return repairRangeAsync(null, null, keyspace, isSequential, dataCenters, hosts, primaryRange, repairedAt,
                columnFamilies);
    }

    @Override
    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential,
                                     Collection<String> dataCenters, Collection<String> hosts, boolean repairedAt, String... columnFamilies) {
        log(" forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts, boolean repairedAt, String... columnFamilies) throws IOException");
        return repairRangeAsync(beginToken, endToken, keyspaceName, isSequential, dataCenters, hosts, null, repairedAt,
                columnFamilies);
    }

    @Override
    @Deprecated
    public int forceRepairAsync(String keyspaceName, boolean isSequential, boolean isLocal, boolean primaryRange,
                                boolean fullRepair, String... columnFamilies) {
        log(" forceRepairAsync(String keyspace, boolean isSequential, boolean isLocal, boolean primaryRange, boolean fullRepair, String... columnFamilies)");
        return repairRangeAsync(null, null, keyspaceName, isSequential, null, null, primaryRange, null, columnFamilies);
    }

    @Override
    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential,
                                     boolean isLocal, boolean repairedAt, String... columnFamilies) {
        log(" forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, boolean isLocal, boolean repairedAt, String... columnFamilies)");
        return forceRepairRangeAsync(beginToken, endToken, keyspaceName, isSequential, null, null, repairedAt,
                columnFamilies);
    }

    @Override
    public void forceTerminateAllRepairSessions() {
        log(" forceTerminateAllRepairSessions()");
        client.post("/storage_service/force_terminate");
    }

    /**
     * transfer this node's data to other machines and remove it from service.
     */
    @Override
    public void decommission() throws InterruptedException {
        log(" decommission() throws InterruptedException");
        client.post("/storage_service/decommission");
    }

    /**
     * @param newToken
     *            token to move this node to. This node will unload its data
     *            onto its neighbors, and bootstrap to the new token.
     */
    @Override
    public void move(String newToken) throws IOException {
        log(" move(String newToken) throws IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "new_token", newToken);
        client.post("/storage_service/move", queryParams);
    }

    /**
     * removeToken removes token (and all data associated with enpoint that had
     * it) from the ring
     *
     * @param hostIdString
     *            the host id to remove
     */
    @Override
    public void removeNode(String hostIdString) {
        log(" removeNode(String token)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "host_id", hostIdString);
        client.post("/storage_service/remove_node", queryParams);
    }

    /**
     * Get the status of a token removal.
     */
    @Override
    public String getRemovalStatus() {
        log(" getRemovalStatus()");
        return client.getStringValue("/storage_service/removal_status");
    }

    /**
     * Force a remove operation to finish.
     */
    @Override
    public void forceRemoveCompletion() {
        log(" forceRemoveCompletion()");
        client.post("/storage_service/force_remove_completion");
    }

    /**
     * set the logging level at runtime<br>
     * <br>
     * If both classQualifer and level are empty/null, it will reload the
     * configuration to reset.<br>
     * If classQualifer is not empty but level is empty/null, it will set the
     * level to null for the defined classQualifer<br>
     * If level cannot be parsed, then the level will be defaulted to DEBUG<br>
     * <br>
     * The logback configuration should have < jmxConfigurator /> set
     *
     * @param classQualifier
     *            The logger's classQualifer
     * @param level
     *            The log level
     * @throws Exception
     *
     * @see ch.qos.logback.classic.Level#toLevel(String)
     */
    @Override
    public void setLoggingLevel(String classQualifier, String level) throws Exception {
        log(" setLoggingLevel(String classQualifier, String level) throws Exception");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "level", level);
        client.post("/system/logger/" + classQualifier, queryParams);
    }

    /** get the runtime logging levels */
    @Override
    public Map<String, String> getLoggingLevels() {
        log(" getLoggingLevels()");
        return client.getMapStrValue("/storage_service/logging_level");
    }

    /**
     * get the operational mode (leaving, joining, normal, decommissioned,
     * client)
     **/
    @Override
    public String getOperationMode() {
        log(" getOperationMode()");
        return client.getStringValue("/storage_service/operation_mode");
    }

    /** Returns whether the storage service is starting or not */
    @Override
    public boolean isStarting() {
        log(" isStarting()");
        return client.getBooleanValue("/storage_service/is_starting");
    }

    /** get the progress of a drain operation */
    @Override
    public String getDrainProgress() {
        log(" getDrainProgress()");
        // FIXME
        // This is a workaround so the nodetool would work
        // it should be revert when the drain progress will be implemented
        // return c.getStringValue("/storage_service/drain");
        return String.format("Drained %s/%s ColumnFamilies", 0, 0);
    }

    /**
     * makes node unavailable for writes, flushes memtables and replays
     * commitlog.
     */
    @Override
    public void drain() throws IOException, InterruptedException, ExecutionException {
        log(" drain() throws IOException, InterruptedException, ExecutionException");
        client.post("/storage_service/drain");
    }

    /**
     * Truncates (deletes) the given columnFamily from the provided keyspace.
     * Calling truncate results in actual deletion of all data in the cluster
     * under the given columnFamily and it will fail unless all hosts are up.
     * All data in the given column family will be deleted, but its definition
     * will not be affected.
     *
     * @param keyspace
     *            The keyspace to delete from
     * @param columnFamily
     *            The column family to delete data from.
     */
    @Override
    public void truncate(String keyspace, String columnFamily) throws TimeoutException, IOException {
        log(" truncate(String keyspace, String columnFamily)throws TimeoutException, IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "cf", columnFamily);
        client.post("/storage_service/truncate/" + keyspace, queryParams);
    }

    /**
     * given a list of tokens (representing the nodes in the cluster), returns a
     * mapping from "token -> %age of cluster owned by that token"
     */
    @Override
    public Map<InetAddress, Float> getOwnership() {
        log(" getOwnership()");
        return client.getMapInetAddressFloatValue("/storage_service/ownership/");
    }

    /**
     * Effective ownership is % of the data each node owns given the keyspace we
     * calculate the percentage using replication factor. If Keyspace == null,
     * this method will try to verify if all the keyspaces in the cluster have
     * the same replication strategies and if yes then we will use the first
     * else a empty Map is returned.
     */
    @Override
    public Map<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException {
        log(" effectiveOwnership(String keyspace) throws IllegalStateException");
        try {
            return client.getMapInetAddressFloatValue("/storage_service/ownership/" + keyspace);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
        }
    }

    @Override
    public List<String> getKeyspaces() {
        log(" getKeyspaces()");
        return client.getListStrValue("/storage_service/keyspaces");
    }

    public Map<String, Set<String>> getColumnFamilyPerKeyspace() {
        Map<String, Set<String>> res = new HashMap<String, Set<String>>();

        JsonArray mbeans = client.getJsonArray("/column_family/");

        for (int i = 0; i < mbeans.size(); i++) {
            JsonObject mbean = mbeans.getJsonObject(i);
            String ks = mbean.getString("ks");
            String cf = mbean.getString("cf");
            if (!res.containsKey(ks)) {
                res.put(ks, new HashSet<String>());
            }
            res.get(ks).add(cf);
        }
        return res;
    }

    @Override
    public List<String> getNonSystemKeyspaces() {
        log(" getNonSystemKeyspaces()");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("type", "user");
        return client.getListStrValue("/storage_service/keyspaces", queryParams);
    }

    @Override
    public Map<String, String> getViewBuildStatuses(String keyspace, String view) {
        log(" getViewBuildStatuses()");
        return client.getMapStrValue("storage_service/view_build_statuses/" + keyspace + "/" + view);
    }

    /**
     * Change endpointsnitch class and dynamic-ness (and dynamic attributes) at
     * runtime
     *
     * @param epSnitchClassName
     *            the canonical path name for a class implementing
     *            IEndpointSnitch
     * @param dynamic
     *            boolean that decides whether dynamicsnitch is used or not
     * @param dynamicUpdateInterval
     *            integer, in ms (default 100)
     * @param dynamicResetInterval
     *            integer, in ms (default 600,000)
     * @param dynamicBadnessThreshold
     *            double, (default 0.0)
     */
    @Override
    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval,
                             Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException {
        log(" updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_bool_query_param(queryParams, "dynamic", dynamic);
        APIClient.set_query_param(queryParams, "epSnitchClassName", epSnitchClassName);
        if (dynamicUpdateInterval != null) {
            queryParams.add("dynamic_update_interval", dynamicUpdateInterval.toString());
        }
        if (dynamicResetInterval != null) {
            queryParams.add("dynamic_reset_interval", dynamicResetInterval.toString());
        }
        if (dynamicBadnessThreshold != null) {
            queryParams.add("dynamic_badness_threshold", dynamicBadnessThreshold.toString());
        }
        client.post("/storage_service/update_snitch", queryParams);
    }

    @Override
    public void setDynamicUpdateInterval(int dynamicUpdateInterval) {

    }

    @Override
    public int getDynamicUpdateInterval() {
        return 0;
    }

    // allows a user to forcibly 'kill' a sick node
    @Override
    public void stopGossiping() {
        log(" stopGossiping()");
        client.delete("/storage_service/gossiping");
    }

    // allows a user to recover a forcibly 'killed' node
    @Override
    public void startGossiping() {
        log(" startGossiping()");
        client.post("/storage_service/gossiping");
    }

    // allows a user to see whether gossip is running or not
    @Override
    public boolean isGossipRunning() {
        log(" isGossipRunning()");
        return client.getBooleanValue("/storage_service/gossiping");
    }

    // allows a user to forcibly completely stop cassandra
    @Override
    public void stopDaemon() {
        log(" stopDaemon()");
        client.post("/storage_service/stop_daemon");
    }

    // to determine if gossip is disabled
    @Override
    public boolean isInitialized() {
        log(" isInitialized()");
        return client.getBooleanValue("/storage_service/is_initialized");
    }

    // allows a user to disable thrift
    @Override
    public void stopRPCServer() {
        log(" stopRPCServer()");
        client.delete("/storage_service/rpc_server");
    }

    // allows a user to reenable thrift
    @Override
    public void startRPCServer() {
        log(" startRPCServer()");
        client.post("/storage_service/rpc_server");
    }

    // to determine if thrift is running
    @Override
    public boolean isRPCServerRunning() {
        log(" isRPCServerRunning()");
        return client.getBooleanValue("/storage_service/rpc_server");
    }

    @Override
    public void stopNativeTransport() {
        log(" stopNativeTransport()");
        client.delete("/storage_service/native_transport");
    }

    @Override
    public void startNativeTransport() {
        log(" startNativeTransport()");
        client.post("/storage_service/native_transport");
    }

    @Override
    public boolean isNativeTransportRunning() {
        log(" isNativeTransportRunning()");
        return client.getBooleanValue("/storage_service/native_transport");
    }

    // allows a node that have been started without joining the ring to join it
    @Override
    public void joinRing() throws IOException {
        log(" joinRing() throws IOException");
        client.post("/storage_service/join_ring");
    }

    @Override
    public boolean isJoined() {
        log(" isJoined()");
        return client.getBooleanValue("/storage_service/join_ring");
    }

    @Override
    public boolean isDrained() {
        return false;
    }

    @Override
    public boolean isDraining() {
        return false;
    }

    @Override
    public void setRpcTimeout(long value) {

    }

    @Override
    public long getRpcTimeout() {
        return 0;
    }

    @Override
    public void setReadRpcTimeout(long value) {

    }

    @Override
    public long getReadRpcTimeout() {
        return 0;
    }

    @Override
    public void setRangeRpcTimeout(long value) {

    }

    @Override
    public long getRangeRpcTimeout() {
        return 0;
    }

    @Override
    public void setWriteRpcTimeout(long value) {

    }

    @Override
    public long getWriteRpcTimeout() {
        return 0;
    }

    @Override
    public void setCounterWriteRpcTimeout(long value) {

    }

    @Override
    public long getCounterWriteRpcTimeout() {
        return 0;
    }

    @Override
    public void setCasContentionTimeout(long value) {

    }

    @Override
    public long getCasContentionTimeout() {
        return 0;
    }

    @Override
    public void setTruncateRpcTimeout(long value) {

    }

    @Override
    public long getTruncateRpcTimeout() {
        return 0;
    }

    @Override
    public void setStreamingSocketTimeout(int value) {

    }

    @Override
    public int getStreamingSocketTimeout() {
        return 0;
    }

    @Override
    public void setStreamThroughputMbPerSec(int value) {
        log(" setStreamThroughputMbPerSec(int value)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("value", Integer.toString(value));
        client.post("/storage_service/stream_throughput", queryParams);
    }

    @Override
    public int getStreamThroughputMbPerSec() {
        log(" getStreamThroughputMbPerSec()");
        return client.getIntValue("/storage_service/stream_throughput");
    }

    public int getCompactionThroughputMbPerSec() {
        log(" getCompactionThroughputMbPerSec()");
        return client.getIntValue("/storage_service/compaction_throughput");
    }

    @Override
    public void setCompactionThroughputMbPerSec(int value) {
        log(" setCompactionThroughputMbPerSec(int value)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("value", Integer.toString(value));
        client.post("/storage_service/compaction_throughput", queryParams);
    }

    @Override
    public int getConcurrentCompactors() {
        return 0;
    }

    @Override
    public void setConcurrentCompactors(int value) {

    }

    @Override
    public boolean isIncrementalBackupsEnabled() {
        log(" isIncrementalBackupsEnabled()");
        return client.getBooleanValue("/storage_service/incremental_backups");
    }

    @Override
    public void setIncrementalBackupsEnabled(boolean value) {
        log(" setIncrementalBackupsEnabled(boolean value)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("value", Boolean.toString(value));
        client.post("/storage_service/incremental_backups", queryParams);
    }

    /**
     * Initiate a process of streaming data for which we are responsible from
     * other nodes. It is similar to bootstrap except meant to be used on a node
     * which is already in the cluster (typically containing no data) as an
     * alternative to running repair.
     *
     * @param sourceDc
     *            Name of DC from which to select sources for streaming or null
     *            to pick any node
     */
    @Override
    public void rebuild(String sourceDc) {
        rebuild(sourceDc, null, null, null);
    }

    /**
     * Same as {@link #rebuild(String)}, but only for specified keyspace and ranges.
     *
     * @param sourceDc Name of DC from which to select sources for streaming or null to pick any node
     * @param keyspace Name of the keyspace which to rebuild or null to rebuild all keyspaces.
     * @param tokens Range of tokens to rebuild or null to rebuild all token ranges. In the format of:
     *               "(start_token_1,end_token_1],(start_token_2,end_token_2],...(start_token_n,end_token_n]"
     */
    @Override
    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources) {
        log(" rebuild(String sourceDc, String keyspace, String tokens, String specificSources)");
        if (keyspace != null) {
            throw new UnsupportedOperationException("Rebuild: 'keyspace' not yet supported");
        }
        if (tokens != null) {
            throw new UnsupportedOperationException("Rebuild: 'token range' not yet supported");
        }
        if (specificSources != null) {
            throw new UnsupportedOperationException("Rebuild: 'specific sources' not yet supported");
        }
        if (sourceDc != null) {
            MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
            APIClient.set_query_param(queryParams, "source_dc", sourceDc);
            client.post("/storage_service/rebuild", queryParams);
        } else {
            client.post("/storage_service/rebuild");
        }
    }

    /** Starts a bulk load and blocks until it completes. */
    @Override
    public void bulkLoad(String directory) {
        log(" bulkLoad(String directory)");
        client.post("/storage_service/bulk_load/" + directory);
    }

    /**
     * Starts a bulk load asynchronously and returns the String representation
     * of the planID for the new streaming session.
     */
    @Override
    public String bulkLoadAsync(String directory) {
        log(" bulkLoadAsync(String directory)");
        return client.getStringValue("/storage_service/bulk_load_async/" + directory);
    }

    @Override
    public void rescheduleFailedDeletions() {
        log(" rescheduleFailedDeletions()");
        client.post("/storage_service/reschedule_failed_deletions");
    }

    /**
     * Load new SSTables to the given keyspace/columnFamily
     *
     * @param ksName
     *            The parent keyspace name
     * @param cfName
     *            The ColumnFamily name where SSTables belong
     */
    @Override
    public void loadNewSSTables(String ksName, String cfName) {
        log(" loadNewSSTables(String ksName, String cfName)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("cf", cfName);
        client.post("/storage_service/sstables/" + ksName, queryParams);
    }

    /**
     * Return a List of Tokens representing a sample of keys across all
     * ColumnFamilyStores.
     *
     * Note: this should be left as an operation, not an attribute (methods
     * starting with "get") to avoid sending potentially multiple MB of data
     * when accessing this mbean by default. See CASSANDRA-4452.
     *
     * @return set of Tokens as Strings
     */
    @Override
    public List<String> sampleKeyRange() {
        log(" sampleKeyRange()");
        return client.getListStrValue("/storage_service/sample_key_range");
    }

    /**
     * rebuild the specified indexes
     */
    @Override
    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames) {
        log(" rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)");
    }

    @Override
    public void resetLocalSchema() throws IOException {
        log(" resetLocalSchema() throws IOException");
        client.post("/storage_service/relocal_schema");
    }

    @Override
    public void reloadLocalSchema() {

    }

    /**
     * Enables/Disables tracing for the whole system. Only thrift requests can
     * start tracing currently.
     *
     * @param probability
     *            ]0,1[ will enable tracing on a partial number of requests with
     *            the provided probability. 0 will disable tracing and 1 will
     *            enable tracing for all requests (which mich severely cripple
     *            the system)
     */
    @Override
    public void setTraceProbability(double probability) {
        log(" setTraceProbability(double probability)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("probability", Double.toString(probability));
        client.post("/storage_service/trace_probability", queryParams);
    }

    /**
     * Returns the configured tracing probability.
     */
    @Override
    public double getTraceProbability() {
        log(" getTraceProbability()");
        return client.getDoubleValue("/storage_service/trace_probability");
    }

    @Override
    public void disableAutoCompaction(String ks, String... columnFamilies) throws IOException {
        log("disableAutoCompaction(String ks, String... columnFamilies)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "cf", APIClient.join(columnFamilies));
        client.delete("/storage_service/auto_compaction/" + ks, queryParams);
    }

    @Override
    public void enableAutoCompaction(String ks, String... columnFamilies) throws IOException {
        log("enableAutoCompaction(String ks, String... columnFamilies)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        APIClient.set_query_param(queryParams, "cf", APIClient.join(columnFamilies));
        try {
            client.post("/storage_service/auto_compaction/" + ks, queryParams);
        } catch (RuntimeException e) {
            // FIXME should throw the right exception
            throw new IOException(e.getMessage());
        }

    }

    @Override
    public void deliverHints(String host) throws UnknownHostException {
        log(" deliverHints(String host) throws UnknownHostException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("host", host);
        client.post("/storage_service/deliver_hints", queryParams);
    }

    /** Returns the name of the cluster */
    @Override
    public String getClusterName() {
        log(" getClusterName()");
        return client.getStringValue("/storage_service/cluster_name");
    }

    /** Returns the cluster partitioner */
    @Override
    public String getPartitionerName() {
        log(" getPartitionerName()");
        return client.getStringValue("/storage_service/partitioner_name");
    }

    /** Returns the threshold for warning of queries with many tombstones */
    @Override
    public int getTombstoneWarnThreshold() {
        log(" getTombstoneWarnThreshold()");
        return client.getIntValue("/storage_service/tombstone_warn_threshold");
    }

    /** Sets the threshold for warning queries with many tombstones */
    @Override
    public void setTombstoneWarnThreshold(int tombstoneDebugThreshold) {
        log(" setTombstoneWarnThreshold(int tombstoneDebugThreshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("debug_threshold", Integer.toString(tombstoneDebugThreshold));
        client.post("/storage_service/tombstone_warn_threshold", queryParams);
    }

    /** Returns the threshold for abandoning queries with many tombstones */
    @Override
    public int getTombstoneFailureThreshold() {
        log(" getTombstoneFailureThreshold()");
        return client.getIntValue("/storage_service/tombstone_failure_threshold");
    }

    /** Sets the threshold for abandoning queries with many tombstones */
    @Override
    public void setTombstoneFailureThreshold(int tombstoneDebugThreshold) {
        log(" setTombstoneFailureThreshold(int tombstoneDebugThreshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("debug_threshold", Integer.toString(tombstoneDebugThreshold));
        client.post("/storage_service/tombstone_failure_threshold", queryParams);
    }

    /** Returns the threshold for rejecting queries due to a large batch size */
    @Override
    public int getBatchSizeFailureThreshold() {
        log(" getBatchSizeFailureThreshold()");
        return client.getIntValue("/storage_service/batch_size_failure_threshold");
    }

    /** Sets the threshold for rejecting queries due to a large batch size */
    @Override
    public void setBatchSizeFailureThreshold(int batchSizeDebugThreshold) {
        log(" setBatchSizeFailureThreshold(int batchSizeDebugThreshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("threshold", Integer.toString(batchSizeDebugThreshold));
        client.post("/storage_service/batch_size_failure_threshold", queryParams);
    }

    /**
     * Sets the hinted handoff throttle in kb per second, per delivery thread.
     */
    @Override
    public void setHintedHandoffThrottleInKB(int throttleInKB) {
        log(" setHintedHandoffThrottleInKB(int throttleInKB)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("throttle", Integer.toString(throttleInKB));
        client.post("/storage_service/hinted_handoff", queryParams);

    }

//    @Override
    public void takeMultipleColumnFamilySnapshot(String tag, String... columnFamilyList) throws IOException {
        log(" takeMultipleColumnFamilySnapshot");
        Map<String, List<String>> keyspaceColumnfamily = new HashMap<String, List<String>>();
        Map<String, Set<String>> kss = getColumnFamilyPerKeyspace();
        Map<String, Map<String, Set<String>>> snapshots = getSnapshotKeyspaceColumnFamily();
        for (String columnFamily : columnFamilyList) {
            String splittedString[] = columnFamily.split("\\.");
            if (splittedString.length == 2) {
                String keyspaceName = splittedString[0];
                String columnFamilyName = splittedString[1];

                if (keyspaceName == null) {
                    throw new IOException("You must supply a keyspace name");
                }
                if (columnFamilyName == null) {
                    throw new IOException("You must supply a column family name");
                }
                if (tag == null || tag.equals("")) {
                    throw new IOException("You must supply a snapshot name.");
                }
                if (!kss.containsKey(keyspaceName)) {
                    throw new IOException("Keyspace " + keyspaceName + " does not exist");
                }
                if (!kss.get(keyspaceName).contains(columnFamilyName)) {
                    throw new IllegalArgumentException(
                            String.format("Unknown keyspace/cf pair (%s.%s)", keyspaceName, columnFamilyName));
                }
                // As there can be multiple column family from same keyspace
                // check if snapshot exist for that specific
                // columnfamily and not for whole keyspace

                if (snapshots.containsKey(tag) && snapshots.get(tag).containsKey(keyspaceName)
                        && snapshots.get(tag).get(keyspaceName).contains(columnFamilyName)) {
                    throw new IOException("Snapshot " + tag + " already exists.");
                }

                if (!keyspaceColumnfamily.containsKey(keyspaceName)) {
                    keyspaceColumnfamily.put(keyspaceName, new ArrayList<String>());
                }

                // Add Keyspace columnfamily to map in order to support
                // atomicity for snapshot process.
                // So no snapshot should happen if any one of the above
                // conditions fail for any keyspace or columnfamily
                keyspaceColumnfamily.get(keyspaceName).add(columnFamilyName);

            } else {
                throw new IllegalArgumentException(
                        "Cannot take a snapshot on secondary index or invalid column family name. You must supply a column family name in the form of keyspace.columnfamily");
            }
        }

        for (Entry<String, List<String>> entry : keyspaceColumnfamily.entrySet()) {
            for (String columnFamily : entry.getValue()) {
                takeColumnFamilySnapshot(entry.getKey(), columnFamily, tag);
            }
        }
    }

    @Override
    public int forceRepairAsync(String keyspace, int parallelismDegree, Collection<String> dataCenters,
                                Collection<String> hosts, boolean primaryRange, boolean fullRepair, String... columnFamilies) {
        log(" forceRepairAsync(keyspace, parallelismDegree, dataCenters, hosts, primaryRange, fullRepair, columnFamilies)");
        Map<String, String> options = new HashMap<String, String>();
        Joiner commas = Joiner.on(",");
        options.put("parallelism", Integer.toString(parallelismDegree));
        if (dataCenters != null) {
            options.put("dataCenters", commas.join(dataCenters));
        }
        if (hosts != null) {
            options.put("hosts", commas.join(hosts));
        }
        options.put("primaryRange", Boolean.toString(primaryRange));
        options.put("incremental", Boolean.toString(!fullRepair));
        if (columnFamilies != null && columnFamilies.length > 0) {
            options.put("columnFamilies", commas.join(columnFamilies));
        }
        return repairAsync(keyspace, options);
    }

    @Override
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, int parallelismDegree,
                                     Collection<String> dataCenters, Collection<String> hosts, boolean fullRepair, String... columnFamilies) {
        log(" forceRepairRangeAsync(beginToken, endToken, keyspaceName, parallelismDegree, dataCenters, hosts, fullRepair, columnFamilies)");
        Map<String, String> options = new HashMap<String, String>();
        Joiner commas = Joiner.on(",");
        options.put("parallelism", Integer.toString(parallelismDegree));
        if (dataCenters != null) {
            options.put("dataCenters", commas.join(dataCenters));
        }
        if (hosts != null) {
            options.put("hosts", commas.join(hosts));
        }
        options.put("incremental", Boolean.toString(!fullRepair));
        options.put("startToken", beginToken);
        options.put("endToken", endToken);
        return repairAsync(keyspaceName, options);
    }

    @Override
    public Map<String, String> getEndpointToHostId() {
        return getHostIdMap();
    }

    @Override
    public Map<String, String> getHostIdToEndpoint() {
        return getHostIdToAddressMap();
    }

    @Override
    public void refreshSizeEstimates() throws ExecutionException {
        // TODO Auto-generated method stub
        log(" refreshSizeEstimates");
    }

    @Override
    public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames)
            throws IOException, ExecutionException, InterruptedException {
        // "splitOutput" afaik not relevant for scylla (yet?...)
        forceKeyspaceCompaction(keyspaceName, tableNames);
    }

    @Override
    public int relocateSSTables(String keyspace, String... cfnames) throws IOException, ExecutionException, InterruptedException {
        return 0;
    }

    @Override
    public int relocateSSTables(int jobs, String keyspace, String... cfnames) throws IOException, ExecutionException, InterruptedException {
        return 0;
    }

    @Override
    public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables)
            throws IOException, ExecutionException, InterruptedException {
        // "jobs" not (yet) relevant for scylla. (though possibly useful...)
        return forceKeyspaceCleanup(keyspaceName, tables);
    }

    @Override
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, int jobs, String keyspaceName,
                     String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
        // "jobs" not (yet) relevant for scylla. (though possibly useful...)
        return scrub(disableSnapshot, skipCorrupted, checkData, 0, keyspaceName, columnFamilies);
    }

    @Override
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
        return 0;
    }

    @Override
    public int verify(boolean extendedVerify, String keyspaceName, String... tableNames)
            throws IOException, ExecutionException, InterruptedException {
        // TODO Auto-generated method stub
        log(" verify");
        return 0;
    }

    @Override
    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames)
            throws IOException, ExecutionException, InterruptedException {
        // "jobs" not (yet) relevant for scylla. (though possibly useful...)
        return upgradeSSTables(keyspaceName, excludeCurrentVersion, tableNames);
    }

    @Override
    public int garbageCollect(String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
        return 0;
    }

    @Override
    public List<String> getNonLocalStrategyKeyspaces() {
        log(" getNonLocalStrategyKeyspaces");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("type", "non_local_strategy");
        return client.getListStrValue("/storage_service/keyspaces", queryParams);
    }

    @Override
    public void setInterDCStreamThroughputMbPerSec(int value) {
        // TODO Auto-generated method stub
        log(" setInterDCStreamThroughputMbPerSec");
    }

    @Override
    public int getInterDCStreamThroughputMbPerSec() {
        // TODO Auto-generated method stub
        log(" getInterDCStreamThroughputMbPerSec");
        return 0;
    }

    @Override
    public boolean resumeBootstrap() {
        log(" resumeBootstrap");
        return false;
    }

}
