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

import static java.util.Collections.emptySet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import com.scylladb.jmx.api.APIClient;

public class ScyllaStorageProxy implements StorageProxyMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final Logger logger = Logger.getLogger(StorageProxy.class.getName());

    public void log(String str) {
        logger.finest(str);
    }

    public static final String UNREACHABLE = "UNREACHABLE";
    protected final APIClient client;

    public ScyllaStorageProxy(APIClient client) {
        this.client = client;
//        super(MBEAN_NAME, client, new ClientRequestMetrics("Read", "storage_proxy/metrics/read"),
//                new ClientRequestMetrics("RangeSlice", "/storage_proxy/metrics/range"),
//                new ClientRequestMetrics("Write", "storage_proxy/metrics/write"),
//                new CASClientRequestMetrics("CASWrite", "storage_proxy/metrics/cas_write"),
//                new CASClientRequestMetrics("CASRead", "storage_proxy/metrics/cas_read"));
    }

    @Override
    public long getTotalHints() {
        log(" getTotalHints()");
        return client.getLongValue("storage_proxy/total_hints");
    }

    @Override
    public boolean getHintedHandoffEnabled() {
        log(" getHintedHandoffEnabled()");
        return client.getBooleanValue("storage_proxy/hinted_handoff_enabled");
    }

//    @Override
    public Set<String> getHintedHandoffEnabledByDC() {
        log(" getHintedHandoffEnabledByDC()");
        return client.getSetStringValue("storage_proxy/hinted_handoff_enabled_by_dc");
    }

    @Override
    public void setHintedHandoffEnabled(boolean b) {
        log(" setHintedHandoffEnabled(boolean b)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("enable", Boolean.toString(b));
        client.post("storage_proxy/hinted_handoff_enabled", queryParams);
    }

//    @Override
    public void setHintedHandoffEnabledByDCList(String dcs) {
        log(" setHintedHandoffEnabledByDCList(String dcs)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("dcs", dcs);
        client.post("storage_proxy/hinted_handoff_enabled_by_dc_list");
    }

    @Override
    public int getMaxHintWindow() {
        log(" getMaxHintWindow()");
        return client.getIntValue("storage_proxy/max_hint_window");
    }

    @Override
    public void setMaxHintWindow(int ms) {
        log(" setMaxHintWindow(int ms)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("ms", Integer.toString(ms));
        client.post("storage_proxy/max_hint_window", queryParams);
    }

    @Override
    public int getMaxHintsInProgress() {
        log(" getMaxHintsInProgress()");
        return client.getIntValue("storage_proxy/max_hints_in_progress");
    }

    @Override
    public void setMaxHintsInProgress(int qs) {
        log(" setMaxHintsInProgress(int qs)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("qs", Integer.toString(qs));
        client.post("storage_proxy/max_hints_in_progress", queryParams);
    }

    @Override
    public int getHintsInProgress() {
        log(" getHintsInProgress()");
        return client.getIntValue("storage_proxy/hints_in_progress");
    }

    @Override
    public Long getRpcTimeout() {
        log(" getRpcTimeout()");
        return client.getLongValue("storage_proxy/rpc_timeout");
    }

    @Override
    public void setRpcTimeout(Long timeoutInMillis) {
        log(" setRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        client.post("storage_proxy/rpc_timeout", queryParams);
    }

    @Override
    public Long getReadRpcTimeout() {
        log(" getReadRpcTimeout()");
        return client.getLongValue("storage_proxy/read_rpc_timeout");
    }

    @Override
    public void setReadRpcTimeout(Long timeoutInMillis) {
        log(" setReadRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        client.post("storage_proxy/read_rpc_timeout", queryParams);
    }

    @Override
    public Long getWriteRpcTimeout() {
        log(" getWriteRpcTimeout()");
        return client.getLongValue("storage_proxy/write_rpc_timeout");
    }

    @Override
    public void setWriteRpcTimeout(Long timeoutInMillis) {
        log(" setWriteRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        client.post("storage_proxy/write_rpc_timeout", queryParams);
    }

    @Override
    public Long getCounterWriteRpcTimeout() {
        log(" getCounterWriteRpcTimeout()");
        return client.getLongValue("storage_proxy/counter_write_rpc_timeout");
    }

    @Override
    public void setCounterWriteRpcTimeout(Long timeoutInMillis) {
        log(" setCounterWriteRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        client.post("storage_proxy/counter_write_rpc_timeout", queryParams);
    }

    @Override
    public Long getCasContentionTimeout() {
        log(" getCasContentionTimeout()");
        return client.getLongValue("storage_proxy/cas_contention_timeout");
    }

    @Override
    public void setCasContentionTimeout(Long timeoutInMillis) {
        log(" setCasContentionTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        client.post("storage_proxy/cas_contention_timeout", queryParams);
    }

    @Override
    public Long getRangeRpcTimeout() {
        log(" getRangeRpcTimeout()");
        return client.getLongValue("storage_proxy/range_rpc_timeout");
    }

    @Override
    public void setRangeRpcTimeout(Long timeoutInMillis) {
        log(" setRangeRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        client.post("storage_proxy/range_rpc_timeout", queryParams);
    }

    @Override
    public Long getTruncateRpcTimeout() {
        log(" getTruncateRpcTimeout()");
        return client.getLongValue("storage_proxy/truncate_rpc_timeout");
    }

    @Override
    public void setTruncateRpcTimeout(Long timeoutInMillis) {
        log(" setTruncateRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        client.post("storage_proxy/truncate_rpc_timeout", queryParams);
    }

    @Override
    public void reloadTriggerClasses() {
        log(" reloadTriggerClasses()");
        client.post("storage_proxy/reload_trigger_classes");
    }

    @Override
    public long getReadRepairAttempted() {
        log(" getReadRepairAttempted()");
        return client.getLongValue("storage_proxy/read_repair_attempted");
    }

    @Override
    public long getReadRepairRepairedBlocking() {
        log(" getReadRepairRepairedBlocking()");
        return client.getLongValue("storage_proxy/read_repair_repaired_blocking");
    }

    @Override
    public long getReadRepairRepairedBackground() {
        log(" getReadRepairRepairedBackground()");
        return client.getLongValue("storage_proxy/read_repair_repaired_background");
    }

    @Override
    public int getOtcBacklogExpirationInterval() {
        return 0; //TODO fix this
    }

    @Override
    public void setOtcBacklogExpirationInterval(int intervalInMillis) {
        //TODO fix this
    }

    /** Returns each live node's schema version */
    @Override
    public Map<String, List<String>> getSchemaVersions() {
        log(" getSchemaVersions()");
        return client.getMapStringListStrValue("storage_proxy/schema_versions");
    }

    @Override
    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections) {
        // TODO Auto-generated method stub
        log(" setNativeTransportMaxConcurrentConnections()");

    }

    @Override
    public Long getNativeTransportMaxConcurrentConnections() {
        // TODO Auto-generated method stub
        log(" getNativeTransportMaxConcurrentConnections()");
        return client.getLongValue("");
    }

    @Override
    public void enableHintsForDC(String dc) {
        // TODO if/when scylla uses hints
        log(" enableHintsForDC()");
    }

    @Override
    public void disableHintsForDC(String dc) {
        // TODO if/when scylla uses hints
        log(" disableHintsForDC()");
    }

    @Override
    public Set<String> getHintedHandoffDisabledDCs() {
        // TODO if/when scylla uses hints
        log(" getHintedHandoffDisabledDCs()");
        return emptySet();
    }

    @Override
    public int getNumberOfTables() {
        // TODO: could be like 1000% more efficient
        JsonArray mbeans = client.getJsonArray("/column_family/");
        return mbeans.size();
    }
}
