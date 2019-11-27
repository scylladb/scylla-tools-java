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
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.cassandra.metrics.CompactionMetrics;

import com.scylladb.jmx.api.APIClient;

/**
 * A singleton which manages a private executor of ongoing compactions.
 * <p/>
 * Scheduling for compaction is accomplished by swapping sstables to be
 * compacted into a set via DataTracker. New scheduling attempts will ignore
 * currently compacting sstables.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
public class ScyllaCompactionManager implements CompactionManagerMBean {
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = Logger.getLogger(CompactionManager.class.getName());
    protected final APIClient client;

    public void log(String str) {
        logger.finest(str);
    }

    public ScyllaCompactionManager(APIClient client) {
        this.client=client;
//        super(MBEAN_OBJECT_NAME, client, new CompactionMetrics());
    }

    /** List of running compaction objects. */
    @Override
    public List<Map<String, String>> getCompactions() {
        log(" getCompactions()");
        List<Map<String, String>> results = new ArrayList<Map<String, String>>();
        JsonArray compactions = client.getJsonArray("compaction_manager/compactions");
        for (int i = 0; i < compactions.size(); i++) {
            JsonObject compaction = compactions.getJsonObject(i);
            Map<String, String> result = new HashMap<String, String>();
            result.put("total", Long.toString(compaction.getJsonNumber("total").longValue()));
            result.put("completed", Long.toString(compaction.getJsonNumber("completed").longValue()));
            result.put("taskType", compaction.getString("task_type"));
            result.put("keyspace", compaction.getString("ks"));
            result.put("columnfamily", compaction.getString("cf"));
            result.put("unit", compaction.getString("unit"));
            result.put("compactionId", "<none>");
            results.add(result);
        }
        return results;
    }

    /** List of running compaction summary strings. */
    @Override
    public List<String> getCompactionSummary() {
        log(" getCompactionSummary()");
        return client.getListStrValue("compaction_manager/compaction_summary");
    }

    /** compaction history **/
    @Override
    public TabularData getCompactionHistory() {
        log(" getCompactionHistory()");
        try {
            return CompactionHistoryTabularData.from(client.getJsonArray("/compaction_manager/compaction_history"));
        } catch (OpenDataException e) {
            return null;
        }
    }

    /**
     * Triggers the compaction of user specified sstables. You can specify files
     * from various keyspaces and columnfamilies. If you do so, user defined
     * compaction is performed several times to the groups of files in the same
     * keyspace/columnfamily.
     *
     * @param dataFiles
     *            a comma separated list of sstable file to compact. must
     *            contain keyspace and columnfamily name in path(for 2.1+) or
     *            file name itself.
     */
    @Override
    public void forceUserDefinedCompaction(String dataFiles) {
        log(" forceUserDefinedCompaction(String dataFiles)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("dataFiles", dataFiles);
        client.post("compaction_manager/force_user_defined_compaction", queryParams);
    }

    @Override
    public void forceUserDefinedCleanup(String dataFiles) {
        //TODO fix add this
    }

    /**
     * Stop all running compaction-like tasks having the provided {@code type}.
     *
     * @param type
     *            the type of compaction to stop. Can be one of: - COMPACTION -
     *            VALIDATION - CLEANUP - SCRUB - INDEX_BUILD
     */
    @Override
    public void stopCompaction(String type) {
        log(" stopCompaction(String type)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("type", type);
        client.post("compaction_manager/stop_compaction", queryParams);
    }

    /**
     * Returns core size of compaction thread pool
     */
    @Override
    public int getCoreCompactorThreads() {
        log(" getCoreCompactorThreads()");
        return client.getIntValue("");
    }

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     *
     * @param number
     *            New maximum of compaction threads
     */
    @Override
    public void setCoreCompactorThreads(int number) {
        log(" setCoreCompactorThreads(int number)");
    }

    /**
     * Returns maximum size of compaction thread pool
     */
    @Override
    public int getMaximumCompactorThreads() {
        log(" getMaximumCompactorThreads()");
        return client.getIntValue("");
    }

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     *
     * @param number
     *            New maximum of compaction threads
     */
    @Override
    public void setMaximumCompactorThreads(int number) {
        log(" setMaximumCompactorThreads(int number)");
    }

    /**
     * Returns core size of validation thread pool
     */
    @Override
    public int getCoreValidationThreads() {
        log(" getCoreValidationThreads()");
        return client.getIntValue("");
    }

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     *
     * @param number
     *            New maximum of compaction threads
     */
    @Override
    public void setCoreValidationThreads(int number) {
        log(" setCoreValidationThreads(int number)");
    }

    /**
     * Returns size of validator thread pool
     */
    @Override
    public int getMaximumValidatorThreads() {
        log(" getMaximumValidatorThreads()");
        return client.getIntValue("");
    }

    /**
     * Allows user to resize maximum size of the validator thread pool.
     *
     * @param number
     *            New maximum of validator threads
     */
    @Override
    public void setMaximumValidatorThreads(int number) {
        log(" setMaximumValidatorThreads(int number)");
    }

    @Override
    public void stopCompactionById(String compactionId) {
        // scylla does not have neither compaction ids nor the file described
        // in:
        // "Ids can be found in the transaction log files whose name starts with
        // compaction_, located in the table transactions folder"
        // (nodetool)
        // TODO: throw?
        log(" stopCompactionById");
    }
}
