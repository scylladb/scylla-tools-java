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

import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import com.scylladb.jmx.api.APIClient;

public class ScyllaCacheService implements CacheServiceMBean {
    private static final Logger logger = Logger.getLogger(CacheService.class.getName());

    protected final APIClient client;

    public void log(String str) {
        logger.finest(str);
    }

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";

    public ScyllaCacheService(APIClient client) {
        this.client=client;
//        super(MBEAN_NAME, client, new CacheMetrics("KeyCache", "key"), new CacheMetrics("RowCache", "row"),
//                new CacheMetrics("CounterCache", "counter"));
    }

    @Override
    public int getRowCacheSavePeriodInSeconds() {
        log(" getRowCacheSavePeriodInSeconds()");
        return client.getIntValue("cache_service/row_cache_save_period");
    }

    @Override
    public void setRowCacheSavePeriodInSeconds(int rcspis) {
        log(" setRowCacheSavePeriodInSeconds(int rcspis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("period", Integer.toString(rcspis));
        client.post("cache_service/row_cache_save_period", queryParams);
    }

    @Override
    public int getKeyCacheSavePeriodInSeconds() {
        log(" getKeyCacheSavePeriodInSeconds()");
        return client.getIntValue("cache_service/key_cache_save_period");
    }

    @Override
    public void setKeyCacheSavePeriodInSeconds(int kcspis) {
        log(" setKeyCacheSavePeriodInSeconds(int kcspis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("period", Integer.toString(kcspis));
        client.post("cache_service/key_cache_save_period", queryParams);
    }

    @Override
    public int getCounterCacheSavePeriodInSeconds() {
        log(" getCounterCacheSavePeriodInSeconds()");
        return client.getIntValue("cache_service/counter_cache_save_period");
    }

    @Override
    public void setCounterCacheSavePeriodInSeconds(int ccspis) {
        log(" setCounterCacheSavePeriodInSeconds(int ccspis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("ccspis", Integer.toString(ccspis));
        client.post("cache_service/counter_cache_save_period", queryParams);
    }

    @Override
    public int getRowCacheKeysToSave() {
        log(" getRowCacheKeysToSave()");
        return client.getIntValue("cache_service/row_cache_keys_to_save");
    }

    @Override
    public void setRowCacheKeysToSave(int rckts) {
        log(" setRowCacheKeysToSave(int rckts)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("rckts", Integer.toString(rckts));
        client.post("cache_service/row_cache_keys_to_save", queryParams);
    }

    @Override
    public int getKeyCacheKeysToSave() {
        log(" getKeyCacheKeysToSave()");
        return client.getIntValue("cache_service/key_cache_keys_to_save");
    }

    @Override
    public void setKeyCacheKeysToSave(int kckts) {
        log(" setKeyCacheKeysToSave(int kckts)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("kckts", Integer.toString(kckts));
        client.post("cache_service/key_cache_keys_to_save", queryParams);
    }

    @Override
    public int getCounterCacheKeysToSave() {
        log(" getCounterCacheKeysToSave()");
        return client.getIntValue("cache_service/counter_cache_keys_to_save");
    }

    @Override
    public void setCounterCacheKeysToSave(int cckts) {
        log(" setCounterCacheKeysToSave(int cckts)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("cckts", Integer.toString(cckts));
        client.post("cache_service/counter_cache_keys_to_save", queryParams);
    }

    /**
     * invalidate the key cache; for use after invalidating row cache
     */
    @Override
    public void invalidateKeyCache() {
        log(" invalidateKeyCache()");
        client.post("cache_service/invalidate_key_cache");
    }

    /**
     * invalidate the row cache; for use after bulk loading via BinaryMemtable
     */
    @Override
    public void invalidateRowCache() {
        log(" invalidateRowCache()");
        client.post("cache_service/invalidate_row_cache");
    }

    @Override
    public void invalidateCounterCache() {
        log(" invalidateCounterCache()");
        client.post("cache_service/invalidate_counter_cache");
    }

    @Override
    public void setRowCacheCapacityInMB(long capacity) {
        log(" setRowCacheCapacityInMB(long capacity)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("capacity", Long.toString(capacity));
        client.post("cache_service/row_cache_capacity", queryParams);
    }

    @Override
    public void setKeyCacheCapacityInMB(long capacity) {
        log(" setKeyCacheCapacityInMB(long capacity)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("capacity", Long.toString(capacity));
        client.post("cache_service/key_cache_capacity", queryParams);
    }

    @Override
    public void setCounterCacheCapacityInMB(long capacity) {
        log(" setCounterCacheCapacityInMB(long capacity)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("capacity", Long.toString(capacity));
        client.post("cache_service/counter_cache_capacity_in_mb", queryParams);
    }

    /**
     * save row and key caches
     *
     * @throws ExecutionException
     *             when attempting to retrieve the result of a task that aborted
     *             by throwing an exception
     * @throws InterruptedException
     *             when a thread is waiting, sleeping, or otherwise occupied,
     *             and the thread is interrupted, either before or during the
     *             activity.
     */
    @Override
    public void saveCaches() throws ExecutionException, InterruptedException {
        log(" saveCaches() throws ExecutionException, InterruptedException");
        client.post("cache_service/save_caches");
    }
}
