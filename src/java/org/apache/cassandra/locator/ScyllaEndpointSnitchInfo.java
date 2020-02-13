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
package org.apache.cassandra.locator;

import static java.util.Collections.singletonMap;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import com.scylladb.jmx.api.APIClient;

public class ScyllaEndpointSnitchInfo implements EndpointSnitchInfoMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=EndpointSnitchInfo";
    private static final Logger logger = Logger.getLogger(EndpointSnitchInfo.class.getName());

    protected final APIClient client;

    public ScyllaEndpointSnitchInfo(APIClient c) {
        this.client = c;
    }

    public void log(String str) {
        logger.finest(str);
    }

    /**
     * Provides the Rack name depending on the respective snitch used, given the
     * host name/ip
     *
     * @param host
     * @throws UnknownHostException
     */
    @Override
    public String getRack(String host) throws UnknownHostException {
        log("getRack(String host) throws UnknownHostException");
        MultivaluedMap<String, String> queryParams = host != null ? new MultivaluedHashMap<String, String>(
                singletonMap("host", InetAddress.getByName(host).getHostAddress())) : null;
        return client.getStringValue("/snitch/rack", queryParams, 10000);
    }

    /**
     * Provides the Datacenter name depending on the respective snitch used,
     * given the hostname/ip
     *
     * @param host
     * @throws UnknownHostException
     */
    @Override
    public String getDatacenter(String host) throws UnknownHostException {
        log(" getDatacenter(String host) throws UnknownHostException");
        MultivaluedMap<String, String> queryParams = host != null ? new MultivaluedHashMap<String, String>(
                singletonMap("host", InetAddress.getByName(host).getHostAddress())) : null;
        return client.getStringValue("/snitch/datacenter", queryParams, 10000);
    }

    /**
     * Provides the snitch name of the cluster
     *
     * @return Snitch name
     */
    @Override
    public String getSnitchName() {
        log(" getSnitchName()");
        return client.getStringValue("/snitch/name");
    }

    @Override
    public String getRack() {
        return client.getStringValue("/snitch/rack", null, 10000);
    }

    @Override
    public String getDatacenter() {
        return client.getStringValue("/snitch/datacenter", null, 10000);
    }
}
