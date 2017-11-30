/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.solr.server;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.RemoteSolrServerConfiguration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Testcase for {@link RemoteSolrServerProvider}
 */
public class RemoteSolrServerProviderIT {

    // common local zk hosts
    private final String[] zkHosts = new String[]{"localhost:9983"};

    private boolean canCreateCollections(String host) throws Exception {
        UpdateRequest req = new UpdateRequest("/admin/collections");
        req.setParam("action", "CREATE");
        String solrCollection = "solr_" + System.nanoTime();
        req.setParam("name", solrCollection);
        req.setParam("numShards", "2");
        req.setParam("replicationFactor", "2");
        req.setParam("collection.configName", "myconf");
        CloudSolrClient cloudSolrServer = new CloudSolrClient(host);
        cloudSolrServer.setZkConnectTimeout(1000);
        NamedList<Object> request = cloudSolrServer.request(req);
        return request != null && request.get("success") != null;
    }

    @Test
    public void testCloudRemoteServerCreation() throws Exception {
        // do this test only if a Solr Cloud server is available
        for (String host : zkHosts) {
            boolean cloudServerAvailable = false;
            try {
                cloudServerAvailable = canCreateCollections(host);
            } catch (Exception e) {
                // do nothing
            }
            if (cloudServerAvailable) {
                String collection = "sample_" + System.nanoTime();
                RemoteSolrServerProvider remoteSolrServerProvider = new RemoteSolrServerProvider(
                        new RemoteSolrServerConfiguration(host, collection, 2, 2, null, 10, 10, null));
                SolrClient solrServer = remoteSolrServerProvider.getSolrServer();
                assertNotNull(solrServer);
                solrServer.close();
                break;
            }
        }
    }
}
