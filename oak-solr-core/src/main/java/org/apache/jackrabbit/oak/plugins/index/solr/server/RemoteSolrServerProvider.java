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

import java.io.File;
import java.io.IOException;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.RemoteSolrServerConfiguration;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider} for remote Solr installations.
 */
public class RemoteSolrServerProvider implements SolrServerProvider {

    private final Logger log = LoggerFactory.getLogger(RemoteSolrServerProvider.class);

    private final RemoteSolrServerConfiguration remoteSolrServerConfiguration;

    private SolrServer solrServer;

    public RemoteSolrServerProvider(RemoteSolrServerConfiguration remoteSolrServerConfiguration) {
        this.remoteSolrServerConfiguration = remoteSolrServerConfiguration;
    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        if (solrServer == null && remoteSolrServerConfiguration.getSolrZkHost() != null && remoteSolrServerConfiguration.getSolrZkHost().length() > 0) {
            try {
                solrServer = initializeWithCloudSolrServer();
            } catch (Exception e) {
                log.warn("unable to initialize SolrCloud client for {}", remoteSolrServerConfiguration.getSolrZkHost(), e);
            }
        }
        if (solrServer == null && remoteSolrServerConfiguration.getSolrHttpUrls() != null && remoteSolrServerConfiguration.getSolrHttpUrls().length == 1
                && remoteSolrServerConfiguration.getSolrHttpUrls()[0] != null && remoteSolrServerConfiguration.getSolrHttpUrls()[0].length() > 0) {
            try {
                solrServer = initializeWithExistingHttpServer();
            } catch (Exception e1) {
                log.warn("unable to initialize Solr HTTP client for {}", remoteSolrServerConfiguration.getSolrHttpUrls(), e1);
            }
        }
        if (solrServer == null) {
            throw new IOException("could not connect to any remote Solr server");
        }
        return solrServer;
    }

    private SolrServer initializeWithExistingHttpServer() throws IOException, SolrServerException {
        // try basic Solr HTTP client
        HttpSolrServer httpSolrServer = new HttpSolrServer(remoteSolrServerConfiguration.getSolrHttpUrls()[0]);
        SolrPingResponse ping = httpSolrServer.ping();
        if (ping != null && 0 == ping.getStatus()) {
            return httpSolrServer;
        } else {
            throw new IOException("the found HTTP Solr server is not alive");
        }

    }

    private SolrServer initializeWithCloudSolrServer() throws IOException, SolrServerException {
        // try SolrCloud client
        CloudSolrServer cloudSolrServer = new CloudSolrServer(remoteSolrServerConfiguration.getSolrZkHost());
        cloudSolrServer.connect();
        cloudSolrServer.setDefaultCollection("collection1"); // workaround for first request when the needed collection may not exist

        // create specified collection if it doesn't exists
        try {
            createCollectionIfNeeded(cloudSolrServer);
        } catch (Throwable t) {
            if (log.isWarnEnabled()) {
                log.warn("could not create the collection on {}", remoteSolrServerConfiguration.getSolrZkHost(), t);
            }
        }

        cloudSolrServer.setDefaultCollection(remoteSolrServerConfiguration.getSolrCollection());

        // SolrCloud may need some time to sync on collection creation (to spread it over the shards / replicas)
        int i = 0;
        while (i < 3) {
            try {
                SolrPingResponse ping = cloudSolrServer.ping();
                if (ping != null && 0 == ping.getStatus()) {
                    return cloudSolrServer;
                } else {
                    throw new IOException("the found SolrCloud server is not alive");
                }
            } catch (Exception e) {
                // wait a bit
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("server is not alive yet, wait a bit", e);
                    }
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                    // do nothing
                }
            }
            i++;
        }
        throw new IOException("the found SolrCloud server is not alive");

    }

    private void createCollectionIfNeeded(CloudSolrServer cloudSolrServer) throws SolrServerException, IOException {
        String solrCollection = remoteSolrServerConfiguration.getSolrCollection();
        try {
            ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();
            SolrZkClient zkClient = zkStateReader.getZkClient();
            if (zkClient.isConnected() && !zkClient.exists("/configs/" + solrCollection, false)) {
                File dir = new File(remoteSolrServerConfiguration != null ? remoteSolrServerConfiguration.getSolrConfDir() : getClass().getResource("/solr-oak-conf").getFile());
                ZkController.uploadConfigDir(zkClient, dir, solrCollection);
                UpdateRequest req = new UpdateRequest("/admin/collections");
                req.setParam("action", "CREATE");
                if (remoteSolrServerConfiguration != null) {
                    req.setParam("numShards", String.valueOf(remoteSolrServerConfiguration.getSolrShardsNo()));
                    req.setParam("replicationFactor", String.valueOf(remoteSolrServerConfiguration.getSolrReplicationFactor()));
                }
                req.setParam("collection.configName", solrCollection);
                req.setParam("name", solrCollection);
                cloudSolrServer.request(req);
            }
        } catch (Exception e) {
            log.warn("could not create collection {}", solrCollection);
            throw new SolrServerException(e);
        }
    }
}
