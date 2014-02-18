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
package org.apache.jackrabbit.oak.plugins.index.solr.http;

import java.io.File;
import java.io.IOException;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.util.OakSolrUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SolrServerProvider} for remote Solr installations.
 */
@Component(metatype = true, immediate = true, label = "Remote Solr Server Provider")
@Service(SolrServerProvider.class)
public class RemoteSolrServerProvider implements SolrServerProvider {

    private final Logger log = LoggerFactory.getLogger(RemoteSolrServerProvider.class);

    private static final String DEFAULT_COLLECTION = "oak";
    private static final String DEFAULT_HTTP_URL = "http://127.0.0.1:8983/solr/oak";
    private static final String DEFAULT_ZK_HOST = "localhost:9983";
    private static final int DEFAULT_SHARDS_NO = 2;
    private static final int DEFAULT_REPLICATION_FACTOR = 2;

    @Property(value = DEFAULT_HTTP_URL, label = "Solr HTTP URL")
    private static final String SOLR_HTTP_URL = "solr.http.url";

    @Property(value = DEFAULT_ZK_HOST, label = "ZooKeeper host")
    private static final String SOLR_ZK_HOST = "solr.zk.host";

    @Property(value = DEFAULT_COLLECTION, label = "Solr collection")
    private static final String SOLR_COLLECTION = "solr.collection";

    @Property(intValue = DEFAULT_SHARDS_NO, label = "No. of collection shards")
    private static final String SOLR_SHARDS_NO = "solr.shards.no";

    @Property(intValue = DEFAULT_REPLICATION_FACTOR, label = "Replication factor")
    private static final String SOLR_REPLICATION_FACTOR = "solr.replication.factor";

    @Property(value = "", label = "Solr configuration directory")
    private static final String SOLR_CONF_DIR = "solr.conf.dir";

    private SolrServer solrServer;
    private String solrHttpUrl;
    private String solrZkHost;
    private String solrCollection;
    private int solrShardsNo;
    private int solrReplicationFactor;
    private String solrConfDir;

    public RemoteSolrServerProvider() {
        this.solrHttpUrl = DEFAULT_HTTP_URL;
        this.solrZkHost = DEFAULT_ZK_HOST;
        this.solrCollection = DEFAULT_COLLECTION;
        this.solrShardsNo = DEFAULT_SHARDS_NO;
        this.solrReplicationFactor = DEFAULT_REPLICATION_FACTOR;
    }

    public RemoteSolrServerProvider(String solrHttpUrl, String solrZkHost,
                                    String solrCollection, int solrShardsNo,
                                    int solrReplicationFactor, String solrConfDir) {
        this.solrHttpUrl = solrHttpUrl;
        this.solrZkHost = solrZkHost;
        this.solrCollection = solrCollection;
        this.solrShardsNo = solrShardsNo;
        this.solrReplicationFactor = solrReplicationFactor;
        this.solrConfDir = solrConfDir;
    }

    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {
        solrHttpUrl = String.valueOf(componentContext.getProperties().get(SOLR_HTTP_URL));
        solrZkHost = String.valueOf(componentContext.getProperties().get(SOLR_ZK_HOST));
        solrCollection = String.valueOf(componentContext.getProperties().get(SOLR_COLLECTION));
        solrShardsNo = Integer.valueOf(componentContext.getProperties().get(SOLR_SHARDS_NO).toString());
        solrReplicationFactor = Integer.valueOf(componentContext.getProperties().get(SOLR_REPLICATION_FACTOR).toString());
        solrConfDir = String.valueOf(componentContext.getProperties().get(SOLR_CONF_DIR));
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHttpUrl = null;
        solrZkHost = null;
        solrCollection = null;
        solrShardsNo = 0;
        solrReplicationFactor = 0;
        solrConfDir = null;
        if (solrServer != null) {
            solrServer.shutdown();
            solrServer = null;
        }
    }


    @Override
    public SolrServer getSolrServer() throws Exception {
        if (solrServer == null) {
            try {
                solrServer = initializeWithCloudSolrServer();
            } catch (Exception e) {
                log.warn("unable to initialize SolrCloud client", e);
                try {
                    solrServer = initializeWithExistingHttpServer();
                } catch (Exception e1) {
                    log.warn("unable to initialize Solr HTTP client", e1);
                }
            }
            if (solrServer == null) {
                throw new IOException("could not connect to any HTTP Solr server");
            }
        }
        return solrServer;
    }

    private SolrServer initializeWithExistingHttpServer() throws IOException, SolrServerException {
        // try basic Solr HTTP client
        HttpSolrServer httpSolrServer = new HttpSolrServer(solrHttpUrl);
        if (OakSolrUtils.checkServerAlive(httpSolrServer)) {
            // TODO : check if the oak core exists, otherwise create it
            return httpSolrServer;
        } else {
            throw new IOException("the found HTTP Solr server is not alive");
        }

    }

    private SolrServer initializeWithCloudSolrServer() throws IOException, SolrServerException {
        // try SolrCloud client
        CloudSolrServer cloudSolrServer = new CloudSolrServer(solrZkHost);
        cloudSolrServer.connect();
        cloudSolrServer.setDefaultCollection("collection1"); // workaround for first request when the needed collection may not exist

        // create specified collection if it doesn't exists
        try {
            createCollectionIfNeeded(cloudSolrServer);
        } catch (Throwable t) {
            if (log.isWarnEnabled()) {
                log.warn("could not create the collection on {}, {}", solrZkHost, t);
            }
        }

        cloudSolrServer.setDefaultCollection(solrCollection);

        // SolrCloud may need some time to sync on collection creation (to spread it over the shards / replicas)
        int i = 0;
        while (i < 3) {
            try {
                OakSolrUtils.checkServerAlive(cloudSolrServer);
                return cloudSolrServer;
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
        try {
            ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();
            SolrZkClient zkClient = zkStateReader.getZkClient();
            if (zkClient.isConnected() && !zkClient.exists("/configs/" + solrCollection, false)) {
                File dir = new File(solrConfDir != null ? solrConfDir : getClass().getResource("/solr-oak-conf").getFile());
                ZkController.uploadConfigDir(zkClient, dir, solrCollection);
                UpdateRequest req = new UpdateRequest("/admin/collections");
                req.setParam("action", "CREATE");
                req.setParam("numShards", String.valueOf(solrShardsNo));
                req.setParam("replicationFactor", String.valueOf(solrReplicationFactor));
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
