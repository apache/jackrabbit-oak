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

import javax.annotation.CheckForNull;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.RemoteSolrServerConfiguration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
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

    public RemoteSolrServerProvider(RemoteSolrServerConfiguration remoteSolrServerConfiguration) {
        this.remoteSolrServerConfiguration = remoteSolrServerConfiguration;
    }

    @CheckForNull
    @Override
    public SolrClient getSolrServer() throws Exception {

        SolrClient solrServer = null;
        if (remoteSolrServerConfiguration.getSolrZkHost() != null && remoteSolrServerConfiguration.getSolrZkHost().length() > 0) {
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

    @CheckForNull
    @Override
    public SolrClient getIndexingSolrServer() throws Exception {
        SolrClient server = getSolrServer();

        if (server instanceof HttpSolrClient) {
            String url = ((HttpSolrClient) server).getBaseURL();
            ConcurrentUpdateSolrClient concurrentUpdateSolrServer = new ConcurrentUpdateSolrClient(url, 1000,
                    Runtime.getRuntime().availableProcessors());
            concurrentUpdateSolrServer.setConnectionTimeout(remoteSolrServerConfiguration.getConnectionTimeout());
            concurrentUpdateSolrServer.setSoTimeout(remoteSolrServerConfiguration.getSocketTimeout());
            concurrentUpdateSolrServer.blockUntilFinished();
            server = concurrentUpdateSolrServer;
        }
        return server;
    }

    @CheckForNull
    @Override
    public SolrClient getSearchingSolrServer() throws Exception {
        return getSolrServer();
    }

    private SolrClient initializeWithExistingHttpServer() throws IOException, SolrServerException {
        // try basic Solr HTTP client
        HttpSolrClient httpSolrServer = new HttpSolrClient(remoteSolrServerConfiguration.getSolrHttpUrls()[0]);
        httpSolrServer.setConnectionTimeout(remoteSolrServerConfiguration.getConnectionTimeout());
        httpSolrServer.setSoTimeout(remoteSolrServerConfiguration.getSocketTimeout());
        SolrPingResponse ping = httpSolrServer.ping();
        if (ping != null && 0 == ping.getStatus()) {
            return httpSolrServer;
        } else {
            throw new IOException("the found HTTP Solr server is not alive");
        }

    }

    private SolrClient initializeWithCloudSolrServer() throws IOException {
        // try SolrCloud client
        CloudSolrClient cloudSolrServer = new CloudSolrClient(remoteSolrServerConfiguration.getSolrZkHost());
        cloudSolrServer.setZkConnectTimeout(remoteSolrServerConfiguration.getConnectionTimeout());
        cloudSolrServer.setZkClientTimeout(remoteSolrServerConfiguration.getSocketTimeout());
        cloudSolrServer.setIdField(OakSolrConfigurationDefaults.PATH_FIELD_NAME);

        if (connectToZK(cloudSolrServer)) {
            log.debug("CloudSolrServer connected");
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

            log.debug("waiting for CloudSolrServer to come alive");
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
            cloudSolrServer.close();
            throw new IOException("the found SolrCloud server is not alive");
        } else {
            throw new IOException("could not connect to Zookeeper hosted at " + remoteSolrServerConfiguration.getSolrZkHost());
        }

    }

    private boolean connectToZK(CloudSolrClient cloudSolrServer) {
        log.debug("connecting {}", cloudSolrServer);
        boolean connected = false;
        for (int i = 0; i < 3; i++) {
            try {
                cloudSolrServer.connect();
                connected = true;
                break;
            } catch (Exception e) {
                log.warn("could not connect to ZK", e);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                    // do nothing
                }
            }
        }
        return connected;
    }

    private void createCollectionIfNeeded(CloudSolrClient cloudSolrServer) throws SolrServerException {
        String solrCollection = remoteSolrServerConfiguration.getSolrCollection();
        ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();
        SolrZkClient zkClient = zkStateReader.getZkClient();
        log.debug("creating {} collection if needed", solrCollection);
        try {
            if (zkClient.isConnected() && !zkClient.exists("/configs/" + solrCollection, false)) {
                String solrConfDir = remoteSolrServerConfiguration.getSolrConfDir();
                Path dir;
                if (solrConfDir != null && solrConfDir.length() > 0) {
                    log.info("uploading config from {}", solrConfDir);
                    dir = Paths.get(solrConfDir);
                } else {
                    Path tempDirectory = Files.createTempDirectory("oak-solr-conf");

                    copy("schema", tempDirectory);
                    copy("solrconfig", tempDirectory);

                    log.info("uploading config from {}", tempDirectory);
                    dir = tempDirectory;
                }
                log.debug("uploading config from {}", dir);
                cloudSolrServer.uploadConfig(dir, solrCollection);

                log.debug("creating collection {}", solrCollection);

                CollectionAdminRequest.Create req = new CollectionAdminRequest.Create();
                CollectionAdminResponse response = req.setCollectionName(solrCollection)
                        .setReplicationFactor(remoteSolrServerConfiguration.getSolrReplicationFactor())
                        .setConfigName(solrCollection)
                        .setNumShards(remoteSolrServerConfiguration.getSolrShardsNo())
                        .process(cloudSolrServer);

                log.info("collection creation response {}", response);

                cloudSolrServer.setParallelUpdates(true);
                cloudSolrServer.request(req);
            }
        } catch (Exception e) {
            log.warn("could not create collection {}", solrCollection);
            throw new SolrServerException(e);
        }
    }

    private void copy(String name, Path tempDirectory) throws IOException {
        InputStream inputStream = getClass().getResourceAsStream("/solr/oak/conf/" + name + ".xml");
        FileOutputStream outputStream = new FileOutputStream(Files.createTempFile(tempDirectory, name, ".xml").toFile());
        IOUtils.copy(inputStream, outputStream);
        inputStream.close();
        outputStream.flush();
        outputStream.close();
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
