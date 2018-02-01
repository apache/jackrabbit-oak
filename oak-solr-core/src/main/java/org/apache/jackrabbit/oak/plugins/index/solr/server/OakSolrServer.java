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

import javax.annotation.Nonnull;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

/**
 * An Oak {@link org.apache.solr.client.solrj.SolrServer}, caching a {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider}
 * for dispatching requests to indexing or searching specialized {@link org.apache.solr.client.solrj.SolrServer}s.
 */
public class OakSolrServer extends SolrServer {

    private final SolrServerConfiguration solrServerConfiguration;
    private final SolrServerProvider solrServerProvider;

    public OakSolrServer(@Nonnull SolrServerConfigurationProvider solrServerConfigurationProvider) {
        this.solrServerConfiguration = solrServerConfigurationProvider.getSolrServerConfiguration();
        try {
            this.solrServerProvider = solrServerConfiguration.getProvider();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
        try {

            SolrClient server = getServer(request);
            return server.request(request);

        } catch (Exception e) {
            throw new SolrServerException(e);
        }
    }

    private synchronized SolrClient getServer(SolrRequest request) throws Exception {
        boolean isIndex = request.getPath().contains("/update");
        SolrServerRegistry.Strategy strategy = isIndex ? SolrServerRegistry.Strategy.INDEXING : SolrServerRegistry.Strategy.SEARCHING;
        SolrClient solrServer = SolrServerRegistry.get(solrServerConfiguration, strategy);
        if (solrServer == null) {
            if (solrServerConfiguration instanceof EmbeddedSolrServerConfiguration) {
                solrServer = solrServerProvider.getSolrServer();
                // the same Solr server has to be used for both
                SolrServerRegistry.register(solrServerConfiguration, solrServer, SolrServerRegistry.Strategy.INDEXING);
                SolrServerRegistry.register(solrServerConfiguration, solrServer, SolrServerRegistry.Strategy.SEARCHING);
            } else {
                solrServer = isIndex ? solrServerProvider.getIndexingSolrServer() : solrServerProvider.getSearchingSolrServer();
                SolrServerRegistry.register(solrServerConfiguration, solrServer, strategy);
            }
        }
        return solrServer;
    }

    @Override
    public void shutdown() {
        try {
            solrServerProvider.close();
            SolrServerRegistry.unregister(solrServerConfiguration, SolrServerRegistry.Strategy.INDEXING);
            SolrServerRegistry.unregister(solrServerConfiguration, SolrServerRegistry.Strategy.SEARCHING);
        } catch (IOException e) {
            // do nothing
        }
    }
}
