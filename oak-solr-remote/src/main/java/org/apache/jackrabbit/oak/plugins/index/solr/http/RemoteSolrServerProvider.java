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

import java.io.IOException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SolrServerProvider} for remote Solr installations.
 */
@Component(metatype = true, immediate = true)
@Service(SolrServerProvider.class)
public class RemoteSolrServerProvider implements SolrServerProvider {

    private final Logger log = LoggerFactory.getLogger(RemoteSolrServerProvider.class);

    private static final String DEFAULT_COLLECTION = "oak";

    @Property(value = "http://127.0.0.1:8983/solr")
    private static final String SOLR_HTTP_URL = "solr.http.url";

    @Property(value = "localhost:9983")
    private static final String SOLR_ZK_HOST = "solr.zk.host";

    private SolrServer solrServer;
    private String solrHttpUrl;
    private String solrZkHost;

    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {
        solrHttpUrl = String.valueOf(componentContext.getProperties().get(SOLR_HTTP_URL));
        solrZkHost = String.valueOf(componentContext.getProperties().get(SOLR_ZK_HOST));
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHttpUrl = null;
        solrZkHost = null;
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
                log.warn("unable to initialize default SolrCloud client");
                try {
                    solrServer = initializeWithExistingHttpServer();
                } catch (Exception e1) {
                    log.warn("unable to initialize default Solr HTTP client");
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
            // TODO : check if oak collection exists, otherwise create it
            return httpSolrServer;
        } else {
            throw new IOException("the found HTTP Solr server is not alive");
        }

    }

    private SolrServer initializeWithCloudSolrServer() throws IOException, SolrServerException {
        // try SolrCloud client
        CloudSolrServer cloudSolrServer = new CloudSolrServer(solrZkHost);
        cloudSolrServer.setDefaultCollection(DEFAULT_COLLECTION);
        if (OakSolrUtils.checkServerAlive(cloudSolrServer)) {
            // TODO : check if oak collection exists, otherwise create it
            return cloudSolrServer;
        } else {
            throw new IOException("the found SolrCloud server is not alive");
        }
    }
}
