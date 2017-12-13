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
package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.CheckForNull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.References;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.OakSolrServer;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.solr.client.solrj.SolrClient;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider}
 */
@Component(metatype = true, label = "Apache Jackrabbit Oak Solr server provider", immediate = true)
@References({
        @Reference(name = "solrServerConfigurationProvider",
                referenceInterface = SolrServerConfigurationProvider.class,
                cardinality = ReferenceCardinality.MANDATORY_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC,
                bind = "bindSolrServerConfigurationProvider",
                unbind = "unbindSolrServerConfigurationProvider",
                updated = "updatedSolrServerConfigurationProvider"
        )
})
@Service(SolrServerProvider.class)
public class SolrServerProviderService implements SolrServerProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Property(options = {
            @PropertyOption(name = "none",
                    value = "None"
            ),
            @PropertyOption(name = "embedded",
                    value = "Embedded Solr"
            ),
            @PropertyOption(name = "remote",
                    value = "Remote Solr"
            )},
            value = "none"
    )
    private static final String SERVER_TYPE = "server.type";

    private final Map<String, SolrServerConfigurationProvider> solrServerConfigurationProviders = new HashMap<String, SolrServerConfigurationProvider>();

    private String serverType;

    private SolrClient cachedSolrServer;

    @Activate
    protected void activate(ComponentContext context) throws Exception {
        serverType = String.valueOf(context.getProperties().get(SERVER_TYPE));
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrServerConfigurationProviders.clear();
        close();
    }

    @Override
    public void close() throws IOException {
        if (cachedSolrServer != null) {
            try {
                cachedSolrServer.shutdown();
            } catch (Exception e) {
                log.error("could not correctly shutdown Solr {} server {}", serverType, cachedSolrServer);
            } finally {
                cachedSolrServer = null;
            }
        }
    }

    protected void bindSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.put(name, solrServerConfigurationProvider);
            try {
                close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    protected void unbindSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.remove(name);
            try {
                close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    protected void updatedSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.put(name, solrServerConfigurationProvider);
            try {
                close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    @CheckForNull
    @Override
    public SolrClient getSolrServer() throws Exception {
        synchronized (solrServerConfigurationProviders) {
            if (cachedSolrServer == null) {
                cachedSolrServer = getServer();
            }
            return cachedSolrServer;
        }
    }

    @CheckForNull
    @Override
    public SolrClient getIndexingSolrServer() throws Exception {
        return getSolrServer();
    }

    @CheckForNull
    @Override
    public SolrClient getSearchingSolrServer() throws Exception {
        return getSolrServer();
    }

    private SolrClient getServer() {
        SolrClient solrServer = null;
        if (serverType != null && !"none".equals(serverType)) {
            SolrServerConfigurationProvider solrServerConfigurationProvider = solrServerConfigurationProviders.get(serverType);
            if (solrServerConfigurationProvider != null) {
                try {
                    solrServer = new OakSolrServer(solrServerConfigurationProvider);
                    log.info("created new SolrServer {}", solrServer);
                } catch (Exception e) {
                    log.error("could not get a SolrServerProvider of type {}", serverType, e);
                }
            }
        }
        return solrServer;
    }

}
