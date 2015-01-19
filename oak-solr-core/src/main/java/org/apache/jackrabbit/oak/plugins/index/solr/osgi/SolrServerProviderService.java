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
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider}
 */
@Component(metatype = true, label = "Oak Solr server provider", immediate = true)
@References({
        @Reference(name = "solrServerConfigurationProvider",
                referenceInterface = SolrServerConfigurationProvider.class,
                cardinality = ReferenceCardinality.MANDATORY_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC,
                bind = "bindSolrServerConfigurationProvider",
                unbind = "unbindSolrServerConfigurationProvider",
                updated = "updateSolrServerConfigurationProvider"
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

    private SolrServer cachedSolrServer;
    private SolrServer cachedSearchingSolrServer;
    private SolrServer cachedIndexingSolrServer;

    @Activate
    protected void activate(ComponentContext context) throws Exception {
        serverType = String.valueOf(context.getProperties().get(SERVER_TYPE));
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrServerConfigurationProviders.clear();
        shutdownSolrServers();
    }

    private void shutdownSolrServers() {
        if (cachedSolrServer != null) {
            try {
                cachedSolrServer.shutdown();
            } catch (Exception e) {
                log.error("could not correctly shutdown Solr {} server {}", serverType, cachedSolrServer);
            } finally {
                cachedSolrServer = null;
            }
        }
        if (cachedIndexingSolrServer != null) {
            try {
                cachedIndexingSolrServer.shutdown();
            } catch (Exception e) {
                log.error("could not correctly shutdown Solr {} server {}", serverType, cachedIndexingSolrServer);
            } finally {
                cachedIndexingSolrServer = null;
            }
        }
        if (cachedSearchingSolrServer != null) {
            try {
                cachedSearchingSolrServer.shutdown();
            } catch (Exception e) {
                log.error("could not correctly shutdown Solr {} server {}", serverType, cachedSearchingSolrServer);
            } finally {
                cachedSearchingSolrServer = null;
            }
        }
    }

    protected void bindSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.put(name, solrServerConfigurationProvider);
            shutdownSolrServers();
        }
    }

    protected void unbindSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.remove(name);
            shutdownSolrServers();
        }
    }

    protected void updatedSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.put(name, solrServerConfigurationProvider);
            shutdownSolrServers();
        }
    }

    @CheckForNull
    @Override
    public SolrServer getSolrServer() throws Exception {
        synchronized (solrServerConfigurationProviders) {
            if (cachedSolrServer == null) {
                cachedSolrServer = getServer();
            }
            return cachedSolrServer;
        }
    }

    @CheckForNull
    @Override
    public SolrServer getIndexingSolrServer() throws Exception {
        synchronized (solrServerConfigurationProviders) {
            if (cachedIndexingSolrServer == null) {
                cachedIndexingSolrServer = getServer();
            }
            return cachedIndexingSolrServer;
        }
    }

    @CheckForNull
    @Override
    public SolrServer getSearchingSolrServer() throws Exception {
        synchronized (solrServerConfigurationProviders) {
            if (cachedSearchingSolrServer == null) {
                cachedSearchingSolrServer = getServer();
            }
            return cachedSearchingSolrServer;
        }
    }

    private SolrServer getServer() {
        SolrServer solrServer = null;
        if (serverType != null && !"none".equals(serverType)) {
            SolrServerConfigurationProvider solrServerConfigurationProvider = solrServerConfigurationProviders.get(serverType);
            if (solrServerConfigurationProvider != null) {
                try {
                    SolrServerConfiguration solrServerConfiguration = solrServerConfigurationProvider.getSolrServerConfiguration();
                    SolrServerProvider solrServerProvider = solrServerConfiguration.getProvider();
                    solrServer = solrServerProvider.getSolrServer();
                    log.info("created new SolrServer {}", solrServer);
                } catch (Exception e) {
                    log.error("could not get a SolrServerProvider of type {}", serverType, e);
                }
            }
        }
        return solrServer;
    }

}
