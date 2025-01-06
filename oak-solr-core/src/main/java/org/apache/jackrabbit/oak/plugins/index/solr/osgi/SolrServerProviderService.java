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

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Option;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.OakSolrServer;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;

import org.apache.solr.client.solrj.SolrClient;

import org.jetbrains.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider}
 */
@Component(
        immediate = true,
        service = { SolrServerProvider.class },
        reference = {
                @Reference(
                        name = "solrServerConfigurationProvider",
                        service = SolrServerConfigurationProvider.class,
                        cardinality = ReferenceCardinality.AT_LEAST_ONE,
                        policy = ReferencePolicy.DYNAMIC,
                        bind = "bindSolrServerConfigurationProvider",
                        unbind = "unbindSolrServerConfigurationProvider",
                        updated = "updatedSolrServerConfigurationProvider"
                )
        }
)
@Designate(
        ocd = SolrServerProviderService.Configuration.class
)
public class SolrServerProviderService implements SolrServerProvider {

    @ObjectClassDefinition(
            id = "org.apache.jackrabbit.oak.plugins.index.solr.osgi.SolrServerProviderService",
            name = "Apache Jackrabbit Oak Solr server provider"
    )
    @interface Configuration {
        @AttributeDefinition(
                name = "Property server.type",
                options = {
                        @Option(value = "none",
                                label = "None"
                        ),
                        @Option(value = "embedded",
                                label = "Embedded Solr"
                        ),
                        @Option(value = "remote",
                                label = "Remote Solr"
                        )}
        )
        OakSolrConfiguration.ServerType server_type() default OakSolrConfiguration.ServerType.none;
    }
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<String, SolrServerConfigurationProvider> solrServerConfigurationProviders = new HashMap<String, SolrServerConfigurationProvider>();

    private String serverType;

    private SolrClient cachedSolrServer;

    @Activate
    protected void activate(Configuration configuration) throws Exception {
        serverType = configuration.server_type().toString();
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
                cachedSolrServer.close();
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

    @Nullable
    @Override
    public SolrClient getSolrServer() throws Exception {
        synchronized (solrServerConfigurationProviders) {
            if (cachedSolrServer == null) {
                cachedSolrServer = getServer();
            }
            return cachedSolrServer;
        }
    }

    @Nullable
    @Override
    public SolrClient getIndexingSolrServer() throws Exception {
        return getSolrServer();
    }

    @Nullable
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
