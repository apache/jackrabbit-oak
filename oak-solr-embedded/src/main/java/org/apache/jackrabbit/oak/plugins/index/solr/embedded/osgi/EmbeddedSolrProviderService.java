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
package org.apache.jackrabbit.oak.plugins.index.solr.embedded.osgi;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.UpToDateNodeStateConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.embedded.EmbeddedSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.embedded.EmbeddedSolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.solr.client.solrj.SolrServer;
import org.osgi.service.component.ComponentContext;

/**
 * OSGi service for the embedded Solr server module.
 */
@Component(metatype = true, label = "Embedded SolrServer provider")
@Service(value = { SolrServerProvider.class, OakSolrConfigurationProvider.class })
public class EmbeddedSolrProviderService implements SolrServerProvider,
        OakSolrConfigurationProvider {

    @Property(value = "/oak:index/solrIdx", label = "configuration path", description = "path to node holding Solr configuration")
    private static final String CONFIGURATION_PATH = "solr.configuration.node.path";

    @Reference
    private NodeStore nodeStore;

    @Reference
    private SolrServerConfigurationProvider solrServerConfigurationProvider;

    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    private SolrServerProvider solrServerProvider;

    @Activate
    public void activate(ComponentContext context) throws Exception {
        try {
            // try reading configuration from the configured repository path
            UpToDateNodeStateConfiguration nodeStateConfiguration = new UpToDateNodeStateConfiguration(
                    nodeStore, String.valueOf(context.getProperties().get(
                    CONFIGURATION_PATH)));
            solrServerProvider = new EmbeddedSolrServerProvider(
                    nodeStateConfiguration.getSolrServerConfiguration());
            oakSolrConfigurationProvider = new EmbeddedSolrConfigurationProvider(
                    nodeStateConfiguration);
        } catch (Exception e) {
            // use the default config and the OSGi based server configuration
            solrServerProvider = new EmbeddedSolrServerProvider(
                    solrServerConfigurationProvider.getSolrServerConfiguration());
            oakSolrConfigurationProvider = new EmbeddedSolrConfigurationProvider();
        }
    }

    @Override
    public OakSolrConfiguration getConfiguration() {
        return oakSolrConfigurationProvider.getConfiguration();
    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        return solrServerProvider.getSolrServer();
    }
}
