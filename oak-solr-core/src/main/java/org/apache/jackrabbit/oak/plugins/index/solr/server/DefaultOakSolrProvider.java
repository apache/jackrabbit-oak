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

import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

/**
 * Default implementation of {@link SolrServerProvider} and {@link OakSolrConfigurationProvider}
 * which hides an {@link EmbeddedSolrServer} configured as per passed {@link NodeState}
 * properties.
 */
public class DefaultOakSolrProvider implements SolrServerProvider, OakSolrConfigurationProvider {

    private final OakSolrNodeStateConfiguration oakSolrConfiguration;

    public DefaultOakSolrProvider(NodeState configurationNodeState) {
        this.oakSolrConfiguration = new OakSolrNodeStateConfiguration(configurationNodeState);
    }

    private SolrServer solrServer;

    private SolrServer createSolrServer() throws Exception {
        CoreContainer coreContainer = new CoreContainer(oakSolrConfiguration.getSolrHomePath());
        coreContainer.load(oakSolrConfiguration.getSolrHomePath(), new File(oakSolrConfiguration.getSolrConfigPath()));
        return new EmbeddedSolrServer(coreContainer, oakSolrConfiguration.getCoreName());
    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        if (solrServer == null) {
            solrServer = createSolrServer();
        }
        return solrServer;
    }

    @Override
    public OakSolrConfiguration getConfiguration() {
        return oakSolrConfiguration;
    }
}
