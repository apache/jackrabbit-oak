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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;

/**
 * Solr index configurations provider based on configuration persisted in the repository
 */
public class NodeStateConfigurationProvider implements SolrServerProvider, OakSolrConfigurationProvider {

    private final OakSolrNodeStateConfiguration configuration;
    private SolrServerProvider solrServerProvider;

    public NodeStateConfigurationProvider(NodeState definition) {
        configuration = new OakSolrNodeStateConfiguration(definition);
        try {
            solrServerProvider = configuration.getSolrServerConfiguration().getProvider();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public NodeStateConfigurationProvider(NodeBuilder definition) {
        this(definition.getBaseState());
    }

    @Nonnull
    @Override
    public OakSolrConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        return solrServerProvider.getSolrServer();
    }

    @Override
    public SolrServer getIndexingSolrServer() throws Exception {
        return solrServerProvider.getIndexingSolrServer();
    }

    @Override
    public SolrServer getSearchingSolrServer() throws Exception {
        return solrServerProvider.getSearchingSolrServer();
    }
}
