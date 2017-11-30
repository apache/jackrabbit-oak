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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.OakSolrNodeStateConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.plugins.index.solr.server.OakSolrServer;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solr based {@link IndexEditorProvider}
 *
 * @see SolrIndexEditor
 */
public class SolrIndexEditorProvider implements IndexEditorProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final SolrServerProvider solrServerProvider;

    private final OakSolrConfigurationProvider oakSolrConfigurationProvider;

    public SolrIndexEditorProvider(
            @Nonnull SolrServerProvider solrServerProvider,
            @Nonnull OakSolrConfigurationProvider oakSolrConfigurationProvider) {
        this.solrServerProvider = solrServerProvider;
        this.oakSolrConfigurationProvider = oakSolrConfigurationProvider;
    }

    @Override
    public Editor getIndexEditor(
            @Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
            throws CommitFailedException {
        SolrIndexEditor editor = null;
        if (SolrQueryIndex.TYPE.equals(type)) {
            try {
                // if index definition contains a persisted configuration, use that
                if (isPersistedConfiguration(definition)) {
                    NodeState nodeState = definition.getNodeState();
                    OakSolrConfiguration configuration = new OakSolrNodeStateConfiguration(nodeState);
                    SolrServerConfigurationProvider configurationProvider = new NodeStateSolrServerConfigurationProvider(definition.getChildNode("server").getNodeState());
                    SolrClient solrServer = new OakSolrServer(configurationProvider);
                    editor = getEditor(configuration, solrServer, callback);
                } else { // otherwise use the default configuration providers (e.g. defined via code or OSGi)
                    OakSolrConfiguration configuration = oakSolrConfigurationProvider.getConfiguration();
                    editor = getEditor(configuration, solrServerProvider.getIndexingSolrServer(), callback);
                }
            } catch (Exception e) {
                log.warn("could not get Solr index editor from {}", definition.getNodeState(), e);
            }
        }
        return editor;
    }

    private boolean isPersistedConfiguration(NodeBuilder definition) {
        return definition.hasChildNode("server");
    }

    private SolrIndexEditor getEditor(OakSolrConfiguration configuration, SolrClient solrServer,
                                      IndexUpdateCallback callback) {
        SolrIndexEditor editor = null;
        try {
            if (solrServer != null) {
                editor = new SolrIndexEditor(solrServer, configuration, callback);
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("no SolrServer provided, cannot perform indexing");
                }
            }
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("unable to create SolrIndexEditor", e);
            }
        }
        return editor;
    }

}
