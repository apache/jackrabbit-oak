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
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

/**
 * {@link QueryIndexProvider} for {@link SolrQueryIndex}
 */
public class SolrQueryIndexProvider implements QueryIndexProvider {

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndexProvider.class);

    private final SolrServerProvider solrServerProvider;

    private final OakSolrConfigurationProvider oakSolrConfigurationProvider;

    private final NodeAggregator aggregator;

    private final Map<NodeState, LMSEstimator> estimators = new WeakHashMap<NodeState, LMSEstimator>();

    public SolrQueryIndexProvider(@Nonnull SolrServerProvider solrServerProvider, @Nonnull OakSolrConfigurationProvider oakSolrConfigurationProvider,
                                  @Nullable NodeAggregator nodeAggregator) {
        this.oakSolrConfigurationProvider = oakSolrConfigurationProvider;
        this.solrServerProvider = solrServerProvider;
        this.aggregator = nodeAggregator;
    }

    public SolrQueryIndexProvider(SolrServerProvider solrServerProvider, OakSolrConfigurationProvider oakSolrConfigurationProvider) {
        this(solrServerProvider, oakSolrConfigurationProvider, null);
    }

    @Nonnull
    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {

        List<QueryIndex> tempIndexes = new ArrayList<QueryIndex>();
        NodeState definitions = nodeState.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : definitions.getChildNodeEntries()) {
            NodeState definition = entry.getNodeState();
            PropertyState type = definition.getProperty(TYPE_PROPERTY_NAME);
            if (type != null
                    && SolrQueryIndex.TYPE.equals(type.getValue(Type.STRING))) {
                try {
                        OakSolrConfiguration configuration = oakSolrConfigurationProvider.getConfiguration();
                        addQueryIndex(tempIndexes, entry.getName(), solrServerProvider.getSearchingSolrServer(), configuration, definition);
                } catch (Exception e) {
                    if (log.isErrorEnabled()) {
                        log.error("unable to create Solr query index at " + entry.getName(), e);
                    }
                }

            }
        }
        return tempIndexes;
    }

    private boolean isPersistedConfiguration(NodeState definition) {
        return definition.hasChildNode("server");
    }

    private void addQueryIndex(List<QueryIndex> tempIndexes, String name, SolrServer solrServer, OakSolrConfiguration configuration, NodeState definition) {
        try {
            if (solrServer != null) {
                LMSEstimator estimator;
                synchronized (estimators) {
                    estimator = estimators.get(definition);
                    if (estimator == null) {
                        estimator = new LMSEstimator();
                        estimators.put(definition, estimator);
                    }
                }
                tempIndexes.add(new SolrQueryIndex(
                        name,
                        solrServer,
                        configuration,
                        aggregator, estimator));
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("cannot create Solr query index as SolrServer is null");
                }
            }
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("unable to create Solr query index at " + name, e);
            }
        }
    }

}
