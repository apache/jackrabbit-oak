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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.OakSolrNodeStateConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.server.OakSolrServer;
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

    /**
     * How often it should check for a new Solr index.
     */
    private static final long NO_INDEX_CACHE_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final SolrServerProvider solrServerProvider;

    private final OakSolrConfigurationProvider oakSolrConfigurationProvider;

    private final NodeAggregator aggregator;

    private final Map<NodeState, LMSEstimator> estimators = new WeakHashMap<NodeState, LMSEstimator>();

    /**
     * The last time when it checked for the existence of a Solr index AND could not find any.
     */
    private volatile long lastNegativeIndexCheck = 0;

    public SolrQueryIndexProvider(@Nonnull SolrServerProvider solrServerProvider, @Nonnull OakSolrConfigurationProvider oakSolrConfigurationProvider,
                                  @Nullable NodeAggregator nodeAggregator) {
        this.oakSolrConfigurationProvider = oakSolrConfigurationProvider;
        this.solrServerProvider = solrServerProvider;
        this.aggregator = nodeAggregator;
    }

    public SolrQueryIndexProvider(@Nonnull SolrServerProvider solrServerProvider, @Nonnull OakSolrConfigurationProvider oakSolrConfigurationProvider) {
        this(solrServerProvider, oakSolrConfigurationProvider, null);
    }

    @Nonnull
    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        List<? extends QueryIndex> queryIndexes;
        // Return immediately if there are not any Solr indexes and the cache timeout has not been exceeded
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastNegativeIndexCheck > NO_INDEX_CACHE_TIMEOUT) {
            queryIndexes = internalGetQueryIndexes(nodeState);
            lastNegativeIndexCheck = queryIndexes.isEmpty() ? currentTime : 0;
        } else {
            queryIndexes = Collections.emptyList();
        }
        return queryIndexes;
    }

    private List<? extends QueryIndex> internalGetQueryIndexes(NodeState nodeState) {
        List<QueryIndex> tempIndexes = new ArrayList<QueryIndex>();
        NodeState definitions = nodeState.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : definitions.getChildNodeEntries()) {
            NodeState definition = entry.getNodeState();
            String name = entry.getName();
            PropertyState type = definition.getProperty(TYPE_PROPERTY_NAME);
            if (type != null && SolrQueryIndex.TYPE.equals(type.getValue(Type.STRING))) {
                try {
                    if (isPersistedConfiguration(definition)) {
                        OakSolrConfiguration configuration = new OakSolrNodeStateConfiguration(definition);
                        SolrServerConfigurationProvider solrServerConfigurationProvider = new NodeStateSolrServerConfigurationProvider(definition.getChildNode("server"));
                        SolrServer solrServer = new OakSolrServer(solrServerConfigurationProvider);
                        // if it does not already exist I need to register an observer that updates / closes this SolrServerProvider when the node is updated/removed
                        addQueryIndex(tempIndexes, name, solrServer, configuration, definition);
                    } else { // otherwise use the default configuration providers
                        OakSolrConfiguration configuration = oakSolrConfigurationProvider.getConfiguration();
                        addQueryIndex(tempIndexes, name, solrServerProvider.getSearchingSolrServer(), configuration, definition);
                    }
                } catch (Exception e) {
                    log.warn("could not get Solr query index from node {}", name, e);
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
