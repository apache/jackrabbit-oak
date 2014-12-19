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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocumentList;

/**
 * {@link org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvanceFulltextQueryIndex} implementation of a Solr
 * {@link org.apache.jackrabbit.oak.spi.query.QueryIndex} index, extending {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex}.
 */
public class AdvancedSolrQueryIndex extends SolrQueryIndex implements QueryIndex.AdvanceFulltextQueryIndex {

    private static final Map<String, Long> cache = new WeakHashMap<String, Long>();

    private final OakSolrConfiguration configuration;
    private final SolrServer solrServer;
    private final NodeAggregator aggregator;
    private final String name;

    public AdvancedSolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration, NodeAggregator aggregator) {
        super(name, solrServer, configuration, aggregator);
        this.name = name;
        this.configuration = configuration;
        this.solrServer = solrServer;
        this.aggregator = aggregator;
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        // TODO : eventually provide multiple plans for (eventually) filtering by ACLs
        // TODO : eventually provide multiple plans for normal paging vs deep paging
        // TODO : eventually support sorting
        if (getMatchingFilterRestrictions(filter) > 0) {
            return Collections.singletonList(planBuilder(filter)
                    .setEstimatedEntryCount(estimateEntryCount(filter))
                    .build());
        } else {
            return Collections.emptyList();
        }
    }

    private long estimateEntryCount(Filter filter) {
        String key = filter.toString();
        long cachedEstimate = cache.get(key) != null ? cache.get(key) : -1;
        long estimatedEntryCount;
        if (cachedEstimate >= 0) {
            estimatedEntryCount = cachedEstimate;
        } else {
            Long updatedEstimation = initializeEstimation(filter);
            cache.put(key, updatedEstimation);
            estimatedEntryCount = updatedEstimation;
        }
        return estimatedEntryCount;
    }

    private Long initializeEstimation(Filter filter) {
        SolrQuery solrQuery = new SolrQuery("*:*");
        try {
            return solrServer.query(solrQuery).getResults().getNumFound() / 3; // 33% of the docs is a reasonable worst case
        } catch (Exception e) {
            return Long.MAX_VALUE;
        }
    }

    private IndexPlan.Builder planBuilder(Filter filter) {
        return new IndexPlan.Builder()
                .setCostPerExecution(solrServer instanceof EmbeddedSolrServer ? 1 : 2) // disk I/O + network I/O
                .setCostPerEntry(0.3) // with properly configured SolrCaches ~70% of the doc fetches should hit them
                .setFilter(filter)
                .setFulltextIndex(true)
                .setIncludesNodeData(true) // we currently include node data
                .setDelayed(true); //Solr is most usually async
    }

    @Override
    void onRetrievedResults(Filter filter, SolrDocumentList docs) {
        // update estimates cache
        cache.put(filter.toString(), docs.getNumFound());
    }

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        return plan.toString();
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState rootState) {
        return super.query(plan.getFilter(), rootState);
    }

    @Override
    public NodeAggregator getNodeAggregator() {
        return aggregator;
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        return super.getCost(filter, rootState);
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        return super.query(filter, rootState);
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        return super.getPlan(filter, rootState);
    }

    @Override
    public String getIndexName() {
        return name;
    }
}
