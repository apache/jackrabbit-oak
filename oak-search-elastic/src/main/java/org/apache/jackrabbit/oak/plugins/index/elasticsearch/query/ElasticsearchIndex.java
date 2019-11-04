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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.query;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexCoordinateFactory;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.SizeEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.util.LMSEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.common.Strings;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexConstants.TYPE_ELASTICSEARCH;

public class ElasticsearchIndex extends FulltextIndex {
    private static final Predicate<NodeState> ELASTICSEARCH_INDEX_DEFINITION_PREDICATE =
            state -> TYPE_ELASTICSEARCH.equals(state.getString(TYPE_PROPERTY_NAME));
    private static final Map<String, LMSEstimator> estimators = new WeakHashMap<>();

    // higher than some threshold below which the query should rather be answered by something else if possible
    private static final double MIN_COST = 100.1;

    private final ElasticsearchIndexCoordinateFactory esIndexCoordFactory;
    private final NodeState root;

    ElasticsearchIndex(@NotNull ElasticsearchIndexCoordinateFactory esIndexCoordFactory, @NotNull NodeState root) {
        this.esIndexCoordFactory = esIndexCoordFactory;
        this.root = root;
    }

    @Override
    protected String getType() {
        return TYPE_ELASTICSEARCH;
    }

    @Override
    protected SizeEstimator getSizeEstimator(IndexPlan plan) {
        return () -> getEstimator(plan.getPlanName()).estimate(plan.getFilter());
    }

    @Override
    protected Predicate<NodeState> getIndexDefinitionPredicate() {
        return ELASTICSEARCH_INDEX_DEFINITION_PREDICATE;
    }

    @Override
    public double getMinimumCost() {
        return MIN_COST;
    }

    @Override
    public String getIndexName() {
        return "elasticsearch";
    }

    @Override
    protected ElasticsearchIndexNode acquireIndexNode(IndexPlan plan) {
        return (ElasticsearchIndexNode) super.acquireIndexNode(plan);
    }

    @Override
    protected IndexNode acquireIndexNode(String indexPath) {
        ElasticsearchIndexNode elasticsearchIndexNode = ElasticsearchIndexNode.fromIndexPath(root, indexPath);
        elasticsearchIndexNode.setFactory(esIndexCoordFactory);
        return elasticsearchIndexNode;
    }

    @Override
    protected String getFulltextRequestString(IndexPlan plan, IndexNode indexNode) {
        return Strings.toString(ElasticsearchResultRowIterator.getESRequest(plan, getPlanResult(plan)));
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState rootState) {
        final Filter filter = plan.getFilter();

        // TODO: sorting

        final FulltextIndexPlanner.PlanResult pr = getPlanResult(plan);
        QueryLimits settings = filter.getQueryLimits();

        Iterator<FulltextResultRow> itr = new ElasticsearchResultRowIterator(esIndexCoordFactory, filter, pr, plan,
                acquireIndexNode(plan), FulltextIndex::shouldInclude, getEstimator(plan.getPlanName()));
        SizeEstimator sizeEstimator = getSizeEstimator(plan);

        /*
        TODO: sync (nrt too??)
        if (pr.hasPropertyIndexResult() || pr.evaluateSyncNodeTypeRestriction()) {
            itr = mergePropertyIndexResult(plan, rootState, itr);
        }
        */

        // no concept of rewound in ES (even if it might be doing it internally, we can't do much about it
        IteratorRewoundStateProvider rewoundStateProvider = () -> 0;
        return new FulltextPathCursor(itr, rewoundStateProvider, plan, settings, sizeEstimator);
    }

    private LMSEstimator getEstimator(String path) {
        estimators.putIfAbsent(path, new LMSEstimator());
        return estimators.get(path);
    }

    @Override
    protected boolean filterReplacedIndexes() {
        return true;
    }
}
