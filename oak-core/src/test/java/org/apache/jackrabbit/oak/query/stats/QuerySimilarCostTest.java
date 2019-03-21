/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query.stats;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexPlan;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.apache.jackrabbit.oak.query.SQL2ParserTest;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.hamcrest.core.IsCollectionContaining;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

import com.google.common.collect.ImmutableList;

/**
 * Tests for cases where two or more indices return a similar cost estimation for the same query
 */
public class QuerySimilarCostTest extends AbstractQueryTest {

    private TestIndexProvider testIndexProvider = new TestIndexProvider();
    private final SQL2Parser p = SQL2ParserTest.createTestSQL2Parser();

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(testIndexProvider)
                .createContentRepository();
    }

    /*
     * Given 2 index plan with similar cost we expect a log at debug level to
     * intimate user to either modify either of the indices or the query
     */
    @Test
    public void testSimilarCostIndices() throws Exception {
        LogCustomizer customLogs = LogCustomizer.forLogger(QueryImpl.class.getName()).enable(Level.DEBUG).create();

        try {
            customLogs.starting();

            NodeUtil node = new NodeUtil(root.getTree("/"));
            String uuid = UUID.randomUUID().toString();
            node.setString(JcrConstants.JCR_UUID, uuid);
            root.commit();

            executeQuery("SELECT * FROM [nt:base] WHERE [jcr:uuid] is not null", SQL2, true, false);

            String expectedLogMessage = String.format("selected index %s "
                    + "with plan testIndexPlan1 and %s with plan testIndexPlan2 have similar costs 11.0 and 11.0 "
                    + "for query Filter(query=SELECT * FROM [nt:base] WHERE [jcr:uuid] is not null, path=*, property=[jcr:uuid=[is not null]]) - check query explanation / index definitions",
                    testIndexProvider.index, testIndexProvider.index);

            Assert.assertThat(customLogs.getLogs(), IsCollectionContaining.hasItems(expectedLogMessage));
        } finally {
            customLogs.finished();
        }
    }

    private static class TestIndexProvider implements QueryIndexProvider {
        TestIndex index = new TestIndex();

        @Override
        public @NotNull List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return ImmutableList.<QueryIndex>of(index);
        }
    }

    private static class TestIndex implements QueryIndex,QueryIndex.AdvancedQueryIndex {

        @Override
        public double getMinimumCost() {
            return PropertyIndexPlan.COST_OVERHEAD + 0.1;
        }

        @Override
        public double getCost(Filter filter, NodeState rootState) {
            return Double.POSITIVE_INFINITY;
        }

        @Override
        public Cursor query(Filter filter, NodeState rootState) {
            return null;
        }

        @Override
        public String getPlan(Filter filter, NodeState rootState) {
            return null;
        }

        @Override
        public String getIndexName() {
            return "test-index";
        }

        @Override
        public List<QueryIndex.IndexPlan> getPlans(Filter filter, List<QueryIndex.OrderEntry> sortOrder, NodeState rootState) {
            IndexPlan.Builder b = new IndexPlan.Builder();
            Filter f = new FilterImpl(null, "SELECT * FROM [nt:file]", new QueryEngineSettings());
            IndexPlan plan1 = b.setEstimatedEntryCount(10).setPlanName("testIndexPlan1").setFilter(f).build();
            IndexPlan plan2 = b.setEstimatedEntryCount(10).setPlanName("testIndexPlan2").setFilter(f).build();

            List<QueryIndex.IndexPlan> indexList = new ArrayList<QueryIndex.IndexPlan>();

            indexList.add(plan1);
            indexList.add(plan2);
            return indexList;
        }

        @Override
        public String getPlanDescription(QueryIndex.IndexPlan plan, NodeState root) {
            return null;
        }

        @Override
        public Cursor query(QueryIndex.IndexPlan plan, NodeState rootState) {
            return null;
        }
    }
}
