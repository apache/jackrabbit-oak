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
package org.apache.jackrabbit.oak;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexPlan;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static javax.jcr.query.Query.JCR_SQL2;
import static org.junit.Assert.assertTrue;

public class IndexCostEvaluationTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private ContentSession session = null;
    private LogCustomizer logCollector;

    @Before
    public void before() throws Exception {
        logCollector = LogCustomizer
                .forLogger(
                        "org.apache.jackrabbit.oak.query.QueryImpl")
                .enable(Level.DEBUG).create();
        logCollector.starting();
        double luceneMinCost = 2.2;
        double elasticMinCost = 2.1;
        TestIndexProvider testProvider = new TestIndexProvider("test-index", luceneMinCost);
        TestIndexProvider testProvider2 = new TestIndexProvider("test-index2", luceneMinCost);
        TestIndexProvider testProvider3 = new TestIndexProvider("test-index3", elasticMinCost);

        Jcr jcr = new Jcr(new Oak(), false)
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(testProvider)
                .with(testProvider2)
                .with(testProvider3);

        ContentRepository repository = jcr.createContentRepository();
        session = repository.login(null, null);
    }

    @After
    public void after() throws IOException {
        session.close();
        logCollector.finished();
    }

    // In cases where two indexes have same min cost i.e. both indexes are on par, we don't skip cost evaluation
    // even of cost from previous index is less than min cost of new index.
    @Test
    public void costEvaluationTest() throws Exception {
        String query = "SELECT * FROM [rep:Authorizable] WHERE [rep:principalName] = 'anonymous'";
        session.getLatestRoot().getQueryEngine().executeQuery(query, JCR_SQL2, 1, 0, null, null);

        boolean evaluationContinueLogPresent = false;
        boolean evaluationSkipLogPresent = false;
        for (String log : logCollector.getLogs()) {
            if (log.equals("minCost: 2.1 of index :test-index2 > best Cost: 2.0 from index: test-index, but both indexes have same minimum cost - cost evaluation will continue")) {
                evaluationContinueLogPresent = true;
            }
            if (log.equals("minCost: 2.11 of index :test-index3 < best Cost: 2.0 from index: test-index. Further index evaluation will be skipped")) {
                evaluationSkipLogPresent = true;
            }
        }
        assertTrue(evaluationContinueLogPresent);
        assertTrue(evaluationSkipLogPresent);
    }

    private static class TestIndexProvider implements QueryIndexProvider {
        private final TestIndex index;

        public TestIndexProvider(String indexName, double minimumCost) {
            this.index = new TestIndex(indexName, minimumCost);
        }

        @Override
        public @NotNull List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return List.of(index);
        }
    }

    private static class TestIndex implements QueryIndex, QueryIndex.AdvancedQueryIndex {

        private final String name;
        private final double minimumCost;

        public TestIndex(String indexName, double minimumCost) {
            this.name = indexName;
            this.minimumCost = minimumCost;
        }

        @Override
        public double getMinimumCost() {
            return minimumCost;
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
            return name;
        }

        @Override
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            IndexPlan.Builder b = new IndexPlan.Builder();
            Filter f = new FilterImpl(null, "SELECT * FROM [nt:file]", new QueryEngineSettings());
            IndexPlan plan1 = b.setEstimatedEntryCount(10).setPlanName("testIndexPlan1").setFilter(f).build();
            List<IndexPlan> indexList = new ArrayList<>();

            indexList.add(plan1);
            return indexList;
        }

        @Override
        public String getPlanDescription(IndexPlan plan, NodeState root) {
            return null;
        }

        @Override
        public Cursor query(IndexPlan plan, NodeState rootState) {
            return null;
        }
    }
}
