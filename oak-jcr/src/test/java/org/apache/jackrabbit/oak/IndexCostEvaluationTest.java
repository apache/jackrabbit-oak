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
import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexPlan;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.SimpleCredentials;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class IndexCostEvaluationTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private static final String TEST_USER_NAME = "testUserName";

    private Repository repository = null;
    private JackrabbitSession session = null;
    private Node root = null;
    private LogCustomizer custom;

    @Before
    public void before() throws Exception {
        custom = LogCustomizer
                .forLogger(
                        "org.apache.jackrabbit.oak.query.QueryImpl")
                .enable(Level.DEBUG).create();
        custom.starting();


        TestIndexProvider testProvider = new TestIndexProvider();
        TestIndexProvider2 testProvider2 = new TestIndexProvider2();
        TestIndexProvider3 testProvider3 = new TestIndexProvider3();

        Jcr jcr = new Jcr()
                .with((QueryIndexProvider) testProvider)
                .with((QueryIndexProvider) testProvider2)
                .with((QueryIndexProvider) testProvider3);

        repository = jcr.createRepository();
        session = (JackrabbitSession) repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        root = session.getRootNode();
    }


    @After
    public void after() {
        custom.finished();
        session.logout();
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
    }

    // In cases where two indexes have same min cost i.e. both indexes are on par, we don't skip cost evaluation
    // even of cost from previous index is less than min cost of new index.
    @Test
    public void costEvaluationTest() throws Exception {
        boolean evaluationContinueLogPresent = false;
        boolean evaluationSkipLogPresent = false;
        for (String log : custom.getLogs()) {
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

    private class TestIndexProvider implements QueryIndexProvider {
        TestIndex index = new TestIndex();

        @Override
        public @NotNull List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return ImmutableList.<QueryIndex>of(index);
        }
    }

    private class TestIndexProvider2 extends TestIndexProvider {
        public TestIndexProvider2() {
            this.index = new TestIndex() {
                public String getIndexName() {
                    return "test-index2";
                }
            };
        }
    }

    private class TestIndexProvider3 extends TestIndexProvider {
        public TestIndexProvider3() {
            this.index = new TestIndex() {
                public String getIndexName() {
                    return "test-index3";
                }

                public double getMinimumCost() {
                    return PropertyIndexPlan.COST_OVERHEAD + 0.11;
                }
            };
        }
    }

    private class TestIndex implements QueryIndex, QueryIndex.AdvancedQueryIndex {

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
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            IndexPlan.Builder b = new IndexPlan.Builder();
            Filter f = new FilterImpl(null, "SELECT * FROM [nt:file]", new QueryEngineSettings());
            IndexPlan plan1 = b.setEstimatedEntryCount(10).setPlanName("testIndexPlan1").setFilter(f).build();
            List<IndexPlan> indexList = new ArrayList<IndexPlan>();

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
