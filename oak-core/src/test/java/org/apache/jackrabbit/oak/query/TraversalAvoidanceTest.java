/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.List;

public class TraversalAvoidanceTest extends AbstractQueryTest {
    private static final String QUERY = "SELECT * FROM [nt:base]";
    private static final String PATH_RESTRICTED_QUERY = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/content')";
    private static final String PATH_RESTRICTED_SLOW_TRAVERSAL_QUERY = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/jcr:system')";

    private TestQueryIndexProvider testIndexProvider = new TestQueryIndexProvider();
    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(testIndexProvider)
                .createContentRepository();
    }

    @Test
    public void noPlansLetTraversalWin() {
        assertPlanSelection(QUERY, "traverse", "Traversal must be used if nothing else participates");
        assertPlanSelection(PATH_RESTRICTED_QUERY, "traverse", "Traversal must be used if nothing" +
                " else participates");
        assertPlanSelection(PATH_RESTRICTED_SLOW_TRAVERSAL_QUERY, "traverse", "Traversal must be" +
                " used if nothing else participates");
    }

    @Test
    public void singlePlanWithoutPathRestrictionWins() {
        testIndexProvider.addPlan("plan1", 1000000, false);

        assertPlanSelection(QUERY, "plan1", "Valid plan without path restriction must win");
    }

    @Test
    public void singlePlanWithPathRestriction() {
        testIndexProvider.addPlan("plan1", 1000000, true);

        assertPlanSelection(PATH_RESTRICTED_QUERY, "plan1", "Valid plan which evaluate path" +
                " restrictions wins with query having path restriction");

        testIndexProvider.restPlans();
        testIndexProvider.addPlan("plan1", 1000000, false);

        assertPlanSelection(PATH_RESTRICTED_QUERY, "traverse", "Valid plan which evaluate path" +
                " restrictions wins with query having path restriction");

        testIndexProvider.restPlans();
        testIndexProvider.addPlan("plan1", 10, false);
        assertPlanSelection(PATH_RESTRICTED_SLOW_TRAVERSAL_QUERY, "plan1", "cost wars still prevail");
    }

    @Test
    public void competingPlans() {
        testIndexProvider.addPlan("plan1", 1000000, true);
        testIndexProvider.addPlan("plan2", 100, true);

        assertPlanSelection(QUERY, "plan2", "Low cost must win");
        assertPlanSelection(PATH_RESTRICTED_QUERY, "plan2", "Low cost must win");
        assertPlanSelection(PATH_RESTRICTED_SLOW_TRAVERSAL_QUERY, "plan2", "Low cost must win");

        testIndexProvider.restPlans();
        testIndexProvider.addPlan("plan1", 1000000, false);
        testIndexProvider.addPlan("plan2", 100, true);

        assertPlanSelection(QUERY, "plan2", "Low cost must win");
        assertPlanSelection(PATH_RESTRICTED_QUERY, "plan2", "Low cost must win");
        assertPlanSelection(PATH_RESTRICTED_SLOW_TRAVERSAL_QUERY, "plan2", "Low cost must win");

        testIndexProvider.restPlans();
        testIndexProvider.addPlan("plan1", 1000000, true);
        testIndexProvider.addPlan("plan2", 100, false);

        assertPlanSelection(QUERY, "plan2", "Low cost must win");
        assertPlanSelection(PATH_RESTRICTED_QUERY, "traverse", "Low cost must win");
        assertPlanSelection(PATH_RESTRICTED_SLOW_TRAVERSAL_QUERY, "plan2", "Low cost must win");

        testIndexProvider.restPlans();
        testIndexProvider.addPlan("plan1", 1000000, false);
        testIndexProvider.addPlan("plan2", 100, false);

        assertPlanSelection(QUERY, "plan2", "Low cost must win");
        assertPlanSelection(PATH_RESTRICTED_QUERY, "traverse", "Low cost must win");
        assertPlanSelection(PATH_RESTRICTED_SLOW_TRAVERSAL_QUERY, "plan2", "Low cost must win");
    }

    class TestQueryIndexProvider implements QueryIndexProvider {
        private final TestQueryIndex queryIndex = new TestQueryIndex();

        void addPlan(String name, long cost, boolean supportsPathRestriction) {
            queryIndex.addPlan(name, cost, supportsPathRestriction);
        }

        void restPlans() {
            queryIndex.resetPlans();
        }

        @Nonnull
        @Override
        public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return ImmutableList.of(queryIndex);
        }
    }

    class TestQueryIndex implements QueryIndex, QueryIndex.AdvancedQueryIndex {
        final String name;

        final List<IndexPlan> plans;

        TestQueryIndex() {
            this("EmptyName");
        }

        TestQueryIndex(String name) {
            this.name = name;
            plans = Lists.newArrayListWithCapacity(5);
        }

        @Override
        public double getMinimumCost() {
            return 1000;//some high number
        }

        @Override
        public double getCost(Filter filter, NodeState rootState) {
            return getCost();
        }
        private double getCost() {
            return 500;//arbitrary cost - useless as we are AdvanceQueryIndex
        }

        @Override
        public Cursor query(Filter filter, NodeState rootState) {
            return query();
        }

        @Override
        public Cursor query(IndexPlan plan, NodeState rootState) {
            return query();
        }
        private Cursor query() {
            return new TestEmptyCursor();
        }

        @Override
        public String getPlan(Filter filter, NodeState rootState) {
            return "Unimportant plan";
        }

        @Override
        public String getIndexName() {
            return "test index";
        }

        @Override
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            return ImmutableList.copyOf(plans);
        }

        @Override
        public String getPlanDescription(IndexPlan plan, NodeState root) {
            return "unimportant plan description (" + plan.getPlanName() + ")";
        }

        void addPlan(String name, long cost, boolean supportsPathRestriction) {
            plans.add(new IndexPlan.Builder()
                    .setCostPerEntry(1)
                    .setCostPerExecution(1)
                    .setEstimatedEntryCount(cost)
                    .setSupportsPathRestriction(supportsPathRestriction)
                    .setPlanName(name)
                    .build());
        }

        void resetPlans() {
            plans.clear();
        }
    }

    class TestEmptyCursor implements Cursor {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public IndexRow next() {
            return null;
        }

        @Override
        public long getSize(Result.SizePrecision precision, long max) {
            return 0;
        }
    }

    private String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, SQL2).get(0);
    }

    private void assertPlanSelection(String query, String expectedPlan, String message) {
        String explain = explain(query);
        Assert.assertTrue(message, explain.contains(expectedPlan));
    }
}
