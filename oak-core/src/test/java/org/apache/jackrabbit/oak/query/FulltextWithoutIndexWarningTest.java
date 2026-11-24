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
package org.apache.jackrabbit.oak.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.cursor.Cursors;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvanceFulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

/**
 * OAK-12270: the warning about a fulltext query without index must only be
 * logged for the query that is executed, not for a discarded alternative.
 */
public class FulltextWithoutIndexWarningTest extends AbstractQueryTest {

    private static final String WARNING = "Fulltext query without index for filter";

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with((QueryIndexProvider) state -> List.of(new PathRestrictedFulltextIndex()))
                .createContentRepository();
    }

    @Before
    public void createContent() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").addChild("doc").setProperty("text", "hello");
        test.addChild("b").addChild("doc").setProperty("text", "hello");
        root.commit();
    }

    @Test
    public void noWarningForDiscardedOriginalOfUnionQuery() {
        // the original query has no path restriction, so the index can't be used;
        // the union alternative uses the index for each path and is executed
        String query = "select [jcr:path] from [nt:base] where contains(*, 'hello') "
                + "and (isdescendantnode('/test/a') or isdescendantnode('/test/b'))";
        List<String> warnings = warningsFor(() ->
                assertEquals(List.of("/test/a/doc", "/test/b/doc"), executeQuery(query, SQL2)));
        assertEquals(List.of(), warnings);
    }

    @Test
    public void warningForExecutedQueryWithoutIndex() {
        String query = "select [jcr:path] from [nt:base] where contains(*, 'hello')";
        List<String> warnings = warningsFor(() -> assertEquals(List.of(), executeQuery(query, SQL2)));
        assertEquals(warnings.toString(), 1, warnings.size());
        assertTrue(warnings.get(0), warnings.get(0).contains("Filter(query=" + query));
    }

    @Test
    public void warningForUnionBranchWithoutIndex() {
        String query = "select [jcr:path] from [nt:base] where contains(*, 'hello') "
                + "and (isdescendantnode('/test/a') or [text] = 'hello')";
        List<String> warnings = warningsFor(() -> executeQuery(query, SQL2));
        assertEquals(warnings.toString(), 1, warnings.size());
    }

    @Test
    public void singleWarningForJoin() {
        String query = "select [a].[jcr:path] from [nt:base] as [a] "
                + "inner join [nt:base] as [b] on ischildnode([a], [b]) "
                + "where contains([a].*, 'hello') and [b].[jcr:path] = '/test/a'";
        List<String> warnings = warningsFor(() -> executeQuery(query, SQL2));
        assertEquals(warnings.toString(), 1, warnings.size());
    }

    private static List<String> warningsFor(Runnable action) {
        LogCustomizer logs = LogCustomizer.forLogger(QueryImpl.class).enable(Level.WARN).create();
        logs.starting();
        try {
            action.run();
            return logs.getLogs().stream().filter(m -> m.startsWith(WARNING)).collect(Collectors.toList());
        } finally {
            logs.finished();
        }
    }

    /**
     * A fulltext index that can only be used for queries restricted to a
     * subtree (similar to an index with "queryPaths"). It returns the child
     * "doc" of the restricted path.
     */
    private static class PathRestrictedFulltextIndex implements AdvanceFulltextQueryIndex {

        @Override
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            if (filter.getFullTextConstraint() == null
                    || filter.getPathRestriction() != Filter.PathRestriction.ALL_CHILDREN) {
                return List.of();
            }
            return List.of(new IndexPlan.Builder()
                    .setCostPerExecution(1)
                    .setCostPerEntry(1)
                    .setEstimatedEntryCount(1)
                    .setFulltextIndex(true)
                    .setFilter(filter)
                    .setPlanName(getIndexName())
                    .build());
        }

        @Override
        public Cursor query(IndexPlan plan, NodeState rootState) {
            Filter filter = plan.getFilter();
            return Cursors.newPathCursor(List.of(filter.getPath() + "/doc"), filter.getQueryLimits());
        }

        @Override
        public String getPlanDescription(IndexPlan plan, NodeState root) {
            return getIndexName();
        }

        @Override
        public String getIndexName() {
            return "path-restricted-fulltext";
        }

        @Override
        public double getMinimumCost() {
            return 1;
        }

        @Override
        public double getCost(Filter filter, NodeState rootState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor query(Filter filter, NodeState rootState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPlan(Filter filter, NodeState rootState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public QueryIndex.NodeAggregator getNodeAggregator() {
            return null;
        }
    }
}
