/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.slf4j.event.Level;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FT_INDEX_STILL_BUILDING_WARN_OAK_12173_DISABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@code FulltextIndex#getPlans()}'s handling of an index that matched
 * the query but has never completed its first (re)indexing cycle ("still
 * building", per {@link IndexTracker#isIndexBuilding(String)}).
 *
 * <p>This is deliberately <em>not</em> a wait-and-retry mechanism: the async
 * indexer's first-build cycle typically takes seconds to minutes in
 * production (see GRANITE-63330), so there is no bounded sleep on the query
 * thread that could plausibly catch it - a query thread should never block on
 * it. All {@code getPlans()} does for a still-building index is skip it
 * immediately (exactly as if no such index existed) and log a rate-limited
 * WARN so the condition is observable instead of silent.
 */
public class LucenePropertyIndexPlansNotReadyTest {

    private final NodeBuilder builder = INITIAL_CONTENT.builder();

    private final IndexTracker tracker = new IndexTracker();

    private final EditorHook hook = new EditorHook(
            new IndexUpdateProvider(new LuceneIndexEditorProvider(), "async", false));

    // Unique per test instance (JUnit creates a fresh instance per @Test method) so
    // that the "index still building" WARN rate-limiting in FulltextIndex - keyed by
    // index path and shared statically across the whole JVM - can't cause one test
    // method's WARN to suppress another's within the same test run.
    private final String indexName = "lucene-" + UUID.randomUUID();

    @Before
    public void setUp() {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, indexName, Set.of("foo"), "async");
        // Definition committed but never (re)indexed - no ":data" child yet,
        // exactly like an index that is still running its first build.
        tracker.update(builder.getNodeState());
    }

    @After
    public void tearDown() {
        FT_INDEX_STILL_BUILDING_WARN_OAK_12173_DISABLE.set(false);
    }

    private Filter rootFilter() {
        FilterImpl f = FilterImpl.newTestInstance();
        f.restrictPath("/", Filter.PathRestriction.EXACT);
        // The lucene index definition only indexes "foo" (see setUp()) - the
        // planner returns no plan for a filter that doesn't restrict an
        // indexed property, regardless of index readiness, so this
        // restriction is required for a plan to ever be produced.
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        return f;
    }

    @Test
    public void planIsEmptyImmediatelyWhenIndexStillBuilding() {
        LucenePropertyIndex index = new LucenePropertyIndex(tracker, null);

        LogCustomizer customLogger = LogCustomizer
                .forLogger(FulltextIndex.class.getName())
                .enable(Level.WARN)
                .create();
        customLogger.starting();
        try {
            long start = System.currentTimeMillis();
            List<IndexPlan> plans = index.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());
            long elapsed = System.currentTimeMillis() - start;

            assertTrue("Plans should be empty - index has never completed its first build", plans.isEmpty());
            // No retry/sleep exists anymore - a still-building index must be
            // skipped essentially instantly, not after any deliberate delay.
            // Generous ceiling to stay robust on slow/shared CI hardware.
            assertTrue("getPlans() must not wait for a still-building index (elapsed=" + elapsed + "ms)",
                    elapsed < 2000);

            List<String> logs = customLogger.getLogs();
            assertTrue("Expected a WARN log noting the index is still building: " + logs,
                    logs.stream().anyMatch(line -> line.contains("first (re)indexing")));
        } finally {
            customLogger.finished();
        }
    }

    @Test
    public void noWarnWhenToggleDisabled() {
        FT_INDEX_STILL_BUILDING_WARN_OAK_12173_DISABLE.set(true);
        LucenePropertyIndex index = new LucenePropertyIndex(tracker, null);

        LogCustomizer customLogger = LogCustomizer
                .forLogger(FulltextIndex.class.getName())
                .enable(Level.WARN)
                .create();
        customLogger.starting();
        try {
            List<IndexPlan> plans = index.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());

            assertTrue("Plans should be empty - index has never completed its first build", plans.isEmpty());
            assertTrue("With the toggle disabled, no WARN should be logged",
                    customLogger.getLogs().isEmpty());
        } finally {
            customLogger.finished();
        }
    }

    @Test
    public void multipleStillBuildingIndexesAllResolveImmediately() {
        // A second, independent still-building index competing for the same
        // query - both match the filter (both index "foo") and neither has a
        // ":data" child yet. There is no retry budget to share anymore, but
        // this guards against that cost ever creeping back in per-index.
        NodeBuilder secondIndex = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(secondIndex, "lucene-" + UUID.randomUUID(), Set.of("foo"), "async");
        tracker.update(builder.getNodeState());

        LucenePropertyIndex index = new LucenePropertyIndex(tracker, null);

        long start = System.currentTimeMillis();
        List<IndexPlan> plans = index.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());
        long elapsed = System.currentTimeMillis() - start;

        assertTrue("Plans should be empty - neither index has completed its first build", plans.isEmpty());
        assertTrue("getPlans() must not wait, regardless of how many still-building indexes it hits "
                        + "(elapsed=" + elapsed + "ms)",
                elapsed < 2000);
    }

    @Test
    public void planIsFoundImmediatelyOnceIndexIsBuilt() throws Exception {
        // Once the async indexer actually finishes the first build (":data"
        // appears), the very next getPlans() call must pick up the index -
        // via a plain, immediate acquireIndexNode(), not a retry loop.
        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        NodeState indexedState = hook.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexedState);

        LucenePropertyIndex index = new LucenePropertyIndex(tracker, null);
        List<IndexPlan> plans = index.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());

        assertEquals("Query should pick up the index as soon as it is built", 1, plans.size());
    }
}
