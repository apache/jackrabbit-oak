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
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FT_INDEX_NOT_READY_RETRY_OAK_12173_DISABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LucenePropertyIndexPlansNotReadyTest {

    @BeforeClass
    public static void warmUpLucene() throws Exception {
        // Creating the very first Lucene IndexWriter in a JVM costs several
        // hundred ms of classloading/JIT (observed ~350-450ms on a cold JVM).
        // The timing-sensitive test below has only a 150ms retry budget
        // (3 attempts x 50ms, deliberately bounded in FulltextIndex to cap
        // added query latency - not adjustable here), so pay that one-time
        // cost up front, against throwaway scratch state, before any timed
        // assertion runs.
        NodeBuilder warmupBuilder = INITIAL_CONTENT.builder();
        NodeBuilder warmupIndex = warmupBuilder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(warmupIndex, "warmup", Set.of("foo"), "async");
        EditorHook warmupHook = new EditorHook(
                new IndexUpdateProvider(new LuceneIndexEditorProvider(), "async", false));
        NodeState before = warmupBuilder.getNodeState();
        warmupBuilder.setProperty("foo", "bar");
        NodeState after = warmupBuilder.getNodeState();
        warmupHook.processCommit(before, after, CommitInfo.EMPTY);
    }

    private final NodeBuilder builder = INITIAL_CONTENT.builder();

    private final IndexTracker tracker = new IndexTracker();

    private final EditorHook hook = new EditorHook(
            new IndexUpdateProvider(new LuceneIndexEditorProvider(), "async", false));

    // Unique per test instance (JUnit creates a fresh instance per @Test method) so
    // that the "index not yet ready" WARN rate-limiting in FulltextIndex - keyed by
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
        FT_INDEX_NOT_READY_RETRY_OAK_12173_DISABLE.set(false);
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
    public void planIsFoundIfIndexBecomesReadyDuringRetryWindow() throws Exception {
        LucenePropertyIndex index = new LucenePropertyIndex(tracker, null);

        Thread builder2 = new Thread(() -> {
            try {
                // Simulates the async indexer finishing the first build shortly
                // after the query started looking for a usable index - well
                // within the retry window (3 attempts x 50ms = 150ms).
                Thread.sleep(20);
                NodeBuilder b = this.builder;
                NodeState before = b.getNodeState();
                b.setProperty("foo", "bar");
                NodeState after = b.getNodeState();
                NodeState indexedState = hook.processCommit(before, after, CommitInfo.EMPTY);
                tracker.update(indexedState);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        builder2.start();

        java.util.List<IndexPlan> plans = index.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());
        builder2.join();

        assertEquals("Query should pick up the index once it becomes ready mid-retry, "
                + "instead of unconditionally falling back to traversal", 1, plans.size());
    }

    @Test
    public void noPlanAndNoRetryWhenToggleDisabled() {
        FT_INDEX_NOT_READY_RETRY_OAK_12173_DISABLE.set(true);
        LucenePropertyIndex index = new LucenePropertyIndex(tracker, null);

        long start = System.currentTimeMillis();
        java.util.List<IndexPlan> plans = index.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());
        long elapsed = System.currentTimeMillis() - start;

        assertTrue("Plans should be empty - index never became ready", plans.isEmpty());
        assertTrue("With the toggle disabled, getPlans() must not block retrying", elapsed < 50);
    }

    @Test
    public void planIsEmptyAndBoundedWhenIndexNeverBecomesReady() {
        // FT_INDEX_NOT_READY_RETRY_OAK_12173_DISABLE is intentionally left at
        // its default (false = retry enabled), matching the production
        // default - this is the worst-case-latency guarantee the retry
        // exists to bound: the index never becomes ready, so getPlans() must
        // exhaust the full retry budget (3 attempts x 50ms = 150ms) and
        // still return an empty list, rather than hanging or looping
        // unboundedly.
        LucenePropertyIndex index = new LucenePropertyIndex(tracker, null);

        LogCustomizer customLogger = LogCustomizer
                .forLogger(FulltextIndex.class.getName())
                .enable(Level.WARN)
                .create();
        customLogger.starting();
        try {
            long start = System.currentTimeMillis();
            java.util.List<IndexPlan> plans = index.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());
            long elapsed = System.currentTimeMillis() - start;

            assertTrue("Plans should be empty - index never became ready", plans.isEmpty());
            // Tolerant window rather than an exact bound, to stay robust on
            // slow/shared CI hardware: comfortably above zero proves the
            // retries actually happened (not an instant give-up), and
            // comfortably under a generous ceiling proves getPlans() didn't
            // hang or loop unboundedly.
            assertTrue("getPlans() should have spent time retrying before giving up (elapsed=" + elapsed + "ms)",
                    elapsed >= 100);
            assertTrue("getPlans() should not block far beyond the bounded retry budget (elapsed=" + elapsed + "ms)",
                    elapsed < 2000);

            List<String> logs = customLogger.getLogs();
            assertTrue("Expected a WARN log noting the index is not yet ready: " + logs,
                    logs.stream().anyMatch(line -> line.contains("not yet ready")));
        } finally {
            customLogger.finished();
        }
    }

    @Test
    public void retryBudgetIsSharedAcrossMultipleNotReadyIndexes() {
        // A second, independent not-ready index competing for the same query -
        // both match the filter (both index "foo") and neither has a ":data"
        // child yet. Before the fix, each not-ready candidate paid its own
        // full retry budget, so two of them meant ~300ms instead of ~150ms.
        NodeBuilder secondIndex = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(secondIndex, "lucene-" + UUID.randomUUID(), Set.of("foo"), "async");
        tracker.update(builder.getNodeState());

        LucenePropertyIndex index = new LucenePropertyIndex(tracker, null);

        long start = System.currentTimeMillis();
        java.util.List<IndexPlan> plans = index.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());
        long elapsed = System.currentTimeMillis() - start;

        assertTrue("Plans should be empty - neither index ever became ready", plans.isEmpty());
        // Same tolerant window as planIsEmptyAndBoundedWhenIndexNeverBecomesReady:
        // proving the *total* time for both not-ready indexes together stays
        // within one retry budget (~150ms), not one budget per index (~300ms+).
        assertTrue("getPlans() should have spent time retrying before giving up (elapsed=" + elapsed + "ms)",
                elapsed >= 100);
        assertTrue("Retry budget must be shared across all not-ready indexes in one getPlans() call, "
                        + "not paid once per index (elapsed=" + elapsed + "ms)",
                elapsed < 250);
    }
}
