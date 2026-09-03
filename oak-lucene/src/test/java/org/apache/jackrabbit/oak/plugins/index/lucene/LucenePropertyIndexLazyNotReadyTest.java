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
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
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
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FT_INDEX_NOT_READY_RETRY_OAK_XXXXX_DISABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@link LucenePropertyIndex#acquireIndexNode(String)} with
 * {@code oak.lucene.nonLazyIndex=false} (the lazy-index code path).
 *
 * <p>This system property is read once into a {@code static final} field when
 * {@link LucenePropertyIndex} is first loaded, so it must be set before that
 * class - or anything that references it - is touched anywhere in this JVM.
 * This is why this is its own top-level test class, run in isolation from
 * {@link LucenePropertyIndexPlansNotReadyTest} (which relies on the
 * NON_LAZY=true default), rather than a method added to it.
 */
public class LucenePropertyIndexLazyNotReadyTest {

    @BeforeClass
    public static void enableLazyIndexMode() throws Exception {
        System.setProperty("oak.lucene.nonLazyIndex", "false");

        // Same one-time Lucene classloading/JIT warm-up as
        // LucenePropertyIndexPlansNotReadyTest#warmUpLucene - see that method
        // for why it's needed before any timed assertion runs.
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

    private final String indexName = "lucene-" + UUID.randomUUID();

    @After
    public void tearDown() {
        FT_INDEX_NOT_READY_RETRY_OAK_XXXXX_DISABLE.set(false);
        System.clearProperty("oak.lucene.nonLazyIndex");
    }

    private Filter rootFilter() {
        FilterImpl f = FilterImpl.newTestInstance();
        f.restrictPath("/", Filter.PathRestriction.EXACT);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        return f;
    }

    @Test
    public void lazyModeStillDetectsNotReadyIndex() {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, indexName, Set.of("foo"), "async");
        // Definition committed but never (re)indexed - no ":data" child yet.
        tracker.update(builder.getNodeState());

        LucenePropertyIndex lucenePropertyIndex = new LucenePropertyIndex(tracker, null);

        long start = System.currentTimeMillis();
        java.util.List<IndexPlan> plans =
                lucenePropertyIndex.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());
        long elapsed = System.currentTimeMillis() - start;

        assertTrue("Plans should be empty - index never became ready", plans.isEmpty());
        // Proves acquireIndexNode(String) returned null and routed through
        // FulltextIndex#getPlans()'s not-ready retry/warn handling instead of
        // silently succeeding via a LazyLuceneIndexNode wrapper (which would
        // return almost instantly here, well under 100ms).
        assertTrue("getPlans() should have spent time in the not-ready retry path (elapsed=" + elapsed + "ms)",
                elapsed >= 100);
    }

    @Test
    public void lazyModePlanIsFoundOnceIndexBecomesReady() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, indexName, Set.of("foo"), "async");
        tracker.update(builder.getNodeState());

        LucenePropertyIndex lucenePropertyIndex = new LucenePropertyIndex(tracker, null);
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new LuceneIndexEditorProvider(), "async", false));

        Thread indexingThread = new Thread(() -> {
            try {
                Thread.sleep(20);
                NodeState before = builder.getNodeState();
                builder.setProperty("foo", "bar");
                NodeState after = builder.getNodeState();
                NodeState indexedState = hook.processCommit(before, after, CommitInfo.EMPTY);
                tracker.update(indexedState);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        indexingThread.start();

        java.util.List<IndexPlan> plans =
                lucenePropertyIndex.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());
        indexingThread.join();

        assertEquals("Query should pick up the index once it becomes ready mid-retry, "
                + "even in lazy-index mode", 1, plans.size());
    }
}
