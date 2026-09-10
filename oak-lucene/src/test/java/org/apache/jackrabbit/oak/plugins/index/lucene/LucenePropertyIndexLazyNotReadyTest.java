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

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
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
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link LucenePropertyIndex#acquireIndexNode(String)} in "lazy" mode
 * (system property {@code oak.lucene.nonLazyIndex=false}).
 *
 * <p>This property is only read once, the first time {@link LucenePropertyIndex}
 * is loaded. So it must be set before that happens, which is why this lives
 * in its own test class instead of a method added to the other
 * {@code LucenePropertyIndex} tests (which use normal, non-lazy mode).
 */
public class LucenePropertyIndexLazyNotReadyTest {

    static {
        System.setProperty("oak.lucene.nonLazyIndex", "false");
    }

    private final NodeBuilder builder = INITIAL_CONTENT.builder();

    private final IndexTracker tracker = new IndexTracker();

    private final String indexName = "lucene-" + UUID.randomUUID();

    @After
    public void tearDown() {
        System.clearProperty("oak.lucene.nonLazyIndex");
    }

    private Filter rootFilter() {
        FilterImpl f = FilterImpl.newTestInstance();
        f.restrictPath("/", Filter.PathRestriction.EXACT);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        return f;
    }

    @Test
    public void lazyModeReturnsNoPlanForIndexWithoutBuiltData() {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, indexName, Set.of("foo"), "async");
        // Definition committed but never (re)indexed - no ":data" child yet.
        tracker.update(builder.getNodeState());

        LucenePropertyIndex lucenePropertyIndex = new LucenePropertyIndex(tracker, null);

        List<IndexPlan> plans =
                lucenePropertyIndex.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());

        // acquireIndexNode(String) really tried to open the index and got
        // null, instead of returning a lazy placeholder that looks fine now
        // but would fail later when actually read.
        assertTrue("Plans should be empty - index has never completed its first build", plans.isEmpty());
    }

    @Test
    public void lazyModePlanIsFoundImmediatelyWhenBuiltButNeverOpened() throws Exception {
        // The index is fully built (":data" exists), but this tracker has
        // never opened it before. Even so, it should be usable right away -
        // no waiting needed, even in lazy mode.
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, indexName, Set.of("foo"), "async");

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new LuceneIndexEditorProvider(), "async", false));
        NodeState indexedState = hook.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexedState);

        LucenePropertyIndex lucenePropertyIndex = new LucenePropertyIndex(tracker, null);

        List<IndexPlan> plans =
                lucenePropertyIndex.getPlans(rootFilter(), Collections.emptyList(), builder.getNodeState());

        assertEquals("Query should pick up an already-built index on its first access, "
                + "even in lazy-index mode", 1, plans.size());
    }
}
