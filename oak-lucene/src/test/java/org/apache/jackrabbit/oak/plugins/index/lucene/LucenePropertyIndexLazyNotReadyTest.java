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

import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the lazy-index branch of {@link LucenePropertyIndex#acquireIndexNode(String)}.
 *
 * <p>Real "lazy mode" is switched on JVM-wide by the system property
 * {@code oak.lucene.nonLazyIndex=false}, read once into a {@code static final}
 * field the first time {@link LucenePropertyIndex} loads - not something a
 * single test class can reliably control once other tests share the same JVM
 * fork. So instead of flipping that global switch, these tests call the
 * package-private {@link LucenePropertyIndex#acquireIndexNode(String, boolean)}
 * overload directly with {@code nonLazy=false}, exercising exactly the same
 * branch the real lazy mode would take, deterministically.
 */
public class LucenePropertyIndexLazyNotReadyTest {

    private final NodeBuilder builder = INITIAL_CONTENT.builder();

    private final IndexTracker tracker = new IndexTracker();

    private final String indexName = "lucene-" + UUID.randomUUID();

    private String indexPath() {
        return "/oak:index/" + indexName;
    }

    private NodeState buildIndex() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, indexName, Set.of("foo"), "async");

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new LuceneIndexEditorProvider(), "async", false));
        return hook.processCommit(before, after, CommitInfo.EMPTY);
    }

    @Test
    public void returnsNullWhenIndexHasNoBuiltData() {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, indexName, Set.of("foo"), "async");
        // Definition committed but never (re)indexed - no ":data" child yet.
        tracker.update(builder.getNodeState());

        LucenePropertyIndex lucenePropertyIndex = new LucenePropertyIndex(tracker, null);

        // acquireIndexNode() made a real open attempt and got null, instead
        // of returning a lazy placeholder that looks fine now but would fail
        // later, when actually read.
        assertNull(lucenePropertyIndex.acquireIndexNode(indexPath(), false));
    }

    @Test
    public void opensForRealWhenBuiltButNeverOpenedBefore() throws Exception {
        tracker.update(buildIndex());

        LucenePropertyIndex lucenePropertyIndex = new LucenePropertyIndex(tracker, null);

        // Not yet open in this tracker (isIndexReady() == false), but ':data'
        // already exists - must resolve via a single, immediate real open,
        // not a lazy placeholder.
        LuceneIndexNode indexNode = lucenePropertyIndex.acquireIndexNode(indexPath(), false);
        try {
            assertNotNull(indexNode);
            assertFalse("A not-yet-open index must be opened for real, not wrapped "
                            + "in a lazy placeholder",
                    indexNode instanceof LucenePropertyIndex.LazyLuceneIndexNode);
        } finally {
            indexNode.release();
        }
    }

    @Test
    public void returnsLazyPlaceholderWhenIndexAlreadyOpen() throws Exception {
        tracker.update(buildIndex());

        // Open it once directly through the tracker, so it's now cached
        // (isIndexReady() == true).
        LuceneIndexNode opened = tracker.acquireIndexNode(indexPath());
        assertNotNull(opened);
        opened.release();
        assertTrue(tracker.isIndexReady(indexPath()));

        LucenePropertyIndex lucenePropertyIndex = new LucenePropertyIndex(tracker, null);
        LuceneIndexNode indexNode = lucenePropertyIndex.acquireIndexNode(indexPath(), false);
        try {
            assertTrue("Once the index is already open, acquireIndexNode() should "
                            + "return the cheap lazy placeholder instead of opening "
                            + "it again",
                    indexNode instanceof LucenePropertyIndex.LazyLuceneIndexNode);
        } finally {
            indexNode.release();
        }
    }
}
