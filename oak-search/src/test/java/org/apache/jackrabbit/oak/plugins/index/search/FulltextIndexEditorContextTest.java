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
package org.apache.jackrabbit.oak.plugins.index.search;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.DocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for OAK-12247 totalIndexedNodes tracking in FulltextIndexEditorContext.
 */
public class FulltextIndexEditorContextTest {

    private NodeState root;
    private NodeBuilder definitionBuilder;
    private TestIndexingContext indexingContext;

    @Before
    public void setUp() {
        MemoryNodeStore store = new MemoryNodeStore();
        root = store.getRoot();
        definitionBuilder = root.builder().child("oak:index").child("testIndex");
        indexingContext = new TestIndexingContext("/oak:index/testIndex");
        FulltextIndexEditorContext.FT_OAK_12247_DISABLE.set(false);
    }

    @After
    public void tearDown() {
        FulltextIndexEditorContext.FT_OAK_12247_DISABLE.set(false);
    }

    @Test
    public void emptyReindex_writesZeroTotalAndTimestamp() throws Exception {
        // Writer returns totalDocCount=0 and indexUpdated=false (empty reindex)
        TestWriterFactory factory = new TestWriterFactory(0L, false);
        TestEditorContext ctx = new TestEditorContext(root, definitionBuilder, factory, indexingContext);
        ctx.setReindex(true);
        ctx.closeWriter();

        NodeState status = definitionBuilder.getNodeState().getChildNode(IndexDefinition.STATUS_NODE);
        PropertyState totalProp = status.getProperty(IndexDefinition.PROP_TOTAL_INDEXED_NODES);
        assertNotNull("totalIndexedNodes must be written even for empty reindex", totalProp);
        assertEquals(0L, totalProp.getValue(Type.LONG).longValue());

        PropertyState tsProp = status.getProperty(IndexDefinition.REINDEX_COMPLETION_TIMESTAMP);
        assertNotNull("REINDEX_COMPLETION_TIMESTAMP must be written even when indexUpdated=false", tsProp);
    }

    @Test
    public void nonEmptyReindex_writesTotalFromWriter() throws Exception {
        // Writer returns totalDocCount=100 and indexUpdated=true
        TestWriterFactory factory = new TestWriterFactory(100L, true);
        TestEditorContext ctx = new TestEditorContext(root, definitionBuilder, factory, indexingContext);
        ctx.setReindex(true);
        ctx.closeWriter();

        NodeState status = definitionBuilder.getNodeState().getChildNode(IndexDefinition.STATUS_NODE);
        PropertyState totalProp = status.getProperty(IndexDefinition.PROP_TOTAL_INDEXED_NODES);
        assertNotNull(totalProp);
        assertEquals(100L, totalProp.getValue(Type.LONG).longValue());
    }

    @Test
    public void incremental_writesTotalFromWriter() throws Exception {
        // Writer directly returns its computed total (e.g. 103 after net +3 on prev 100)
        TestWriterFactory factory = new TestWriterFactory(103L, true);
        TestEditorContext ctx = new TestEditorContext(root, definitionBuilder, factory, indexingContext);
        ctx.closeWriter();

        NodeState status = definitionBuilder.getNodeState().getChildNode(IndexDefinition.STATUS_NODE);
        PropertyState totalProp = status.getProperty(IndexDefinition.PROP_TOTAL_INDEXED_NODES);
        assertNotNull(totalProp);
        assertEquals(103L, totalProp.getValue(Type.LONG).longValue());
    }

    @Test
    public void writerReturnsMinusOne_totalNotWritten() throws Exception {
        // Writer returns -1 (not tracked) → no property written
        TestWriterFactory factory = new TestWriterFactory(-1L, true);
        TestEditorContext ctx = new TestEditorContext(root, definitionBuilder, factory, indexingContext);
        ctx.closeWriter();

        NodeState status = definitionBuilder.getNodeState().getChildNode(IndexDefinition.STATUS_NODE);
        assertNull("totalIndexedNodes must NOT be written when writer returns -1",
                status.getProperty(IndexDefinition.PROP_TOTAL_INDEXED_NODES));
    }

    @Test
    public void toggleDisabled_totalNotWritten() throws Exception {
        FulltextIndexEditorContext.FT_OAK_12247_DISABLE.set(true);

        TestWriterFactory factory = new TestWriterFactory(50L, true);
        TestEditorContext ctx = new TestEditorContext(root, definitionBuilder, factory, indexingContext);
        ctx.closeWriter();

        NodeState status = definitionBuilder.getNodeState().getChildNode(IndexDefinition.STATUS_NODE);
        assertNull("totalIndexedNodes must NOT be written when toggle is disabled",
                status.getProperty(IndexDefinition.PROP_TOTAL_INDEXED_NODES));
    }

    // --------------- helpers ---------------

    /**
     * Minimal concrete subclass of FulltextIndexEditorContext for testing.
     * It bypasses createIndexDefinition() by passing a pre-built IndexDefinition.
     */
    private static class TestEditorContext extends FulltextIndexEditorContext<Object> {

        TestEditorContext(NodeState root, NodeBuilder definitionBuilder,
                          FulltextIndexWriterFactory<Object> factory,
                          IndexingContext indexingContext) {
            super(root, definitionBuilder,
                    new IndexDefinition(root, definitionBuilder.getNodeState(), "/oak:index/testIndex"),
                    IndexUpdateCallback.NOOP, factory, new ExtractedTextCache(0, 0),
                    indexingContext, true);
        }

        /** Expose the private reindex field via the existing enableReindexMode mechanism. */
        void setReindex(boolean reindex) {
            if (reindex) {
                enableReindexMode();
            }
        }

        @Override
        public IndexDefinition.Builder<IndexDefinition> newDefinitionBuilder() {
            return new IndexDefinition.BaseBuilder();
        }

        @Override
        public DocumentMaker<Object> newDocumentMaker(IndexDefinition.IndexingRule rule, String path) {
            return null;
        }
    }

    /**
     * Writer factory that creates a {@link StubWriter} reporting a pre-configured
     * {@code totalDocCount} from {@link FulltextIndexWriter#getTotalDocCount()}.
     */
    private static class TestWriterFactory implements FulltextIndexWriterFactory<Object> {
        private final long totalDocCount;
        private final boolean indexUpdated;

        TestWriterFactory(long totalDocCount, boolean indexUpdated) {
            this.totalDocCount = totalDocCount;
            this.indexUpdated = indexUpdated;
        }

        @Override
        public FulltextIndexWriter<Object> newInstance(IndexDefinition definition,
                                                        NodeBuilder definitionBuilder,
                                                        CommitInfo commitInfo,
                                                        boolean reindex) {
            return new StubWriter(totalDocCount, indexUpdated);
        }
    }

    private static class StubWriter implements FulltextIndexWriter<Object> {
        private final long totalDocCount;
        private final boolean indexUpdated;

        StubWriter(long totalDocCount, boolean indexUpdated) {
            this.totalDocCount = totalDocCount;
            this.indexUpdated = indexUpdated;
        }

        @Override public void updateDocument(String path, Object doc) {}
        @Override public void deleteDocuments(String path) {}

        @Override
        public boolean close(long timestamp) {
            return indexUpdated;
        }

        @Override
        public long getTotalDocCount() {
            return totalDocCount;
        }
    }

    private static class TestIndexingContext implements IndexingContext {
        private final String indexPath;

        TestIndexingContext(String indexPath) {
            this.indexPath = indexPath;
        }

        @Override public String getIndexPath() { return indexPath; }
        @Override public CommitInfo getCommitInfo() { return CommitInfo.EMPTY; }
        @Override public boolean isReindexing() { return false; }
        @Override public boolean isAsync() { return true; }
        @Override public void indexUpdateFailed(Exception e) {}
        @Override public void registerIndexCommitCallback(IndexCommitCallback callback) {}
    }
}
