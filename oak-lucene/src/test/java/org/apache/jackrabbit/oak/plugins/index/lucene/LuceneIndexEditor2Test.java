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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditor;
import org.junit.After;
import org.junit.Before;

public class LuceneIndexEditor2Test {

    @Before
    public void resetToggles() {
        FulltextIndexEditor.FT_OAK_12244_DISABLE.set(false);
        FulltextIndexEditor.FT_OAK_12365_DISABLE.set(false);
    }

    @After
    public void restoreToggles() {
        FulltextIndexEditor.FT_OAK_12244_DISABLE.set(false);
        FulltextIndexEditor.FT_OAK_12365_DISABLE.set(false);
    }

    private final NodeState root = INITIAL_CONTENT;
    private NodeState before = root;
    private final IndexUpdateCallback updateCallback = mock(IndexUpdateCallback.class);
    private final ExtractedTextCache extractedTextCache = new ExtractedTextCache(0, 0);
    private final TestIndexingContext indexingContext = new TestIndexingContext();
    private final TestWriterFactory writerFactory = new TestWriterFactory();
    private final TestPropertyUpdateCallback propCallback = new TestPropertyUpdateCallback();
    private final TestWriter writer = new TestWriter();
    private final String indexPath = "/oak:index/fooIndex";

    @Test
    public void basics() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);
        NodeBuilder builder = before.builder();
        builder.child("a").setProperty("foo", "bar");

        hook.processCommit(root, builder.getNodeState(), CommitInfo.EMPTY);

        assertThat(writer.docs.keySet(), containsInAnyOrder("/a"));
    }

    @Test
    public void simplePropertyUpdateCallback() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("foo").propertyIndex();

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        ctx.setPropertyUpdateCallback(propCallback);

        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        //Property added
        NodeBuilder builder = before.builder();
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").setProperty("foo2", "bar");
        builder.child("a").child("b");

        before = hook.processCommit(root, builder.getNodeState(), CommitInfo.EMPTY);
        propCallback.state.assertState("/a", "foo", UpdateState.ADDED);
        assertEquals(1, propCallback.invocationCount);
        assertEquals(1, propCallback.doneInvocationCount);
        propCallback.reset();

        //Property updated
        builder = before.builder();
        builder.child("a").setProperty("foo", "bar2");
        builder.child("a").setProperty("foo2", "bar2");
        before = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);

        propCallback.state.assertState("/a", "foo", UpdateState.UPDATED);

        assertEquals(1, propCallback.invocationCount);
        propCallback.reset();

        //Property deleted
        builder = before.builder();
        builder.child("a").removeProperty("foo");
        builder.child("a").removeProperty("foo2");
        before = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);

        propCallback.state.assertState("/a", "foo", UpdateState.DELETED);
        assertEquals(1, propCallback.invocationCount);
        propCallback.reset();
    }

    @Test
    public void relativeProperties() throws Exception{
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("jcr:content/metadata/foo").propertyIndex();
        defnb.aggregateRule("nt:base").include("*");

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        ctx.setPropertyUpdateCallback(propCallback);

        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        //Property added
        NodeBuilder builder = before.builder();
        builder.child("a").child("jcr:content").child("metadata").setProperty("foo", "bar");
        builder.child("a").setProperty("foo2", "bar");

        before = hook.processCommit(root, builder.getNodeState(), CommitInfo.EMPTY);
        propCallback.state.assertState("/a", "jcr:content/metadata/foo", UpdateState.ADDED);
        assertEquals(1, propCallback.invocationCount);
        propCallback.reset();

        //Property updated
        builder = before.builder();
        builder.child("a").child("jcr:content").child("metadata").setProperty("foo", "bar2");
        builder.child("a").setProperty("foo2", "bar2");
        before = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);

        propCallback.state.assertState("/a", "jcr:content/metadata/foo", UpdateState.UPDATED);

        assertEquals(1, propCallback.invocationCount);
        propCallback.reset();

        //Property deleted
        builder = before.builder();
        builder.child("a").child("jcr:content").child("metadata").removeProperty("foo");
        builder.child("a").removeProperty("foo2");
        before = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);

        propCallback.state.assertState("/a", "jcr:content/metadata/foo", UpdateState.DELETED);
        assertEquals(1, propCallback.invocationCount);
        propCallback.reset();
    }

    @Test
    public void nodeGainsMixinTriggersIndexUpdate() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("mix:title").property("jcr:title").propertyIndex();

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        // Commit 1: node exists without the mixin — must not be indexed
        NodeBuilder builder = before.builder();
        builder.child("a").setProperty("jcr:title", "hello");
        before = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertFalse("Node without mixin should not be indexed", writer.docs.containsKey("/a"));

        // Commit 2: mixin added to existing node — must be indexed
        builder = before.builder();
        builder.child("a").setProperty(JcrConstants.JCR_MIXINTYPES, List.of("mix:title"), Type.NAMES);
        hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Node after gaining mixin should be added to index", writer.docs.containsKey("/a"));
    }

    @Test
    public void nodeLosesMixinTriggersDocumentDeletion() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("mix:title").property("jcr:title").propertyIndex();

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        // Commit 1: node with mixin — must be indexed
        NodeBuilder builder = before.builder();
        builder.child("a")
                .setProperty(JcrConstants.JCR_MIXINTYPES, List.of("mix:title"), Type.NAMES)
                .setProperty("jcr:title", "hello");
        before = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Node with mixin should be indexed", writer.docs.containsKey("/a"));

        // Commit 2: mixin removed — existing index document must be deleted
        builder = before.builder();
        builder.child("a").removeProperty(JcrConstants.JCR_MIXINTYPES);
        hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Removing mixin should trigger deleteDocument for the node", writer.deletedPaths.contains("/a"));
    }

    @Test
    public void nodeGainsMixinDoesNotTriggerIndexUpdateWhenToggleDisabled() throws Exception {
        FulltextIndexEditor.FT_OAK_12244_DISABLE.set(true);

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("mix:title").property("jcr:title").propertyIndex();

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        // Commit 1: node exists without the mixin
        NodeBuilder builder = before.builder();
        builder.child("a").setProperty("jcr:title", "hello");
        before = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertFalse("Node without mixin should not be indexed", writer.docs.containsKey("/a"));

        // Commit 2: mixin added — with toggle disabled, node must not be indexed
        builder = before.builder();
        builder.child("a").setProperty(JcrConstants.JCR_MIXINTYPES, List.of("mix:title"), Type.NAMES);
        hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertFalse("Mixin tracking disabled: node gaining mixin should not be indexed", writer.docs.containsKey("/a"));
    }

    @Test
    public void nodeLosesMixinDoesNotTriggerDocumentDeletionWhenToggleDisabled() throws Exception {
        FulltextIndexEditor.FT_OAK_12244_DISABLE.set(true);

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("mix:title").property("jcr:title").propertyIndex();

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        // Commit 1: node with mixin — indexed because it's a new node
        NodeBuilder builder = before.builder();
        builder.child("a")
                .setProperty(JcrConstants.JCR_MIXINTYPES, List.of("mix:title"), Type.NAMES)
                .setProperty("jcr:title", "hello");
        before = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Node with mixin should be indexed", writer.docs.containsKey("/a"));

        // Commit 2: mixin removed — with toggle disabled, stale document must not be deleted
        builder = before.builder();
        builder.child("a").removeProperty(JcrConstants.JCR_MIXINTYPES);
        hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertFalse("Mixin tracking disabled: removing mixin should not trigger deleteDocument", writer.deletedPaths.contains("/a"));
    }

    @Test
    public void nodeLosesLastAggregatedPropertyTriggersDocumentDeletion() throws Exception {
        // OAK-12365: root's rule/type is unchanged; only the aggregated child content disappears.
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("jcr:content/status").propertyIndex();
        defnb.aggregateRule("nt:base").include("*");

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        // Commit 1: aggregated child property present — root must be indexed
        NodeBuilder builder = before.builder();
        builder.child("a").child("jcr:content").setProperty("status", "published");
        before = hook.processCommit(root, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Root with aggregated property should be indexed", writer.docs.containsKey("/a"));

        // Commit 2: last aggregated property removed, root's own rule/type unchanged —
        // stale root document must be deleted
        builder = before.builder();
        builder.child("a").child("jcr:content").removeProperty("status");
        hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Removing the last aggregated property should trigger deleteDocument for the root",
                writer.deletedPaths.contains("/a"));
    }

    @Test
    public void nodeLosesLastAggregatedPropertyTriggersDocumentDeletionWhenMixinToggleDisabled() throws Exception {
        // Fix lives in addOrUpdate(), so it also covers the legacy (FT_OAK_12244 disabled) path.
        FulltextIndexEditor.FT_OAK_12244_DISABLE.set(true);

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("jcr:content/status").propertyIndex();
        defnb.aggregateRule("nt:base").include("*");

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        // Commit 1: aggregated child property present — root must be indexed
        NodeBuilder builder = before.builder();
        builder.child("a").child("jcr:content").setProperty("status", "published");
        before = hook.processCommit(root, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Root with aggregated property should be indexed", writer.docs.containsKey("/a"));

        // Commit 2: last aggregated property removed — stale root document must still be
        // deleted even with mixin-transition tracking disabled
        builder = before.builder();
        builder.child("a").child("jcr:content").removeProperty("status");
        hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Removing the last aggregated property should trigger deleteDocument regardless of FT_OAK_12244",
                writer.deletedPaths.contains("/a"));
    }

    @Test
    public void nodeLosesLastAggregatedPropertyDoesNotTriggerDeletionWhenOak12365ToggleDisabled() throws Exception {
        FulltextIndexEditor.FT_OAK_12365_DISABLE.set(true);

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("jcr:content/status").propertyIndex();
        defnb.aggregateRule("nt:base").include("*");

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        NodeBuilder builder = before.builder();
        builder.child("a").child("jcr:content").setProperty("status", "published");
        before = hook.processCommit(root, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Root with aggregated property should be indexed", writer.docs.containsKey("/a"));

        builder = before.builder();
        builder.child("a").child("jcr:content").removeProperty("status");
        hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertFalse("OAK-12365 disabled: removing the last aggregated property should not trigger deleteDocument",
                writer.deletedPaths.contains("/a"));
    }

    @Test
    public void nodeKeepsOneOfMultipleAggregatedPropertiesDoesNotTriggerDeletion() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.indexRule("nt:base").property("jcr:content/status").propertyIndex();
        defnb.indexRule("nt:base").property("jcr:content/type").propertyIndex();
        defnb.aggregateRule("nt:base").include("*");

        NodeState defnState = defnb.build();
        IndexDefinition defn = new IndexDefinition(root, defnState, indexPath);
        LuceneIndexEditorContext ctx = newContext(defnState.builder(), defn, true);
        EditorHook hook = createHook(ctx);

        updateBefore(defnb);

        NodeBuilder builder = before.builder();
        builder.child("a").child("jcr:content").setProperty("status", "published");
        builder.child("a").child("jcr:content").setProperty("type", "page");
        before = hook.processCommit(root, builder.getNodeState(), CommitInfo.EMPTY);
        assertTrue("Root with aggregated properties should be indexed", writer.docs.containsKey("/a"));

        builder = before.builder();
        builder.child("a").child("jcr:content").removeProperty("status");
        hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
        assertFalse("Removing one of several aggregated properties should not delete the root document",
                writer.deletedPaths.contains("/a"));
    }

    private void updateBefore(LuceneIndexDefinitionBuilder defnb) {
        NodeBuilder builder = before.builder();
        NodeBuilder cb = TestUtil.child(builder, PathUtils.getParentPath(indexPath));
        cb.setChildNode(PathUtils.getName(indexPath), defnb.build());
        before = builder.getNodeState();
    }

    private EditorHook createHook(LuceneIndexEditorContext context) {
        IndexEditorProvider provider = (type, definition, root, callback) -> {
            if (TYPE_LUCENE.equals(type)) {
                return new LuceneIndexEditor(context);
            }
            return null;
        };

        String async = context.isAsyncIndexing() ? "async" : null;
        IndexUpdateProvider updateProvider = new IndexUpdateProvider(provider, async, false);
        return new EditorHook(updateProvider);
    }

    private LuceneIndexEditorContext newContext(NodeBuilder defnBuilder, IndexDefinition defn, boolean asyncIndex) {
        return new LuceneIndexEditorContext(root, defnBuilder, defn, updateCallback, writerFactory,
                extractedTextCache, null, indexingContext, asyncIndex);
    }


    private static class TestPropertyUpdateCallback implements PropertyUpdateCallback {
        int invocationCount;
        CallbackState state;
        int doneInvocationCount;

        @Override
        public void propertyUpdated(String nodePath, String propertyRelativePath, PropertyDefinition pd,
                                    PropertyState before, PropertyState after) {
            assertNotNull(nodePath);
            assertNotNull(propertyRelativePath);
            assertNotNull(pd);

            if (before == null && after == null) {
                fail("Both states cannot be null at same time");
            }

            state = new CallbackState(nodePath, propertyRelativePath, pd, before, after);
            invocationCount++;
        }

        @Override
        public void done() {
            doneInvocationCount++;
        }

        void reset(){
            state = null;
            invocationCount = 0;
            doneInvocationCount = 0;
        }
    }

    enum UpdateState {ADDED, UPDATED, DELETED}

    private static class CallbackState {
        final String nodePath;
        final String propertyPath;
        final PropertyDefinition pd;
        final PropertyState before;
        final PropertyState after;


        public CallbackState(String nodePath, String propertyPath, PropertyDefinition pd,
                             PropertyState before, PropertyState after) {
            this.nodePath = nodePath;
            this.propertyPath = propertyPath;
            this.pd = pd;
            this.before = before;
            this.after = after;
        }

        public void assertState(String expectedPath, String expectedName, UpdateState us) {
            assertEquals(expectedPath, nodePath);
            assertEquals(expectedName, propertyPath);

            switch (us) {
                case ADDED: assertNotNull(after); assertNull(before); break;
                case UPDATED: assertNotNull(after); assertNotNull(before); break;
                case DELETED: assertNull(after); assertNotNull(before); break;
            }
        }
    }


    private class TestWriterFactory implements FulltextIndexWriterFactory<Iterable<? extends IndexableField>> {
        @Override
        public LuceneIndexWriter newInstance(IndexDefinition definition, NodeBuilder definitionBuilder,
                                             CommitInfo commitInfo, boolean reindex) {
            return writer;
        }
    }

    private static class TestWriter implements LuceneIndexWriter {
        Set<String> deletedPaths = new HashSet<>();
        Map<String, Iterable<? extends IndexableField>> docs = new HashMap<>();
        boolean closed;

        @Override
        public void updateDocument(String path, Iterable<? extends IndexableField> doc) {
            docs.put(path, doc);
        }

        @Override
        public void deleteDocumentTree(String path) {
            deletedPaths.add(path);
        }

        @Override
        public void deleteDocument(String path) {
            deletedPaths.add(path);
        }

        @Override
        public boolean close(long timestamp) {
            closed = true;
            return true;
        }
    }

    private class TestIndexingContext implements IndexingContext {
        CommitInfo info = CommitInfo.EMPTY;
        boolean reindexing;
        boolean async;

        @Override
        public String getIndexPath() {
            return indexPath;
        }

        @Override
        public CommitInfo getCommitInfo() {
            return info;
        }

        @Override
        public boolean isReindexing() {
            return reindexing;
        }

        @Override
        public boolean isAsync() {
            return async;
        }

        @Override
        public void indexUpdateFailed(Exception e) {

        }

        @Override
        public void registerIndexCommitCallback(IndexCommitCallback callback) {

        }
    }
}
