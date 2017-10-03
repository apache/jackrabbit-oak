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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class LuceneIndexEditor2Test {

    private NodeState root = INITIAL_CONTENT;
    private NodeState before = root;
    private IndexUpdateCallback updateCallback = mock(IndexUpdateCallback.class);
    private ExtractedTextCache extractedTextCache = new ExtractedTextCache(0, 0);
    private TestIndexingContext indexingContext = new TestIndexingContext();
    private TestWriterFactory writerFactory = new TestWriterFactory();
    private TestPropertyUpdateCallback propCallback = new TestPropertyUpdateCallback();
    private TestWriter writer = new TestWriter();
    private String indexPath = "/oak:index/fooIndex";

    @Test
    public void basics() throws Exception{
        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
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
        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
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
        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
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

    private void updateBefore(IndexDefinitionBuilder defnb) {
        NodeBuilder builder = before.builder();
        NodeBuilder cb = TestUtil.child(builder, PathUtils.getParentPath(indexPath));
        cb.setChildNode(PathUtils.getName(indexPath), defnb.build());
        before = builder.getNodeState();
    }

    private EditorHook createHook(LuceneIndexEditorContext context) {
        IndexEditorProvider provider = new IndexEditorProvider() {
            @CheckForNull
            @Override
            public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                                         @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
                    throws CommitFailedException {
                if ("lucene".equals(type)) {
                    return new LuceneIndexEditor(context);
                }
                return null;
            }
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
        public void done() throws CommitFailedException {
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


    private class TestWriterFactory implements LuceneIndexWriterFactory {
        @Override
        public LuceneIndexWriter newInstance(IndexDefinition definition,
                                             NodeBuilder definitionBuilder, boolean reindex) {
            return writer;
        }
    }

    private static class TestWriter implements LuceneIndexWriter {
        Set<String> deletedPaths = new HashSet<>();
        Map<String, Iterable<? extends IndexableField>> docs = new HashMap<>();
        boolean closed;

        @Override
        public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
            docs.put(path, doc);
        }

        @Override
        public void deleteDocuments(String path) throws IOException {
            deletedPaths.add(path);
        }

        @Override
        public boolean close(long timestamp) throws IOException {
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
