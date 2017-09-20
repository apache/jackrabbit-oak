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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.IndexingMode;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.DocumentQueue;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneIndexEditorProviderTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();

    @Test
    public void readOnlyBuilderUsedForSync() throws Exception {
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(null,
                null,
                null,
                null,
                Mounts.defaultMountInfoProvider());
        editorProvider.setIndexingQueue(mock(DocumentQueue.class));

        IndexUpdateCallback callback = new TestCallback("/oak:index/fooIndex", newCommitInfo(), false, false);
        NodeBuilder defnBuilder = createIndexDefinition("fooIndex").builder();
        Editor editor = editorProvider.getIndexEditor(TYPE_LUCENE, defnBuilder, root, callback);
        LuceneIndexEditor luceneEditor = (LuceneIndexEditor) editor;

        NodeBuilder builderFromContext =
                (NodeBuilder) FieldUtils.readField(luceneEditor.getContext(), "definitionBuilder", true);

        try {
            builderFromContext.setProperty("foo", "bar");
            fail("Should have been read only builder");
        } catch (UnsupportedOperationException ignore) {

        }
    }

    @Test
    public void reuseOldIndexDefinition() throws Exception{
        IndexTracker tracker = mock(IndexTracker.class);
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(null,
                tracker,
                null,
                null,
                Mounts.defaultMountInfoProvider());
        editorProvider.setIndexingQueue(mock(DocumentQueue.class));
        //Set up a different IndexDefinition which needs to be returned
        //from tracker with a marker property
        NodeBuilder testBuilder = createIndexDefinition("fooIndex").builder();
        testBuilder.setProperty("foo", "bar");
        IndexDefinition defn = new IndexDefinition(root, testBuilder.getNodeState(), "/foo");
        when(tracker.getIndexDefinition("/oak:index/fooIndex")).thenReturn(defn);

        IndexUpdateCallback callback = new TestCallback("/oak:index/fooIndex", newCommitInfo(), false, false);
        NodeBuilder defnBuilder = createIndexDefinition("fooIndex").builder();
        Editor editor = editorProvider.getIndexEditor(TYPE_LUCENE, defnBuilder, root, callback);
        LuceneIndexEditor luceneEditor = (LuceneIndexEditor) editor;
        LuceneIndexEditorContext context = luceneEditor.getContext();

        //Definition should reflect the marker property
        assertEquals("bar", context.getDefinition().getDefinitionNodeState().getString("foo"));
    }

    @Test
    public void editorNullInCaseOfReindex() throws Exception{
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(null,
                null,
                null,
                null,
                Mounts.defaultMountInfoProvider());
        editorProvider.setIndexingQueue(mock(DocumentQueue.class));
        IndexUpdateCallback callback = new TestCallback("/oak:index/fooIndex", newCommitInfo(), true, false);
        NodeBuilder defnBuilder = createIndexDefinition("fooIndex").builder();
        Editor editor = editorProvider.getIndexEditor(TYPE_LUCENE, defnBuilder, root, callback);
        assertNull(editor);
    }

    private NodeState createIndexDefinition(String idxName) {
        NodeBuilder idx = newLucenePropertyIndexDefinition(builder.child("oak:index"),
                idxName, ImmutableSet.of("foo"), "async");
        TestUtil.enableIndexingMode(idx, IndexingMode.NRT);
        LuceneIndexEditorContext.configureUniqueId(idx);
        IndexDefinition.updateDefinition(idx);
        return idx.getNodeState();
    }

    private CommitInfo newCommitInfo() {
        CommitInfo info = new CommitInfo("admin", "s1",
                ImmutableMap.<String, Object>of(CommitContext.NAME, new SimpleCommitContext()));
        return info;
    }

    private static class TestCallback implements IndexUpdateCallback, IndexingContext, ContextAwareCallback {
        private final String indexPath;
        private final CommitInfo commitInfo;
        private final boolean reindexing;
        private final boolean async;

        private TestCallback(String indexPath, CommitInfo commitInfo, boolean reindexing, boolean async) {
            this.indexPath = indexPath;
            this.commitInfo = commitInfo;
            this.reindexing = reindexing;
            this.async = async;
        }

        @Override
        public String getIndexPath() {
            return indexPath;
        }

        @Override
        public CommitInfo getCommitInfo() {
            return commitInfo;
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
        public void indexUpdate() throws CommitFailedException {

        }

        @Override
        public IndexingContext getIndexingContext() {
            return this;
        }

        @Override
        public void registerIndexCommitCallback(IndexCommitCallback callback) {

        }
    }

}