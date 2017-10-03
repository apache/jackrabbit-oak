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
import static org.mockito.Mockito.mock;

public class LuceneIndexEditor2Test {

    private NodeState root = INITIAL_CONTENT;
    private NodeState before = root;
    private IndexUpdateCallback updateCallback = mock(IndexUpdateCallback.class);
    private ExtractedTextCache extractedTextCache = new ExtractedTextCache(0, 0);
    private TestIndexingContext indexingContext = new TestIndexingContext();
    private TestWriterFactory writerFactory = new TestWriterFactory();
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
