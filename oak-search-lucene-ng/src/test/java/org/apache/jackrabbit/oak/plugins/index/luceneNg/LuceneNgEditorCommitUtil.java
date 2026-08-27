/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;

import java.io.IOException;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;

/**
 * Test support for driving the Lucene 9 index editor through <em>real commits</em> (the way
 * production does), instead of constructing {@code LuceneNgIndexEditor} directly and calling
 * {@code enter}/{@code leave} by hand.
 *
 * <p>{@code LuceneNgIndexEditor} sits on the shared {@code FulltextIndexEditor} framework and
 * cannot be instantiated at an arbitrary sub-path with its own {@code IndexWriter}. The supported
 * way to exercise it is to run an {@link EditorHook} over a content commit — that builds the real
 * {@code FulltextIndexEditorContext}, obtains the {@code IndexingContext}/{@code ContextAwareCallback},
 * and writes the segments into the committed node state, exactly as the production
 * {@link LuceneNgIndexEditorProvider} does. Tests then open a {@link DirectoryReader} over that
 * committed {@code /oak:index/<name>/lucene9} storage to assert on the observable index contents
 * (documents, fields, doc-values, facets).</p>
 *
 * <p>Every index definition driven this way must be a <b>synchronous</b> {@code lucene9} index
 * (no {@code async} property, {@code type=lucene9}), so the {@link EditorHook} processes it inline.</p>
 */
final class LuceneNgEditorCommitUtil {

    private LuceneNgEditorCommitUtil() {
    }

    /**
     * Runs the Lucene 9 index editor over the {@code before -> after} diff via a real
     * {@link EditorHook}/{@link IndexUpdateProvider} and returns the resulting (indexed) node state.
     */
    static NodeState commit(NodeState before, NodeState after) throws CommitFailedException {
        EditorHook hook = new EditorHook(new IndexUpdateProvider(
                new LuceneNgIndexEditorProvider(new LuceneNgIndexTracker())));
        return hook.processCommit(before, after, CommitInfo.EMPTY);
    }

    /**
     * Full (re)index of a node state that already carries the {@code lucene9} index definition and
     * the content to index, diffed against the base {@code INITIAL_CONTENT}. Because the definition
     * is new in {@code after}, this triggers a reindex and indexes every matching node in {@code after}.
     */
    static NodeState reindex(NodeState after) throws CommitFailedException {
        return commit(INITIAL_CONTENT, after);
    }

    /**
     * Opens a read-only {@link DirectoryReader} over the committed Lucene storage of the index
     * definition at {@code indexDefPath} (e.g. {@code /oak:index/test}). The Lucene directory name is
     * the definition's node name, matching {@code LuceneNgFulltextIndexWriterFactory}.
     */
    static DirectoryReader openReader(NodeState indexed, String indexDefPath) throws IOException {
        NodeBuilder b = indexed.builder();
        for (String segment : PathUtils.elements(indexDefPath)) {
            b = b.child(segment);
        }
        String indexName = PathUtils.getName(indexDefPath);
        return DirectoryReader.open(
                new OakDirectory(b.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), indexName, true));
    }

    /**
     * Number of live documents in the committed index, tolerant of the "nothing was indexed" case:
     * if the reindex produced no documents at all the Lucene directory may hold no readable commit,
     * which is reported here as {@code 0} rather than throwing.
     */
    static int numDocs(NodeState indexed, String indexDefPath) throws IOException {
        try (DirectoryReader reader = openReader(indexed, indexDefPath)) {
            return reader.numDocs();
        } catch (IndexNotFoundException e) {
            return 0;
        }
    }
}
