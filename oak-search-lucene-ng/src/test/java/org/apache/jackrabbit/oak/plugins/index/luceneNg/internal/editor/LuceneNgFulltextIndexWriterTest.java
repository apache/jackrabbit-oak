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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.editor;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexStorage;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.LuceneNgIndexCopier;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link LuceneNgFulltextIndexWriterFactory} opens a working writer over the
 * same {@link OakDirectory}-backed storage that {@code LuceneNgIndexEditor} uses directly
 * today, and that {@link LuceneNgFulltextIndexWriter} correctly adapts
 * update/delete/commit/close calls onto the underlying Lucene {@code IndexWriter}.
 */
public class LuceneNgFulltextIndexWriterTest {

    @TempDir
    public Path tempDir;

    @Test
    public void incrementalWriterReadsPrefetchedSegmentsAndReindexSkipsOldCache() throws Exception {
        NodeBuilder builder = EMPTY_NODE.builder();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition.Builder()
                .root(EMPTY_NODE).defn(EMPTY_NODE).indexPath("/oak:index/test").uid("uid").build();
        try (LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, Files.createTempDirectory(tempDir, "folder").toFile(), false)) {
            LuceneNgFulltextIndexWriterFactory factory = new LuceneNgFulltextIndexWriterFactory(copier);
            FulltextIndexWriter<Document> first = factory.newInstance(definition, builder, null, true);
            first.updateDocument("/a", newDoc("/a"));
            first.close(0);
            assertEquals(0, copier.getDownloadCount(), "reindex must not prefetch obsolete segments");

            FulltextIndexWriter<Document> second = factory.newInstance(definition, builder, null, false);
            assertTrue(copier.getDownloadCount() > 0, "incremental writer must copy the segments it reads");
            int downloads = copier.getDownloadCount();
            second.deleteDocument("/a");
            second.updateDocument("/b", newDoc("/b"));
            second.close(0);
            assertTrue(copier.getReaderLocalReadCount() > 0, "writer must read existing segment data locally");
            assertDocCount(definition, builder, "/a", 0);
            assertDocCount(definition, builder, "/b", 1);

            FulltextIndexWriter<Document> reindex = factory.newInstance(definition, builder, null, true);
            assertEquals(downloads, copier.getDownloadCount());
            reindex.updateDocument("/c", newDoc("/c"));
            reindex.close(0);
            assertDocCount(definition, builder, "/b", 0);
            assertDocCount(definition, builder, "/c", 1);
        }
    }

    @Test
    public void incrementalWriterOpensWithoutDownloadingTheIndex() throws Exception {
        NodeBuilder builder = EMPTY_NODE.builder();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition.Builder()
                .root(EMPTY_NODE).defn(EMPTY_NODE).indexPath("/oak:index/test").uid("uid").build();
        FulltextIndexWriter<Document> initial =
                new LuceneNgFulltextIndexWriterFactory().newInstance(definition, builder, null, true);
        initial.updateDocument("/a", newDoc("/a"));
        initial.close(0);
        List<Runnable> pendingCopies = new ArrayList<>();
        try (LuceneNgIndexCopier copier = new LuceneNgIndexCopier(pendingCopies::add,
                Files.createTempDirectory(tempDir, "folder").toFile(), false)) {
            FulltextIndexWriter<Document> writer =
                    new LuceneNgFulltextIndexWriterFactory(copier).newInstance(definition, builder, null, false);

            assertEquals(0, copier.getDownloadCount());
            writer.updateDocument("/b", newDoc("/b"));
            writer.close(0);
        }
        assertDocCount(definition, builder, "/a", 1);
        assertDocCount(definition, builder, "/b", 1);
    }

    @Test
    public void failedWriterPrefetchFallsBackToRemote() throws Exception {
        NodeBuilder builder = EMPTY_NODE.builder();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(EMPTY_NODE, EMPTY_NODE, "/oak:index/test");
        LuceneNgFulltextIndexWriterFactory plain = new LuceneNgFulltextIndexWriterFactory();
        FulltextIndexWriter<Document> first = plain.newInstance(definition, builder, null, true);
        first.updateDocument("/a", newDoc("/a"));
        first.close(0);
        try (LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, Files.createTempDirectory(tempDir, "folder").toFile(), false) {
            @Override
            public Directory wrapForWrite(IndexDefinition def, OakDirectory remote, OakDirectory snapshot, String dirName)
                    throws IOException {
                throw new IOException("local disk unavailable");
            }
        }) {
            FulltextIndexWriter<Document> second = new LuceneNgFulltextIndexWriterFactory(copier)
                    .newInstance(definition, builder, null, false);
            second.updateDocument("/b", newDoc("/b"));
            second.close(0);
            assertDocCount(definition, builder, "/a", 1);
            assertDocCount(definition, builder, "/b", 1);
        }
    }

    @Test
    public void failedPublicationPreservesTheCommittedIndex() throws Exception {
        org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore store =
                new org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore();
        NodeBuilder initial = store.getRoot().builder();
        NodeBuilder firstDefinition = initial.child("oak:index").child("test");
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
                EMPTY_NODE, firstDefinition.getNodeState(), "/oak:index/test");
        LuceneNgFulltextIndexWriterFactory factory = new LuceneNgFulltextIndexWriterFactory();
        FulltextIndexWriter<Document> original = factory.newInstance(definition, firstDefinition, null, true);
        original.updateDocument("/original", newDoc("/original"));
        original.close(0);
        store.merge(initial, org.apache.jackrabbit.oak.spi.commit.EmptyHook.INSTANCE,
                org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
        NodeState published = store.getRoot();
        NodeBuilder staged = published.builder();
        NodeBuilder stagedDefinition = staged.getChildNode("oak:index").getChildNode("test");
        stagedDefinition.getChildNode(LuceneNgIndexStorage.STORAGE_NODE_NAME).remove();
        FulltextIndexWriter<Document> rebuilt = factory.newInstance(definition, stagedDefinition, null, true);
        rebuilt.updateDocument("/replacement", newDoc("/replacement"));
        rebuilt.close(0);
        assertDocCount(definition, stagedDefinition, "/replacement", 1);
        assertDocCount(definition, stagedDefinition, "/original", 0);
        org.junit.jupiter.api.Assertions.assertThrows(org.apache.jackrabbit.oak.api.CommitFailedException.class,
                () -> store.merge(staged, (before, after, info) -> {
                    throw new org.apache.jackrabbit.oak.api.CommitFailedException("Test", 1, "Rejected publication");
                }, org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY));
        assertEquals(published, store.getRoot());
        NodeBuilder committedDefinition = store.getRoot().getChildNode("oak:index")
                .getChildNode("test").builder();
        assertDocCount(definition, committedDefinition, "/original", 1);
        assertDocCount(definition, committedDefinition, "/replacement", 0);
    }

    @Test
    public void writesAndDeletesDocumentsThroughTheAdaptedInterface() throws Exception {
        NodeBuilder definitionBuilder = EMPTY_NODE.builder();
        LuceneNgIndexDefinition definition =
                new LuceneNgIndexDefinition(EMPTY_NODE, EMPTY_NODE, "/oak:index/test");

        LuceneNgFulltextIndexWriterFactory factory = new LuceneNgFulltextIndexWriterFactory();

        // Write three documents (one of them a descendant of another) via a reindexing writer.
        FulltextIndexWriter<Document> writer = factory.newInstance(definition, definitionBuilder, null, true);
        writer.updateDocument("/a", newDoc("/a"));
        writer.updateDocument("/a/b", newDoc("/a/b"));
        writer.updateDocument("/c", newDoc("/c"));
        boolean updated = writer.close(System.currentTimeMillis());
        assertTrue(updated, "close() must report that the index was updated");

        assertDocCount(definition, definitionBuilder, "/a", 1);
        assertDocCount(definition, definitionBuilder, "/a/b", 1);
        assertDocCount(definition, definitionBuilder, "/c", 1);

        // Re-open (non-reindexing) and exercise both delete flavours the interface offers:
        // deleteDocumentTree("/a") must remove /a and its descendant /a/b, while
        // deleteDocument("/c") must remove only the exact document at /c.
        FulltextIndexWriter<Document> writer2 = factory.newInstance(definition, definitionBuilder, null, false);
        writer2.deleteDocumentTree("/a");
        writer2.deleteDocument("/c");
        boolean updatedByDeletes = writer2.close(System.currentTimeMillis());
        assertTrue(updatedByDeletes, "close() must report that the index was updated by the deletes");

        assertDocCount(definition, definitionBuilder, "/a", 0);
        assertDocCount(definition, definitionBuilder, "/a/b", 0);
        assertDocCount(definition, definitionBuilder, "/c", 0);
    }

    /**
     * Regression test for the {@link FulltextIndexWriter#close(long)} contract: "true if index
     * was updated or any write happened". A writer on which no {@code updateDocument} /
     * {@code deleteDocumentTree} / {@code deleteDocument} call was made must report {@code
     * false} on close, since nothing was written. This matters downstream: {@code
     * FulltextIndexEditorContext.closeWriter()} only rewrites the index's {@code :status}
     * properties (lastUpdated, indexedNodes, ...) when {@code close()} returns {@code true}, and
     * {@code LuceneNgIndexTracker.isUpdateNeeded()} relies on {@code :status} staying untouched
     * across no-op commits to avoid an unnecessary whole-subtree diff triggering an IndexNode
     * reopen.
     */
    @Test
    public void closeReturnsFalseWhenNothingWasWrittenOrDeleted() throws Exception {
        NodeBuilder definitionBuilder = EMPTY_NODE.builder();
        LuceneNgIndexDefinition definition =
                new LuceneNgIndexDefinition(EMPTY_NODE, EMPTY_NODE, "/oak:index/test");

        LuceneNgFulltextIndexWriterFactory factory = new LuceneNgFulltextIndexWriterFactory();
        FulltextIndexWriter<Document> writer = factory.newInstance(definition, definitionBuilder, null, true);

        boolean updated = writer.close(System.currentTimeMillis());

        assertFalse(updated, "close() must report false when no write/delete happened before it");
    }

    /**
     * Regression test for the bug where {@code LuceneNgFulltextIndexWriterFactory} created an
     * {@link OakDirectory} as a local variable and never passed it on for closing: only
     * {@link OakDirectory#close()} (in write mode) persists the authoritative file listing
     * ({@code PROP_DIR_LISTING}); without it, every open falls back to an expensive child-node
     * scan. Asserts directly on the persisted property rather than merely that a fresh reader
     * can still open the data — the latter would also pass via the child-scan fallback and
     * therefore wouldn't catch this bug.
     */
    @Test
    public void closePersistsDirectoryListing() throws Exception {
        NodeBuilder definitionBuilder = EMPTY_NODE.builder();
        LuceneNgIndexDefinition definition =
                new LuceneNgIndexDefinition(EMPTY_NODE, EMPTY_NODE, "/oak:index/test");

        LuceneNgFulltextIndexWriterFactory factory = new LuceneNgFulltextIndexWriterFactory();
        FulltextIndexWriter<Document> writer = factory.newInstance(definition, definitionBuilder, null, true);
        writer.updateDocument("/a", newDoc("/a"));
        writer.close(System.currentTimeMillis());

        // Independent, read-only view over the same storage subtree the writer just closed.
        NodeState storageState = LuceneNgIndexStorage.storageState(definitionBuilder.getNodeState());
        PropertyState dirListing = storageState.getProperty(LuceneNgIndexConstants.PROP_DIR_LISTING);

        assertNotNull(dirListing,
                "PROP_DIR_LISTING must be persisted once the writer's directory is closed");
        assertTrue(dirListing.count() > 0,
                "PROP_DIR_LISTING must list at least the segment files just written");
        assertEquals(Type.STRINGS, dirListing.getType());
    }

    @Test
    public void indexFilesUseTheDefinitionBlobSize() throws Exception {
        NodeBuilder definitionBuilder = EMPTY_NODE.builder();
        NodeState definitionState = EMPTY_NODE.builder()
                .setProperty(FulltextIndexConstants.BLOB_SIZE, 4096L).getNodeState();
        LuceneNgIndexDefinition definition =
                new LuceneNgIndexDefinition(EMPTY_NODE, definitionState, "/oak:index/test");

        FulltextIndexWriter<Document> writer =
                new LuceneNgFulltextIndexWriterFactory().newInstance(definition, definitionBuilder, null, true);
        writer.updateDocument("/a", newDoc("/a"));
        writer.close(0);

        NodeState storage = LuceneNgIndexStorage.storageState(definitionBuilder.getNodeState());
        for (String fileName : storage.getChildNodeNames()) {
            assertEquals(4096L, storage.getChildNode(fileName).getLong(LuceneNgIndexConstants.PROP_BLOB_SIZE), fileName);
        }
        assertDocCount(definition, definitionBuilder, "/a", 1);
    }

    @Test
    public void mergesRunOnTheIndexingThread() {
        assertInstanceOf(SerialMergeScheduler.class,
                LuceneNgFulltextIndexWriterFactory.newIndexWriterConfig(false).getMergeScheduler());
        assertInstanceOf(SerialMergeScheduler.class,
                LuceneNgFulltextIndexWriterFactory.newIndexWriterConfig(true).getMergeScheduler());
    }

    private static Document newDoc(String path) {
        Document doc = new Document();
        doc.add(new StringField(FieldNames.PATH, path, Field.Store.YES));
        return doc;
    }

    /**
     * Opens a fresh read-only {@link OakDirectory} over the same storage location the writer
     * factory used and asserts the number of documents whose {@link FieldNames#PATH} field
     * matches {@code path}. Mirrors the read-back pattern used in
     * {@code LuceneNgIndexEditorTest} and {@code LuceneNgIndexStorageTest}.
     */
    private static void assertDocCount(LuceneNgIndexDefinition definition, NodeBuilder definitionBuilder,
                                        String path, int expectedCount) throws Exception {
        NodeBuilder storage = LuceneNgIndexStorage.getOrCreateStorageBuilder(definitionBuilder);
        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(storage, definition.getIndexName(), true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new TermQuery(new Term(FieldNames.PATH, path)), 10);
            assertEquals(expectedCount, hits.totalHits.value, "Unexpected document count for path " + path);
        }
    }
}
