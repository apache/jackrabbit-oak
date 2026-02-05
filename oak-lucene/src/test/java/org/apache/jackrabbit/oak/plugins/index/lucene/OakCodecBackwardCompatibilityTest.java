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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test to verify backward compatibility with indexes created using the old OakCodec
 * (Lucene 4.7.2 formats). This test reads a pre-generated index from test resources
 * and verifies that it can be read correctly.
 * 
 * The test index was generated using OakCodecIndexGenerator on the old codebase
 * (before Lucene upgrade) and contains 5 documents with id, title, and content fields.
 */
public class OakCodecBackwardCompatibilityTest {

    private static final String INDEX_RESOURCE_PATH = "lucene-backward-compat/oakCodec-index";
    private static final String[] INDEX_FILES = {
        "_0.cfe", "_0.cfs", "_0.si", "segments_1", "segments.gen"
    };

    private Path tempIndexDir;
    private Directory directory;

    @Before
    public void setUp() throws IOException {
        // Copy index files from resources to a temp directory
        tempIndexDir = Files.createTempDirectory("oakCodec-test-index");
        copyIndexFromResources();
        directory = new SimpleFSDirectory(tempIndexDir);
    }

    @After
    public void tearDown() throws IOException {
        if (directory != null) {
            directory.close();
        }
        if (tempIndexDir != null) {
            deleteDirectory(tempIndexDir.toFile());
        }
    }

    @Test
    public void testCanReadOldOakCodecIndex() throws IOException {
        // Verify we can open the index
        try (IndexReader reader = DirectoryReader.open(directory)) {
            assertNotNull("Should be able to open index reader", reader);
            assertEquals("Index should contain 5 documents", 5, reader.numDocs());
        }
    }

    @Test
    public void testCanSearchOldOakCodecIndex() throws IOException {
        try (IndexReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            
            // Search for all documents
            TopDocs allDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertEquals("Should find 5 documents", 5, allDocs.totalHits);
            
            // Search for specific document by id
            Query idQuery = new TermQuery(new Term("id", "1"));
            TopDocs idResults = searcher.search(idQuery, 1);
            assertEquals("Should find document with id=1", 1, idResults.totalHits);
        }
    }

    @Test
    public void testCanReadStoredFields() throws IOException {
        try (IndexReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            
            // Find document with id=4 (contains version info)
            Query query = new TermQuery(new Term("id", "4"));
            TopDocs results = searcher.search(query, 1);
            assertEquals(1, results.totalHits);
            
            Document doc = searcher.doc(results.scoreDocs[0].doc);
            assertNotNull("Document should not be null", doc);
            assertEquals("4", doc.get("id"));
            assertEquals("title4", doc.get("title"));
            assertTrue("Content should mention OakCodec", 
                    doc.get("content").contains("OakCodec"));
            assertTrue("Content should mention Lucene 4.7.2", 
                    doc.get("content").contains("4.7.2"));
        }
    }

    @Test
    public void testSegmentInfoShowsOldCodec() throws IOException {
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);

        // Verify segment info can be read
        assertTrue("Should have at least one segment", segmentInfos.size() > 0);

        // The codec name should be "oakCodec" for the old index
        String codecName = segmentInfos.info(0).info.getCodec().getName();
        System.out.println("Codec name from segment: " + codecName);
        assertEquals("Codec should be oakCodec", "oakCodec", codecName);
    }

    // ==================== OakCodec5 Tests ====================

    @Test
    public void testCanWriteAndReadWithOakCodec5() throws IOException {
        // Create a new index with OakCodec5
        Path oakCodec5Dir = Files.createTempDirectory("oakCodec5-test-index");
        try (Directory oakCodec5Directory = new SimpleFSDirectory(oakCodec5Dir)) {
            // Write documents using OakCodec5
            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            config.setCodec(new OakCodec5());

            try (IndexWriter writer = new IndexWriter(oakCodec5Directory, config)) {
                for (int i = 1; i <= 3; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                    doc.add(new StringField("title", "oakCodec5-title" + i, Field.Store.YES));
                    doc.add(new TextField("content", "This document was created with OakCodec5 (Lucene 5.x)", Field.Store.YES));
                    writer.addDocument(doc);
                }
            }

            // Read back and verify
            try (IndexReader reader = DirectoryReader.open(oakCodec5Directory)) {
                assertEquals("Should have 3 documents", 3, reader.numDocs());

                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs results = searcher.search(new TermQuery(new Term("id", "2")), 1);
                assertEquals(1, results.totalHits);

                Document doc = searcher.doc(results.scoreDocs[0].doc);
                assertEquals("oakCodec5-title2", doc.get("title"));
                assertTrue(doc.get("content").contains("OakCodec5"));
            }

            // Verify codec name in segment info
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(oakCodec5Directory);
            String codecName = segmentInfos.info(0).info.getCodec().getName();
            assertEquals("Codec should be oakCodec5", "oakCodec5", codecName);
        } finally {
            deleteDirectory(oakCodec5Dir.toFile());
        }
    }

    @Test
    public void testOakCodec5UsesLucene5Formats() throws IOException {
        // Create a new index with OakCodec5 and verify it uses Lucene 5.x formats
        try (Directory ramDir = new RAMDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            config.setCodec(new OakCodec5());

            try (IndexWriter writer = new IndexWriter(ramDir, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                doc.add(new TextField("content", "Test content for Lucene 5.x format verification", Field.Store.YES));
                writer.addDocument(doc);
            }

            // Verify the codec is OakCodec5
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(ramDir);

            String codecName = segmentInfos.info(0).info.getCodec().getName();
            assertEquals("oakCodec5", codecName);

            // Verify we can read the document
            try (IndexReader reader = DirectoryReader.open(ramDir)) {
                assertEquals(1, reader.numDocs());
            }
        }
    }

    @Test
    public void testBothCodecsCanCoexist() throws IOException {
        // This test verifies that both OakCodec (Lucene 4.x) and OakCodec5 (Lucene 5.x)
        // can be used in the same JVM - reading old indexes while writing new ones

        // First, verify we can still read the old OakCodec index
        try (IndexReader oldReader = DirectoryReader.open(directory)) {
            assertEquals("Old index should have 5 documents", 5, oldReader.numDocs());
        }

        // Then, create a new index with OakCodec5
        try (Directory ramDir = new RAMDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            config.setCodec(new OakCodec5());

            try (IndexWriter writer = new IndexWriter(ramDir, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "new1", Field.Store.YES));
                doc.add(new TextField("content", "New document with OakCodec5", Field.Store.YES));
                writer.addDocument(doc);
            }

            try (IndexReader newReader = DirectoryReader.open(ramDir)) {
                assertEquals("New index should have 1 document", 1, newReader.numDocs());
            }
        }

        // Verify we can still read the old index after creating a new one
        try (IndexReader oldReader = DirectoryReader.open(directory)) {
            assertEquals("Old index should still have 5 documents", 5, oldReader.numDocs());
        }
    }

    @Test
    public void testCanAddNewDocumentsToOldIndexWithOakCodec5() throws IOException {
        // This test verifies that we can open an existing oakCodec (Lucene 4.x) index
        // and add new documents using oakCodec5 (Lucene 5.x)

        // Verify initial state - old index has 5 documents
        try (IndexReader reader = DirectoryReader.open(directory)) {
            assertEquals("Initial index should have 5 documents", 5, reader.numDocs());
        }

        // Open the old index and add new documents using OakCodec5
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setCodec(new OakCodec5());
        config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        try (IndexWriter writer = new IndexWriter(directory, config)) {
            // Add new documents
            for (int i = 100; i <= 102; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new StringField("title", "new-title" + i, Field.Store.YES));
                doc.add(new TextField("content", "This document was added with OakCodec5 to an existing oakCodec index", Field.Store.YES));
                writer.addDocument(doc);
            }
        }

        // Verify we can read all documents (old + new)
        try (IndexReader reader = DirectoryReader.open(directory)) {
            assertEquals("Index should now have 8 documents (5 old + 3 new)", 8, reader.numDocs());

            IndexSearcher searcher = new IndexSearcher(reader);

            // Verify we can still find old documents
            TopDocs oldResults = searcher.search(new TermQuery(new Term("id", "1")), 1);
            assertEquals("Should find old document with id=1", 1, oldResults.totalHits);
            Document oldDoc = searcher.doc(oldResults.scoreDocs[0].doc);
            assertEquals("Old document should have id=1", "1", oldDoc.get("id"));
            assertEquals("Old document should have title1", "title1", oldDoc.get("title"));
            assertNotNull("Old document content should not be null", oldDoc.get("content"));

            // Verify we can find new documents
            TopDocs newResults = searcher.search(new TermQuery(new Term("id", "101")), 1);
            assertEquals("Should find new document with id=101", 1, newResults.totalHits);
            Document newDoc = searcher.doc(newResults.scoreDocs[0].doc);
            assertEquals("new-title101", newDoc.get("title"));
            assertTrue("New document should mention OakCodec5",
                    newDoc.get("content").contains("OakCodec5"));
        }

        // Check segment info - should have multiple segments with different codecs
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        assertTrue("Should have at least 2 segments", segmentInfos.size() >= 2);

        // Print codec info for debugging
        System.out.println("Segments after adding new documents:");
        for (int i = 0; i < segmentInfos.size(); i++) {
            String codecName = segmentInfos.info(i).info.getCodec().getName();
            int docCount = segmentInfos.info(i).info.maxDoc();
            System.out.println("  Segment " + i + ": codec=" + codecName + ", docs=" + docCount);
        }
    }

    @Test
    public void testCompactMixedCodecIndex() throws IOException {
        // This test verifies that we can compact (force merge) an index that has
        // segments with both oakCodec (Lucene 4.x) and oakCodec5 (Lucene 5.x)

        // Step 1: Verify initial state - old index has 5 documents with oakCodec
        try (IndexReader reader = DirectoryReader.open(directory)) {
            assertEquals("Initial index should have 5 documents", 5, reader.numDocs());
        }

        SegmentInfos initialSegments = SegmentInfos.readLatestCommit(directory);
        assertEquals("Initial index should have 1 segment", 1, initialSegments.size());
        assertEquals("Initial segment should use oakCodec", "oakCodec",
                initialSegments.info(0).info.getCodec().getName());

        // Step 2: Add new documents using OakCodec5
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setCodec(new OakCodec5());
        config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 100; i <= 104; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new StringField("title", "new-title" + i, Field.Store.YES));
                doc.add(new TextField("content", "Document added with OakCodec5", Field.Store.YES));
                writer.addDocument(doc);
            }
        }

        // Step 3: Verify we now have mixed segments
        SegmentInfos mixedSegments = SegmentInfos.readLatestCommit(directory);
        assertTrue("Should have at least 2 segments after adding documents", mixedSegments.size() >= 2);

        System.out.println("Segments before compaction:");
        boolean hasOakCodec = false;
        boolean hasOakCodec5 = false;
        for (int i = 0; i < mixedSegments.size(); i++) {
            String codecName = mixedSegments.info(i).info.getCodec().getName();
            int docCount = mixedSegments.info(i).info.maxDoc();
            System.out.println("  Segment " + i + ": codec=" + codecName + ", docs=" + docCount);
            if ("oakCodec".equals(codecName)) hasOakCodec = true;
            if ("oakCodec5".equals(codecName)) hasOakCodec5 = true;
        }
        assertTrue("Should have oakCodec segment", hasOakCodec);
        assertTrue("Should have oakCodec5 segment", hasOakCodec5);

        // Step 4: Force merge (compact) the index to a single segment
        IndexWriterConfig compactConfig = new IndexWriterConfig(new StandardAnalyzer());
        compactConfig.setCodec(new OakCodec5());
        compactConfig.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        try (IndexWriter writer = new IndexWriter(directory, compactConfig)) {
            writer.forceMerge(1); // Merge to single segment
        }

        // Step 5: Verify the compacted index
        SegmentInfos compactedSegments = SegmentInfos.readLatestCommit(directory);
        assertEquals("Compacted index should have 1 segment", 1, compactedSegments.size());

        String compactedCodec = compactedSegments.info(0).info.getCodec().getName();
        System.out.println("Segment after compaction: codec=" + compactedCodec +
                ", docs=" + compactedSegments.info(0).info.maxDoc());

        assertEquals("Compacted segment should use oakCodec5", "oakCodec5", compactedCodec);

        // Step 6: Verify all documents are still readable
        try (IndexReader reader = DirectoryReader.open(directory)) {
            assertEquals("Compacted index should have 10 documents (5 old + 5 new)", 10, reader.numDocs());

            IndexSearcher searcher = new IndexSearcher(reader);

            // Verify old documents are still there
            TopDocs oldResults = searcher.search(new TermQuery(new Term("id", "1")), 1);
            assertEquals("Should find old document with id=1", 1, oldResults.totalHits);
            Document oldDoc = searcher.doc(oldResults.scoreDocs[0].doc);
            assertEquals("title1", oldDoc.get("title"));

            // Verify new documents are still there
            TopDocs newResults = searcher.search(new TermQuery(new Term("id", "102")), 1);
            assertEquals("Should find new document with id=102", 1, newResults.totalHits);
            Document newDoc = searcher.doc(newResults.scoreDocs[0].doc);
            assertEquals("new-title102", newDoc.get("title"));

            // Verify total count with MatchAllDocsQuery
            TopDocs allDocs = searcher.search(new MatchAllDocsQuery(), 20);
            assertEquals("Should find all 10 documents", 10, allDocs.totalHits);
        }

        System.out.println("SUCCESS: Index compacted from mixed oakCodec/oakCodec5 to single oakCodec5 segment");
    }

    private void copyIndexFromResources() throws IOException {
        for (String fileName : INDEX_FILES) {
            String resourcePath = INDEX_RESOURCE_PATH + "/" + fileName;
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
                if (is == null) {
                    throw new IOException("Resource not found: " + resourcePath);
                }
                Path targetPath = tempIndexDir.resolve(fileName);
                Files.copy(is, targetPath, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            for (File child : dir.listFiles()) {
                deleteDirectory(child);
            }
        }
        dir.delete();
    }
}

