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
package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexWriterPoolTest {
    private final static Logger LOG = LoggerFactory.getLogger(IndexWriterPoolTest.class);

    private static class TestWriter implements LuceneIndexWriter {
        private final int delayMillis;
        // The writers must be thread safe
        final Set<String> deletedPaths = ConcurrentHashMap.newKeySet();
        final Map<String, Document> docs = new ConcurrentHashMap<>();
        boolean closed;

        public TestWriter() {
            this(0);
        }

        public TestWriter(int delayMillis) {
            this.delayMillis = delayMillis;
        }

        private void delay() {
            if (delayMillis > 0) {
                try {
                    LOG.info("Delaying {}", delayMillis);
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void updateDocument(String path, Iterable<? extends IndexableField> doc) {
            delay();
            docs.put(path, (Document) doc);
        }

        @Override
        public void deleteDocuments(String path) {
            delay();
            deletedPaths.add(path);
        }

        @Override
        public boolean close(long timestamp) {
            delay();
            closed = true;
            return true;
        }
    }

    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty(IndexWriterPool.OAK_INDEXER_PARALLEL_WRITER_MAX_BATCH_SIZE, "5")
            .and(IndexWriterPool.OAK_INDEXER_PARALLEL_WRITER_NUMBER_THREADS, "8")
            .and(IndexWriterPool.OAK_INDEXER_PARALLEL_WRITER_QUEUE_SIZE, "100");

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    @Test
    public void testSingleWriter() throws IOException {
        IndexWriterPool indexWriterPool = new IndexWriterPool();
        TestWriter writer = new TestWriter();
        Document doc = TestUtil.newDoc("value");
        indexWriterPool.updateDocument(writer, "test", doc);
        indexWriterPool.deleteDocuments(writer, "test");
        boolean closeResult = indexWriterPool.closeWriter(writer, 30);
        indexWriterPool.close();

        assertTrue(writer.closed);
        assertTrue(closeResult);
        assertEquals(Set.of("test"), writer.deletedPaths);
        assertEquals(Map.of("test", doc), writer.docs);
    }

    @Test
    public void testMultipleWriters() throws IOException {
        IndexWriterPool indexWriterPool = new IndexWriterPool();
        List<TestWriter> writers = IntStream.range(0, 5).mapToObj(TestWriter::new).collect(Collectors.toList());
        List<Document> updateDocs = IntStream.range(0, 200).mapToObj(i -> TestUtil.newDoc("test-doc-" + i)).collect(Collectors.toList());

        int i = 0;
        for (var doc : updateDocs) {
            indexWriterPool.updateDocument(writers.get(i%writers.size()), doc.get(FieldNames.PATH), doc);
            i++;
        }
        for (var w : writers) {
            indexWriterPool.closeWriter(w, 0);
        }
        indexWriterPool.close();

        List<Document> documentsWritten = new ArrayList<>();
        for (var w : writers) {
            documentsWritten.addAll(w.docs.values());
            LOG.info("w: {}, d: {}", w.docs.size(), w.deletedPaths.size());
        }

        assertEquals(updateDocs.size(), documentsWritten.size());
        assertEquals(Set.copyOf(updateDocs), Set.copyOf(documentsWritten));
        assertTrue(writers.stream().allMatch(w -> w.deletedPaths.isEmpty()));
    }

    @Test
    public void testEnqueueAfterClose() throws IOException {
        IndexWriterPool indexWriterPool = new IndexWriterPool();
        indexWriterPool.close();
        // Subsequent calls to close should be ignored
        indexWriterPool.close();
        // the following method should throw IllegalStateException
        try {
            indexWriterPool.updateDocument(new TestWriter(), "test", TestUtil.newDoc("value"));
            fail("updateDocument did not throw expected exception");
        } catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testCloseWriterPoolWithoutClosingWriters() throws IOException {
        IndexWriterPool indexWriterPool = new IndexWriterPool();
        TestWriter writer = new TestWriter(100);
        Document doc = TestUtil.newDoc("value");
        indexWriterPool.updateDocument(writer, "test", doc);
        indexWriterPool.deleteDocuments(writer, "test-deletion");
        indexWriterPool.close();

        assertEquals(Map.of("test", doc), writer.docs);
        assertEquals(Set.of("test-deletion"), writer.deletedPaths);
    }
}
