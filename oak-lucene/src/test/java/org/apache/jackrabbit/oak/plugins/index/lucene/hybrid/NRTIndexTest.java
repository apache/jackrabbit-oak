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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.IndexingMode;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.document.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NRTIndexTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();

    private IndexCopier indexCopier;
    private NRTIndexFactory indexFactory;

    @Before
    public void setUp() throws IOException {
        indexCopier = new IndexCopier(sameThreadExecutor(), temporaryFolder.getRoot());
        indexFactory = new NRTIndexFactory(indexCopier, StatisticsProvider.NOOP);
        indexFactory.setAssertAllResourcesClosed(true);
        LuceneIndexEditorContext.configureUniqueId(builder);
    }

    @After
    public void cleanup() throws IOException {
        indexFactory.close();
        indexCopier.close();
    }

    @Test
    public void getReaderWithoutWriter() throws Exception{
        IndexDefinition idxDefn = getSyncIndexDefinition("/foo");

        NRTIndex idx1 = indexFactory.createIndex(idxDefn);
        List<LuceneIndexReader> readers = idx1.getReaders();
        assertNotNull(readers);
        assertTrue(readers.isEmpty());

        idx1.close();
        assertTrue(idx1.isClosed());

        //Closing multiple times should not raise exception
        idx1.close();
    }

    @Test
    public void writerCreation() throws Exception{
        IndexDefinition idxDefn = getSyncIndexDefinition("/foo");
        NRTIndex idx = indexFactory.createIndex(idxDefn);
        LuceneIndexWriter writer = idx.getWriter();

        assertNotNull(writer);
        assertNotNull(idx.getIndexDir());
        List<LuceneIndexReader> readers = idx.getReaders();
        assertEquals(1, readers.size());

        LuceneIndexWriter writer2 = idx.getWriter();
        assertSame(writer, writer2);
    }

    @Test
    public void dirDeletedUponClose() throws Exception{
        IndexDefinition idxDefn = getSyncIndexDefinition("/foo");
        NRTIndex idx = indexFactory.createIndex(idxDefn);
        LuceneIndexWriter writer = idx.getWriter();
        File indexDir = idx.getIndexDir();

        assertTrue(indexDir.exists());

        idx.close();
        assertFalse(indexDir.exists());

        try{
            idx.getReaders();
            fail();
        } catch (IllegalStateException ignore){

        }

        try{
            idx.getWriter();
            fail();
        } catch (IllegalStateException ignore){

        }
    }

    @Test
    public void multipleUpdateForSamePath() throws Exception{
        IndexDefinition idxDefn = getSyncIndexDefinition("/foo");
        NRTIndex idx = indexFactory.createIndex(idxDefn);
        LuceneIndexWriter writer = idx.getWriter();

        Document document = new Document();
        document.add(newPathField("/a/b"));

        writer.updateDocument("/a/b", document);
        assertEquals(1, idx.getPrimaryReaderForTest().numDocs());

        writer.updateDocument("/a/b", document);

        //Update for same path should not lead to deletion
        assertEquals(2, idx.getPrimaryReaderForTest().numDocs());
        assertEquals(0, idx.getPrimaryReaderForTest().numDeletedDocs());
    }

    @Test
    public void previousIndexInitialized() throws Exception{
        IndexDefinition idxDefn = getSyncIndexDefinition("/foo");
        NRTIndex idx1 = indexFactory.createIndex(idxDefn);
        LuceneIndexWriter w1 = idx1.getWriter();

        Document d1 = new Document();
        d1.add(newPathField("/a/b"));
        w1.updateDocument("/a/b", d1);

        NRTIndex idx2 = indexFactory.createIndex(idxDefn);
        assertEquals(1, idx2.getReaders().size());

        LuceneIndexWriter w2 = idx2.getWriter();
        assertEquals(2, idx2.getReaders().size());

        assertNotEquals(idx1.getIndexDir(), idx2.getIndexDir());
    }

    @Test
    public void sameReaderIfNoChange() throws Exception{
        IndexDefinition idxDefn = getSyncIndexDefinition("/foo");
        NRTIndex idx1 = indexFactory.createIndex(idxDefn);
        LuceneIndexWriter w1 = idx1.getWriter();

        Document d1 = new Document();
        d1.add(newPathField("/a/b"));
        w1.updateDocument("/a/b", d1);

        List<LuceneIndexReader> readers = idx1.getReaders();
        List<LuceneIndexReader> readers2 = idx1.getReaders();

        assertSame(readers, readers2);

        w1.updateDocument("/a/b", d1);
        List<LuceneIndexReader> readers3 = idx1.getReaders();
        assertNotSame(readers2, readers3);
    }

    private IndexDefinition getSyncIndexDefinition(String indexPath) {
        TestUtil.enableIndexingMode(builder, IndexingMode.NRT);

        return new IndexDefinition(root, builder.getNodeState(), indexPath);
    }



}