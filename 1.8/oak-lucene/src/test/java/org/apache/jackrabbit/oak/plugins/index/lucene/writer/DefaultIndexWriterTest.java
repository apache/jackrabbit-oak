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

import java.io.File;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DefaultDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.FSDirectoryFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DefaultIndexWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = EMPTY_NODE.builder();

    private LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig();

    @Test
    public void lazyInit() throws Exception {
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        DefaultIndexWriter writer = createWriter(defn, false);
        assertFalse(writer.close(0));
    }

    @Test
    public void writeInitializedUponReindex() throws Exception {
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        DefaultIndexWriter writer = createWriter(defn, true);
        assertTrue(writer.close(0));
    }

    @Test
    public void indexUpdated() throws Exception {
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        DefaultIndexWriter writer = createWriter(defn, false);

        Document document = new Document();
        document.add(newPathField("/a/b"));

        writer.updateDocument("/a/b", document);

        assertTrue(writer.close(0));
    }

    @Test
    public void indexWriterConfig_Scheduler_Remote() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        DefaultIndexWriter writer = createWriter(defn, true);

        IndexWriter w = writer.getWriter();
        assertThat(w.getConfig().getMergeScheduler(), instanceOf(SerialMergeScheduler.class));
    }

    @Test
    public void indexWriterConfig_Scheduler_Local() throws Exception{
        FSDirectoryFactory fsdir = new FSDirectoryFactory(folder.getRoot());
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        DefaultIndexWriter writer = new DefaultIndexWriter(defn, builder,
                fsdir, INDEX_DATA_CHILD_NAME, SUGGEST_DATA_CHILD_NAME, true, writerConfig);

        IndexWriter w = writer.getWriter();
        assertThat(w.getConfig().getMergeScheduler(), instanceOf(ConcurrentMergeScheduler.class));
    }

    @Test
    public void configRAMSize() throws Exception{
        writerConfig = new LuceneIndexWriterConfig(42);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        DefaultIndexWriter writer = createWriter(defn, true);

        IndexWriter w = writer.getWriter();
        assertEquals(w.getConfig().getRAMBufferSizeMB(), 42, 0);
    }

    @Test
    public void useAddForReindex() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        DefaultIndexWriter writer = createWriter(defn, true);

        Document document = new Document();
        document.add(newPathField("/a/b"));

        writer.updateDocument("/a/b", document);

        assertFalse(writer.getWriter().hasDeletions());
        writer.close(100);
    }

    @Test
    public void useUpdateForNormalIndexing() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        DefaultIndexWriter writer = createWriter(defn, false);

        Document document = new Document();
        document.add(newPathField("/a/b"));

        writer.updateDocument("/a/b", document);

        assertTrue(writer.getWriter().hasDeletions());
        writer.close(100);
    }

    private DefaultIndexWriter createWriter(IndexDefinition defn, boolean reindex) {
        return new DefaultIndexWriter(defn, builder,
                new DefaultDirectoryFactory(null, null), INDEX_DATA_CHILD_NAME,
                SUGGEST_DATA_CHILD_NAME, reindex, writerConfig);
    }


}