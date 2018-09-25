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
import java.util.Collections;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndex;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexNodeManager;
import org.apache.jackrabbit.oak.plugins.index.search.update.ReaderRefreshPolicy;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newDoc;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.search.FieldNames.PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class IndexNodeManagerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();

    private IndexCopier indexCopier;
    private NRTIndexFactory nrtFactory;
    private LuceneIndexReaderFactory readerFactory;

    @Before
    public void setUp() throws IOException {
        indexCopier = new IndexCopier(sameThreadExecutor(), temporaryFolder.getRoot());
        nrtFactory = new NRTIndexFactory(indexCopier, StatisticsProvider.NOOP);
        readerFactory = new DefaultIndexReaderFactory(Mounts.defaultMountInfoProvider(), indexCopier);
        LuceneIndexEditorContext.configureUniqueId(builder);
    }

    @After
    public void cleanup() throws IOException {
        nrtFactory.close();
        indexCopier.close();
    }

    @Test
    public void nullIndexNode() throws Exception{
        assertNull(LuceneIndexNodeManager.open("/foo", root, builder.getNodeState(), readerFactory, null));
        assertNull(LuceneIndexNodeManager.open("/foo", root, builder.getNodeState(), readerFactory, nrtFactory));
    }

    @Test
    public void nonNullIndex_OnlyNRT() throws Exception{
        LuceneIndexNodeManager nodeManager = LuceneIndexNodeManager.open("/foo", root, createNRTIndex(), readerFactory, nrtFactory);
        LuceneIndexNode node = nodeManager.acquire();
        assertNotNull(node.getSearcher());
        TopDocs docs = node.getSearcher().search(new TermQuery(new Term(PATH, "/content/en")), 100);
        assertEquals(0, docs.totalHits);
        node.release();

        node.getLocalWriter().updateDocument("/content/en", newDoc("/content/en"));
        node.refreshReadersOnWriteIfRequired();

        node = nodeManager.acquire();
        docs = node.getSearcher().search(new TermQuery(new Term(PATH, "/content/en")), 100);
        assertEquals(1, docs.totalHits);
    }

    @Test
    public void nullIndex_NonFreshIndex() throws Exception{
        NodeBuilder builder = createNRTIndex().builder();
        NodeBuilder rootBuilder = root.builder();
        rootBuilder.child(IndexNodeManager.ASYNC).setProperty("async", "async");
        assertNull(LuceneIndexNodeManager.open("/foo", rootBuilder.getNodeState(), builder.getNodeState(), readerFactory, nrtFactory));
    }

    @Test
    public void lockAndRefreshPolicy() throws Exception {
        NodeState state = createNRTIndex();
        LuceneIndexDefinition definition = new LuceneIndexDefinition(root, state, "/foo");
        NRTIndex nrtIndex = nrtFactory.createIndex(definition);
        NRTIndex mock = spy(nrtIndex);
        doReturn(new FailingPolicy()).when(mock).getRefreshPolicy();
        LuceneIndexNodeManager node = new LuceneIndexNodeManager("/foo", definition, Collections.emptyList(), mock);

        try {
            node.acquire();
            fail();
        } catch (Exception ignore) {

        }

        node.close();
    }

    @Test
    public void indexOpenedBeforeFistCycle() throws Exception {
        NodeState nrtIndex = createNRTIndex();
        NodeState asyncIndex = nrtIndex.builder().setProperty("async", ImmutableList.of("async"), STRINGS).getNodeState();
        NodeState nonAsyncIndex;
        {
            NodeBuilder builder = nrtIndex.builder();
            builder.removeProperty("async");
            nonAsyncIndex = builder.getNodeState();
        }

        assertNotNull("nrtIndex; Non existing /:async",
                LuceneIndexNodeManager.open("/foo", root, nrtIndex, readerFactory, nrtFactory));
        assertNull("asyncIndex; Non existing /:async",
                LuceneIndexNodeManager.open("/foo", root, asyncIndex, readerFactory, nrtFactory));
        assertNull("nonAsyncIndex; Non existing /:async",
                LuceneIndexNodeManager.open("/foo", root, nonAsyncIndex, readerFactory, nrtFactory));

        // Fake an empty /:async - first indexing cycle isn't done yet
        builder.child(":async");
        root = builder.getNodeState();

        assertNotNull("nrtIndex; empty /:async",
                LuceneIndexNodeManager.open("/foo", root, nrtIndex, readerFactory, nrtFactory));
        assertNull("asyncIndex; empty /:async",
                LuceneIndexNodeManager.open("/foo", root, asyncIndex, readerFactory, nrtFactory));
        assertNull("nonAsyncIndex; empty /:async",
                LuceneIndexNodeManager.open("/foo", root, nonAsyncIndex, readerFactory, nrtFactory));


        // Fake async indexing cycle done with no data
        builder.child(":async").setProperty("async", "some-random-id");
        root = builder.getNodeState();

        assertNull("nrtIndex; fake async cycle run",
                LuceneIndexNodeManager.open("/foo", root, nrtIndex, readerFactory, nrtFactory));
        assertNull("asyncIndex; fake async cycle run",
                LuceneIndexNodeManager.open("/foo", root, asyncIndex, readerFactory, nrtFactory));
        assertNull("nonAsyncIndex; fake async cycle run",
                LuceneIndexNodeManager.open("/foo", root, nonAsyncIndex, readerFactory, nrtFactory));
    }

    @Test
    public void indexWithIndexedDataOpenedBeforeFistCycle() throws Exception {
        NodeState nrtIndex = createNRTIndex();
        {
            NodeBuilder indexBuilder = nrtIndex.builder();
            LuceneIndexDefinition indexDefinition = new LuceneIndexDefinition(root, indexBuilder.getNodeState(), "/foo");
            IndexWriterConfig config = IndexWriterUtils.getIndexWriterConfig(indexDefinition, false);
            OakDirectory directory = new OakDirectory(indexBuilder, indexDefinition, false);
            IndexWriter writer = new IndexWriter(directory, config);
            writer.addDocument(newDoc("/content/en").getFields());
            writer.close();
            directory.close();
            nrtIndex = indexBuilder.getNodeState();
        }
        NodeState asyncIndex = nrtIndex.builder().setProperty("async", ImmutableList.of("async"), STRINGS).getNodeState();
        NodeState nonAsyncIndex;
        {
            NodeBuilder builder = nrtIndex.builder();
            builder.removeProperty("async");
            nonAsyncIndex = builder.getNodeState();
        }

        // absent or empty /:async doesn't make sense with already indexed data available.
        // So, we're considering only this case.
        // Fake async indexing cycle done with no data
        builder.child(":async").setProperty("async", "some-random-id");
        root = builder.getNodeState();

        assertNotNull("nrtIndex; fake async cycle run",
                LuceneIndexNodeManager.open("/foo", root, nrtIndex, readerFactory, nrtFactory));
        assertNotNull("asyncIndex; fake async cycle run",
                LuceneIndexNodeManager.open("/foo", root, asyncIndex, readerFactory, nrtFactory));
        assertNotNull("nonAsyncIndex; fake async cycle run",
                LuceneIndexNodeManager.open("/foo", root, nonAsyncIndex, readerFactory, nrtFactory));
    }

    @Test
    public void hasIndexingRun() {
        NodeState nrtIndex = createNRTIndex();
        NodeState asyncIndex = nrtIndex.builder().setProperty("async", ImmutableList.of("async"), STRINGS).getNodeState();
        NodeState nonAsyncIndex;
        {
            NodeBuilder builder = nrtIndex.builder();
            builder.removeProperty("async");
            nonAsyncIndex = builder.getNodeState();
        }

        assertFalse("nrtIndex; Non existing /:async",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", nrtIndex));
        assertFalse("asyncIndex; Non existing /:async",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", asyncIndex));
        assertFalse("nonAsyncIndex; Non existing /:async",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", nonAsyncIndex));

        // Fake an empty /:async - first indexing cycle isn't done yet
        builder.child(":async");
        root = builder.getNodeState();

        assertFalse("nrtIndex; Empty /:async",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", nrtIndex));
        assertFalse("asyncIndex; Non existing /:async",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", asyncIndex));
        assertFalse("nonAsyncIndex; Non existing /:async",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", nonAsyncIndex));

        // Fake async indexing cycle done
        builder.child(":async").setProperty("async", "some-random-id");
        root = builder.getNodeState();

        assertTrue("nrtIndex; fake async cycle run",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", nrtIndex));
        assertTrue("asyncIndex; fake async cycle run",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", asyncIndex));
        assertFalse("nonAsyncIndex; fake async cycle run",
                LuceneIndexNodeManager.hasAsyncIndexerRun(root, "/foo", nonAsyncIndex));
    }

    private static NodeState createNRTIndex(){
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.indexRule("nt:base").property("foo").propertyIndex();
        idx.async("async", "sync");
        return idx.build();
    }

    private static class FailingPolicy implements ReaderRefreshPolicy {

        @Override
        public void refreshOnReadIfRequired(Runnable refreshCallback) {
            throw new IllegalStateException();
        }

        @Override
        public void refreshOnWriteIfRequired(Runnable refreshCallback) {
            throw new IllegalStateException();
        }
    }
}