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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.IndexingQueue;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class SynchronousPropertyIndexTest extends AbstractQueryTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private LuceneIndexProvider luceneIndexProvider;
    private IndexingQueue queue = mock(IndexingQueue.class);
    private NodeStore nodeStore = new MemoryNodeStore();
    private Whiteboard wb;


    private IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
    private String indexPath  = "/oak:index/foo";

    @After
    public void tearDown() throws IOException {
        luceneIndexProvider.close();
        new ExecutorCloser(executorService).close();
    }

    @Override
    protected ContentRepository createRepository() {
        IndexCopier copier;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IndexTracker tracker = new IndexTracker();
        luceneIndexProvider = new LuceneIndexProvider(tracker);
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier,
                tracker,
                null,
                null,
                Mounts.defaultMountInfoProvider());

        editorProvider.setIndexingQueue(queue);

        Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) luceneIndexProvider)
                .with((Observer) luceneIndexProvider)
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .with(new NodeCounterEditorProvider())
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

        wb = oak.getWhiteboard();
        return oak.createContentRepository();
    }

    @Test
    public void uniquePropertyCommit() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        root.commit();

        createPath("/b").setProperty("foo", "bar");
        try {
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertEquals(CONSTRAINT, e.getType());
        }
    }

    @Test
    public void uniquePropertyCommit_Async() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        root.commit();
        runAsyncIndex();

        //Remove the :property-index node to simulate bucket change
        //This time commit would trigger a lucene query
        NodeBuilder builder = nodeStore.getRoot().builder();
        String propIdxStorePath = concat(indexPath, HybridPropertyIndexUtil.PROPERTY_INDEX);
        NodeBuilder propIndex = TestUtil.child(builder, propIdxStorePath);
        propIndex.remove();
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        root.refresh();

        createPath("/b").setProperty("foo", "bar");
        try {
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertEquals(CONSTRAINT, e.getType());
        }
    }

    private void runAsyncIndex() {
        AsyncIndexUpdate async = (AsyncIndexUpdate) WhiteboardUtils.getService(wb,
                Runnable.class, input -> input instanceof AsyncIndexUpdate);
        assertNotNull(async);
        async.run();
        if (async.isFailing()) {
            fail("AsyncIndexUpdate failed");
        }
        root.refresh();
    }

    private void addIndex(String indexPath, IndexDefinitionBuilder defnb){
        defnb.build(createPath(indexPath));
    }

    private Tree createPath(String path){
        Tree base = root.getTree("/");
        for (String e : PathUtils.elements(path)){
            base = base.hasChild(e) ? base.getChild(e) : base.addChild(e);
        }
        return base;
    }
}
