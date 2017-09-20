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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.IndexingMode;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class LocalIndexWriterFactoryTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();

    private EditorHook syncHook;
    private EditorHook asyncHook;
    private CommitInfo info;
    private LuceneIndexEditorProvider editorProvider;
    private IndexTracker tracker;

    @Before
    public void setUp() throws IOException {
        tracker = new IndexTracker();
        DocumentQueue queue = new DocumentQueue(100, tracker, sameThreadExecutor());
        editorProvider = new LuceneIndexEditorProvider(
                null,
                null,
                null,
                Mounts.defaultMountInfoProvider()
        );
        editorProvider.setIndexingQueue(queue);
        syncHook = new EditorHook(new IndexUpdateProvider(editorProvider));
        asyncHook = new EditorHook(new IndexUpdateProvider(editorProvider, "async", false));
    }

    @After
    public void cleanup() throws IOException {
    }

    @Test
    public void ignoreReindexCase() throws Exception{
        createIndexDefinition("fooIndex", IndexingMode.NRT);

        builder.child("a").setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        syncHook.processCommit(EMPTY_NODE, after, newCommitInfo());

        //This is reindex case so nothing would be indexed
        //So now holder should be present in context
        assertNotNull(getHolder());
        assertNotNull(getCommitAttribute(LuceneDocumentHolder.NAME));
    }

    @Test
    public void localIndexWriter() throws Exception{
        NodeState indexed = createAndPopulateAsyncIndex(IndexingMode.NRT);
        builder = indexed.builder();
        builder.child("b").setProperty("foo", "bar");
        builder.child("c").setProperty("foo", "bar");
        builder.child("a").remove();
        NodeState after = builder.getNodeState();
        syncHook.processCommit(indexed, after, newCommitInfo());

        LuceneDocumentHolder holder = getHolder();
        assertNotNull(holder);

        //2 add none for delete
        assertEquals(2, getIndexedDocList(holder, "/oak:index/fooIndex").size());
    }

    @Test
    public void mutlipleIndex() throws Exception{
        NodeState indexed = createAndPopulateTwoAsyncIndex(IndexingMode.NRT);
        builder = indexed.builder();
        builder.child("b").setProperty("foo", "bar");
        builder.child("c").setProperty("bar", "foo");
        builder.child("a").remove();
        NodeState after = builder.getNodeState();
        syncHook.processCommit(indexed, after, newCommitInfo());

        LuceneDocumentHolder holder = getHolder();
        assertNotNull(holder);

        //1 add  - bar
        assertEquals(1, getIndexedDocList(holder, "/oak:index/fooIndex").size());

        //1 add  - bar
        assertEquals(1, getIndexedDocList(holder, "/oak:index/barIndex").size());

    }

    @Test
    public void syncIndexing() throws Exception{
        NodeState indexed = createAndPopulateAsyncIndex(IndexingMode.SYNC);
        builder = indexed.builder();
        builder.child("b").setProperty("foo", "bar");
        builder.child("c").setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        syncHook.processCommit(indexed, after, newCommitInfo());

        LuceneDocumentHolder holder = getHolder();
        assertNotNull(holder);

        //2 add none for delete
        assertEquals(2, getIndexedDocList(holder,"/oak:index/fooIndex").size());
    }

    @Test
    public void inMemoryDocLimit() throws Exception{
        NodeState indexed = createAndPopulateAsyncIndex(IndexingMode.NRT);
        editorProvider.setInMemoryDocsLimit(5);
        editorProvider.setIndexingQueue(new DocumentQueue(1, tracker, sameThreadExecutor()));
        builder = indexed.builder();
        for (int i = 0; i < 10; i++) {
            builder.child("b" + i).setProperty("foo", "bar");
        }
        NodeState after = builder.getNodeState();
        syncHook.processCommit(indexed, after, newCommitInfo());

        LuceneDocumentHolder holder = getHolder();
        //5 for in memory list and 1 in queue
        assertEquals(5 + 1, getIndexedDocList(holder, "/oak:index/fooIndex").size());
    }

    private NodeState createAndPopulateAsyncIndex(IndexingMode indexingMode) throws CommitFailedException {
        createIndexDefinition("fooIndex", indexingMode);

        //Have some stuff to be indexed
        builder.child("a").setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        return asyncHook.processCommit(EMPTY_NODE, after, newCommitInfo());
    }

    private NodeState createAndPopulateTwoAsyncIndex(IndexingMode indexingMode) throws CommitFailedException {
        createIndexDefinition("fooIndex", indexingMode);
        createIndexDefinition("barIndex", indexingMode);

        //Have some stuff to be indexed
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").setProperty("bar", "foo");
        NodeState after = builder.getNodeState();
        return asyncHook.processCommit(EMPTY_NODE, after, newCommitInfo());
    }

    private LuceneDocumentHolder getHolder(){
        return (LuceneDocumentHolder) getCommitAttribute(LuceneDocumentHolder.NAME);
    }

    private Object getCommitAttribute(String name){
        CommitContext cc = (CommitContext) info.getInfo().get(CommitContext.NAME);
        return cc.get(name);
    }

    private CommitInfo newCommitInfo(){
        info = new CommitInfo("admin", "s1",
                ImmutableMap.<String, Object>of(CommitContext.NAME, new SimpleCommitContext()));
        return info;
    }

    private void createIndexDefinition(String idxName, IndexingMode indexingMode) {
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        TestUtil.enableIndexingMode(idx.getBuilderTree(), indexingMode);
        idx.indexRule("nt:base").property("foo").propertyIndex();
        builder.child("oak:index").setChildNode(idxName, idx.build());
    }

    private static List<String> getIndexedDocList(LuceneDocumentHolder holder, String indexPath){
        List<String> paths = Lists.newArrayList();
        for (LuceneDocInfo doc : holder.getAllLuceneDocInfo()){
            if (doc.getIndexPath().equals(indexPath)){
                paths.add(doc.getDocPath());
            }
        }
        return paths;
    }
}