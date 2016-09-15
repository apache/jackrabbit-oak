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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.core.SimpleCommitContext;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class LocalIndexWriterFactoryTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();

    private EditorHook syncHook;
    private EditorHook asyncHook;
    private CommitInfo info;

    @Before
    public void setUp() throws IOException {
        IndexEditorProvider editorProvider = new LuceneIndexEditorProvider(
                null,
                null,
                null,
                Mounts.defaultMountInfoProvider()
        );

        syncHook = new EditorHook(new IndexUpdateProvider(editorProvider));
        asyncHook = new EditorHook(new IndexUpdateProvider(editorProvider, "async", false));
    }

    @After
    public void cleanup() throws IOException {
    }

    @Test
    public void ignoreReindexCase() throws Exception{
        createIndexDefinition("fooIndex");

        builder.child("a").setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        syncHook.processCommit(EMPTY_NODE, after, newCommitInfo());

        //This is reindex case so nothing would be indexed
        //So now holder should be present in context
        assertNull(getHolder());
        assertNull(getCommitAttribute(LocalIndexWriterFactory.COMMIT_PROCESSED_BY_LOCAL_LUCENE_EDITOR));
    }

    @Test
    public void holderNotInitializedUnlessIndexed() throws Exception{
        NodeState indexed = createAndPopulateAsyncIndex();
        builder = indexed.builder();
        builder.child("b");
        NodeState after = builder.getNodeState();
        syncHook.processCommit(indexed, after, newCommitInfo());

        //This is incremental index case but no entry for fooIndex
        //so holder should be null
        assertNull(getHolder());
        assertNotNull(getCommitAttribute(LocalIndexWriterFactory.COMMIT_PROCESSED_BY_LOCAL_LUCENE_EDITOR));
    }

    @Test
    public void localIndexWriter() throws Exception{
        NodeState indexed = createAndPopulateAsyncIndex();
        builder = indexed.builder();
        builder.child("b").setProperty("foo", "bar");
        builder.child("c").setProperty("foo", "bar");
        builder.child("a").remove();
        NodeState after = builder.getNodeState();
        syncHook.processCommit(indexed, after, newCommitInfo());

        LuceneDocumentHolder holder = getHolder();
        assertNotNull(holder);

        //2 add none for delete
        assertEquals(2, holder.getAsyncIndexedDocList("/oak:index/fooIndex").size());
    }

    @Test
    public void mutlipleIndex() throws Exception{
        NodeState indexed = createAndPopulateTwoAsyncIndex();
        builder = indexed.builder();
        builder.child("b").setProperty("foo", "bar");
        builder.child("c").setProperty("bar", "foo");
        builder.child("a").remove();
        NodeState after = builder.getNodeState();
        syncHook.processCommit(indexed, after, newCommitInfo());

        LuceneDocumentHolder holder = getHolder();
        assertNotNull(holder);

        //1 add  - bar
        assertEquals(1, holder.getAsyncIndexedDocList("/oak:index/fooIndex").size());

        //1 add  - bar
        assertEquals(1, holder.getAsyncIndexedDocList("/oak:index/barIndex").size());

    }

    private NodeState createAndPopulateAsyncIndex() throws CommitFailedException {
        createIndexDefinition("fooIndex");

        //Have some stuff to be indexed
        builder.child("a").setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        return asyncHook.processCommit(EMPTY_NODE, after, newCommitInfo());
    }

    private NodeState createAndPopulateTwoAsyncIndex() throws CommitFailedException {
        createIndexDefinition("fooIndex");
        createIndexDefinition("barIndex");

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

    private void createIndexDefinition(String idxName) {
        NodeBuilder idx = newLucenePropertyIndexDefinition(builder.child("oak:index"),
                idxName, ImmutableSet.of("foo"), "async");
        idx.setProperty(createProperty(IndexConstants.ASYNC_PROPERTY_NAME, of("sync" , "async"), STRINGS));
    }

}