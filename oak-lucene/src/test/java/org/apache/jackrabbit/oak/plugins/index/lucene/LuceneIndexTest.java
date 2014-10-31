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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexCopier;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class LuceneIndexTest {

    private static final Analyzer analyzer = LuceneIndexConstants.ANALYZER;

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(
                    new LuceneIndexEditorProvider().with(analyzer)));

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void testLucene() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene",
                ImmutableSet.of(TYPENAME_STRING));

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LuceneIndex(tracker, analyzer, null);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(createPlan(filter), indexed);
        assertTrue(cursor.hasNext());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testLuceneLazyCursor() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene",
                ImmutableSet.of(TYPENAME_STRING));

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");

        for(int i = 0; i < LuceneIndex.LUCENE_QUERY_BATCH_SIZE; i++){
            builder.child("parent").child("child"+i).setProperty("foo", "bar");
        }

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LuceneIndex(tracker, analyzer, null);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(createPlan(filter), indexed);

        List<String> paths = copyOf(transform(cursor, new Function<IndexRow, String>() {
            public String apply(IndexRow input) {
                return input.getPath();
            }
        }));
        assertTrue(!paths.isEmpty());
        assertEquals(LuceneIndex.LUCENE_QUERY_BATCH_SIZE + 1, paths.size());
    }

    @Test
    public void testLucene2() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene",
                ImmutableSet.of(TYPENAME_STRING));

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar");
        builder.child("a").child("b").child("c").setProperty("foo", "bar");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LuceneIndex(tracker, analyzer, null);
        FilterImpl filter = createFilter(NT_BASE);
        // filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(createPlan(filter), indexed);

        assertTrue(cursor.hasNext());
        assertEquals("/a/b/c", cursor.next().getPath());
        assertEquals("/a/b", cursor.next().getPath());
        assertEquals("/a", cursor.next().getPath());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testLucene3() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene",
                ImmutableSet.of(TYPENAME_STRING));

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar", Type.NAME);
        builder.child("a").child("b").child("c")
                .setProperty("foo", "bar", Type.NAME);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LuceneIndex(tracker, analyzer, null);
        FilterImpl filter = createFilter(NT_BASE);
        // filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(createPlan(filter), indexed);

        assertTrue(cursor.hasNext());
        assertEquals("/a", cursor.next().getPath());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeState system = root.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    @Test
    public void testTokens() {
        assertEquals(ImmutableList.of("parent", "child"),
                LuceneIndex.tokenize("/parent/child", analyzer));
        assertEquals(ImmutableList.of("p1234", "p5678"),
                LuceneIndex.tokenize("/p1234/p5678", analyzer));
        assertEquals(ImmutableList.of("first", "second"),
                LuceneIndex.tokenize("first_second", analyzer));
        assertEquals(ImmutableList.of("first1", "second2"),
                LuceneIndex.tokenize("first1_second2", analyzer));
        assertEquals(ImmutableList.of("first", "second"),
                LuceneIndex.tokenize("first. second", analyzer));
        assertEquals(ImmutableList.of("first", "second"),
                LuceneIndex.tokenize("first.second", analyzer));

        assertEquals(ImmutableList.of("hello", "world"),
                LuceneIndex.tokenize("hello-world", analyzer));
        assertEquals(ImmutableList.of("hello", "wor*"),
                LuceneIndex.tokenize("hello-wor*", analyzer));
        assertEquals(ImmutableList.of("*llo", "world"),
                LuceneIndex.tokenize("*llo-world", analyzer));
        assertEquals(ImmutableList.of("*llo", "wor*"),
                LuceneIndex.tokenize("*llo-wor*", analyzer));
    }

    @Test
    public void luceneWithFSDirectory() throws Exception{
        //Issue is not reproducible with MemoryNodeBuilder and
        //MemoryNodeState as they cannot determine change in childNode without
        //entering
        NodeStore nodeStore = new SegmentNodeStore();
        final IndexTracker tracker = new IndexTracker();
        ((Observable)nodeStore).addObserver(new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                tracker.update(root);
            }
        });
        builder = nodeStore.getRoot().builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder idxb = newLuceneIndexDefinition(index, "lucene",
                ImmutableSet.of(TYPENAME_STRING));
        idxb.setProperty(PERSISTENCE_NAME, PERSISTENCE_FILE);
        idxb.setProperty(PERSISTENCE_PATH, getIndexDir());

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        builder = nodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");

        NodeState indexed = nodeStore.merge(builder, HOOK, CommitInfo.EMPTY);

        assertQuery(tracker, indexed, "foo", "bar");

        builder = nodeStore.getRoot().builder();
        builder.setProperty("foo2", "bar2");
        indexed = nodeStore.merge(builder, HOOK, CommitInfo.EMPTY);

        assertQuery(tracker, indexed, "foo2", "bar2");
    }

    @Test
    public void luceneWithCopyOnReadDir() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene",
                ImmutableSet.of(TYPENAME_STRING));

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        File indexRootDir = new File(getIndexDir());
        IndexTracker tracker = new IndexTracker(new IndexCopier(sameThreadExecutor(), indexRootDir));
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo", "bar");

        builder = indexed.builder();
        builder.setProperty("foo2", "bar2");
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo2", "bar2");
    }


    private void assertQuery(IndexTracker tracker, NodeState indexed, String key, String value){
        AdvancedQueryIndex queryIndex = new LuceneIndex(tracker, analyzer, null);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty(key, Operator.EQUAL,
                PropertyValues.newString(value));
        Cursor cursor = queryIndex.query(createPlan(filter), indexed);
        assertTrue(cursor.hasNext());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    private String getIndexDir(){
        File dir = new File("target", "indexdir"+System.nanoTime());
        return dir.getAbsolutePath();
    }

    private IndexPlan createPlan(Filter filter){
        return new IndexPlan.Builder()
                .setFilter(filter)
                .setAttribute(LuceneIndex.ATTR_INDEX_PATH, "/oak:index/lucene")
                .build();
    }

}
