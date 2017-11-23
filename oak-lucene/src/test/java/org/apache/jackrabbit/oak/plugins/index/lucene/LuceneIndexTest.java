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

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static java.util.Arrays.asList;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANALYZERS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_FILTERS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ANL_TOKENIZER;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.NT_TEST;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.createNodeWithType;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newLuceneIndexDefinitionV2;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils.getIndexWriterConfig;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DefaultDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("ConstantConditions")
public class LuceneIndexTest {

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(
                    new LuceneIndexEditorProvider()));

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    private Set<File> dirs = newHashSet();

    private IndexTracker tracker;

    @Test
    public void testLuceneV1NonExistentProperty() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder defn = newLuceneIndexDefinition(index, "lucene", ImmutableSet.of("String"));
        defn.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V1.getVersion());

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "value-with-dash");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LuceneIndex(tracker, null);

        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.setFullTextConstraint(FullTextParser.parse("foo", "value-with*"));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, builder.getNodeState());
        Cursor cursor = queryIndex.query(plans.get(0), indexed);
        assertTrue(cursor.hasNext());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());

        //Now perform a query against a field which does not exist
        FilterImpl filter2 = createFilter(NT_BASE);
        filter2.restrictPath("/", Filter.PathRestriction.EXACT);
        filter2.setFullTextConstraint(FullTextParser.parse("baz", "value-with*"));
        List<IndexPlan> plans2 = queryIndex.getPlans(filter2, null, builder.getNodeState());
        Cursor cursor2 = queryIndex.query(plans2.get(0), indexed);
        assertFalse(cursor2.hasNext());
    }


    @Test
    public void testLucene() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        List<IndexPlan>  plans = queryIndex.getPlans(filter, null, builder.getNodeState());
        Cursor cursor = queryIndex.query(plans.get(0), indexed);
        assertTrue(cursor.hasNext());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testLuceneLazyCursor() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");

        for(int i = 0; i < LuceneIndex.LUCENE_QUERY_BATCH_SIZE; i++){
            builder.child("parent").child("child"+i).setProperty("foo", "bar");
        }

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

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
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar");
        builder.child("a").child("b").child("c").setProperty("foo", "bar");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        // filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        assertTrue(cursor.hasNext());
        assertEquals("/a/b/c", cursor.next().getPath());
        assertEquals("/a/b", cursor.next().getPath());
        assertEquals("/a", cursor.next().getPath());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }


    @Test
    public void testLucene3() throws Exception {
        NodeBuilder index = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        NodeBuilder rules = index.child(INDEX_RULES);
        NodeBuilder fooProp = rules.child("nt:base").child(LuceneIndexConstants.PROP_NODE).child("foo");
        fooProp.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        fooProp.setProperty(LuceneIndexConstants.PROP_INCLUDED_TYPE, PropertyType.TYPENAME_STRING);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar", Type.NAME);
        builder.child("a").child("b").child("c")
                .setProperty("foo", "bar", Type.NAME);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        // filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        assertTrue(cursor.hasNext());
        assertEquals("/a", cursor.next().getPath());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testCursorStability() throws Exception {
        NodeBuilder index = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        NodeBuilder rules = index.child(INDEX_RULES);
        NodeBuilder fooProp = rules.child("nt:base").child(LuceneIndexConstants.PROP_NODE).child("foo");
        fooProp.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

        //1. Create 60 nodes
        NodeState before = builder.getNodeState();
        int noOfDocs = LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE + 10;
        for (int i = 0; i < noOfDocs; i++) {
            builder.child("a"+i).setProperty("foo", (long)i);
        }
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);

        //Perform query and get hold of cursor
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictProperty("foo", Operator.GREATER_OR_EQUAL, PropertyValues.newLong(0L));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        //Trigger loading of cursor
        assertTrue(cursor.hasNext());

        //Now before traversing further go ahead and delete all but 10 nodes
        before = indexed;
        builder = indexed.builder();
        for (int i = 0; i < noOfDocs - 10; i++) {
            builder.child("a"+i).remove();
        }
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        builder = indexed.builder();

        //Ensure that Lucene actually removes deleted docs
        NodeBuilder idx = builder.child(INDEX_DEFINITIONS_NAME).child("lucene");
        purgeDeletedDocs(idx, new IndexDefinition(root, idx.getNodeState(), "/foo"));
        int numDeletes = getDeletedDocCount(idx, new IndexDefinition(root, idx.getNodeState(), "/foo"));
        Assert.assertEquals(0, numDeletes);

        //Update the IndexSearcher
        tracker.update(builder.getNodeState());

        //its hard to get correct size estimate as post deletion cursor
        // would have already picked up 50 docs which would not be considered
        //deleted by QE for the revision at which query was triggered
        //So just checking for >
        List<String> resultPaths = Lists.newArrayList();
        while(cursor.hasNext()){
            resultPaths.add(cursor.next().getPath());
        }

        Set<String> uniquePaths = Sets.newHashSet(resultPaths);
        assertEquals(resultPaths.size(), uniquePaths.size());
        assertTrue(!uniquePaths.isEmpty());
    }

    private void purgeDeletedDocs(NodeBuilder idx, IndexDefinition definition) throws IOException {
        Directory dir = new DefaultDirectoryFactory(null, null).newInstance(definition, idx, LuceneIndexConstants.INDEX_DATA_CHILD_NAME, false);
        IndexWriter writer = new IndexWriter(dir, getIndexWriterConfig(definition, true));
        writer.forceMergeDeletes();
        writer.close();
    }

    public int getDeletedDocCount(NodeBuilder idx, IndexDefinition definition) throws IOException {
        Directory  dir = new DefaultDirectoryFactory(null, null).newInstance(definition, idx, LuceneIndexConstants.INDEX_DATA_CHILD_NAME, false);
        IndexReader reader = DirectoryReader.open(dir);
        int numDeletes = reader.numDeletedDocs();
        reader.close();
        return numDeletes;
    }

    @Test
    public void testPropertyNonExistence() throws Exception {
        root = TestUtil.registerTestNodeType(builder).getNodeState();

        NodeBuilder index = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        NodeBuilder rules = index.child(INDEX_RULES);
        NodeBuilder propNode = rules.child(NT_TEST).child(LuceneIndexConstants.PROP_NODE);

        NodeBuilder fooProp = propNode.child("foo");
        fooProp.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        fooProp.setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true);

        NodeState before = builder.getNodeState();
        createNodeWithType(builder, "a", NT_TEST).setProperty("foo", "bar");
        createNodeWithType(builder, "b", NT_TEST).setProperty("foo", "bar");
        createNodeWithType(builder, "c", NT_TEST);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);

        FilterImpl filter = createFilter(NT_TEST);
        filter.restrictProperty("foo", Operator.EQUAL, null);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/c"));
    }

    @Test
    public void testPropertyExistence() throws Exception {
        root = TestUtil.registerTestNodeType(builder).getNodeState();

        NodeBuilder index = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        NodeBuilder rules = index.child(INDEX_RULES);
        NodeBuilder propNode = rules.child(NT_TEST).child(LuceneIndexConstants.PROP_NODE);

        NodeBuilder fooProp = propNode.child("foo");
        fooProp.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        fooProp.setProperty(LuceneIndexConstants.PROP_NOT_NULL_CHECK_ENABLED, true);

        NodeState before = builder.getNodeState();
        createNodeWithType(builder, "a", NT_TEST).setProperty("foo", "bar");
        createNodeWithType(builder, "b", NT_TEST).setProperty("foo", "bar");
        createNodeWithType(builder, "c", NT_TEST);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);

        FilterImpl filter = createFilter(NT_TEST);
        filter.restrictProperty("foo", Operator.NOT_EQUAL, null);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/a","/b"));
    }


    @Test
    public void testRelativePropertyNonExistence() throws Exception {
        root = TestUtil.registerTestNodeType(builder).getNodeState();

        NodeBuilder index = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        NodeBuilder rules = index.child(INDEX_RULES);
        NodeBuilder propNode = rules.child(NT_TEST).child(LuceneIndexConstants.PROP_NODE);

        propNode.child("bar")
                .setProperty(LuceneIndexConstants.PROP_NAME, "jcr:content/bar")
                .setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true)
                .setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true);

        NodeState before = builder.getNodeState();

        NodeBuilder a1 = createNodeWithType(builder, "a1", NT_TEST);
        a1.child("jcr:content").setProperty("bar", "foo");

        NodeBuilder b1 = createNodeWithType(builder, "b1", NT_TEST);
        b1.child("jcr:content");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);

        FilterImpl filter = createFilter(NT_TEST);
        filter.restrictProperty("jcr:content/bar", Operator.EQUAL, null);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/b1"));

        builder.child("b1").child("jcr:content").setProperty("bar", "foo");
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        filter = createFilter(NT_TEST);
        filter.restrictProperty("jcr:content/bar", Operator.EQUAL, null);
        assertFilter(filter, queryIndex, indexed, Collections.<String>emptyList());
    }

    @Test
    public void testPathRestrictions() throws Exception {
        NodeBuilder idx = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        idx.setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a1").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar");
        builder.child("a").child("b").child("c").setProperty("foo", "bar");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);

        FilterImpl filter = createTestFilter();
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/"));

        filter = createTestFilter();
        filter.restrictPath("/", Filter.PathRestriction.DIRECT_CHILDREN);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/a", "/a1"));

        filter = createTestFilter();
        filter.restrictPath("/a", Filter.PathRestriction.DIRECT_CHILDREN);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/a/b"));

        filter = createTestFilter();
        filter.restrictPath("/a", Filter.PathRestriction.ALL_CHILDREN);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/a/b", "/a/b/c"));
    }

    @Test
    public void nodeNameIndex() throws Exception{
        NodeBuilder index = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        NodeBuilder rules = index.child(INDEX_RULES);
        NodeBuilder ruleNode = rules.child(NT_FILE);
        ruleNode.setProperty(LuceneIndexConstants.INDEX_NODE_NAME, true);

        NodeState before = builder.getNodeState();
        createNodeWithType(builder, "foo", NT_FILE);
        createNodeWithType(builder, "camelCase", NT_FILE);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);

        FilterImpl filter = createFilter(NT_FILE);
        filter.restrictProperty(QueryConstants.RESTRICTION_LOCAL_NAME, Operator.EQUAL, PropertyValues.newString("foo"));
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/foo"));

        filter = createFilter(NT_FILE);
        filter.restrictProperty(QueryConstants.RESTRICTION_LOCAL_NAME, Operator.LIKE, PropertyValues.newString("camelCase"));
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/camelCase"));

        filter = createFilter(NT_FILE);
        filter.restrictProperty(QueryConstants.RESTRICTION_LOCAL_NAME, Operator.LIKE, PropertyValues.newString("camel%"));
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/camelCase"));
    }

    private FilterImpl createTestFilter(){
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        return filter;
    }

    @Test
    public void analyzerWithStopWords() throws Exception{
        NodeBuilder nb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "lucene",
                of(TYPENAME_STRING));
        TestUtil.useV2(nb);
        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "fox jumping");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);

        FilterImpl filter = createFilter("nt:base");

        filter.setFullTextConstraint(new FullTextTerm(null, "fox jumping", false, false, null));
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/"));

        //No stop word configured so default analyzer would also check for 'was'
        filter.setFullTextConstraint(new FullTextTerm(null, "fox was jumping", false, false, null));
        assertFilter(filter, queryIndex, indexed, Collections.<String>emptyList());

        //Change the default analyzer to use the default stopword set
        //and trigger a reindex such that new analyzer is used
        NodeBuilder anlnb = nb.child(ANALYZERS).child(ANL_DEFAULT);
        anlnb.child(ANL_TOKENIZER).setProperty(ANL_NAME, "whitespace");
        anlnb.child(ANL_FILTERS).child("stop");
        nb.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        before = after;
        after = builder.getNodeState();

        indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);
        tracker.update(indexed);
        queryIndex = new LucenePropertyIndex(tracker);

        filter.setFullTextConstraint(new FullTextTerm(null, "fox jumping", false, false, null));
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/"));

        //Now this should get passed as the analyzer would ignore 'was'
        filter.setFullTextConstraint(new FullTextTerm(null, "fox was jumping", false, false, null));
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/"));
    }

    @Test
    public void customScoreQuery() throws Exception{
        NodeBuilder nb = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "lucene",
                of(TYPENAME_STRING));
        TestUtil.useV2(nb);
        nb.setProperty(LuceneIndexConstants.PROP_SCORER_PROVIDER, "testScorer");

        NodeState before = builder.getNodeState();
        builder.child("a").setProperty("jcr:createdBy", "bar bar");
        builder.child("b").setProperty("jcr:createdBy", "foo bar");
        NodeState after = builder.getNodeState();
        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);
        tracker = new IndexTracker();
        tracker.update(indexed);

        SimpleScorerFactory factory = new SimpleScorerFactory();
        ScorerProvider provider = new ScorerProvider() {

            String scorerName = "testScorer";
            @Override
            public String getName() {
                return scorerName;
            }
            @Override
            public CustomScoreQuery createCustomScoreQuery(Query query) {
                return new ModifiedCustomScoreQuery(query);
            }

            class ModifiedCustomScoreQuery extends CustomScoreQuery {
                private Query query;
                public ModifiedCustomScoreQuery(Query query) {
                    super(query);
                    this.query = query;
                }

                @Override
                public CustomScoreProvider getCustomScoreProvider(AtomicReaderContext context) {
                    return new CustomScoreProvider(context) {
                        public float customScore(int doc, float subQueryScore, float valSrcScore) {
                            AtomicReader atomicReader = context.reader();
                            try {
                                Document document = atomicReader.document(doc);
                                // boosting docs created by foo
                                String fieldValue = document.get("full:jcr:createdBy");
                                if (fieldValue != null && fieldValue.contains("foo")) {
                                    valSrcScore *= 2.0;
                                }
                            } catch (IOException e) {
                                return subQueryScore * valSrcScore;
                            }
                            return subQueryScore * valSrcScore;
                        }
                    };
                }
            }
        };

        factory.providers.put(provider.getName(), provider);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker, factory);

        FilterImpl filter = createFilter(NT_BASE);
        filter.setFullTextConstraint(new FullTextTerm(null, "bar", false, false, null));
        assertFilter(filter, queryIndex, indexed, asList("/b", "/a"), true);
    }

    @Test
    public void testTokens() {
        Analyzer analyzer = LuceneIndexConstants.ANALYZER;
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
        NodeStore nodeStore = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        tracker = new IndexTracker();
        ((Observable)nodeStore).addObserver(new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
                tracker.update(root);
            }
        });
        builder = nodeStore.getRoot().builder();

        //Also initialize the NodeType registry required for Lucene index to work
        builder.setChildNode(JCR_SYSTEM, INITIAL_CONTENT.getChildNode(JCR_SYSTEM));
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder idxb = newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo", "foo2"), null);
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
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo", "foo2"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        File indexRootDir = new File(getIndexDir());
        tracker = new IndexTracker(new IndexCopier(sameThreadExecutor(), indexRootDir));
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo", "bar");

        builder = indexed.builder();
        builder.setProperty("foo2", "bar2");
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo2", "bar2");
    }

    @Test
    public void luceneWithCopyOnReadDirAndReindex() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder defnState =
                newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo", "foo2", "foo3"), null);
        IndexDefinition definition = new IndexDefinition(root, defnState.getNodeState(), "/foo");

        //1. Create index in two increments
        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");

        NodeState indexed = HOOK.processCommit(before, builder.getNodeState(),CommitInfo.EMPTY);

        IndexCopier copier = new IndexCopier(sameThreadExecutor(), new File(getIndexDir()));
        tracker = new IndexTracker(copier);
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo", "bar");
        assertEquals(0, copier.getInvalidFileCount());

        builder = indexed.builder();
        builder.setProperty("foo2", "bar2");
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo2", "bar2");
        assertEquals(0, copier.getInvalidFileCount());

        //2. Reindex. This would create index with different index content
        builder = indexed.builder();
        builder.child(INDEX_DEFINITIONS_NAME).child("lucene").setProperty(REINDEX_PROPERTY_NAME, true);
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);

        defnState = builder.child(INDEX_DEFINITIONS_NAME).child("lucene");
        definition = new IndexDefinition(root, defnState.getNodeState(), "/foo");
        assertQuery(tracker, indexed, "foo2", "bar2");
        //If reindex case handled properly then invalid count should be zero
        assertEquals(0, copier.getInvalidFileCount());
        assertEquals(2, copier.getIndexRootDirectory().getLocalIndexes("/oak:index/lucene").size());

        //3. Update again. Now with close of previous reader
        //orphaned directory must be removed
        builder = indexed.builder();
        builder.setProperty("foo3", "bar3");
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);
        assertQuery(tracker, indexed, "foo3", "bar3");
        assertEquals(0, copier.getInvalidFileCount());
        List<LocalIndexDir> idxDirs = copier.getIndexRootDirectory().getLocalIndexes("/oak:index/lucene");
        List<LocalIndexDir> nonEmptyDirs = Lists.newArrayList();
        for (LocalIndexDir dir : idxDirs){
            if (!dir.isEmpty()){
                nonEmptyDirs.add(dir);
            }
        }
        assertEquals(1, nonEmptyDirs.size());
    }

    @Test
    public void multiValuesForOrderedIndexShouldNotThrow() {
        NodeBuilder index = newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "lucene", null);
        NodeBuilder singleProp = TestUtil.child(index, "indexRules/nt:base/properties/single");
        singleProp.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        singleProp.setProperty(LuceneIndexConstants.PROP_ORDERED, true);
        singleProp.setProperty(LuceneIndexConstants.PROP_INCLUDED_TYPE, PropertyType.TYPENAME_STRING);

        NodeState before = builder.getNodeState();
        builder.setProperty("single", asList("baz", "bar"), Type.STRINGS);
        NodeState after = builder.getNodeState();

        try {
            HOOK.processCommit(before, after, CommitInfo.EMPTY);
        } catch (CommitFailedException e) {
            fail("Exception thrown when indexing invalid content");
        }
    }

    @Test
    public void indexNodeLockHandling() throws Exception{
        tracker = new IndexTracker();

        //Create 2 indexes. /oak:index/lucene and /test/oak:index/lucene
        //The way LuceneIndexLookup works is. It collect child first and then
        //parent
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene", of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo"), STRINGS));

        index = builder.child("test").child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb2 = newLuceneIndexDefinitionV2(index, "lucene", of(TYPENAME_STRING));
        nb2.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb2.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo"), STRINGS));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        QueryIndex.AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/test", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));

        builder = indexed.builder();
        NodeBuilder dir = builder.child("oak:index").child("lucene").child(":data");

        //Mutate the blob to fail on access i.e. create corrupt index
        List<Blob> blobs = new ArrayList<Blob>();
        Blob b = dir.child("segments_1").getProperty(JCR_DATA).getValue(Type.BINARY, 0);
        FailingBlob fb = new FailingBlob(IOUtils.toByteArray(b.getNewStream()));
        blobs.add(fb);
        dir.child("segments_1").setProperty(JCR_DATA, blobs, BINARIES);
        indexed = builder.getNodeState();
        tracker.update(indexed);

        List<IndexPlan> list = queryIndex.getPlans(filter, null, indexed);
        assertEquals("There must be only one plan", 1, list.size());
        IndexPlan plan = list.get(0);
        assertEquals("Didn't get the expected plan", "/test/oak:index/lucene", plan.getPlanName());
    }

    @Test
    public void indexNameIsIndexPath() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        String indexPath = "/oak:index/lucene";
        IndexDefinition defn = new IndexDefinition(root, indexed.getChildNode("oak:index").getChildNode("lucene"), indexPath);

        assertEquals(indexPath, defn.getIndexName());
        assertEquals(indexPath, defn.getIndexPath());
    }


    @After
    public void cleanUp(){
        if (tracker != null) {
            tracker.close();
        }
        for (File d: dirs){
            FileUtils.deleteQuietly(d);
        }
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private void assertQuery(IndexTracker tracker, NodeState indexed, String key, String value){
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty(key, Operator.EQUAL,
                PropertyValues.newString(value));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);
        assertTrue(cursor.hasNext());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    private static List<String> assertFilter(Filter filter, AdvancedQueryIndex queryIndex,
                                             NodeState indexed, List<String> expected) {
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        List<String> paths = newArrayList();
        while (cursor.hasNext()) {
            paths.add(cursor.next().getPath());
        }
        Collections.sort(paths);
        for (String p : expected) {
            assertTrue("Expected path " + p + " not found", paths.contains(p));
        }
        assertEquals("Result set size is different \nExpected: " +
                expected + "\nActual: " + paths, expected.size(), paths.size());
        return paths;
    }

    private static List<String> assertFilter(Filter filter, AdvancedQueryIndex queryIndex,
                                             NodeState indexed, List<String> expected, boolean ordered) {
        if (!ordered) {
            return assertFilter(filter, queryIndex, indexed, expected);
        }

        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        List<String> paths = newArrayList();
        while (cursor.hasNext()) {
            paths.add(cursor.next().getPath());
        }
        for (String p : expected) {
            assertTrue("Expected path " + p + " not found", paths.contains(p));
        }
        assertEquals("Result set size is different", expected.size(), paths.size());
        return paths;
    }

    private String getIndexDir(){
        File dir = new File("target", "indexdir"+System.nanoTime());
        dirs.add(dir);
        return dir.getAbsolutePath();
    }

    private static class SimpleScorerFactory implements ScorerProviderFactory {
        final Map<String,ScorerProvider> providers = Maps.newHashMap();
        @Override
        public ScorerProvider getScorerProvider(String name) {
            return providers.get(name);
        }
    }

    private static class FailingBlob extends ArrayBasedBlob {
        public FailingBlob(byte[] b) {
            super(b);
        }

        @Nonnull
        @Override
        public InputStream getNewStream() {
            throw new UnsupportedOperationException();
        }
    }
}