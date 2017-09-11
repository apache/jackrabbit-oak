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

import static com.google.common.collect.ImmutableSet.of;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newLuceneIndexDefinitionV2;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.CIHelper;
import org.apache.jackrabbit.oak.plugins.blob.datastore.CachingFileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.test.ISO8601;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class LuceneIndexEditorTest {
    private EditorHook HOOK;

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    private IndexTracker tracker = new IndexTracker();

    private IndexNode indexNode;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Parameterized.Parameter
    public boolean useBlobStore;

    @Parameterized.Parameters(name = "{index}: useBlobStore ({0})")
    public static List<Boolean[]> fixtures() {
        return ImmutableList.of(new Boolean[] {true}, new Boolean[] {false});
    }

    @Before
    public void setup() throws Exception {
        if (useBlobStore) {
            LuceneIndexEditorProvider provider = new LuceneIndexEditorProvider();
            CachingFileDataStore ds = DataStoreUtils
                .createCachingFDS(temporaryFolder.newFolder().getAbsolutePath(),
                    temporaryFolder.newFolder().getAbsolutePath());
            provider.setBlobStore(new DataStoreBlobStore(ds));
            HOOK = new EditorHook(new IndexUpdateProvider(provider));
        } else {
            HOOK = new EditorHook(new IndexUpdateProvider(new LuceneIndexEditorProvider()));
        }
    }

    @Test
    public void testLuceneWithFullText() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder idxnb = newLuceneIndexDefinitionV2(index, "lucene",
                of(TYPENAME_STRING));
        IndexDefinition defn = new IndexDefinition(root, idxnb.getNodeState(), "/foo");
        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        builder.child("test").setProperty("price", 100);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        //system fields starts with ':' so need to be escaped
        assertEquals("/test", query(escape(FieldNames.createAnalyzedFieldName("foo"))+":fox", defn));
        assertNull("Non string properties not indexed by default",
                getPath(NumericRangeQuery.newLongRange("price", 100L, 100L, true, true)));
    }

    @Test
    public void noChangeIfNonIndexedDelete() throws Exception{
        NodeState before = builder.getNodeState();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene", of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo"), STRINGS));


        builder.child("test").setProperty("foo", "bar");
        builder.child("test").child("a");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);
        assertEquals("/test", getPath(new TermQuery(new Term("foo", "bar"))));

        NodeState luceneIdxState1 = NodeStateUtils.getNode(indexed, "/oak:index/lucene");

        before = indexed;
        builder = indexed.builder();
        builder.getChildNode("test").getChildNode("a").remove();
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        NodeState luceneIdxState2 = NodeStateUtils.getNode(indexed, "/oak:index/lucene");
        assertEquals(luceneIdxState1, luceneIdxState2);
    }

    private String escape(String name) {
        return name.replace(":", "\\:");
    }

    @Test
    public void testLuceneWithNonFullText() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene",
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo", "price", "weight", "bool", "creationTime"), STRINGS));
        IndexDefinition defn = new IndexDefinition(root, nb.getNodeState(), "/foo");
        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        builder.child("test").setProperty("bar", "kite is flying");
        builder.child("test").setProperty("price", 100);
        builder.child("test").setProperty("weight", 10.0);
        builder.child("test").setProperty("bool", true);
        builder.child("test").setProperty("truth", true);
        builder.child("test").setProperty("creationTime", createCal("05/06/2014"));
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertNull("Fulltext search should not work", query("foo:fox",defn));
        assertEquals("/test", getPath(new TermQuery(new Term("foo", "fox is jumping"))));
        assertNull("bar must NOT be indexed", getPath(new TermQuery(new Term("bar", "kite is flying"))));

        //Long
        assertEquals("/test", getPath(NumericRangeQuery.newDoubleRange("weight", 8D, 12D, true, true)));

        //Double
        assertEquals("/test", getPath(NumericRangeQuery.newLongRange("price", 100L, 100L, true, true)));

        //Boolean
        assertEquals("/test", getPath(new TermQuery(new Term("bool", "true"))));
        assertNull("truth must NOT be indexed", getPath(new TermQuery(new Term("truth", "true"))));

        //Date
        assertEquals("/test", getPath(NumericRangeQuery.newLongRange("creationTime",
                dateToTime("05/05/2014"), dateToTime("05/07/2014"), true, true)));
    }

    @Test
    public void noOfDocsIndexedNonFullText() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene",
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo"), STRINGS));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        builder.child("test2").setProperty("bar", "kite is flying");
        builder.child("test3").setProperty("foo", "wind is blowing");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertEquals(2, getSearcher().getIndexReader().numDocs());
    }

    @Test
    public void saveDirectoryListing() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene",
            of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo"), STRINGS));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        NodeState dir = indexed.getChildNode("oak:index").getChildNode("lucene").getChildNode(":data");
        assertTrue(dir.hasProperty(OakDirectory.PROP_DIR_LISTING));
    }

    /**
     * 1. Index property foo in /test
     * 2. Then modify some other property in /test
     *
     * This should not cause the index to be updated
     */
    @Test
    public void nonIncludedPropertyChange() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene",
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo"),
                STRINGS));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        builder.child("test2").setProperty("foo", "bird is chirping");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertEquals(2, getSearcher().getIndexReader().numDocs());

        assertEquals("/test", getPath(new TermQuery(new Term("foo", "fox is jumping"))));

        releaseIndexNode();
        before = indexed;
        builder = before.builder();
        builder.child("test").setProperty("bar", "kite is flying");
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertEquals(2, getSearcher().getIndexReader().numDocs());
        assertEquals("change in non included property should not cause " +
                "index update",0, getSearcher().getIndexReader().numDeletedDocs());
    }

    @Test
    public void testLuceneWithRelativeProperty() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene",
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo", "jcr:content/mime",
                "jcr:content/metadata/type"), STRINGS));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        builder.child("test").child("jcr:content").setProperty("mime", "text");
        builder.child("test").child("jcr:content").child("metadata").setProperty("type", "image");
        builder.child("jcr:content").setProperty("count", "text");
        builder.child("jcr:content").child("boom").child("metadata").setProperty("type", "image");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertEquals(1, getSearcher().getIndexReader().numDocs());

        assertEquals("/test", getPath(new TermQuery(new Term("foo", "fox is jumping"))));
        assertEquals("/test", getPath(new TermQuery(new Term("jcr:content/mime", "text"))));
        assertEquals("/test", getPath(new TermQuery(new Term("jcr:content/metadata/type", "image"))));
        assertNull("bar must NOT be indexed", getPath(new TermQuery(new Term("count", "text"))));

        releaseIndexNode();
        before = indexed;
        builder = before.builder();
        builder.child("test").child("jcr:content").setProperty("mime", "pdf");
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertEquals("/test", getPath(new TermQuery(new Term("jcr:content/mime", "pdf"))));

        releaseIndexNode();
        before = indexed;
        builder = before.builder();
        builder.child("test").child("jcr:content").remove();
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);
        assertNull("removes must be persisted too, 1st level",
                getPath(new TermQuery(new Term("jcr:content/mime", "pdf"))));
        assertNull("removes must be persisted too, 2nd level",
                getPath(new TermQuery(new Term("jcr:content/metadata/type",
                        "image"))));
    }

    @Test
    public void indexVersionSwitchOnReindex() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinition(index, "lucene",
                of(TYPENAME_STRING));

        //1. Trigger a index so that next index step does not see it as a fresh index
        NodeState indexed = HOOK.processCommit(EMPTY_NODE, builder.getNodeState(), CommitInfo.EMPTY);
        builder = indexed.builder();

        //By default logic would use current version. To simulate upgrade we forcefully set
        //version to V1
        builder.child(INDEX_DEFINITIONS_NAME).child("lucene").setProperty(IndexDefinition.INDEX_VERSION,
                IndexFormatVersion.V1.getVersion());

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        NodeState after = builder.getNodeState();

        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        assertEquals(IndexFormatVersion.V1, new IndexDefinition(root,
                indexed.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("lucene"), "/foo").getVersion());

        //3. Trigger a reindex and version should switch to current
        builder = indexed.builder();
        before = indexed;
        builder.child(INDEX_DEFINITIONS_NAME).child("lucene").setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        assertEquals(IndexFormatVersion.getDefault(), new IndexDefinition(root,
                indexed.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("lucene"), "/foo").getVersion());

    }

    @Test
    public void autoFormatUpdate() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene",
                of(TYPENAME_STRING));

        //1. Trigger a index so that next index step does not see it as a fresh index
        NodeState indexed = HOOK.processCommit(EMPTY_NODE, builder.getNodeState(), CommitInfo.EMPTY);

        IndexDefinition defn = new IndexDefinition(root, indexed.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("lucene"), "/foo");
        assertFalse(defn.isOfOldFormat());
    }

    @Test
    public void copyOnWriteAndLocks() throws Exception {
        assumeFalse(CIHelper.windows());

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        IndexCopier copier = new IndexCopier(executorService, temporaryFolder.getRoot());

        FailOnDemandEditorProvider failingProvider = new FailOnDemandEditorProvider();
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(
                        new CompositeIndexEditorProvider(
                                failingProvider,
                                new LuceneIndexEditorProvider(copier))));

        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, "lucene", of(TYPENAME_STRING));
        IndexUtils.createIndexDefinition(index, "failingIndex", false, false, of("foo"), null);


        //1. Get initial set indexed. So that next cycle is normal indexing
        NodeState indexed = hook.processCommit(EMPTY_NODE, builder.getNodeState(), CommitInfo.EMPTY);
        builder = indexed.builder();

        NodeState before = indexed;
        builder.child("test").setProperty("a", "fox is jumping");
        NodeState after = builder.getNodeState();

        //2. Ensure that Lucene gets triggered but close is not called
        failingProvider.setShouldFail(true);
        try {
            hook.processCommit(before, after, CommitInfo.EMPTY);
            fail();
        } catch (CommitFailedException ignore){

        }

        //3. Disable the troubling editor
        failingProvider.setShouldFail(false);

        //4. Now commit should process fine
        hook.processCommit(before, after, CommitInfo.EMPTY);

        executorService.shutdown();
    }


    @Test
    public void multiplexingWriter() throws Exception{
        newLucenePropertyIndex("lucene", "foo");
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/libs", "/apps").build();
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(
                        new LuceneIndexEditorProvider(null, new ExtractedTextCache(0, 0), null, mip)));

        NodeState indexed = hook.processCommit(EMPTY_NODE, builder.getNodeState(), CommitInfo.EMPTY);
        builder = indexed.builder();
        NodeState before = indexed;
        builder.child("content").child("en").setProperty("foo", "bar");
        builder.child("libs").child("install").setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        builder = indexed.builder();

        assertEquals(1, numDocs(mip.getMountByName("foo")));
        assertEquals(1, numDocs(mip.getDefaultMount()));
    }

    private int numDocs(Mount m) throws IOException {
        String indexDirName = MultiplexersLucene.getIndexDirName(m);
        NodeBuilder defnBuilder = builder.child(INDEX_DEFINITIONS_NAME).child("lucene");
        Directory d = new OakDirectory(defnBuilder, indexDirName, new IndexDefinition(root, defnBuilder.getNodeState(), "/foo"), true);
        IndexReader r = DirectoryReader.open(d);
        return r.numDocs();
    }


    //@Test
    public void checkLuceneIndexFileUpdates() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinition(index, "lucene",
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "bar", "baz"), STRINGS));
        //nb.removeProperty(REINDEX_PROPERTY_NAME);

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");

        //InfoStream.setDefault(new PrintStreamInfoStream(System.out));
        before = commitAndDump(before, builder.getNodeState());

        builder = before.builder();
        builder.child("test2").setProperty("bar", "ship is sinking");
        before = commitAndDump(before, builder.getNodeState());

        builder = before.builder();
        builder.child("test3").setProperty("baz", "horn is blowing");
        before = commitAndDump(before, builder.getNodeState());

        builder = before.builder();
        builder.child("test2").remove();
        before = commitAndDump(before, builder.getNodeState());

        builder = before.builder();
        builder.child("test2").setProperty("bar", "ship is back again");
        before = commitAndDump(before, builder.getNodeState());
    }

    @After
    public void releaseIndexNode(){
        if(indexNode != null){
            indexNode.release();
        }
        indexNode = null;
    }

    private NodeState newLucenePropertyIndex(String indexName, String propName){
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinitionV2(index, indexName,
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of(propName), STRINGS));
        return builder.getNodeState();
    }

    private String query(String query, IndexDefinition defn) throws IOException, ParseException {
        QueryParser queryParser = new QueryParser(VERSION, "", defn.getAnalyzer());
        return getPath(queryParser.parse(query));
    }

    private String getPath(Query query) throws IOException {
        TopDocs td = getSearcher().search(query, 100);
        if (td.totalHits > 0){
            if(td.totalHits > 1){
                fail("More than 1 result found for query " + query);
            }
            return getSearcher().getIndexReader().document(td.scoreDocs[0].doc).get(PATH);
        }
        return null;
    }

    private IndexSearcher getSearcher(){
        if(indexNode == null){
            indexNode = tracker.acquireIndexNode("/oak:index/lucene");
        }
        return indexNode.getSearcher();
    }

    private NodeState commitAndDump(NodeState before, NodeState after) throws CommitFailedException, IOException {
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);
        dumpIndexDir();
        return indexed;
    }

    private void dumpIndexDir() throws IOException {
        Directory dir = ((DirectoryReader)getSearcher().getIndexReader()).directory();

        System.out.println("================");
        String[] fileNames = dir.listAll();
        Arrays.sort(fileNames);
        for (String file : fileNames){
            System.out.printf("%s - %d %n", file, dir.fileLength(file));
        }
        releaseIndexNode();
    }

    static Calendar createCal(String dt) throws java.text.ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(dt));
        return cal;
    }

    static long dateToTime(String dt) throws java.text.ParseException {
        return FieldFactory.dateToLong(ISO8601.format(createCal(dt)));
    }

    private static class FailOnDemandEditorProvider implements IndexEditorProvider {

        private boolean shouldFail;

        @Override
        public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                                     @Nonnull NodeState root,
                                     @Nonnull IndexUpdateCallback callback) throws CommitFailedException {
            if (PropertyIndexEditorProvider.TYPE.equals(type)) {
                return new FailOnDemandEditor();
            }
            return null;
        }

        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }

        private class FailOnDemandEditor extends DefaultEditor implements IndexEditor {
            @Override
            public void leave(NodeState before, NodeState after)
                    throws CommitFailedException {
                throwExceptionIfTold();
                super.leave(before, after);
            }

            void throwExceptionIfTold() throws CommitFailedException {
                if (shouldFail) {
                    throw new CommitFailedException("commit",1 , null);
                }
            }
        }
    }

}
