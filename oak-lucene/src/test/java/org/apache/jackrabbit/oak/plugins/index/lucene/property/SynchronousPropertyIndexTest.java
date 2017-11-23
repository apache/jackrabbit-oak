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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.IndexingQueue;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class SynchronousPropertyIndexTest extends AbstractQueryTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private LuceneIndexProvider luceneIndexProvider;
    private IndexingQueue queue = mock(IndexingQueue.class);
    private NodeStore nodeStore = new MemoryNodeStore();
    private NRTIndexFactory nrtIndexFactory;
    private Whiteboard wb;


    private IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
    private String indexPath  = "/oak:index/foo";
    private DelayingIndexEditor delayingEditorProvider = new DelayingIndexEditor();
    private TestUtil.OptionalEditorProvider optionalEditorProvider = new TestUtil.OptionalEditorProvider();

    @Before
    public void setUp(){
        setTraversalEnabled(false);
    }

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

        nrtIndexFactory = new NRTIndexFactory(copier, Clock.SIMPLE, 1000, StatisticsProvider.NOOP);
        MountInfoProvider mip = defaultMountInfoProvider();
        LuceneIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(mip, copier);
        IndexTracker tracker = new IndexTracker(indexReaderFactory,nrtIndexFactory);

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
                .with(delayingEditorProvider)
                .with(optionalEditorProvider)
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

    /**
     * Test the scenario where a unique property is removed and then another one
     * with same value is added again
     */
    @Test
    public void uniqueProperty_RemovedAndAsync() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        createPath("/a").setProperty("foo2", "bar2");
        root.commit();
        runAsyncIndex();

        createPath("/a").removeProperty("foo");
        root.commit();

        createPath("/b").setProperty("foo", "bar");
        root.commit();
    }

    @Test
    public void uniqueProperty_RemoveAndAddInSameCommit() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        root.commit();
        runAsyncIndex();

        createPath("/a").remove();
        createPath("/b").setProperty("foo", "bar");
        root.commit();
    }

    @Test
    public void nonUniqueIndex() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").propertyIndex().sync();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        root.commit();

        assertQuery("select * from [nt:base] where [foo] = 'bar'", asList("/a"));

        runAsyncIndex();

        createPath("/b").setProperty("foo", "bar");
        root.commit();

        assertQuery("select * from [nt:base] where [foo] = 'bar'", asList("/a", "/b"));

        //Do multiple runs which lead to path being returned from both property and lucene
        //index. But the actual result should only contain unique paths
        runAsyncIndex();
        runAsyncIndex();

        assertQuery("select * from [nt:base] where [foo] = 'bar'", asList("/a", "/b"));
    }

    @Test
    public void uniquePaths() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").propertyIndex().unique();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        root.commit();

        assertQuery("select * from [nt:base] where [foo] = 'bar'", singletonList("/a"));

        runAsyncIndex();
        createPath("/b").setProperty("foo", "bar2");
        root.commit();

        runAsyncIndex();
        createPath("/c").setProperty("foo", "bar3");
        root.commit();

        assertQuery("select * from [nt:base] where [foo] = 'bar'", singletonList("/a"));
        assertQuery("select * from [nt:base] where [foo] = 'bar2'", singletonList("/b"));
        assertQuery("select * from [nt:base] where [foo] = 'bar3'", singletonList("/c"));

        createPath("/d").setProperty("foo", "bar");
        try {
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertEquals(CONSTRAINT, e.getType());
        }
    }

    @Test
    public void queryPlan() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").sync();

        addIndex(indexPath, defnb);
        root.commit();

        assertThat(explain("select * from [nt:base] where [jcr:content/foo] = 'bar'"),
                containsString("sync:(foo[jcr:content/foo] bar)"));
        assertThat(explain("select * from [nt:base] where [foo] = 'bar'"),
                containsString("sync:(foo bar)"));
    }


    @Test
    public void relativePropertyTransform() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").sync();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a/jcr:content").setProperty("foo", "bar");
        createPath("/b").setProperty("foo", "bar");
        root.commit();

        assertQuery("select * from [nt:base] where [jcr:content/foo] = 'bar'", singletonList("/a"));
    }

    @Test
    public void nonRootIndex() throws Exception{
        createPath("/content/oak:index");
        root.commit();

        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").sync();

        indexPath = "/content/oak:index/fooIndex";
        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        createPath("/content/a").setProperty("foo", "bar");
        createPath("/content/a/jcr:content").setProperty("foo", "bar");
        root.commit();

        assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') " +
                "and [jcr:content/foo] = 'bar'", singletonList("/content/a"));

        assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') " +
                "and [foo] = 'bar'", asList("/content/a", "/content/a/jcr:content"));

    }

    @Test
    public void asyncIndexerReindexAndPropertyIndexes() throws Exception{
        defnb.async("async", "nrt");
        defnb.indexRule("nt:base").property("foo").sync();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        root.commit();

        Semaphore s = new Semaphore(0);
        delayingEditorProvider.semaphore = s;

        AtomicReference<Throwable> th = new AtomicReference<>();

        Thread t = new Thread(this::runAsyncIndex);
        t.setUncaughtExceptionHandler((t1, e) -> th.set(e));
        t.start();

        while (!s.hasQueuedThreads()) {
            Thread.yield();
        }

        createPath("/b").setProperty("foo", "bar");
        root.commit();

        s.release(2);
        t.join();

        if (th.get() != null) {
            throw new AssertionError(th.get());
        }
    }

    String testNodeTypes =
            "[oak:TestMixA]\n" +
                    "  mixin\n" +
                    "\n" +
                    "[oak:TestSuperType] \n" +
                    " - * (UNDEFINED) multiple\n" +
                    "\n" +
                    "[oak:TestTypeA] > oak:TestSuperType\n" +
                    " - * (UNDEFINED) multiple\n" +
                    "\n" +
                    " [oak:TestTypeB] > oak:TestSuperType, oak:TestMixA\n" +
                    " - * (UNDEFINED) multiple\n" +
                    "\n" +
                    "  [oak:TestTypeC] > oak:TestMixA\n" +
                    " - * (UNDEFINED) multiple";

    @Test
    public void nodeTypeIndexing() throws Exception{
        registerTestNodTypes();

        defnb.async("async", "nrt");
        defnb.nodeTypeIndex();
        defnb.indexRule("oak:TestSuperType").sync();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a", "oak:TestSuperType");
        createPath("/b", "oak:TestTypeB");
        root.commit();

        assertQuery("select * from [oak:TestSuperType]", asList("/a", "/b"));

        assertThat(explain("select * from [oak:TestSuperType]"),
                containsString(indexPath));
    }

    @Test
    public void nodeType_mixins() throws Exception{
        registerTestNodTypes();

        defnb.async("async", "nrt");
        defnb.nodeTypeIndex();
        defnb.indexRule("oak:TestMixA").sync();

        addIndex(indexPath, defnb);
        root.commit();

        createPath("/a", "oak:Unstructured", singletonList("oak:TestMixA"));
        createPath("/b", "oak:TestTypeB");
        createPath("/c", "oak:TestTypeA");
        root.commit();

        assertThat(explain("select * from [oak:TestMixA]"),  containsString(indexPath));
        assertQuery("select * from [oak:TestMixA]", asList("/a", "/b"));
    }

    private void registerTestNodTypes() throws IOException, CommitFailedException {
        optionalEditorProvider.delegate = new TypeEditorProvider();
        NodeTypeRegistry.register(root, IOUtils.toInputStream(testNodeTypes, "utf-8"), "test nodeType");
        //Flush the changes to nodetypes
        root.commit();
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

    private String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
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

    private Tree createPath(String path, String primaryType){
        return createPath(path, primaryType, Collections.emptyList());
    }

    private Tree createPath(String path, String primaryType, List<String> mixins){
        Tree t = createPath(path);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, primaryType, Type.NAME);
        if (!mixins.isEmpty()) {
            t.setProperty(JcrConstants.JCR_MIXINTYPES, mixins, Type.NAMES);
        }
        return t;
    }

    private static class DelayingIndexEditor implements IndexEditorProvider {
        private Semaphore semaphore;
        @CheckForNull
        @Override
        public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                                     @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
                throws CommitFailedException {
            ContextAwareCallback ccb = (ContextAwareCallback) callback;
            if (semaphore != null && ccb.getIndexingContext().isAsync()) {
                semaphore.acquireUninterruptibly();
            }
            return null;
        }


    }
}
