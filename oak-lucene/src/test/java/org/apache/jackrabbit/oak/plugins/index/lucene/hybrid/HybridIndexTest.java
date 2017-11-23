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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import ch.qos.logback.classic.Level;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.IndexingMode;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LucenePropertyIndex;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.OptionalEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder.IndexRule;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

public class HybridIndexTest extends AbstractQueryTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    private OptionalEditorProvider optionalEditorProvider = new OptionalEditorProvider();
    private NRTIndexFactory nrtIndexFactory;
    private LuceneIndexProvider luceneIndexProvider;
    private NodeStore nodeStore;
    private DocumentQueue queue;
    private Clock clock = new Clock.Virtual();
    private Whiteboard wb;

    private long refreshDelta = TimeUnit.SECONDS.toMillis(1);

    @After
    public void tearDown() throws IOException {
        luceneIndexProvider.close();
        new ExecutorCloser(executorService).close();
        nrtIndexFactory.close();
    }

    @Override
    protected ContentRepository createRepository() {
        IndexCopier copier;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MountInfoProvider mip = defaultMountInfoProvider();

        nrtIndexFactory = new NRTIndexFactory(copier, clock, TimeUnit.MILLISECONDS.toSeconds(refreshDelta), StatisticsProvider.NOOP);
        nrtIndexFactory.setAssertAllResourcesClosed(true);
        LuceneIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(mip, copier);
        IndexTracker tracker = new IndexTracker(indexReaderFactory,nrtIndexFactory);
        luceneIndexProvider = new LuceneIndexProvider(tracker);
        queue = new DocumentQueue(100, tracker, sameThreadExecutor());
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier,
                tracker,
                null,
                null,
                mip);
        editorProvider.setIndexingQueue(queue);

        LocalIndexObserver localIndexObserver = new LocalIndexObserver(queue, StatisticsProvider.NOOP);

        nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) luceneIndexProvider)
                .with((Observer) luceneIndexProvider)
                .with(localIndexObserver)
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .with(optionalEditorProvider)
                .with(new NodeCounterEditorProvider())
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

        wb = oak.getWhiteboard();
        return oak.createContentRepository();
    }

    @Test
    public void hybridIndex() throws Exception{
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, IndexingMode.NRT);
        root.commit();

        //Get initial indexing done as local indexing only work
        //for incremental indexing
        createPath("/a").setProperty("foo", "bar");
        root.commit();

        runAsyncIndex();

        setTraversalEnabled(false);
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        //Add new node. This would not be reflected in result as local index would not be updated
        createPath("/b").setProperty("foo", "bar");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        //Now let some time elapse such that readers can be refreshed
        clock.waitUntil(clock.getTime() + refreshDelta + 1);

        //Now recently added stuff should be visible without async indexing run
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b"));

        createPath("/c").setProperty("foo", "bar");
        root.commit();

        //Post async index it should still be upto date
        runAsyncIndex();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b", "/c"));
    }

    @Test
    public void noTextExtractionForSyncCommit() throws Exception{
        String idxName = "hybridtest";
        Tree idx = createFulltextIndex(root.getTree("/"), idxName);
        TestUtil.enableIndexingMode(idx, IndexingMode.NRT);
        root.commit();

        runAsyncIndex();

        AccessRecordingBlob testBlob =
                new AccessRecordingBlob("<?xml version=\"1.0\" encoding=\"UTF-8\"?><msg>sky is blue</msg>".getBytes());

        Tree test = root.getTree("/").addChild("test");
        TestUtil.createFileNode(test, "msg", testBlob, "application/xml");
        root.commit();

        assertEquals(0, testBlob.accessCount);
        assertQuery("select * from [nt:base] where CONTAINS(*, 'sky')", Collections.<String>emptyList());

        runAsyncIndex();
        assertEquals(1, testBlob.accessCount);
        assertQuery("select * from [nt:base] where CONTAINS(*, 'sky')", of("/test/msg/jcr:content"));

    }

    @Test
    public void hybridIndexSync() throws Exception{
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, IndexingMode.SYNC);
        root.commit();

        //Get initial indexing done as local indexing only work
        //for incremental indexing
        createPath("/a").setProperty("foo", "bar");
        root.commit();

        runAsyncIndex();

        setTraversalEnabled(false);
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        //Add new node. This should get immediately reelected as its a sync index
        createPath("/b").setProperty("foo", "bar");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b"));
    }

    @Test
    public void usageBeforeFirstIndex() throws Exception{
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, IndexingMode.SYNC);
        root.commit();

        createPath("/a").setProperty("foo", "bar");
        root.commit();
        setTraversalEnabled(false);
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        //Add new node. This should get immediately reelected as its a sync index
        createPath("/b").setProperty("foo", "bar");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b"));

        runAsyncIndex();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b"));

        createPath("/c").setProperty("foo", "bar");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b", "/c"));
    }

    @Test
    public void newNodeTypesFoundLater() throws Exception{
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, ImmutableSet.of("foo", "bar"));
        TestUtil.enableIndexingMode(idx, IndexingMode.SYNC);
        root.commit();

        setTraversalEnabled(false);

        createPath("/a").setProperty("foo", "bar");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        optionalEditorProvider.delegate = new TypeEditorProvider(false);
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        root.refresh();

        Tree b = createPath("/b");
        b.setProperty(JcrConstants.JCR_PRIMARYTYPE, "oak:TestNode", Type.NAME);
        b.setProperty("bar", "foo");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [bar] = 'foo'", of("/b"));
    }

    @Test
    public void newNodeTypesFoundLater2() throws Exception{
        String idxName = "hybridtest";
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.indexRule("oak:TestNode")
                .property(JcrConstants.JCR_PRIMARYTYPE).propertyIndex();
        idx.indexRule("nt:base")
                .property("foo").propertyIndex()
                .property("bar").propertyIndex();
        idx.async("async","sync");
        idx.build(root.getTree("/").getChild("oak:index").addChild(idxName));

        //By default nodetype index indexes every nodetype. Declare a specific list
        //such that it does not indexes test nodetype
        Tree nodeType = root.getTree("/oak:index/nodetype");
        if (!nodeType.hasProperty(IndexConstants.DECLARING_NODE_TYPES)){
            nodeType.setProperty(IndexConstants.DECLARING_NODE_TYPES, ImmutableList.of("nt:file"), Type.NAMES);
            nodeType.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        }

        root.commit();

        setTraversalEnabled(false);

        createPath("/a").setProperty("foo", "bar");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        optionalEditorProvider.delegate = new TypeEditorProvider(false);
        NodeTypeRegistry.register(root, IOUtils.toInputStream(TestUtil.TEST_NODE_TYPE), "test nodeType");
        root.refresh();

        Tree b = createPath("/b");
        b.setProperty(JcrConstants.JCR_PRIMARYTYPE, "oak:TestNode", Type.NAME);
        b.setProperty("bar", "foo");

        Tree c = createPath("/c");
        c.setProperty(JcrConstants.JCR_PRIMARYTYPE, "oak:TestNode", Type.NAME);
        root.commit();

        String query = "select [jcr:path] from [oak:TestNode] ";
        assertThat(explain(query), containsString("/oak:index/hybridtest"));
        assertQuery(query, of("/b", "/c"));
    }

    @Test
    public void noFileLeaks() throws Exception{
        nrtIndexFactory.setDirectoryFactory(new NRTDirectoryFactory() {
            @Override
            public Directory createNRTDir(IndexDefinition definition, File indexDir) throws IOException {
                Directory fsdir = new SimpleFSDirectory(indexDir, NoLockFactory.getNoLockFactory());
                //TODO make these configurable
                return new NRTCachingDirectory(fsdir, 0.001, 0.001);
            }
        });
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, IndexingMode.SYNC);
        root.commit();
        runAsyncIndex();

        createPath("/a").setProperty("foo", "bar");
        root.commit();
        runAsyncIndex();

        System.out.printf("Open file count - At start %d%n", getOpenFileCount());
        long fileCount1 = createTestDataAndRunAsync("/content/a", 100);
        long fileCount2 = createTestDataAndRunAsync("/content/b", 100);
        long fileCount3 = createTestDataAndRunAsync("/content/c", 100);
        long fileCount4 = createTestDataAndRunAsync("/content/d", 1);
        long fileCount5 = createTestDataAndRunAsync("/content/e", 1);
        System.out.printf("Open file count - At end %d", getOpenFileCount());

        assertThat(fileCount4, lessThanOrEqualTo(fileCount3));
    }

    @Test
    public void paging() throws Exception{
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, IndexingMode.SYNC);
        root.commit();
        runAsyncIndex();

        createTestData("/content", LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE * 2);
        runAsyncIndex();

        String query = "select [jcr:path] from [nt:base] where [foo] = 'bar'";
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        Iterator<? extends ResultRow> itr = result.getRows().iterator();
        int count = 0;
        for (int i = 0; i < LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE - 10; i++) {
            assertTrue(itr.hasNext());
            itr.next();
            count++;
        }

        createTestData("/content2", 5);
        LogCustomizer lc = LogCustomizer.forLogger(LucenePropertyIndex.class.getName())
                .filter(Level.WARN)
                .create();
        lc.starting();

        int size = Iterators.size(itr);

        if (!lc.getLogs().isEmpty()){
            fail(lc.getLogs().toString());
        }

        lc.finished();

        int totalSize = LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE * 2 + 5;
        assertEquals(totalSize, count + size);
    }

    private long createTestDataAndRunAsync(String parentPath, int count) throws Exception {
        createTestData(parentPath, count);
        System.out.printf("Open file count - Post creation of %d nodes at %s is %d%n",count, parentPath, getOpenFileCount());
        runAsyncIndex();
        long openCount = getOpenFileCount();
        System.out.printf("Open file count - Post async run at %s is %d%n",parentPath, openCount);
        return openCount;
    }

    private void createTestData(String parentPath, int count) throws CommitFailedException {
        createPath(parentPath);
        root.commit();

        for (int i = 0; i < count; i++) {
            Tree parent = root.getTree(parentPath);
            Tree t = parent.addChild("testNode"+i);
            t.setProperty("foo", "bar");
            root.commit();
        }
    }

    private static long getOpenFileCount() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("java.lang:type=OperatingSystem");
        Long val = null;
        try {
            val = (Long) server.getAttribute(name, "OpenFileDescriptorCount");
        } catch (AttributeNotFoundException e) {
            //This attribute is only present if the os is unix i.e. when UnixOperatingSystemMXBean
            //is the mbean in use. If running on windows the test would be assumed to be true
            assumeNoException(e);
        }
        //dumpOpenFilePaths();
        return val;
    }

    private static void dumpOpenFilePaths() throws IOException {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        String pid = jvmName.split("@")[0];

        CommandLine cl = new CommandLine("/bin/sh");
        cl.addArguments(new String[]{"-c", "lsof -p "+pid+" | grep '/nrt'"}, false);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(baos));
        executor.execute(cl);
        System.out.println(new String(baos.toByteArray()));
    }

    private String explain(String query){
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private void runAsyncIndex() {
        AsyncIndexUpdate async = (AsyncIndexUpdate) WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
            @Override
            public boolean apply(@Nullable Runnable input) {
                return input instanceof AsyncIndexUpdate;
            }
        });
        assertNotNull(async);
        async.run();
        if (async.isFailing()) {
            fail("AsyncIndexUpdate failed");
        }
        root.refresh();
    }

    private Tree createPath(String path){
        Tree base = root.getTree("/");
        for (String e : PathUtils.elements(path)){
            base = base.addChild(e);
        }
        return base;
    }

    private static Tree createIndex(Tree index, String name, Set<String> propNames){
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        IndexRule rule = idx.indexRule("nt:base");
        for (String propName : propNames){
            rule.property(propName).propertyIndex();
        }
        Tree idxTree = index.getChild("oak:index").addChild(name);
        idx.build(idxTree);
        return idxTree;
    }

    private static Tree createFulltextIndex(Tree index, String name){
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.evaluatePathRestrictions();
        idx.indexRule("nt:base")
                .property(LuceneIndexConstants.REGEX_ALL_PROPS, true)
                .analyzed()
                .nodeScopeIndex()
                .useInExcerpt();
        Tree idxTree = index.getChild("oak:index").addChild(name);
        idx.build(idxTree);
        return idxTree;
    }

    private static class AccessRecordingBlob extends ArrayBasedBlob {
        int accessCount = 0;
        public AccessRecordingBlob(byte[] value) {
            super(value);
        }

        @Nonnull
        @Override
        public InputStream getNewStream() {
            accessCount++;
            return super.getNewStream();
        }
    }
}
