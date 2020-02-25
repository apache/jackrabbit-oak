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
import java.text.ParseException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.jcr.GuestCredentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.security.auth.login.LoginException;

import ch.qos.logback.classic.Level;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import net.bytebuddy.utility.RandomString;
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
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LucenePropertyIndex;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.OptionalEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder.IndexRule;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_REFRESH_DEFN;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_STATISTICAL;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

public class HybridIndexTest extends AbstractQueryTest implements Runnable {
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
    private QueryManager qm;

    private long refreshDelta = TimeUnit.SECONDS.toMillis(1);

    @After
    public void tearDown() throws IOException {
        System.out.println(System.currentTimeMillis() +Thread.currentThread().getName() + "1");
        luceneIndexProvider.close();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() +Thread.currentThread().getName() + "2");

        new ExecutorCloser(executorService).close();
        System.out.println(System.currentTimeMillis() +Thread.currentThread().getName() + "3");

        nrtIndexFactory.close();
        System.out.println(System.currentTimeMillis() +Thread.currentThread().getName() + "4");

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
        IndexTracker tracker = new IndexTracker(indexReaderFactory, nrtIndexFactory);
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

        oak = new Oak(nodeStore)
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
        ContentRepository repo = oak.createContentRepository();


        /*jcr = new Jcr(oak);
        jcrRepo = jcr.createRepository();

        try {
            //session1 = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
            //closer.register(session::logout);

        // we'd always query anonymously
        Session anonSession = jcrRepo.login(new GuestCredentials());
        //closer.register(anonSession::logout);
        qm = anonSession.getWorkspace().getQueryManager();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }*/

        return repo;
    }

    Repository jcrRepo;
    Jcr jcr;
    Oak oak;

    @Test
    public void hybridIndex() throws Exception {
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.NRT);


        //Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        //facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        //indexNode.setProperty(PROP_REFRESH_DEFN, true);
        //session.save();


        idx.addChild(FACETS).setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);

        root.commit();

        runAsyncIndex();


        //Get initial indexing done as local indexing only work
        //for incremental indexing
        int i = 0;
        while (true) {
            testing();
            if (i == 0) {
                ++i;
                Thread t = new Thread(this);
                t.start();
            }
        }
        //Thread a = new Thread();


    }

    int NUM_LABELS = 4;
    private static final int NUM_LEAF_NODES = STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
    //private String nodeType = "oak:Unstructured";
    private String nodeType = "nt:unstructured";

    private void createSmallDataset() throws RepositoryException {
        createSmallDataset(0);
    }

    private void createSmallDataset(int k) throws RepositoryException {
        Random rGen = new Random(42);

        Tree par = createPath("/parent" + k);
//        par.setProperty("jcr:primaryType", nodeType, Type.NAME);
        par.setProperty("foo", "bar");

        for (int i = 0; i < NUM_LABELS * 2; i++) {
            Tree subPar = par.addChild("par" + i);
            // subPar.setProperty("jcr:primaryType", nodeType, Type.NAME);
            for (int j = 0; j < NUM_LEAF_NODES / (2 * NUM_LABELS); j++) {
                Tree child = subPar.addChild("c" + j);
                child.setProperty("cons", "val");
                //   child.setProperty("jcr:primaryType", nodeType, Type.NAME);

                // Add a random label out of "l0", "l1", "l2", "l3"
                int labelNum = rGen.nextInt(NUM_LABELS);
                child.setProperty("foo", "l" + labelNum);
                // child.setProperty("jcr:primaryType", nodeType, Type.NAME);
            }
        }
    }

    private void createSmallDataset1() throws RepositoryException {
        Random rGen = new Random(42);

        Tree par = createPath("/parent");
        //Tree par = createPath("/");
        par.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        for (int i = 0; i < NUM_LABELS; i++) {
            Tree subPar = par.addChild("par" + i);
            subPar.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            for (int j = 0; j < NUM_LEAF_NODES / (2 * NUM_LABELS); j++) {
                Tree child = subPar.addChild("c" + j);
                child.setProperty("cons", "val");
                child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

                // Add a random label out of "l0", "l1", "l2", "l3"
                int labelNum = rGen.nextInt(NUM_LABELS);
                child.setProperty("foo", "l1" + labelNum);
                child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            }
        }
    }

    Thread t;

    @Test
    public void hybridIndexmine() throws Exception {
        Thread.currentThread().setName("mainThread");
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName);
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.NRT);
        setTraversalEnabled(false);
        root.commit();
        jcr = new Jcr(oak);
        jcrRepo = jcr.createRepository();
        for (int i = 0; i < 1; i++) {
            createSmallDataset(i);

            if (i == 0) {
                clock.waitUntil(clock.getTime() + refreshDelta + 1);
                //runAsyncIndex();
            }
            root.commit();///----------first refresh
            //Thread.sleep(1000);
            runAsyncIndex();
            createSmallDataset(2);
            clock.waitUntil(clock.getTime() + refreshDelta + 1);
//clock.waitUntil(clock.getTime() + refreshDelta + 1);
            root.commit();

            //Thread.sleep(5000);

            try {
                Session anonSession = jcrRepo.login(new GuestCredentials());
                qm = anonSession.getWorkspace().getQueryManager();

                Query q = qm.createQuery("SELECT [rep:facet(foo)] FROM [nt:base] WHERE [cons] = 'val'", SQL2);
                System.out.println(System.currentTimeMillis() +"Quering start" + Thread.currentThread().getName() + "-----------");
                QueryResult qr = q.execute();
                System.out.println(System.currentTimeMillis() +"Quering stop " + Thread.currentThread().getName() + "-----------");
                t = new Thread(this);
                t.start();


                //t.join();

                /*root.getTree("/parent0").remove();
                createSmallDataset(3);
                clock.waitUntil(clock.getTime() + refreshDelta + 1);
                clock.waitUntil(clock.getTime() + refreshDelta + 1);
                root.commit();
                runAsyncIndex();*/


                //createToomuchData(10);
                //clock.waitUntil(clock.getTime() + refreshDelta + 1);
                //root.commit();
                //  anonSession.refresh(true);
                //  runAsyncIndex();

                FacetResult facetResult;// = new FacetResult(qr); //--------second refresh

                try {
                System.out.println(System.currentTimeMillis() +"Facet start" + Thread.currentThread().getName() + "-----------");
                    facetResult = new FacetResult(qr); //--------second refresh
                System.out.println(System.currentTimeMillis() +"Facet stop " + Thread.currentThread().getName() + "-----------");
                } catch (RuntimeException e) {
                    System.out.println(System.currentTimeMillis() +"Already closed " + Thread.currentThread().getName() + "-----------");
                    throw e;
                }
                Map<String, Integer> map = Maps.newHashMap();

                Set<String> dims = facetResult.getDimensions();
                for (String dim : dims) {
                    List<FacetResult.Facet> facets = facetResult.getFacets(dim);
                    for (FacetResult.Facet facet : facets) {
                        map.put(facet.getLabel(), facet.getCount());
                    }
                }
                System.out.println(System.currentTimeMillis() +Thread.currentThread().getName() + ", " + map.isEmpty());
                // t.join();
                //Thread.sleep(10000);
                System.out.println(System.currentTimeMillis() +Thread.currentThread().getName() + ", " + map.isEmpty());
            } catch (RepositoryException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        }
    }

    String loop = RandomString.make(7);

    private void createToomuchData(long numNodes) {
//    long numNodes = 100000;
        long i = 0;
        while (i < numNodes) {
            createPath("/" + loop + i).setProperty("cons", "val");
            i++;
        }
        loop = RandomString.make(7);
    }

    void testing() throws CommitFailedException, InterruptedException {
        try {
            createSmallDataset();

        } catch (RepositoryException e) {
            e.printStackTrace();
        }
        root.commit();
        runAsyncIndex();
        //   assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));
//executeQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", SQL2);
        //Now let some time elapse such that readers can be refreshed
        clock.waitUntil(clock.getTime() + refreshDelta + 1);


        try {
//            Query q = qm.createQuery("SELECT [rep:facet(foo)] FROM [nt:base] WHERE [cons] = 'val'", SQL2);
            Query q = qm.createQuery("select [jcr:path] from [nt:base] where [foo] = 'l1'", SQL2);
            QueryResult qr = q.execute();
            Result queryResult = executeQuery("select [jcr:path] from [nt:base] where [cons] = 'val'", SQL2, NO_BINDINGS);
            FacetResult facetResult = new FacetResult(qr);
            Map<String, Integer> map = Maps.newHashMap();

            Set<String> dims = facetResult.getDimensions();
            for (String dim : dims) {
                List<FacetResult.Facet> facets = facetResult.getFacets(dim);
                for (FacetResult.Facet facet : facets) {
                    map.put(facet.getLabel(), facet.getCount());
                }
            }
        } catch (RepositoryException | ParseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    @Test
    public void noTextExtractionForSyncCommit() throws Exception {
        String idxName = "hybridtest";
        Tree idx = createFulltextIndex(root.getTree("/"), idxName);
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.NRT);
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
    public void hybridIndexSync() throws Exception {
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.SYNC);
        root.commit();

        //Get initial indexing done as local indexing only work
        //for incremental indexing
        createPath("/a").setProperty("foo", "bar");
        createSmallDataset();
        root.commit();

        runAsyncIndex();

        setTraversalEnabled(false);
        executeQuery("select [jcr:path] from [nt:base] where [foo] = 'l2'", SQL2);
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a"));

        //Add new node. This should get immediately reelected as its a sync index
        createPath("/b").setProperty("foo", "bar");
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [foo] = 'bar'", of("/a", "/b"));
    }

    @Test
    public void usageBeforeFirstIndex() throws Exception {
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.SYNC);
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
    public void newNodeTypesFoundLater() throws Exception {
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, ImmutableSet.of("foo", "bar"));
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.SYNC);
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
    public void newNodeTypesFoundLater2() throws Exception {
        String idxName = "hybridtest";
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.indexRule("oak:TestNode")
                .property(JcrConstants.JCR_PRIMARYTYPE).propertyIndex();
        idx.indexRule("nt:base")
                .property("foo").propertyIndex()
                .property("bar").propertyIndex();
        idx.async("async", "sync");
        idx.build(root.getTree("/").getChild("oak:index").addChild(idxName));

        //By default nodetype index indexes every nodetype. Declare a specific list
        //such that it does not indexes test nodetype
        Tree nodeType = root.getTree("/oak:index/nodetype");
        if (!nodeType.hasProperty(IndexConstants.DECLARING_NODE_TYPES)) {
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
    public void noFileLeaks() throws Exception {
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
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.SYNC);
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
    public void paging() throws Exception {
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName, Collections.singleton("foo"));
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.SYNC);
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

        if (!lc.getLogs().isEmpty()) {
            fail(lc.getLogs().toString());
        }

        lc.finished();

        int totalSize = LucenePropertyIndex.LUCENE_QUERY_BATCH_SIZE * 2 + 5;
        assertEquals(totalSize, count + size);
    }

    private long createTestDataAndRunAsync(String parentPath, int count) throws Exception {
        createTestData(parentPath, count);
        System.out.printf("Open file count - Post creation of %d nodes at %s is %d%n", count, parentPath, getOpenFileCount());
        runAsyncIndex();
        long openCount = getOpenFileCount();
        System.out.printf("Open file count - Post async run at %s is %d%n", parentPath, openCount);
        return openCount;
    }

    private void createTestData(String parentPath, int count) throws CommitFailedException {
        createPath(parentPath);
        root.commit();

        for (int i = 0; i < count; i++) {
            Tree parent = root.getTree(parentPath);
            Tree t = parent.addChild("testNode" + i);
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
        cl.addArguments(new String[]{"-c", "lsof -p " + pid + " | grep '/nrt'"}, false);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(baos));
        executor.execute(cl);
        System.out.println(System.currentTimeMillis() +new String(baos.toByteArray()));
    }

    private String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private void runAsyncIndex() {
        AsyncIndexUpdate async = (AsyncIndexUpdate) WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
            @Override
            public boolean test(@Nullable Runnable input) {
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

    public void run() {

        Thread.currentThread().setName("branch thread");
        Query q = null;
        try {
            clock.waitUntil(clock.getTime() + refreshDelta + 1);
            q = qm.createQuery("SELECT [rep:facet(foo)] FROM [nt:base] WHERE [cons] = 'val'", SQL2);


System.out.println(System.currentTimeMillis() +"Quering start" + Thread.currentThread().getName() + "-----------");
            QueryResult qr = q.execute();
            System.out.println(System.currentTimeMillis() +"Quering stop " + Thread.currentThread().getName() + "-----------");


//            FacetResult facetResult;
//            try {
//                facetResult = new FacetResult(qr); //--------second refresh
//            } catch (RuntimeException e) {
//                System.out.println(System.currentTimeMillis() +"Failing to close " + Thread.currentThread().getName() + "=====");
//                throw e;
//            }
//
//            Map<String, Integer> map = Maps.newHashMap();
//
//            Set<String> dims = facetResult.getDimensions();
//            for (String dim : dims) {
//                List<FacetResult.Facet> facets = facetResult.getFacets(dim);
//                for (FacetResult.Facet facet : facets) {
//                    map.put(facet.getLabel(), facet.getCount());
//                }
//            }
            System.out.println(System.currentTimeMillis() +Thread.currentThread().getName() + ", ");
        } catch (RepositoryException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void run1() {
        while (true) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            AsyncIndexUpdate async = (AsyncIndexUpdate) WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
                @Override
                public boolean test(@Nullable Runnable input) {
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

    }

    private Tree createPath(String path) {
        Tree base = root.getTree("/");
        for (String e : PathUtils.elements(path)) {
            base = base.addChild(e);
        }
        return base;
    }

//    private static Tree createIndex(Tree index, String name, Set<String> propNames){
//        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
//        IndexRule rule = idx.indexRule("nt:base");
//        for (String propName : propNames){
//            rule.property(propName).propertyIndex();
//        }
//        Tree idxTree = index.getChild("oak:index").addChild(name);
//        idx.build(idxTree);
//        return idxTree;
//    }


    private static Tree createIndex(Tree index, String name, Set<String> propNames) {
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        IndexRule rule = idx.indexRule("nt:base");
        for (String propName : propNames) {
            rule.property(propName).propertyIndex();
        }
        Tree idxTree = index.getChild("oak:index").addChild(name);
        idx.build(idxTree);
        return idxTree;
    }

    private static final String FACET_PROP = "facets";

    //private Session mysession;
    private Tree createIndex(Tree index, String name) throws RepositoryException {
        IndexDefinitionBuilder idxBuilder = new IndexDefinitionBuilder();
//        idxBuilder.noAsync().evaluatePathRestrictions()
//                .indexRule("nt:base")
//               .property("cons")
//               .propertyIndex()
//                .property("foo").propertyIndex()
//                .getBuilderTree().setProperty(PROP_FACETS, true);

        idxBuilder.noAsync()
                .indexRule("nt:base")
                .property("cons").propertyIndex()
                .property("foo").propertyIndex()
                .getBuilderTree().setProperty(PROP_FACETS, true);


        Tree facetConfig = idxBuilder.getBuilderTree().addChild(FACET_PROP);
        facetConfig.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        facetConfig.setProperty("secure", "statistical");
        facetConfig.setProperty("topChildren", "100");

//        Node indexNode = getOrCreateByPath("/oak:index", "nt:unstructured", session)
//                .addNode("index", INDEX_DEFINITIONS_NODE_TYPE);
//        idxBuilder.build(indexNode);
//        session.save();

        Tree idxTree = index.getChild("oak:index").addChild(name);
        idxBuilder.build(idxTree);
        return idxTree;

    }

    private static Tree createFulltextIndex(Tree index, String name) {
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.evaluatePathRestrictions();
        idx.indexRule("nt:base")
                .property(FulltextIndexConstants.REGEX_ALL_PROPS, true)
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

        @NotNull
        @Override
        public InputStream getNewStream() {
            accessCount++;
            return super.getNewStream();
        }
    }
}
