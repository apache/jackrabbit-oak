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

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexNodeManager;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LucenePropertyIndex;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.OptionalEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
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
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class MultithreadedOldLuceneFacetProviderReadFailureTest extends AbstractQueryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private static final int NUM_LABELS = 4;
    private static final int NUM_LEAF_NODES = STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
    private static final String FACET_PROP = "facets";
    private static final long REFRESH_DELTA = TimeUnit.SECONDS.toMillis(1);

    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    private OptionalEditorProvider optionalEditorProvider = new OptionalEditorProvider();
    private NRTIndexFactory nrtIndexFactory;
    private LuceneIndexProvider luceneIndexProvider;
    private NodeStore nodeStore;
    private DocumentQueue queue;
    private Clock clock = new Clock.Virtual();
    private Whiteboard wb;
    private QueryManager qm;
    private Repository jcrRepo;
    private Jcr jcr;
    private Oak oak;
    private Thread thread;
    // backup original system properties i.e. before test started
    private final Properties backupProperties = (Properties) System.getProperties().clone();

    @After
    public void tearDown() throws IOException {
        luceneIndexProvider.close();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new ExecutorCloser(executorService).close();
        nrtIndexFactory.close();
        // restore original system properties i.e. before test started
        System.setProperties(backupProperties);
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
        nrtIndexFactory = new NRTIndexFactory(copier, clock, TimeUnit.MILLISECONDS.toSeconds(REFRESH_DELTA), StatisticsProvider.NOOP);
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
        return repo;
    }

    private void createSmallDataset(int k) throws RepositoryException {
        Random rGen = new Random(42);
        Tree par = createPath("/parent" + k);
        par.setProperty("foo", "bar");
        for (int i = 0; i < NUM_LABELS * 2; i++) {
            Tree subPar = par.addChild("par" + i);
            for (int j = 0; j < NUM_LEAF_NODES / (2 * NUM_LABELS); j++) {
                Tree child = subPar.addChild("c" + j);
                child.setProperty("cons", "val");
                int labelNum = rGen.nextInt(NUM_LABELS);
                child.setProperty("foo", "l" + labelNum);
            }
        }
    }

    /*
        The facet test delays threads intentionally so that index reader closes before reading facets. But sometimes it may fail
        because of load on a system. You can change the system property below between 30-50 and see if this test passes on
        your system.
        The following test is ignored as it is not a consistent test and result may vary based on system load etc.
        This test is a race condition and can be test for failure by using different values for  OLD_FACET_PROVIDER_TEST_FAILURE_SLEEP_INSTRUMENT_NAME
        The probable value where this will fail is between 30ms to 60ms.
        This test's counter part is DelayedFacetReadTest which test new implementation solving the race condition on index reader opening
        and closing.
     */
    @Ignore
    @Test(expected = RuntimeException.class)
    public void facet() throws Exception {
        // Explicitly setting following configs to run LuceneFacetProvider (old) and a thread sleep of 50 ms in refresh readers. Refer: OAK-8898
        System.setProperty(LucenePropertyIndex.OLD_FACET_PROVIDER_CONFIG_NAME, "true");
        System.setProperty(LuceneIndexNodeManager.OLD_FACET_PROVIDER_TEST_FAILURE_SLEEP_INSTRUMENT_NAME, "40");
        Thread.currentThread().setName("main");
        String idxName = "hybridtest";
        Tree idx = createIndex(root.getTree("/"), idxName);
        TestUtil.enableIndexingMode(idx, FulltextIndexConstants.IndexingMode.NRT);
        setTraversalEnabled(false);
        root.commit();
        jcr = new Jcr(oak);
        jcrRepo = jcr.createRepository();
        createSmallDataset(0);
        clock.waitUntil(clock.getTime() + REFRESH_DELTA + 1);
        root.commit();
        runAsyncIndex();
        createSmallDataset(2);
        clock.waitUntil(clock.getTime() + REFRESH_DELTA + 1);
        root.commit();
        Session anonSession = jcrRepo.login(new GuestCredentials());
        qm = anonSession.getWorkspace().getQueryManager();
        Query q = qm.createQuery("SELECT [rep:facet(foo)] FROM [nt:base] WHERE [cons] = 'val'", SQL2);
        QueryResult qr = q.execute();

        thread = new Thread(new Runnable() {
            public void run() {
                Thread.currentThread().setName("branch");
                Query q = null;
                try {
                    clock.waitUntil(clock.getTime() + REFRESH_DELTA + 1);
                    q = qm.createQuery("SELECT [rep:facet(foo)] FROM [nt:base] WHERE [cons] = 'val'", SQL2);
                    QueryResult qr = q.execute();
                } catch (RepositoryException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        RowIterator it;
        try {
            it = qr.getRows();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("org.apache.lucene.store.AlreadyClosedException: this IndexReader is closed"));
            e.printStackTrace(System.out);
            throw e;
        }
        String firstColumnName = qr.getColumnNames()[0];
        if (it.hasNext()) {
            Value v = it.nextRow().getValue(firstColumnName);
        }
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

    private Tree createPath(String path) {
        Tree base = root.getTree("/");
        for (String e : PathUtils.elements(path)) {
            base = base.addChild(e);
        }
        return base;
    }

    private Tree createIndex(Tree index, String name) throws RepositoryException {
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder();
        idxBuilder.noAsync()
                .indexRule("nt:base")
                .property("cons").propertyIndex()
                .property("foo").propertyIndex()
                .getBuilderTree().setProperty(PROP_FACETS, true);
        Tree facetConfig = idxBuilder.getBuilderTree().addChild(FACET_PROP);
        facetConfig.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        facetConfig.setProperty("secure", "statistical");
        facetConfig.setProperty("topChildren", "100");
        Tree idxTree = index.getChild("oak:index").addChild(name);
        idxBuilder.build(idxTree);
        return idxTree;
    }

}
