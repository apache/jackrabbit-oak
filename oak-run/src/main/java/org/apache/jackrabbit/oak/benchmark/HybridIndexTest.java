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

package org.apache.jackrabbit.oak.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.DocumentQueue;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.LocalIndexObserver;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;

public class HybridIndexTest extends AbstractTest<HybridIndexTest.TestContext> {

    private static final ScheduledExecutorService executorService = MoreExecutors.getExitingScheduledExecutorService(
            (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(5));
    private static final boolean metricStatsEnabled =
            Boolean.parseBoolean(System.getProperty("metricStatsEnabled", "true"));
    private static MetricStatisticsProvider metricStatsProvider;
    public static final StatisticsProvider STATISTICS_PROVIDER = getStatsProvider(metricStatsEnabled);

    enum Status {
        NONE, STARTING, STARTED, STOPPING, STOPPED, ABORTED;

        private int count;

        public void inc(){
            count++;
        }

        public int count(){
            return count;
        }

        public Status next(){
            Status[] ss = values();
            if (ordinal() == ss.length - 1){
                return ss[0];
            }
            return ss[ordinal() + 1];
        }
    }

    private final Random random = new Random(42); //fixed seed
    private String indexedPropName = "foo";
    private int nodesPerIteration = Status.values().length;
    private int numOfIndexes = Integer.getInteger("numOfIndexes", 10);
    private int refreshDeltaMillis = Integer.getInteger("refreshDeltaMillis", 1000);
    private int asyncInterval = Integer.getInteger("asyncInterval", 5);
    private int queueSize = Integer.getInteger("queueSize", 1000);
    private boolean hybridIndexEnabled = Boolean.getBoolean("hybridIndexEnabled");

    private boolean searcherEnabled = Boolean.parseBoolean(System.getProperty("searcherEnabled", "true"));
    private File indexCopierDir;
    private IndexCopier copier;
    private NRTIndexFactory nrtIndexFactory;
    private LuceneIndexProvider luceneIndexProvider;
    private LuceneIndexEditorProvider luceneEditorProvider;
    private DocumentQueue queue;
    private LocalIndexObserver localIndexObserver;
    private RepositoryInitializer indexInitializer = new PropertyIndexInitializer();
    private TestContext defaultContext;
    private final File workDir;
    private Whiteboard whiteboard;
    private Searcher searcher;
    private Mutator mutator;
    private final AtomicInteger indexedNodeCount = new AtomicInteger();
    private List<TestContext> contexts = new ArrayList<>();

    public HybridIndexTest(File workDir) {
        this.workDir = workDir;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    Jcr jcr = new Jcr(oak);
                    if (hybridIndexEnabled) {
                        prepareLuceneIndexer(workDir);
                        jcr.with((QueryIndexProvider) luceneIndexProvider)
                                .with((Observer) luceneIndexProvider)
                                .with(localIndexObserver)
                                .with(luceneEditorProvider);
                        indexInitializer = new LuceneIndexInitializer();
                    }
                    whiteboard = oak.getWhiteboard();
                    jcr.with(indexInitializer);

                    //Async indexing is enabled for both property and lucene
                    //as for property it relies on counter index
                    oak.withAsyncIndexing("async", asyncInterval);
                    return jcr;
                }
            });
        }
        return super.createRepository(fixture);
    }

    @Override
    public void beforeSuite() throws Exception {
        if (hybridIndexEnabled) {
            runAsyncIndex();
        }
        defaultContext = new TestContext();
        contexts.add(defaultContext);
        searcher = new Searcher();
        mutator = new Mutator();

        if (searcherEnabled) {
            addBackgroundJob(searcher);
        }

        addBackgroundJob(mutator);
    }

    @Override
    protected TestContext prepareThreadExecutionContext() throws RepositoryException {
        TestContext ctx = new TestContext();
        contexts.add(ctx);
        return ctx;
    }

    @Override
    protected void runTest() throws Exception {
        runTest(defaultContext);
    }

    @Override
    protected void runTest(TestContext ctx)  throws Exception {
        //Create tree in breadth first fashion with each node having 50 child
        Node parent = ctx.session.getNode(ctx.paths.remove());
        Status status = Status.NONE;
        for (int i = 0; i < nodesPerIteration; i++) {
            Node child = parent.addNode(nextNodeName());
            child.setProperty(indexedPropName, status.name());
            ctx.session.save();
            ctx.paths.add(child.getPath());
            indexedNodeCount.incrementAndGet();
            status.inc();
            status = status.next();
        }
    }

    @Override
    protected void disposeThreadExecutionContext(TestContext context) throws RepositoryException {
        context.dispose();
    }

    @Override
    protected void afterSuite() throws Exception {
        if (hybridIndexEnabled){
            //TODO This to avoid issue with Indexing still running post afterSuite call
            //TO handle this properly we would need a callback after repository shutdown
            //and before NodeStore teardown
            getAsyncIndexUpdate().close();
            queue.close();
            nrtIndexFactory.close();
        }

        dumpStats();

        if (indexCopierDir != null) {
            FileUtils.deleteDirectory(indexCopierDir);
        }
        System.out.printf("numOfIndexes: %d, refreshDeltaMillis: %d, asyncInterval: %d, queueSize: %d , " +
                        "hybridIndexEnabled: %s, metricStatsEnabled: %s %n", numOfIndexes, refreshDeltaMillis,
                asyncInterval, queueSize, hybridIndexEnabled, metricStatsEnabled);
        System.out.printf("Searcher: %d, Mutator: %d, indexedNodeCount: %d %n", searcher.resultSize, mutator
                .mutationCount, indexedNodeCount.get());
    }

    private void dumpStats() {
        if (!metricStatsEnabled) {
            return;
        }
        ConsoleReporter.forRegistry(metricStatsProvider.getRegistry())
                .outputTo(System.out)
                .filter(new MetricFilter() {
                    @Override
                    public boolean matches(String name, Metric metric) {
                        return name.startsWith("HYBRID") || name.startsWith("DOCUMENT_NS_MERGE");
                    }
                })
                .build()
                .report();
    }

    protected class TestContext {
        final Session session = loginWriter();
        final Queue<String> paths = new LinkedBlockingDeque<>();

        final Node dump;

        public TestContext() throws RepositoryException {
            dump = session.getRootNode()
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED);
            session.save();
            paths.add(dump.getPath());
        }

        public void dispose() throws RepositoryException {
            dump.remove();
            session.logout();
        }
    }

    private String randomStatus() {
        Status status = Status.values()[random.nextInt(Status.values().length)];
        status.inc();
        return status.name();
    }

    private void prepareLuceneIndexer(File workDir) {
        try {
            indexCopierDir = createTemporaryFolderIn(workDir);
            copier = new IndexCopier(executorService, indexCopierDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        nrtIndexFactory = new NRTIndexFactory(copier, Clock.SIMPLE,
                TimeUnit.MILLISECONDS.toSeconds(refreshDeltaMillis));
        MountInfoProvider mip = Mounts.defaultMountInfoProvider();
        LuceneIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(mip, copier);
        IndexTracker tracker = new IndexTracker(indexReaderFactory, nrtIndexFactory);
        luceneIndexProvider = new LuceneIndexProvider(tracker);
        luceneEditorProvider = new LuceneIndexEditorProvider(copier,
                tracker,
                null, //extractedTextCache
                null, //augmentorFactory
                mip);

        StatisticsProvider sp = STATISTICS_PROVIDER;
        queue = new DocumentQueue(queueSize, tracker, executorService, sp);
        localIndexObserver = new LocalIndexObserver(queue, sp);
    }

    private void runAsyncIndex() {
        checkNotNull(getAsyncIndexUpdate()).run();
    }

    private AsyncIndexUpdate getAsyncIndexUpdate() {
        return (AsyncIndexUpdate)WhiteboardUtils.getService(whiteboard, Runnable.class, new Predicate<Runnable>() {
                @Override
                public boolean apply(@Nullable Runnable input) {
                    return input instanceof AsyncIndexUpdate;
                }
            });
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static File createTemporaryFolderIn(File parentFolder) throws IOException {
        File createdFolder = File.createTempFile("oak-", "", parentFolder);
        createdFolder.delete();
        createdFolder.mkdir();
        return createdFolder;
    }

    private static StatisticsProvider getStatsProvider(boolean metricStatsEnabled){
        StatisticsProvider sp = StatisticsProvider.NOOP;
        if (metricStatsEnabled) {
            metricStatsProvider = new MetricStatisticsProvider(null, executorService);
            sp = metricStatsProvider;
        }
        return sp;
    }


    private class PropertyIndexInitializer implements RepositoryInitializer {

        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
            NodeBuilder oakIndex = IndexUtils.getOrCreateOakIndex(builder);
            addPropIndexDefn(oakIndex, indexedPropName);
            for (int i = 0; i < numOfIndexes - 1; i++) {
                addPropIndexDefn(oakIndex, indexedPropName + i);
            }
        }

        private void addPropIndexDefn(NodeBuilder parent, String propName){
            try {
                IndexUtils.createIndexDefinition(parent, propName, false,
                        singleton(propName), null, "property", null);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }

        }
    }

    private class LuceneIndexInitializer implements RepositoryInitializer {
        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
            NodeBuilder oakIndex = IndexUtils.getOrCreateOakIndex(builder);

            IndexDefinitionBuilder defnBuilder = new IndexDefinitionBuilder();
            defnBuilder.evaluatePathRestrictions();
            defnBuilder.async("async", "sync");
            defnBuilder.indexRule("nt:base").property(indexedPropName).propertyIndex();
            //defnBuilder.codec("oakCodec");

            for (int i = 0; i < numOfIndexes - 1; i++) {
                defnBuilder.indexRule("nt:base").property(indexedPropName + i).propertyIndex();
            }

            oakIndex.setChildNode(indexedPropName, defnBuilder.build());
        }
    }

    private class Searcher implements Runnable {
        final Session session = loginWriter();
        int resultSize = 0;
        @Override
        public void run() {
            try{
                run0();
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }

        private void run0() throws RepositoryException {
            session.refresh(false);
            QueryManager qm = session.getWorkspace().getQueryManager();
            Query q = qm.createQuery("select * from [nt:base] where [" + indexedPropName + "] = $status", Query.JCR_SQL2);
            q.bindValue("status", session.getValueFactory().createValue(randomStatus()));
            QueryResult result = q.execute();

            //With property index at time traversing index wins (somehow reporting lower cost)
            //and that leads to warning. So limit the iterator size
            resultSize += Iterators.size(Iterators.limit(result.getNodes(), 500));
        }
    }

    private class Mutator implements Runnable {
        final Session session = loginWriter();
        int mutationCount = 0;
        @Override
        public void run() {
            try{
                run0();
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }

        private void run0() throws RepositoryException {
            TestContext ctx = contexts.get(random.nextInt(contexts.size()));
            String path = ctx.paths.peek();
            session.refresh(false);
            if (path != null){
                Node node = session.getNode(path);
                if(node.hasProperty(indexedPropName)){
                    String value = node.getProperty(indexedPropName).getString();
                    String newValue = Status.valueOf(value).next().name();
                    node.setProperty(indexedPropName, newValue);
                    session.save();
                    mutationCount++;
                }
            }
        }
    }
}
