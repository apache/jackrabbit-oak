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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
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

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.DocumentQueue;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.LocalIndexObserver;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.PropertyIndexCleaner;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder.PropertyRule;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

public class HybridIndexTest extends AbstractTest<HybridIndexTest.TestContext> {
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
    private int cleanerIntervalInSecs = Integer.getInteger("cleanerIntervalInSecs", 10);
    private int queueSize = Integer.getInteger("queueSize", 1000);
    private boolean hybridIndexEnabled = Boolean.getBoolean("hybridIndexEnabled");
    private boolean dumpStats = Boolean.getBoolean("dumpStats");
    private boolean useOakCodec = Boolean.parseBoolean(System.getProperty("useOakCodec", "true"));
    private boolean syncIndexing = Boolean.parseBoolean(System.getProperty("syncIndexing", "false"));
    private String indexingMode = System.getProperty("indexingMode", "nrt");

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
    private final StatisticsProvider statsProvider;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ExecutorService executorService = MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(5));
    private final List<Registration> regs = new ArrayList<>();
    private BackgroundObserver backgroundObserver;


    public HybridIndexTest(File workDir, StatisticsProvider statsProvider) {
        this.workDir = workDir;
        this.statsProvider = statsProvider;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    Jcr jcr = new Jcr(oak);
                    whiteboard = oak.getWhiteboard();
                    prepareLuceneIndexer(workDir, getNodeStore(oak));

                    backgroundObserver = new BackgroundObserver(luceneIndexProvider, executorService, 5);

                    jcr.with((QueryIndexProvider) luceneIndexProvider)
                            .with(backgroundObserver)
                            .with(luceneEditorProvider)
                            .with(new NodeTypeIndexFixerInitializer());

                    if (hybridIndexEnabled) {
                        jcr.with(localIndexObserver);
                        indexInitializer = new LuceneIndexInitializer();
                    }

                    jcr.with(indexInitializer);

                    //Configure the default global fulltext index as it impacts
                    //both pure property index based setup and nrt based
                    //So more closer to real world
                    jcr.with(new LuceneFullTextInitializer());

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
        //TODO This to avoid issue with Indexing still running post afterSuite call
        //TO handle this properly we would need a callback after repository shutdown
        //and before NodeStore teardown
        getAsyncIndexUpdate().close();

        if (backgroundObserver != null){
            backgroundObserver.close();
        }

        int sleepCount = 0;
        while (backgroundObserver.getMBean().getQueueSize()> 0 && ++sleepCount < 100) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        for (Registration r : regs) {
            r.unregister();
        }

        //Close hybrid stuff after async is closed
        if (hybridIndexEnabled){
            queue.close();
            nrtIndexFactory.close();
        }

        if (indexCopierDir != null) {
            FileUtils.deleteDirectory(indexCopierDir);
        }
        System.out.printf("numOfIndexes: %d, refreshDeltaMillis: %d, asyncInterval: %d, queueSize: %d , " +
                        "hybridIndexEnabled: %s, indexingMode: %s, useOakCodec: %s, cleanerIntervalInSecs: %d, " +
                        "syncIndexing: %s %n",
                numOfIndexes, refreshDeltaMillis, asyncInterval, queueSize, hybridIndexEnabled,
                indexingMode, useOakCodec, cleanerIntervalInSecs, syncIndexing);

        if (dumpStats) {
            dumpStats();
        }
    }

    @Override
    protected String[] statsNames() {
        return new String[]{"Searcher", "Mutator", "Indexed"};
    }

    @Override
    protected String[] statsFormats() {
        return new String[]{"%8d", "%8d", "%8d"};
    }

    @Override
    protected Object[] statsValues() {
        return new Object[]{searcher.resultSize, mutator.mutationCount, indexedNodeCount.get()};
    }

    @Override
    protected String comment() {
        List<String> commentElements = new ArrayList<>();
        if (hybridIndexEnabled){
            commentElements.add(indexingMode);

            if (useOakCodec){
                commentElements.add("oakCodec");
            }
            if (syncIndexing) {
                commentElements.add("sync");
            }
        } else {
            commentElements.add("property");
        }

        commentElements.add("numIdxs:"+ numOfIndexes);
        return Joiner.on(',').join(commentElements);
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

    private void prepareLuceneIndexer(File workDir, NodeStore nodeStore) {
        try {
            indexCopierDir = createTemporaryFolderIn(workDir);
            copier = new IndexCopier(executorService, indexCopierDir, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IndexPathService indexPathService = new IndexPathServiceImpl(nodeStore);
        AsyncIndexInfoService asyncIndexInfoService = new AsyncIndexInfoServiceImpl(nodeStore);

        nrtIndexFactory = new NRTIndexFactory(copier, Clock.SIMPLE,
                TimeUnit.MILLISECONDS.toSeconds(refreshDeltaMillis), StatisticsProvider.NOOP);
        MountInfoProvider mip = Mounts.defaultMountInfoProvider();
        LuceneIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(mip, copier);

        IndexTracker tracker = new IndexTracker(indexReaderFactory, nrtIndexFactory);

        luceneIndexProvider = new LuceneIndexProvider(tracker);
        luceneEditorProvider = new LuceneIndexEditorProvider(copier,
                tracker,
                null, //extractedTextCache
                null, //augmentorFactory
                mip);

        queue = new DocumentQueue(queueSize, tracker, executorService, statsProvider);
        localIndexObserver = new LocalIndexObserver(queue, statsProvider);
        luceneEditorProvider.setIndexingQueue(queue);

        if (syncIndexing) {
            PropertyIndexCleaner cleaner = new PropertyIndexCleaner(nodeStore, indexPathService, asyncIndexInfoService, statsProvider);
            regs.add(scheduleWithFixedDelay(whiteboard, cleaner,
                    cleanerIntervalInSecs, true, true));
        }


        Thread.setDefaultUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception", e));
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

    private void dumpStats() {
        IndexStatsMBean indexStats = WhiteboardUtils.getService(whiteboard, IndexStatsMBean.class);
        System.out.println(indexStats.getConsolidatedExecutionStats());
        String queueSize = Arrays.toString(statsProvider.getStats().getTimeSeries("HYBRID_QUEUE_SIZE", false)
                .getValuePerSecond());
        System.out.println("Queue size - " + queueSize);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static File createTemporaryFolderIn(File parentFolder) throws IOException {
        File createdFolder = File.createTempFile("oak-", "", parentFolder);
        createdFolder.delete();
        createdFolder.mkdir();
        return createdFolder;
    }

    private static NodeStore getNodeStore(Oak oak) {
        try {
            Field f = Oak.class.getDeclaredField("store");
            f.setAccessible(true);
            return (NodeStore) f.get(oak);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                NodeBuilder idx = IndexUtils.createIndexDefinition(parent, propName, false,
                        singleton(propName), null, "property", null);
                if ( propName.equals(indexedPropName)) {
                    idx.setProperty("tags", singletonList("fooIndex"), Type.STRINGS);
                }
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
            defnBuilder.async("async", indexingMode, "async");
            PropertyRule pr = defnBuilder.indexRule("nt:base").property(indexedPropName).propertyIndex();
            if (syncIndexing) {
                pr.sync();
            }
            if (useOakCodec) {
                defnBuilder.codec("oakCodec");
            }

            for (int i = 0; i < numOfIndexes - 1; i++) {
                defnBuilder.indexRule("nt:base").property(indexedPropName + i).propertyIndex();
            }

            oakIndex.setChildNode(indexedPropName, defnBuilder.build());
            oakIndex.child(indexedPropName).setProperty("tags", singletonList("fooIndex"), Type.STRINGS);
        }
    }

    private class LuceneFullTextInitializer implements RepositoryInitializer {
        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
            NodeBuilder oakIndex = IndexUtils.getOrCreateOakIndex(builder);

            IndexDefinitionBuilder defnBuilder = new IndexDefinitionBuilder();
            defnBuilder.async("async", "async");
            defnBuilder.codec("Lucene46");
            defnBuilder.indexRule("nt:base")
                    .property(LuceneIndexConstants.REGEX_ALL_PROPS, true)
                    .nodeScopeIndex();
            oakIndex.setChildNode("globalIndex", defnBuilder.build());
        }
    }

    private class NodeTypeIndexFixerInitializer implements RepositoryInitializer {

        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
            //Due to OAK-1150 currently all nodes get indexed
            //With explicit list on those nodes would be indexed
            NodeBuilder nodetype = builder.getChildNode("oak:index").getChildNode("nodetype");
            if (nodetype.exists()) {
                List<String> nodetypes = Lists.newArrayList();
                if (nodetype.hasProperty(DECLARING_NODE_TYPES)){
                    nodetypes = Lists.newArrayList(nodetype.getProperty(DECLARING_NODE_TYPES).getValue(Type.STRINGS));
                }

                if (nodetypes.isEmpty()) {
                    nodetypes.add(INDEX_DEFINITIONS_NODE_TYPE);
                    nodetypes.add("rep:Authorizable");
                    nodetype.setProperty(DECLARING_NODE_TYPES, nodetypes, Type.NAMES);
                    nodetype.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
                }
            }

            //Disable counter index to disable traversal
            NodeBuilder counter = builder.getChildNode("oak:index").getChildNode("counter");
            if (counter.exists()) {
                counter.setProperty("type", "disabled");
            }
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
            Query q = qm.createQuery("select * from [nt:base] where [" + indexedPropName + "] = $status " +
                    "option(index tag fooIndex)", Query.JCR_SQL2);
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
