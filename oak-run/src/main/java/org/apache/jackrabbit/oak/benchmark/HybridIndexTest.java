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
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
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

    private ScheduledExecutorService executorService = MoreExecutors.getExitingScheduledExecutorService(
            (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(5));
    private final Random random = new Random(42); //fixed seed
    private String indexedPropName = "foo";
    private int nodesPerIteration = Integer.getInteger("nodesPerIteration", 100);
    private int numOfIndexes = Integer.getInteger("numOfIndexes", 10);
    private int refreshDeltaMillis = Integer.getInteger("refreshDeltaMillis", 1000);
    private int asyncInterval = Integer.getInteger("asyncInterval", 5);
    private int queueSize = Integer.getInteger("queueSize", 1000);
    private boolean hybridIndexEnabled = Boolean.getBoolean("hybridIndexEnabled");
    private boolean metricStatsEnabled = Boolean.getBoolean("metricStatsEnabled");
    private File indexCopierDir;
    private IndexCopier copier;
    private NRTIndexFactory nrtIndexFactory;
    private LuceneIndexProvider luceneIndexProvider;
    private LuceneIndexEditorProvider luceneEditorProvider;
    private DocumentQueue queue;
    private LocalIndexObserver localIndexObserver;
    private RepositoryInitializer indexInitializer = new PropertyIndexInitializer();
    private TestContext defaultContext;
    private final List<String> indexedValues = ImmutableList.of("STARTING", "STARTED", "STOPPING",
            "STOPPED", "ABORTED");
    private final File workDir;
    private Whiteboard whiteboard;
    private MetricStatisticsProvider metricStatsProvider;

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
                        oak.withAsyncIndexing("async", asyncInterval);
                        prepareLuceneIndexer(workDir);
                        jcr.with((QueryIndexProvider) luceneIndexProvider)
                                .with((Observer) luceneIndexProvider)
                                .with(localIndexObserver)
                                .with(luceneEditorProvider);
                        indexInitializer = new LuceneIndexInitializer();
                    }
                    whiteboard = oak.getWhiteboard();
                    jcr.with(indexInitializer);
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
    }

    @Override
    protected TestContext prepareThreadExecutionContext() throws RepositoryException {
        return new TestContext();
    }

    @Override
    protected void runTest() throws Exception {
        runTest(defaultContext);
    }

    @Override
    protected void runTest(TestContext ctx)  throws Exception {
        for (int i = 0; i < nodesPerIteration; i++) {
            ctx.dump.addNode(nextNodeName()).setProperty(indexedPropName, nextIndexedValue());
            ctx.session.save();
        }
    }

    @Override
    protected void disposeThreadExecutionContext(TestContext context) throws RepositoryException {
        context.dispose();
    }

    @Override
    protected void afterSuite() throws Exception {
        if (hybridIndexEnabled){
            queue.close();
            nrtIndexFactory.close();
            dumpStats();
        }

        if (indexCopierDir != null) {
            FileUtils.deleteDirectory(indexCopierDir);
        }
        System.out.printf("numOfIndexes: %d, refreshDeltaMillis: %d, asyncInterval: %d, queueSize: %d , " +
                        "hybridIndexEnabled: %s, metricStatsEnabled: %s %n", numOfIndexes, refreshDeltaMillis,
                asyncInterval, queueSize, hybridIndexEnabled, metricStatsEnabled);
    }

    private void dumpStats() {
        ConsoleReporter.forRegistry(metricStatsProvider.getRegistry())
                .outputTo(System.out)
                .filter(new MetricFilter() {
                    @Override
                    public boolean matches(String name, Metric metric) {
                        return name.startsWith("HYBRID");
                    }
                })
                .build()
                .report();
    }

    protected class TestContext {
        final Session session = loginWriter();

        final Node dump;

        public TestContext() throws RepositoryException {
            dump = session.getRootNode()
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED);
            session.save();
        }

        public void dispose() throws RepositoryException {
            dump.remove();
            session.save();
            session.logout();
        }
    }

    private String nextIndexedValue() {
        return indexedValues.get(random.nextInt(indexedValues.size()));
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

        StatisticsProvider sp = StatisticsProvider.NOOP;
        if (metricStatsEnabled) {
            metricStatsProvider = new MetricStatisticsProvider(null, executorService);
            sp = metricStatsProvider;
        }
        queue = new DocumentQueue(queueSize, tracker, executorService, sp);
        localIndexObserver = new LocalIndexObserver(queue, sp);
    }

    private void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(whiteboard, Runnable.class, new Predicate<Runnable>() {
            @Override
            public boolean apply(@Nullable Runnable input) {
                return input instanceof AsyncIndexUpdate;
            }
        });
        checkNotNull(async).run();
    }

    private static File createTemporaryFolderIn(File parentFolder) throws IOException {
        File createdFolder = File.createTempFile("oak-", "", parentFolder);
        createdFolder.delete();
        createdFolder.mkdir();
        return createdFolder;
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

            for (int i = 0; i < numOfIndexes - 1; i++) {
                defnBuilder.indexRule("nt:base").property(indexedPropName + i).propertyIndex();
            }

            oakIndex.setChildNode(indexedPropName, defnBuilder.build());
        }
    }

}
