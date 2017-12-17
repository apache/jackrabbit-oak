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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollector;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean.STATUS_DONE;
import static org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean.STATUS_RUNNING;
import static org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback.IndexProgress.COMMIT_SUCCEDED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory.PROP_UNSAFE_FOR_ACTIVE_DELETION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActiveDeletedBlobCollectorMBeanImplTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Whiteboard wb;

    private NodeStore nodeStore;

    private List<String> indexPaths = Lists.newArrayList();

    private final Clock clock = new Clock.Virtual();

    @Before
    public void setUp() {
        wb = new OsgiWhiteboard(context.bundleContext());
        nodeStore = new MemoryNodeStore();
    }

    @After
    public void after() {
        indexPaths.clear();
        ActiveDeletedBlobCollectorFactory.NOOP.flagActiveDeletionUnsafe(false);
    }

    @Test
    public void onlyRunningIndexesRequireToBeWaitedOn() {
        IndexPathService indexPathService = MockRegistrar.getIndexPathsService(indexPaths);

        final StatusSupplier statusSupplier = new StatusSupplier();

        final AtomicLong returnExecCount = new AtomicLong(2L);

        AsyncIndexInfoService asyncIndexInfoService = MockRegistrar.getAsyncIndexInfoService(newArrayList(
                new IndexMBeanInfoSupplier("foo-async", statusSupplier, returnExecCount::get)
        ));

        ActiveDeletedBlobCollectorMBean bean = getTestBean(indexPathService, asyncIndexInfoService);

        long start = clock.getTime();
        bean.flagActiveDeletionUnsafeForCurrentState();
        long elapsed = clock.getTime() - start;
        assertTrue("Non running index lane was polled for " + TimeUnit.MILLISECONDS.toSeconds(elapsed) + " seconds.",
                elapsed < TimeUnit.SECONDS.toMillis(5));

        // running index with stalled exec count waits for 2 minutes
        statusSupplier.status = STATUS_RUNNING;
        start = clock.getTime();
        bean.flagActiveDeletionUnsafeForCurrentState();
        elapsed = clock.getTime() - start;
        assertTrue("Running index lane without changing execCnt was polled for " + TimeUnit.MILLISECONDS.toSeconds(elapsed) + " seconds.",
                elapsed > TimeUnit.SECONDS.toMillis(120) && elapsed < TimeUnit.SECONDS.toMillis(125));

        // running index with not stalled exec count doesn't wait
        statusSupplier.status = STATUS_RUNNING;
        asyncIndexInfoService = MockRegistrar.getAsyncIndexInfoService(newArrayList(
                new IndexMBeanInfoSupplier("foo-async", statusSupplier, returnExecCount::incrementAndGet)
        ));
        bean = getTestBean(indexPathService, asyncIndexInfoService);

        start = clock.getTime();
        bean.flagActiveDeletionUnsafeForCurrentState();
        elapsed = clock.getTime() - start;
        assertTrue("Running index lane without changing execCnt was polled for " + TimeUnit.MILLISECONDS.toSeconds(elapsed) + " seconds.",
                elapsed < TimeUnit.SECONDS.toMillis(5));
    }

    @Test
    public void headIndexFilesGetMarkedUnsafe() throws Exception {
        String indexPath = "/fooIndex";
        createFakeIndex(indexPath);

        IndexPathService indexPathService = MockRegistrar.getIndexPathsService(indexPaths);
        AsyncIndexInfoService asyncIndexInfoService = MockRegistrar.getAsyncIndexInfoService(newArrayList(
                new IndexMBeanInfoSupplier("foo-async", () -> STATUS_DONE, () -> 2L)
        ));

        ActiveDeletedBlobCollectorMBean bean = getTestBean(indexPathService, asyncIndexInfoService);

        bean.flagActiveDeletionUnsafeForCurrentState();

        NodeState indexFile = getFakeIndexFile(indexPath);

        assertTrue(indexFile.getBoolean(PROP_UNSAFE_FOR_ACTIVE_DELETION));
    }

    @Test
    public void pauseResumeSetsInMemFlag() {
        IndexPathService indexPathService = MockRegistrar.getIndexPathsService(indexPaths);
        AsyncIndexInfoService asyncIndexInfoService = MockRegistrar.getAsyncIndexInfoService(newArrayList(
                new IndexMBeanInfoSupplier("foo-async", () -> STATUS_DONE, () -> 2L)
        ));

        ActiveDeletedBlobCollectorMBean bean = getTestBean(indexPathService, asyncIndexInfoService);

        assertFalse("Bean should delegate the call correctly",
                bean.isActiveDeletionUnsafe());

        bean.flagActiveDeletionUnsafeForCurrentState();

        assertTrue("Active deleted blob collector isn't notified to stop marking",
                ActiveDeletedBlobCollectorFactory.NOOP.isActiveDeletionUnsafe());

        assertTrue("Bean should delegate the call correctly",
                bean.isActiveDeletionUnsafe());

        bean.flagActiveDeletionSafe();

        assertFalse("Active deleted blob collector isn't notified to resume marking",
                ActiveDeletedBlobCollectorFactory.NOOP.isActiveDeletionUnsafe());

        assertFalse("Bean should delegate the call correctly",
                bean.isActiveDeletionUnsafe());
    }

    @Test
    public void timedOutWhileWaitingForIndexerShouldAutoResume() {
        IndexPathService indexPathService = MockRegistrar.getIndexPathsService(indexPaths);
        AsyncIndexInfoService asyncIndexInfoService = MockRegistrar.getAsyncIndexInfoService(newArrayList(
                new IndexMBeanInfoSupplier("foo-async", () -> STATUS_RUNNING, () -> 2L)
        ));

        ActiveDeletedBlobCollectorMBean bean = getTestBean(indexPathService, asyncIndexInfoService);

        bean.flagActiveDeletionUnsafeForCurrentState();

        assertFalse("Timing out on running indexer didn't resume marking blobs",
                bean.isActiveDeletionUnsafe());
    }

    @Test
    public void failureToFlagAllIndexFilesShouldAutoResume() {
        IndexPathService indexPathService = MockRegistrar.getIndexPathsService(indexPaths);
        AsyncIndexInfoService asyncIndexInfoService = MockRegistrar.getAsyncIndexInfoService(newArrayList(
                new IndexMBeanInfoSupplier("foo-async", () -> STATUS_DONE, () -> 2L)
        ));

        NodeStore failingNodeStore = new MemoryNodeStore() {
            @Nonnull
            @Override
            public synchronized NodeState merge(@Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
                                                @Nonnull CommitInfo info) throws CommitFailedException {
                throw new CommitFailedException("TestFail", 1, "We must never merge");
            }
        };

        ActiveDeletedBlobCollectorMBeanImpl bean =
                new ActiveDeletedBlobCollectorMBeanImpl(ActiveDeletedBlobCollectorFactory.NOOP, wb, failingNodeStore,
                        indexPathService, asyncIndexInfoService,
                        new MemoryBlobStore(), sameThreadExecutor());
        bean.clock = clock;

        bean.flagActiveDeletionUnsafeForCurrentState();

        assertFalse("Failure to update head index files didn't resume marking blobs",
                bean.isActiveDeletionUnsafe());
    }

    @Test
    public void orderOfFlaggingWaitForIndexersAndUpdateIndexFiles() {
        final AtomicBoolean isPaused = new AtomicBoolean();
        final AtomicBoolean hadWaitedForIndex = new AtomicBoolean();

        IndexPathService indexPathService = mock(IndexPathService.class);
        when(indexPathService.getIndexPaths()).then(mockObj -> {
            assertTrue("Must wait for indexers before going to update index files", hadWaitedForIndex.get());
            return indexPaths;
        });
        AsyncIndexInfoService asyncIndexInfoService = MockRegistrar.getAsyncIndexInfoService(newArrayList(
                new IndexMBeanInfoSupplier("foo-async", () -> {
                    assertTrue("Must pause before waiting for indexers", isPaused.get());
                    hadWaitedForIndex.set(true);
                    return STATUS_DONE;
                }, () -> 2L)
        ));

        ActiveDeletedBlobCollectorMBeanImpl bean =
                new ActiveDeletedBlobCollectorMBeanImpl(new PauseNotifyingActiveDeletedBlobCollector(() -> {
                    isPaused.set(true);
                    return null;
                }), wb, nodeStore,
                        indexPathService, asyncIndexInfoService,
                        new MemoryBlobStore(), sameThreadExecutor());
        bean.clock = clock;

        bean.flagActiveDeletionUnsafeForCurrentState();
    }

    @Test
    public void clonedNSWithSharedDS() throws Exception {
        MemoryBlobStore bs = new MemoryBlobStore();
        bs.setBlockSizeMin(48);

        MemoryDocumentStore mds1 = new MemoryDocumentStore();

        DocumentNodeStore dns1 = builderProvider.newBuilder()
                .setDocumentStore(mds1).setBlobStore(bs).build();

        // Create initial repo with InitialContent. It has enough data to create blobs
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();
        ContentRepository repository = new Oak(dns1)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
        ContentSession session = repository.login(null, null);
        Root root = session.getLatestRoot();
        TestUtil.createFulltextIndex(root.getTree("/"), "testIndex");
        root.commit();

        // pause active deletion
        IndexPathService indexPathService = new IndexPathServiceImpl(dns1);
        AsyncIndexInfoService asyncIndexInfoService = MockRegistrar.getAsyncIndexInfoService(newArrayList(
                new IndexMBeanInfoSupplier("foo-async", () -> STATUS_DONE, () -> 2L)
        ));
        ActiveDeletedBlobCollectorMBeanImpl bean =
                new ActiveDeletedBlobCollectorMBeanImpl(ActiveDeletedBlobCollectorFactory.NOOP, wb, dns1,
                        indexPathService, asyncIndexInfoService,
                        new MemoryBlobStore(), sameThreadExecutor());
        bean.clock = clock;

        bean.flagActiveDeletionUnsafeForCurrentState();

        // we try here to create some churn and we want some files to get created at dns1
        // BUT get deleted at dns2. "segments_1" is one such file.
        // since our "creation" of churn is assumed, we should assert that dns1 has "segments_1"
        // (and later dns2 doesn't have it)
        root = session.getLatestRoot();
        assertTrue("First pass indexing should generate segments_1",
                root.getTree("/oak:index/testIndex/:data/segments_1").exists());

        // shutdown first instance
        dns1.dispose();

        // clone
        MemoryDocumentStore mds2 = mds1.copy();
        DocumentNodeStore dns2 = builderProvider.newBuilder().setDocumentStore(mds2).setBlobStore(bs).build();

        // create some churn to delete some index files - using clone store
        // we'd setup lucene editor with active deletion collector

        DeletedFileTrackingADBC deletedFileTrackingADBC = new DeletedFileTrackingADBC(
                new File(temporaryFolder.getRoot(), "adbc-workdir"));
        editorProvider = new LuceneIndexEditorProvider(null, null,
                new ExtractedTextCache(0, 0),
                null, Mounts.defaultMountInfoProvider(),
                deletedFileTrackingADBC);
        repository = new Oak(dns2)
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
        session = repository.login(null, null);
        root = session.getLatestRoot();
        Tree rootTree = root.getTree("/");
        for (int i = 0; i < 20; i++) {
            Tree child = rootTree.addChild("a" + i);
            for (int j = 0; j < 20; j++) {
                child.setProperty("foo" + j, "bar" + j);
            }
        }

        root.commit();
        //since our index is not async, we are unable to track commit progress automatically.
        // OR, iow, we need to play the rold of AsyncIndexUpdate explicitly
        deletedFileTrackingADBC.blobDeletionCallback.commitProgress(COMMIT_SUCCEDED);

        deletedFileTrackingADBC.purgeBlobsDeleted(Clock.SIMPLE.getTime() + TimeUnit.SECONDS.toMillis(1), bs);

        root = session.getLatestRoot();
        assertFalse("Churn created via dns2 should delete segments_1",
                root.getTree("/oak:index/testIndex/:data/segments_1").exists());

        dns2.dispose();

        // validate index using dns1 which should still have valid index data even
        // after dns2's churn
        dns1 = builderProvider.newBuilder().setDocumentStore(mds1).setBlobStore(bs).build();

        IndexConsistencyChecker checker = new IndexConsistencyChecker(dns1.getRoot(), "/oak:index/testIndex",
                new File(temporaryFolder.getRoot(), "checker-workdir"));
        IndexConsistencyChecker.Result result = checker.check(IndexConsistencyChecker.Level.BLOBS_ONLY);
        assertFalse("Nodestore1 can't read blobs: " + result.missingBlobIds + " while reading index",
                result.missingBlobs);
    }

    private class IndexMBeanInfoSupplier {
        private final String name;
        private final Supplier<String> statusSupplier;
        private final Supplier<Long> execCntSupplier;

        IndexMBeanInfoSupplier(String name, Supplier<String> statusSupplier, Supplier<Long> execCntSupplier) {
            this.name = name;
            this.statusSupplier = statusSupplier;
            this.execCntSupplier = execCntSupplier;
        }

        String getName() {
            return name;
        }

        String getStatus() {
            return statusSupplier.get();
        }

        long getExecCnt() {
            return execCntSupplier.get();
        }
    }

    private static class MockRegistrar {
        static AsyncIndexInfoService getAsyncIndexInfoService(List<IndexMBeanInfoSupplier> infoSuppliers) {

            AsyncIndexInfoService service = mock(AsyncIndexInfoService.class);

            List<String> asyncLanes = Lists.newArrayList();

            for (IndexMBeanInfoSupplier info : infoSuppliers) {
                String lane = info.getName();

                IndexStatsMBean bean = mock(IndexStatsMBean.class);
                when(bean.getName()).thenReturn(lane);
                when(bean.getStatus()).then(mockObj -> info.getStatus());
                when(bean.getTotalExecutionCount()).then(mockObj -> info.getExecCnt());

                when(service.getInfo(lane)).then(mockObj -> new AsyncIndexInfo(
                        lane,
                        1324L,
                        4567L,
                        STATUS_RUNNING.equals(info.getStatus()),
                        bean
                ));

                asyncLanes.add(lane);
            }
            when(service.getAsyncLanes()).thenReturn(asyncLanes);

            return service;
        }

        static IndexPathService getIndexPathsService(List<String> indexPaths) {
            IndexPathService service = mock(IndexPathService.class);
            when(service.getIndexPaths()).thenReturn(indexPaths);
            return service;
        }
    }

    private static class StatusSupplier implements Supplier<String> {
        String status = STATUS_DONE;

        @Override
        public String get() {
            return status;
        }
    }

    private static class PauseNotifyingActiveDeletedBlobCollector
            implements ActiveDeletedBlobCollector {
        private final Callable callback;

        PauseNotifyingActiveDeletedBlobCollector (@Nonnull Callable callback) {
            this.callback = callback;
        }
        @Override
        public BlobDeletionCallback getBlobDeletionCallback() {
            return ActiveDeletedBlobCollectorFactory.NOOP.getBlobDeletionCallback();
        }

        @Override
        public void purgeBlobsDeleted(long before, GarbageCollectableBlobStore blobStore) {
            ActiveDeletedBlobCollectorFactory.NOOP.purgeBlobsDeleted(before, blobStore);
        }

        @Override
        public void cancelBlobCollection() {
            ActiveDeletedBlobCollectorFactory.NOOP.cancelBlobCollection();
        }

        @Override
        public void flagActiveDeletionUnsafe(boolean toFlag) {
            try {
                callback.call();
            } catch (Exception e) {
                // ignored
            }
            ActiveDeletedBlobCollectorFactory.NOOP.flagActiveDeletionUnsafe(toFlag);
        }

        @Override
        public boolean isActiveDeletionUnsafe() {
            return ActiveDeletedBlobCollectorFactory.NOOP.isActiveDeletionUnsafe();
        }
    }

    private static class DeletedFileTrackingADBC implements ActiveDeletedBlobCollector {
        final List<String> deletedFiles = newArrayList();
        BlobDeletionCallback blobDeletionCallback = null;

        private final ActiveDeletedBlobCollector delegate;

        DeletedFileTrackingADBC(File tempFolder) {
            delegate = ActiveDeletedBlobCollectorFactory.newInstance(tempFolder, sameThreadExecutor());
        }

        @Override
        public BlobDeletionCallback getBlobDeletionCallback() {
            final BlobDeletionCallback deletionCallback = delegate.getBlobDeletionCallback();
            blobDeletionCallback = new BlobDeletionCallback() {
                @Override
                public void deleted(String blobId, Iterable<String> ids) {
                    deletedFiles.add(Iterables.getLast(ids));
                    deletionCallback.deleted(blobId, ids);
                }

                @Override
                public boolean isMarkingForActiveDeletionUnsafe() {
                    return deletionCallback.isMarkingForActiveDeletionUnsafe();
                }

                @Override
                public void commitProgress(IndexProgress indexProgress) {
                    deletionCallback.commitProgress(indexProgress);
                }
            };
            return blobDeletionCallback;
        }

        @Override
        public void purgeBlobsDeleted(long before, GarbageCollectableBlobStore blobStore) {
            delegate.purgeBlobsDeleted(before, blobStore);
        }

        @Override
        public void cancelBlobCollection() {
            delegate.cancelBlobCollection();
        }

        @Override
        public void flagActiveDeletionUnsafe(boolean toFlag) {
            delegate.flagActiveDeletionUnsafe(toFlag);
        }

        @Override
        public boolean isActiveDeletionUnsafe() {
            return delegate.isActiveDeletionUnsafe();
        }
    }

    private void createFakeIndex(String indexPath) throws CommitFailedException {
        String indexFileName = "fakeIndexFile";

        // create index in node store
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder nodeBuilder = rootBuilder;
        for (String elem : PathUtils.elements(indexPath)) {
            nodeBuilder = nodeBuilder.child(elem);
        }
        nodeBuilder.setProperty(TYPE_PROPERTY_NAME, TYPE_LUCENE);

        nodeBuilder = nodeBuilder.child(":data");
        nodeBuilder.child(indexFileName);

        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        indexPaths.add(indexPath);
    }

    private NodeState getFakeIndexFile(String indexPath) {
        String indexFileName = "fakeIndexFile";

        NodeState state = nodeStore.getRoot();
        for (String elem : PathUtils.elements(indexPath)) {
            state = state.getChildNode(elem);
        }
        state = state.getChildNode(":data").getChildNode(indexFileName);
        return state;
    }

    private ActiveDeletedBlobCollectorMBean getTestBean(IndexPathService indexPathService, AsyncIndexInfoService asyncIndexInfoService) {
        ActiveDeletedBlobCollectorMBeanImpl bean =
                new ActiveDeletedBlobCollectorMBeanImpl(ActiveDeletedBlobCollectorFactory.NOOP, wb, nodeStore,
                        indexPathService, asyncIndexInfoService,
                        new MemoryBlobStore(), sameThreadExecutor());
        bean.clock = clock;

        return bean;
    }
}
