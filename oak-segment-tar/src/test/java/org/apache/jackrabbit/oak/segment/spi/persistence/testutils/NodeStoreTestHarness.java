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
package org.apache.jackrabbit.oak.segment.spi.persistence.testutils;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.AbstractFileStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class NodeStoreTestHarness implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStoreTestHarness.class);

    /**
     * The rule provides factory methods for {@code NodeStoreTestHarness} instances and manages their lifecycle.
     */
    public static final class Rule extends ExternalResource {

        private final TemporaryFolder tempFolderRule = new TemporaryFolder(new File("target"));

        private final List<Closeable> closeables = new ArrayList<>();

        @Override
        public Statement apply(Statement base, Description description) {
            return RuleChain.outerRule(tempFolderRule)
                    .around(new ExternalResource() {
                        @Override
                        protected void after() {
                            Collections.reverse(closeables);
                            closeables.forEach(closeable -> {
                                try {
                                    closeable.close();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                            super.after();
                        }
                    }).apply(base, description);
        }

        private void registerCloseable(Closeable closeable) {
            closeables.add(closeable);
        }

        public NodeStoreTestHarness createHarness(SegmentNodeStorePersistence persistence)
                throws IOException, InvalidFileStoreVersionException {
            final NodeStoreTestHarness nodeStoreTestHarness = new NodeStoreTestHarness(persistence, tempFolderRule.newFolder(), false);
            registerCloseable(nodeStoreTestHarness);
            return nodeStoreTestHarness;
        }

        public NodeStoreTestHarness createHarnessWithFolder(Function<File, SegmentNodeStorePersistence> persistenceFactory)
                throws IOException, InvalidFileStoreVersionException {
            final File dummyDirectory = tempFolderRule.newFolder();
            final NodeStoreTestHarness nodeStoreTestHarness = new NodeStoreTestHarness(
                    persistenceFactory.apply(dummyDirectory),
                    dummyDirectory,
                    false
            );
            registerCloseable(nodeStoreTestHarness);
            return nodeStoreTestHarness;
        }
    }

    private final File dummyDirectory;

    private boolean readOnly;

    private final SegmentNodeStorePersistence persistence;

    private final List<String> filesToBeDeletedByGcFileReaper = new CopyOnWriteArrayList<>();

    private volatile AbstractFileStore fileStore;

    private volatile SegmentNodeStore nodeStore;

    // latch used to wait for tar files to be cleaned up after GC
    private volatile CountDownLatch gcLatch = new CountDownLatch(0);

    private NodeStoreTestHarness(SegmentNodeStorePersistence persistence, File dummyDirectory, boolean readOnly) throws InvalidFileStoreVersionException, IOException {
        this.persistence = new PersistenceDecorator(persistence, this::fileDeleted);
        this.dummyDirectory = dummyDirectory;
        this.readOnly = readOnly;
        initializeFileStore();
    }

    private void fileDeleted(String archiveName) {
        filesToBeDeletedByGcFileReaper.remove(archiveName);
        if (filesToBeDeletedByGcFileReaper.isEmpty()) {
            gcLatch.countDown();
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly() throws InvalidFileStoreVersionException, IOException {
        this.readOnly = true;
        startNewTarFile(); // closes and re-initializes the file store
    }

    public SegmentNodeStorePersistence getPersistence() {
        return persistence;
    }

    public AbstractFileStore getFileStore() {
        return fileStore;
    }

    public SegmentNodeStore getNodeStore() {
        return nodeStore;
    }

    public boolean runGC() throws IOException, InterruptedException, InvalidFileStoreVersionException {
        gcLatch = new CountDownLatch(1);
        try {
            Optional.of(fileStore)
                    .filter(FileStore.class::isInstance)
                    .map(FileStore.class::cast)
                    .ifPresent(store -> {
                        try {
                            store.fullGC();
                            store.flush();
                        } catch (IOException e) {
                            throw new RuntimeException("rethrown as unchecked", e);
                        }
                    });

        } catch (RuntimeException e) {
            if (Objects.equals(e.getMessage(), "rethrown as unchecked") && e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }

        try {
            LOG.info("waiting for file reaper to delete {}", filesToBeDeletedByGcFileReaper);
            startNewTarFile(); // optional: file reaper is triggered on close(), so starting a new file this speeds up the test
            return gcLatch.await(6, TimeUnit.SECONDS); // FileReaper is scheduled to run every 5 seconds
        } finally {
            LOG.info("finished waiting, gc and file reaper are now complete");
        }
    }

    public void startNewTarFile() throws IOException, InvalidFileStoreVersionException {
        try {
            getFileStore().close();
        } finally {
            initializeFileStore();
        }
    }

    public void writeAndCommit(Consumer<NodeBuilder> action) throws CommitFailedException {
        final SegmentNodeStore nodeStore = getNodeStore();
        NodeBuilder builder = nodeStore.getRoot().builder();
        action.accept(builder);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    public NodeState getRoot() {
        return getNodeStore().getRoot();
    }

    public NodeState getNodeState(String path) {
        return StreamSupport.stream(PathUtils.elements(path).spliterator(), false)
                .reduce(getRoot(), NodeState::getChildNode, (nodeState, nodeState2) -> nodeState);
    }

    @Override
    public void close() throws IOException {
        fileStore.close();
    }

    private void initializeFileStore() throws InvalidFileStoreVersionException, IOException {
        if (isReadOnly()) {
            initializeReadOnlyFileStore();
        } else {
            initializeReadWriteFileStore();
        }
    }

    private void initializeReadWriteFileStore() throws InvalidFileStoreVersionException, IOException {
        final FileStore fileStore = FileStoreBuilder.fileStoreBuilder(dummyDirectory)
                .withGCMonitor(new DelegatingGCMonitor() {
                    @Override
                    public void info(String message, Object... arguments) {
                        // this is a poor man's way to wait for GC completion, but probably good enough for a test
                        if (message.endsWith("cleanup marking files for deletion: {}")) {
                            if (arguments.length == 1 && arguments[0] instanceof String) {
                                final ArrayList<String> localFiles = Arrays.stream(((String) arguments[0]).split(","))
                                        .map(String::trim)
                                        .filter(name -> !Objects.equals(name, "none"))
                                        .collect(Collectors.toCollection(ArrayList::new));
                                if (gcLatch.getCount() > 0) {
                                    LOG.info("adding files to be deleted {}", localFiles);
                                    filesToBeDeletedByGcFileReaper.addAll(localFiles);
                                }
                                if (filesToBeDeletedByGcFileReaper.isEmpty()) {
                                    gcLatch.countDown();
                                }
                            }
                        }
                        super.info(message, arguments);
                    }
                })
                .withCustomPersistence(persistence)
                .withMaxFileSize(1)
                .withGCOptions(new SegmentGCOptions().setEstimationDisabled(true))
                .build();

        this.fileStore = fileStore;
        this.nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    private void initializeReadOnlyFileStore() throws InvalidFileStoreVersionException, IOException {
        final ReadOnlyFileStore readOnlyFileStore = FileStoreBuilder.fileStoreBuilder(dummyDirectory)
                .withCustomPersistence(persistence)
                .buildReadOnly();
        this.fileStore = readOnlyFileStore;
        this.nodeStore = SegmentNodeStoreBuilders.builder(readOnlyFileStore).build();
    }

    public SegmentArchiveManager createArchiveManager() throws IOException {
        return getPersistence().createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
    }
}
