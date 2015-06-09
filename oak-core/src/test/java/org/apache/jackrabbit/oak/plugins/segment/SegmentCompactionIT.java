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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateCancelledFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.io.File.createTempFile;
import static java.lang.String.valueOf;
import static java.lang.System.getProperty;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_OLD;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.MEMORY_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.file.FileStore.newFileStore;
import static org.junit.Assume.assumeTrue;
import static org.slf4j.helpers.MessageFormatter.arrayFormat;
import static org.slf4j.helpers.MessageFormatter.format;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

import javax.annotation.Nonnull;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.compaction.DefaultCompactionStrategyMBean;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStoreGCMonitor;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a longeivity test for SegmentMK compaction. The test schedules a number
 * of readers, writers, a compactor and holds some references for a certain time.
 * All of which can be interactively modified through the accompanying
 * {@link SegmentCompactionITMBean}, the
 * {@link org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategyMBean} and the
 * {@link org.apache.jackrabbit.oak.plugins.segment.file.GCMonitorMBean}.
 *
 * TODO Leverage longeivity test support from OAK-2771 once we have it.
 */
public class SegmentCompactionIT {
    /** Only run if explicitly asked to via -Dtest=SegmentCompactionIT */
    private static final boolean ENABLED =
            SegmentCompactionIT.class.getSimpleName().equals(getProperty("test"));
    private static final Logger LOG = LoggerFactory.getLogger(SegmentCompactionIT.class);

    private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    private final Random rnd = new Random();
    private final ListeningScheduledExecutorService scheduler =
            listeningDecorator(newScheduledThreadPool(50));
    private final FileStoreGCMonitor fileStoreGCMonitor = new FileStoreGCMonitor(Clock.SIMPLE);
    private final TestGCMonitor gcMonitor = new TestGCMonitor(fileStoreGCMonitor);
    private final Set<ListenableScheduledFuture<?>> writers = newConcurrentHashSet();
    private final Set<ListenableScheduledFuture<?>> readers = newConcurrentHashSet();
    private final Set<Reference> references = newConcurrentHashSet();
    private final SegmentCompactionITMBean segmentCompactionMBean = new SegmentCompactionITMBean();
    private final CompactionStrategy compactionStrategy = new CompactionStrategy(
            false, false, CLEAN_OLD, 60000, MEMORY_THRESHOLD_DEFAULT) {
        @Override
        public boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception {
            return nodeStore.locked(setHead);
        }
    };

    private File directory;
    private FileStore fileStore;
    private SegmentNodeStore nodeStore;
    private Registration mBeanRegistration;

    private volatile ListenableFuture<?> compactor = immediateCancelledFuture();
    private volatile int maxReaders = 10;
    private volatile int maxWriters = 10;
    private volatile long maxStoreSize = 200000000000L;
    private volatile int maxBlobSize = 1000000;
    private volatile int maxReferences = 10;
    private volatile int compactionInterval = 1;
    private volatile boolean stopping;
    private volatile Reference rootReference;

    public synchronized void stop() {
        stopping = true;
        notifyAll();
    }

    public void addReaders(int count) {
        for (int c = 0; c < count; c++) {
            scheduleReader();
        }
    }

    public void removeReaders(int count) {
        remove(readers, count);
    }

    public void addWriters(int count) {
        for (int c = 0; c < count; c++) {
            scheduleWriter();
        }
    }

    public void removeWriters(int count) {
        remove(writers, count);
    }

    public void removeReferences(int count) {
        Iterator<Reference> it = references.iterator();
        while (it.hasNext() && count-- > 0) {
            it.next().run();
            it.remove();
        }
    }

    private static void remove(Set<ListenableScheduledFuture<?>> futures, int count) {
        Iterator<ListenableScheduledFuture<?>> it = futures.iterator();
        while (it.hasNext() && count-- > 0) {
            it.next().cancel(false);
        }
    }

    private Registration registerMBean(Object mBean, final ObjectName objectName)
            throws NotCompliantMBeanException, InstanceAlreadyExistsException,
            MBeanRegistrationException {
        mBeanServer.registerMBean(mBean, objectName);
        return new Registration(){
            @Override
            public void unregister() {
                try {
                    mBeanServer.unregisterMBean(objectName);
                } catch (Exception e) {
                    LOG.error("Error unregistering Segment Compaction MBean", e);
                }
            }
        };
    }

    @Before
    public void setUp() throws IOException, MalformedObjectNameException, NotCompliantMBeanException,
            InstanceAlreadyExistsException, MBeanRegistrationException {
        assumeTrue(ENABLED);

        mBeanRegistration = new CompositeRegistration(
            registerMBean(segmentCompactionMBean, new ObjectName("IT:TYPE=Segment Compaction")),
            registerMBean(new DefaultCompactionStrategyMBean(compactionStrategy),
                    new ObjectName("IT:TYPE=Compaction Strategy")),
            registerMBean(fileStoreGCMonitor, new ObjectName("IT:TYPE=GC Monitor")));

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                fileStoreGCMonitor.run();
            }
        }, 1, 1, SECONDS);

        directory = createTempFile(getClass().getSimpleName(), "dir", new File("target"));
        directory.delete();
        directory.mkdir();

        fileStore = newFileStore(directory).withGCMonitor(gcMonitor).create();
        nodeStore = new SegmentNodeStore(fileStore);
        fileStore.setCompactionStrategy(compactionStrategy);
    }

    @After
    public void tearDown() throws InterruptedException {
        try {
            if (mBeanRegistration != null) {
                mBeanRegistration.unregister();
            }
            scheduler.shutdown();
            if (fileStore != null) {
                fileStore.close();
            }
            if (directory != null) {
                deleteDirectory(directory);
            }
        } catch (IOException e) {
            LOG.error("Error cleaning directory", e);
        }
    }

    @Test
    public void run() throws InterruptedException {
        scheduleCompactor();
        addReaders(maxReaders);
        addWriters(maxWriters);

        synchronized (this) {
            while (!stopping) {
                wait();
            }
        }
    }

    private synchronized void scheduleCompactor() {
        LOG.info("Scheduling compaction after {} minutes", compactionInterval);
        compactor.cancel(false);
        compactor = scheduler.schedule((new Compactor(fileStore, gcMonitor)), compactionInterval, MINUTES);
        addCallback(compactor, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                scheduleCompactor();
            }

            @Override
            public void onFailure(Throwable t) {
                segmentCompactionMBean.error("Compactor error", t);
            }
        });
    }

    private void scheduleWriter() {
        if (writers.size() < maxWriters) {
            final ListenableScheduledFuture<Void> writer = scheduler.schedule(
                    new RandomWriter(rnd, nodeStore, rnd.nextInt(500), "W" + rnd.nextInt(5)),
                    rnd.nextInt(30), SECONDS);
            writers.add(writer);
            addCallback(writer, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    writers.remove(writer);
                    if (!writer.isCancelled()) {
                        scheduleWriter();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    writers.remove(writer);
                    segmentCompactionMBean.error("Writer error", t);
                }
            });
        }
    }

    private void scheduleReader() {
        if (readers.size() < maxReaders) {
            final ListenableScheduledFuture<?> reader = rnd.nextBoolean()
                ? scheduler.schedule(new RandomNodeReader(rnd, nodeStore), rnd.nextInt(30), SECONDS)
                : scheduler.schedule(new RandomPropertyReader(rnd, nodeStore), rnd.nextInt(30), SECONDS);
            readers.add(reader);
            addCallback(reader, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object node) {
                    readers.remove(reader);
                    if (!reader.isCancelled()) {
                        if (rnd.nextBoolean()) {
                            scheduleReference(node);
                        } else {
                            scheduleReader();
                        }
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    readers.remove(reader);
                    segmentCompactionMBean.error("Node reader error", t);
                }
            });
        }
    }

    private void scheduleReference(Object object) {
        if (references.size() < maxReferences) {
            final Reference reference = new Reference(object);
            final ListenableScheduledFuture<?> ref = scheduler.schedule(
                    reference, rnd.nextInt(600), SECONDS);
            references.add(reference);
            addCallback(ref, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                    references.remove(reference);
                    if (!ref.isCancelled()) {
                        scheduleReader();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    references.remove(reference);
                    segmentCompactionMBean.error("Reference error", t);
                }
            });
        } else {
            scheduleReader();
        }
    }

    private class RandomWriter implements Callable<Void> {
        private final Random rnd;
        private final NodeStore nodeStore;
        private final int opCount;
        private final String itemPrefix;

        RandomWriter(Random rnd, NodeStore nodeStore, int opCount, String itemPrefix) {
            this.rnd = rnd;
            this.nodeStore = nodeStore;
            this.opCount = opCount;
            this.itemPrefix = itemPrefix;
        }

        @Override
        public Void call() throws Exception {
            NodeBuilder root = nodeStore.getRoot().builder();
            boolean deleteOnly = fileStore.size() > maxStoreSize;
            for (int k = 0; k < opCount; k++) {
                modify(nodeStore, root, deleteOnly);
            }
            nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            return null;
        }

        private void modify(NodeStore nodeStore, NodeBuilder nodeBuilder, boolean deleteOnly)
                throws IOException {
            int k = rnd.nextInt(100);
            if (k < 10) {
                chooseRandomNode(nodeBuilder).remove();
            } else if (k < 20) {
                removeRandomProperty(chooseRandomNode(nodeBuilder));
            } else if (k < 60 && !deleteOnly)  {
                addRandomNode(nodeBuilder);
            } else if (k < 80 && !deleteOnly) {
                addRandomValue(nodeBuilder);
            } else if (!deleteOnly) {
                addRandomBlob(nodeStore, nodeBuilder);
            }
        }

        private NodeBuilder chooseRandomNode(NodeBuilder nodeBuilder) {
            NodeBuilder childBuilder = nodeBuilder;
            for (int k = 0; k < rnd.nextInt(1000); k++) {
                childBuilder = randomStep(nodeBuilder, nodeBuilder = childBuilder);
            }
            return childBuilder;
        }

        private NodeBuilder randomStep(NodeBuilder parent, NodeBuilder node) {
            int count = (int) node.getChildNodeCount(Long.MAX_VALUE);
            int k = rnd.nextInt(count + 1);
            if (k == 0) {
                return parent;
            } else {
                String name = get(node.getChildNodeNames(), k - 1);
                return node.getChildNode(name);
            }
        }

        private void removeRandomProperty(NodeBuilder nodeBuilder) {
            int count = (int) nodeBuilder.getPropertyCount();
            if (count > 0) {
                PropertyState property = get(nodeBuilder.getProperties(), rnd.nextInt(count));
                nodeBuilder.removeProperty(property.getName());
            }
        }

        private void addRandomNode(NodeBuilder nodeBuilder) {
            if (nodeBuilder.getChildNodeCount(1000) < 1000) {
                chooseRandomNode(nodeBuilder).setChildNode('N' + itemPrefix + rnd.nextInt(1000));
            }
        }

        private void addRandomValue(NodeBuilder nodeBuilder) {
            if (nodeBuilder.getPropertyCount() < 1000) {
                chooseRandomNode(nodeBuilder).setProperty('P' + itemPrefix + rnd.nextInt(1000),
                        randomAlphabetic(rnd.nextInt(10000)));
            }
        }

        private void addRandomBlob(NodeStore nodeStore, NodeBuilder nodeBuilder) throws IOException {
            if (nodeBuilder.getPropertyCount() < 1000) {
                chooseRandomNode(nodeBuilder).setProperty('B' + itemPrefix + rnd.nextInt(1000),
                        createBlob(nodeStore, rnd.nextInt(maxBlobSize)));
            }
        }

        private Blob createBlob(NodeStore nodeStore, int size) throws IOException {
            byte[] data = new byte[size];
            new Random().nextBytes(data);
            return nodeStore.createBlob(new ByteArrayInputStream(data));
        }
    }

    private abstract static class RandomReader<T> implements Callable<T> {
        protected final Random rnd;
        protected final NodeStore nodeStore;

        RandomReader(Random rnd, NodeStore nodeStore) {
            this.rnd = rnd;
            this.nodeStore = nodeStore;
        }

        private NodeState randomStep(NodeState parent, NodeState node) {
            int count = (int) node.getChildNodeCount(Long.MAX_VALUE);
            int k = rnd.nextInt(count + 1);
            if (k == 0) {
                return parent;
            } else {
                String name = get(node.getChildNodeNames(), k - 1);
                return node.getChildNode(name);
            }
        }

        protected final NodeState chooseRandomNode(NodeState parent) {
            NodeState child = parent;
            for (int k = 0; k < rnd.nextInt(1000); k++) {
                child = randomStep(parent, parent = child);
            }
            return child;
        }

        protected final PropertyState chooseRandomProperty(NodeState node) throws Exception {
            int count = (int) node.getPropertyCount();
            if (count > 0) {
                return get(node.getProperties(), rnd.nextInt(count));
            } else {
                return null;
            }
        }
    }

    private static class RandomNodeReader extends RandomReader<NodeState> {
        RandomNodeReader(Random rnd, NodeStore nodeStore) {
            super(rnd, nodeStore);
        }

        @Override
        public NodeState call() throws Exception {
            return chooseRandomNode(nodeStore.getRoot());
        }
    }

    private static class RandomPropertyReader extends RandomReader<PropertyState> {
        RandomPropertyReader(Random rnd, NodeStore nodeStore) {
            super(rnd, nodeStore);
        }

        @Override
        public PropertyState call() throws Exception {
            return chooseRandomProperty(chooseRandomNode(nodeStore.getRoot()));
        }
    }

    private static class Reference implements Runnable {
        private volatile Object referent;

        Reference(Object referent) {
            this.referent = referent;
        }

        @Override
        public void run() {
            referent = null;
        }
    }

    private static class Compactor implements Runnable {
        private final FileStore fileStore;
        private final TestGCMonitor gcMonitor;

        Compactor(FileStore fileStore, TestGCMonitor gcMonitor) {
            this.fileStore = fileStore;
            this.gcMonitor = gcMonitor;
        }

        @Override
        public void run() {
            if (gcMonitor.isCleaned()) {
                LOG.info("Running compaction");
                gcMonitor.resetCleaned();
                fileStore.maybeCompact(true);
            } else {
                LOG.info("Not running compaction as no cleanup has taken place");
            }
        }
    }

    private static class TestGCMonitor implements GCMonitor {
        private final GCMonitor delegate;
        private volatile boolean cleaned = true;
        private volatile long lastCompacted;

        TestGCMonitor(GCMonitor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void info(String message, Object... arguments) {
            System.out.println(arrayFormat(message, arguments).getMessage());
            delegate.info(message, arguments);
        }

        @Override
        public void warn(String message, Object... arguments) {
            System.out.println(arrayFormat(message, arguments).getMessage());
            delegate.warn(message, arguments);
        }

        @Override
        public void error(String message, Exception exception) {
            System.out.println(format(message, exception).getMessage());
            delegate.error(message, exception);
        }

        @Override
        public void skipped(String reason, Object... arguments) {
            cleaned = true;
            System.out.println(arrayFormat(reason, arguments).getMessage());
            delegate.skipped(reason, arguments);
        }

        @Override
        public void compacted() {
            delegate.compacted();
            lastCompacted = System.currentTimeMillis();
        }

        @Override
        public void cleaned(long reclaimedSize, long currentSize) {
            cleaned = true;
            delegate.cleaned(reclaimedSize, currentSize);
        }

        public boolean isCleaned() {
            return cleaned;
        }

        public void resetCleaned() {
            cleaned = false;
        }

        public long getLastCompacted() {
            return lastCompacted;
        }
    }

    private class SegmentCompactionITMBean extends AnnotatedStandardMBean implements SegmentCompactionMBean {
        private String lastError;

        SegmentCompactionITMBean() {
            super(SegmentCompactionMBean.class);
        }

        @Override
        public void stop() {
            SegmentCompactionIT.this.stop();
        }

        @Override
        public void setCompactionInterval(int minutes) {
            if (compactionInterval != minutes) {
                compactionInterval = minutes;
                scheduleCompactor();
            }
        }

        @Override
        public int getCompactionInterval() {
            return compactionInterval;
        }

        @Override
        public String getLastCompaction() {
            return valueOf(new Date(gcMonitor.getLastCompacted()));
        }

        @Override
        public void setMaxReaders(int count) {
            checkArgument(count >= 0);
            maxReaders = count;
            if (count > readers.size()) {
                addReaders(count - readers.size());
            } else {
                removeReaders(readers.size() - count);
            }
        }

        @Override
        public int getMaxReaders() {
            return maxReaders;
        }

        @Override
        public void setMaxWriters(int count) {
            checkArgument(count >= 0);
            maxWriters = count;
            if (count > writers.size()) {
                addWriters(count - writers.size());
            } else {
                removeWriters(writers.size() - count);
            }
        }

        @Override
        public int getMaxWriters() {
            return maxWriters;
        }

        @Override
        public void setMaxStoreSize(long size) {
            maxStoreSize = size;
        }

        @Override
        public long getMaxStoreSize() {
            return maxStoreSize;
        }

        @Override
        public void setMaxBlobSize(int size) {
            maxBlobSize = size;
        }

        @Override
        public int getMaxBlobSize() {
            return maxBlobSize;
        }

        @Override
        public void setMaxReferences(int count) {
            checkArgument(count >= 0);
            maxReferences = count;
            if (count < references.size()) {
                removeReferences(references.size() - count);
            }
        }

        @Override
        public int getMaxReferences() {
            return maxReferences;
        }

        @Override
        public void setRootReference(boolean set) {
            if (set && rootReference == null) {
                rootReference = new Reference(nodeStore.getRoot());
            } else {
                rootReference = null;
            }
        }

        @Override
        public boolean getRootReference() {
            return rootReference != null;
        }

        @Override
        public int getReaderCount() {
            return readers.size();
        }

        @Override
        public int getWriterCount() {
            return writers.size();
        }

        @Override
        public int getReferenceCount() {
            return references.size();
        }

        @Override
        public long getFileStoreSize() {
            try {
                return fileStore.size();
            } catch (IOException e) {
                error("Error getting size of file store", e);
                return -1;
            }
        }

        @Override
        public long getCompactionMapWeight() {
            return fileStore.getTracker().getCompactionMap().getEstimatedWeight();
        }

        @Override
        public int getCompactionMapDepth() {
            return fileStore.getTracker().getCompactionMap().getDepth();
        }

        @Override
        public String getLastError() {
            return lastError;
        }

        void error(String message, Throwable t) {
            if (!(t instanceof CancellationException)) {
                StringWriter sw = new StringWriter();
                sw.write(message + ": ");
                t.printStackTrace(new PrintWriter(sw));
                lastError = sw.toString();

                LOG.error(message, t);
            }
        }
    }
}
