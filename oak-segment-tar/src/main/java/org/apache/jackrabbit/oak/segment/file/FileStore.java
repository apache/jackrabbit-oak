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
package org.apache.jackrabbit.oak.segment.file;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.PrintableBytes.newPrintableBytes;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.base.Supplier;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.ShutDown.ShutDownCloser;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.oak.stats.TimerStats.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The storage implementation for tar files.
 */
public class FileStore extends AbstractFileStore {
    private static final Logger log = LoggerFactory.getLogger(FileStore.class);
    private static final int MB = 1024 * 1024;

    /**
     * Name of the {@link CounterStats counter} exposing the number of {@code TarReader}
     * instances in use by {@link TarFiles}.
     */
    private static final String TAR_READER_COUNT = "TAR_READER_COUNT";

    /**
     * Name of the {@link CounterStats counter} exposing the number of segments.
     */
    private static final String SEGMENT_COUNT = "SEGMENT_COUNT";


    private static GarbageCollectionStrategy newGarbageCollectionStrategy() {
        if (Boolean.getBoolean("gc.classic")) {
            return new SynchronizedGarbageCollectionStrategy(new DefaultGarbageCollectionStrategy());
        }
        return new SynchronizedGarbageCollectionStrategy(new CleanupFirstGarbageCollectionStrategy());
    }

    @NotNull
    private final SegmentWriter segmentWriter;

    @NotNull
    private final GarbageCollector garbageCollector;

    private final TarFiles tarFiles;

    private final RepositoryLock repositoryLock;

    private volatile TarRevisions revisions;

    /**
     * Scheduler for running <em>short</em> background operations
     */
    private final Scheduler fileStoreScheduler = new Scheduler("FileStore background tasks");

    /**
     * List of old tar file generations that are waiting to be removed. They can
     * not be removed immediately, because they first need to be closed, and the
     * JVM needs to release the memory mapped file references.
     */
    private final FileReaper fileReaper;

    /**
     * This flag is periodically updated by calling the {@code SegmentGCOptions}
     * at regular intervals.
     */
    private final AtomicBoolean sufficientDiskSpace = new AtomicBoolean(true);

    /**
     * This flag is raised whenever the available memory falls under a specified
     * threshold. See {@link GCMemoryBarrier}
     */
    private final AtomicBoolean sufficientMemory = new AtomicBoolean(true);

    private final FileStoreStats stats;

    private final ShutDown shutDown = new ShutDown();

    @NotNull
    private final SegmentNotFoundExceptionListener snfeListener;

    private final GarbageCollectionStrategy garbageCollectionStrategy = newGarbageCollectionStrategy();

    private final boolean eagerSegmentCaching;

    FileStore(final FileStoreBuilder builder) throws InvalidFileStoreVersionException, IOException {
        super(builder);

        SegmentNodeStorePersistence persistence = builder.getPersistence();
        repositoryLock = persistence.lockRepository();
        StatisticsProvider statsProvider = builder.getStatsProvider();

        this.segmentWriter = defaultSegmentWriterBuilder("sys")
                .withGeneration(() -> getGcGeneration().nonGC())
                .withWriterPool()
                .with(builder.getCacheManager()
                        .withAccessTracking("WRITE", statsProvider))
                .build(this);

        newManifestChecker(persistence, builder.getStrictVersionCheck()).checkAndUpdateManifest();

        this.stats = new FileStoreStats(statsProvider, this, 0);

        CounterStats readerCountStats = statsProvider.getCounterStats(TAR_READER_COUNT, DEFAULT);
        CounterStats segmentCountStats = statsProvider.getCounterStats(SEGMENT_COUNT, DEFAULT);
        TarFiles.Builder tarFilesBuilder = TarFiles.builder()
                .withDirectory(directory)
                .withMemoryMapping(memoryMapping)
                .withTarRecovery(recovery)
                .withIOMonitor(ioMonitor)
                .withFileStoreMonitor(stats)
                .withMaxFileSize(builder.getMaxFileSize() * MB)
                .withPersistence(builder.getPersistence())
                .withReaderCountStats(readerCountStats)
                .withSegmentCountStats(segmentCountStats);

        this.tarFiles = tarFilesBuilder.build();
        long size = this.tarFiles.size();
        this.stats.init(size);

        this.fileReaper = this.tarFiles.createFileReaper();

        this.garbageCollector = new GarbageCollector(
            builder.getGcOptions(),
            builder.getGcListener(),
            new GCJournal(persistence.getGCJournalFile()),
            sufficientMemory,
            fileReaper,
            tarFiles,
            tracker,
            segmentReader,
            () -> revisions,
            getBlobStore(),
            segmentCache,
            segmentWriter,
            stats,
            Canceller.newCanceller()
                .withCondition("not enough disk space", () -> !sufficientDiskSpace.get())
                .withCondition("not enough memory", () -> !sufficientMemory.get())
                .withCondition("FileStore is shutting down", shutDown::isShutDown),
            this::flush,
            generation ->
                defaultSegmentWriterBuilder("c")
                    .with(builder.getCacheManager().withAccessTracking("COMPACT", statsProvider))
                    .withGeneration(generation)
                    .withoutWriterPool()
                    .build(this)
        );

        this.snfeListener = builder.getSnfeListener();
        this.eagerSegmentCaching = builder.getEagerSegmentCaching();

        TimerStats flushTimer = statsProvider.getTimer("oak.segment.flush", METRICS_ONLY);
        fileStoreScheduler.scheduleWithFixedDelay(format("TarMK flush [%s]", directory), 5, SECONDS, () -> {
            Context timer = flushTimer.time();
            try {
                tryFlush();
            } finally {
                timer.stop();
            }
        });

        fileStoreScheduler.scheduleWithFixedDelay(format("TarMK filer reaper [%s]", directory), 5, SECONDS,
                                                  fileReaper::reap);

        fileStoreScheduler.scheduleWithFixedDelay(format("TarMK disk space check [%s]", directory), 1, MINUTES, () -> {
           try (ShutDownCloser ignore = shutDown.tryKeepAlive()) {
               if (shutDown.isShutDown()) {
                   log.debug("Shut down in progress, skipping disk space check");
               } else {
                   checkDiskSpace(builder.getGcOptions());
               }
           }
        });

        log.info("TarMK opened at {}, mmap={}, offHeapAccess={}, size={}",
            directory,
            memoryMapping,
            offHeapAccess,
            newPrintableBytes(size)
        );
        log.debug("TAR files: {}", tarFiles);
    }

    FileStore bind(TarRevisions revisions) throws IOException {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            this.revisions = revisions;
            this.revisions.bind(this, tracker, initialNode());
            return this;
        }
    }

    @NotNull
    private Supplier<RecordId> initialNode() {
        return new Supplier<RecordId>() {
            @Override
            public RecordId get() {
                try {
                    SegmentWriter writer = defaultSegmentWriterBuilder("init").build(FileStore.this);
                    NodeBuilder builder = EMPTY_NODE.builder();
                    builder.setChildNode("root", EMPTY_NODE);
                    SegmentNodeState node = new SegmentNodeState(segmentReader, writer, getBlobStore(), writer.writeNode(builder.getNodeState()));
                    writer.flush();
                    return node.getRecordId();
                } catch (IOException e) {
                    String msg = "Failed to write initial node";
                    log.error(msg, e);
                    throw new IllegalStateException(msg, e);
                }
            }
        };
    }

    @NotNull
    private GCGeneration getGcGeneration() {
        return revisions.getHead().getSegmentId().getGcGeneration();
    }

    /**
     * @return  a runnable for running garbage collection
     */
    public Runnable getGCRunner() {
        return new SafeRunnable(format("TarMK revision gc [%s]", directory), () -> {
            try (ShutDownCloser ignored = shutDown.keepAlive()) {
                garbageCollector.run(garbageCollectionStrategy);
            } catch (IOException e) {
                log.error("Error running revision garbage collection", e);
            }
        });
    }

    /**
     * @return the currently active gc write monitor
     */
    public GCNodeWriteMonitor getGCNodeWriteMonitor() {
        return garbageCollector.getGCNodeWriteMonitor();
    }

    /**
     * @return the size of this store.
     */
    private long size() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return tarFiles.size();
        }
    }

    public int readerCount() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return tarFiles.readerCount();
        }
    }

    /**
     * @return  the number of segments in the segment store
     */
    public int getSegmentCount() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return tarFiles.segmentCount();
        }
    }

    public FileStoreStats getStats() {
        return stats;
    }

    /*
     * Callers of this method must hold the shutdown lock
     */
    private void doFlush() throws IOException {
        if (revisions == null) {
            log.debug("No TarRevisions available, skipping flush");
            return;
        }
        revisions.flush(() -> {
            segmentWriter.flush();
            tarFiles.flush();
            stats.flushed();
        });
    }

    /**
     * Flush all pending changes
     */
    public void flush() throws IOException {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            doFlush();
        }
    }

    /**
     * Try to flush all pending changes to disk if possible without waiting
     * for a lock or other resources currently not available.
     */
    public void tryFlush() {
        try (ShutDownCloser ignore = shutDown.tryKeepAlive()) {
            if (shutDown.isShutDown()) {
                log.debug("Shut down in progress, skipping flush");
            } else if (revisions == null) {
                log.debug("No TarRevisions available, skipping flush");
            } else {
                revisions.tryFlush(() -> {
                    segmentWriter.flush();
                    tarFiles.flush();
                    stats.flushed();
                });
            }
        } catch (IOException e) {
            log.warn("Failed to flush the TarMK at {}", directory, e);
        }
    }

    /**
     * Run full garbage collection: estimation, compaction, cleanup.
     */
    public void fullGC() throws IOException {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            garbageCollector.runFull(garbageCollectionStrategy);
        }
    }

    /**
     * Run tail garbage collection.
     */
    public void tailGC() throws IOException {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            garbageCollector.runTail(garbageCollectionStrategy);
        }
    }

    /**
     * Copy every referenced record in data (non-bulk) segments. Bulk segments
     * are fully kept (they are only removed in cleanup, if there is no
     * reference to them).
     * @return {@code true} on success, {@code false} otherwise.
     */
    public boolean compactFull() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return garbageCollector.compactFull(garbageCollectionStrategy).isSuccess();
        } catch (IOException e) {
            log.warn("Unable to perform full compaction", e);
            return false;
        }
    }

    public boolean compactTail() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return garbageCollector.compactTail(garbageCollectionStrategy).isSuccess();
        } catch (IOException e) {
            log.warn("Unable to perform tail compaction");
            return false;
        }
    }

    /**
     * Run garbage collection on the segment level: reclaim those data segments
     * that are from an old segment generation and those bulk segments that are not
     * reachable anymore.
     * Those tar files that shrink by at least 25% are rewritten to a new tar generation
     * skipping the reclaimed segments.
     */
    public void cleanup() throws IOException {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            fileReaper.add(garbageCollector.cleanup(garbageCollectionStrategy));
        }
    }

    /**
     * Finds all external blob references that are currently accessible
     * in this repository and adds them to the given collector. Useful
     * for collecting garbage in an external data store.
     * <p>
     * Note that this method only collects blob references that are already
     * stored in the repository (at the time when this method is called), so
     * the garbage collector will need some other mechanism for tracking
     * in-memory references and references stored while this method is
     * running.
     * @param collector  reference collector called back for each blob reference found
     */
    @Override
    public void collectBlobReferences(Consumer<String> collector) throws IOException {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            garbageCollector.collectBlobReferences(collector);
        }
    }

    /**
     * Cancel a running revision garbage collection compaction process as soon as possible.
     * Does nothing if gc is not running.
     */
    public void cancelGC() {
        garbageCollector.cancel();
    }

    @Override
    @NotNull
    public SegmentWriter getWriter() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return segmentWriter;
        }
    }

    @Override
    @NotNull
    public TarRevisions getRevisions() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return revisions;
        }
    }

    @Override
    public void close() {
        try (ShutDownCloser ignored = shutDown.shutDown()) {
            // avoid deadlocks by closing (and joining) the background
            // thread before acquiring the synchronization lock
            fileStoreScheduler.close();

            try {
                doFlush();
            } catch (IOException e) {
                log.warn("Unable to flush the store", e);
            }

            Closer closer = Closer.create();
            closer.register(repositoryLock::unlock);
            closer.register(tarFiles) ;
            closer.register(revisions);

            closeAndLogOnFail(closer);
        }

        // Try removing pending files in case the scheduler didn't have a chance to run yet
        System.gc(); // for any memory-mappings that are no longer used
        fileReaper.reap();

        log.info("TarMK closed: {}", directory);
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return tarFiles.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
        }
    }

    @Override
    @NotNull
    public Segment readSegment(final SegmentId id) {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return segmentCache.getSegment(id, () -> readSegmentUncached(tarFiles, id));
        } catch (ExecutionException | UncheckedExecutionException e) {
            SegmentNotFoundException snfe = asSegmentNotFoundException(e, id);
            snfeListener.notify(id, snfe);
            stats.notify(id, snfe);
            throw snfe;
        }
    }

    @Override
    public void writeSegment(SegmentId id, byte[] buffer, int offset, int length) throws IOException {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            Segment segment = null;

            // If the segment is a data segment, create a new instance of Segment to
            // access some internal information stored in the segment and to store
            // in an in-memory cache for later use.

            GCGeneration generation = GCGeneration.NULL;
            Set<UUID> references = null;
            Set<String> binaryReferences = null;

            if (id.isDataSegmentId()) {
                Buffer data;

                if (offset > 4096) {
                    data = Buffer.allocate(length);
                    data.put(buffer, offset, length);
                    data.rewind();
                } else {
                    data = Buffer.wrap(buffer, offset, length);
                }

                segment = new Segment(tracker, segmentReader, id, data);

                if (eagerSegmentCaching) {
                    segmentCache.putSegment(segment);
                }

                generation = segment.getGcGeneration();
                references = readReferences(segment);
                binaryReferences = readBinaryReferences(segment);
            }

            tarFiles.writeSegment(
                id.asUUID(),
                buffer,
                offset,
                length,
                generation,
                references,
                binaryReferences
            );

            // Keep this data segment in memory as it's likely to be accessed soon.
            if (!eagerSegmentCaching && segment != null) {
                segmentCache.putSegment(segment);
            }
        }
    }

    private void checkDiskSpace(SegmentGCOptions gcOptions) {
        long repositoryDiskSpace = size();
        long availableDiskSpace = directory.getFreeSpace();
        boolean updated = SegmentGCOptions.isDiskSpaceSufficient(repositoryDiskSpace, availableDiskSpace);
        boolean previous = sufficientDiskSpace.getAndSet(updated);

        if (previous && !updated) {
            log.warn("Available disk space ({}) is too low, current repository size is approx. {}",
                    humanReadableByteCount(availableDiskSpace),
                    humanReadableByteCount(repositoryDiskSpace));
        }

        if (updated && !previous) {
            log.info("Available disk space ({}) is sufficient again for repository operations, current repository size is approx. {}",
                    humanReadableByteCount(availableDiskSpace),
                    humanReadableByteCount(repositoryDiskSpace));
        }
    }

}
