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

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Integer.getInteger;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.FULL;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.TAIL;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.CLEANUP;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION_FORCE_COMPACT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION_RETRY;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.ESTIMATION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.IDLE;
import static org.apache.jackrabbit.oak.segment.file.PrintableBytes.newPrintableBytes;
import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newOldReclaimer;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.EXPEDITE_OPTION;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.timeout;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.segment.CheckpointCompactor;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType;
import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;
import org.apache.jackrabbit.oak.segment.file.ShutDown.ShutDownCloser;
import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles.CleanupResult;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The storage implementation for tar files.
 */
public class FileStore extends AbstractFileStore {

    private static final Logger log = LoggerFactory.getLogger(FileStore.class);

    /**
     * Minimal interval in milli seconds between subsequent garbage collection cycles.
     * Garbage collection invoked via {@link #fullGC()} will be skipped unless at least
     * the specified time has passed since its last successful invocation.
     */
    private static final long GC_BACKOFF = getInteger("oak.gc.backoff", 10*3600*1000);

    private static final int MB = 1024 * 1024;

    static final String LOCK_FILE_NAME = "repo.lock";

    /**
     * GC counter for logging purposes
     */
    private static final AtomicLong GC_COUNT = new AtomicLong(0);

    @Nonnull
    private final SegmentWriter segmentWriter;

    @Nonnull
    private final GarbageCollector garbageCollector;

    private final TarFiles tarFiles;

    private final RandomAccessFile lockFile;

    @Nonnull
    private final FileLock lock;

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
    private final FileReaper fileReaper = new FileReaper();

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

    @Nonnull
    private final SegmentNotFoundExceptionListener snfeListener;

    FileStore(final FileStoreBuilder builder) throws InvalidFileStoreVersionException, IOException {
        super(builder);

        lockFile = new RandomAccessFile(new File(directory, LOCK_FILE_NAME), "rw");
        try {
            lock = lockFile.getChannel().lock();
        } catch (OverlappingFileLockException ex) {
            throw new IllegalStateException(directory.getAbsolutePath()
                    + " is in use by another store.", ex);
        }

        this.segmentWriter = defaultSegmentWriterBuilder("sys")
                .withGeneration(() -> getGcGeneration().nonGC())
                .withWriterPool()
                .with(builder.getCacheManager()
                        .withAccessTracking("WRITE", builder.getStatsProvider()))
                .build(this);
        this.garbageCollector = new GarbageCollector(
                builder.getGcOptions(),
                builder.getGcListener(),
                new GCJournal(directory),
                builder.getCacheManager()
                        .withAccessTracking("COMPACT", builder.getStatsProvider()));

        newManifestChecker(directory, builder.getStrictVersionCheck()).checkAndUpdateManifest();

        this.stats = new FileStoreStats(builder.getStatsProvider(), this, 0);
        this.tarFiles = TarFiles.builder()
                .withDirectory(directory)
                .withMemoryMapping(memoryMapping)
                .withTarRecovery(recovery)
                .withIOMonitor(ioMonitor)
                .withFileStoreMonitor(stats)
                .withMaxFileSize(builder.getMaxFileSize() * MB)
                .build();
        long size = this.tarFiles.size();
        this.stats.init(size);

        this.snfeListener = builder.getSnfeListener();

        fileStoreScheduler.scheduleAtFixedRate(format("TarMK flush [%s]", directory), 5, SECONDS,
                                               this::tryFlush);

        fileStoreScheduler.scheduleAtFixedRate(format("TarMK filer reaper [%s]", directory), 5, SECONDS,
                                               fileReaper::reap);

        fileStoreScheduler.scheduleAtFixedRate(format("TarMK disk space check [%s]", directory), 1, MINUTES, () -> {
           try (ShutDownCloser ignore = shutDown.tryKeepAlive()) {
               if (shutDown.isShutDown()) {
                   log.debug("Shut down in progress, skipping disk space check");
               } else {
                   checkDiskSpace(builder.getGcOptions());
               }
           }
        });

        log.info("TarMK opened at {}, mmap={}, size={}",
            directory,
            memoryMapping,
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

    @Nonnull
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

    @Nonnull
    private GCGeneration getGcGeneration() {
        return revisions.getHead().getSegmentId().getGcGeneration();
    }

    /**
     * @return  a runnable for running garbage collection
     */
    public Runnable getGCRunner() {
        return new SafeRunnable(format("TarMK revision gc [%s]", directory), () -> {
            try (ShutDownCloser ignored = shutDown.keepAlive()) {
                garbageCollector.run();
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
            garbageCollector.runFull();
        }
    }

    /**
     * Run tail garbage collection.
     */
    public void tailGC() throws IOException {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            garbageCollector.runTail();
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
            return garbageCollector.compactFull().isSuccess();
        }
    }

    public boolean compactTail() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return garbageCollector.compactTail().isSuccess();
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
            fileReaper.add(garbageCollector.cleanup());
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
    @Nonnull
    public SegmentWriter getWriter() {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return segmentWriter;
        }
    }

    @Override
    @Nonnull
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
            closer.register(lockFile);
            closer.register(lock::release);
            closer.register(tarFiles);
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
    @Nonnull
    public Segment readSegment(final SegmentId id) {
        try (ShutDownCloser ignored = shutDown.keepAlive()) {
            return segmentCache.getSegment(id, () -> readSegmentUncached(tarFiles, id));
        } catch (ExecutionException | UncheckedExecutionException e) {
            SegmentNotFoundException snfe = asSegmentNotFoundException(e, id);
            snfeListener.notify(id, snfe);
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
                ByteBuffer data;

                if (offset > 4096) {
                    data = ByteBuffer.allocate(length);
                    data.put(buffer, offset, length);
                    data.rewind();
                } else {
                    data = ByteBuffer.wrap(buffer, offset, length);
                }

                segment = new Segment(tracker, segmentReader, id, data);
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
            if (segment != null) {
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

    private class GarbageCollector {

        @Nonnull
        private final SegmentGCOptions gcOptions;

        /**
         * {@code GcListener} listening to this instance's gc progress
         */
        @Nonnull
        private final PrefixedGCListener gcListener;

        @Nonnull
        private final GCJournal gcJournal;

        @Nonnull
        private final WriterCacheManager cacheManager;

        @Nonnull
        private GCNodeWriteMonitor compactionMonitor = GCNodeWriteMonitor.EMPTY;

        private volatile boolean cancelled;

        /**
         * Timestamp of the last time {@link #fullGC()} or {@link #tailGC()} was
         * successfully invoked. 0 if never.
         */
        private long lastSuccessfullGC;

        /**
         * Last compaction type used to determine which predicate to use during
         * {@link #cleanup() cleanup}. Defaults to {@link GCType#FULL FULL}, which is
         * conservative and safe in case it does not match the real type (e.g. because
         * of a system restart).
         */
        @Nonnull
        private GCType lastCompactionType = FULL;

        GarbageCollector(
                @Nonnull SegmentGCOptions gcOptions,
                @Nonnull GCListener gcListener,
                @Nonnull GCJournal gcJournal,
                @Nonnull WriterCacheManager cacheManager) {
            this.gcOptions = gcOptions;
            this.gcListener = new PrefixedGCListener(gcListener, GC_COUNT);
            this.gcJournal = gcJournal;
            this.cacheManager = cacheManager;
        }

        GCNodeWriteMonitor getGCNodeWriteMonitor() {
            return compactionMonitor;
        }

        synchronized void run() throws IOException {
            switch (gcOptions.getGCType()) {
                case FULL:
                    runFull();
                    break;
                case TAIL:
                    runTail();
                    break;
                default:
                    throw new IllegalStateException("Invalid GC type");
            }
        }

        synchronized void runFull() throws IOException {
            run(true, this::compactFull);
        }

        synchronized void runTail() throws IOException {
            run(false, this::compactTail);
        }

        private void run(boolean full, Supplier<CompactionResult> compact) throws IOException {
            try {
                GC_COUNT.incrementAndGet();

                gcListener.info("started");

                long dt = System.currentTimeMillis() - lastSuccessfullGC;
                if (dt < GC_BACKOFF) {
                    gcListener.skipped("skipping garbage collection as it already ran " +
                        "less than {} hours ago ({} s).", GC_BACKOFF / 3600000, dt / 1000);
                    return;
                }

                boolean sufficientEstimatedGain = true;
                if (gcOptions.isEstimationDisabled()) {
                    gcListener.info("estimation skipped because it was explicitly disabled");
                } else if (gcOptions.isPaused()) {
                    gcListener.info("estimation skipped because compaction is paused");
                } else {
                    gcListener.info("estimation started");
                    gcListener.updateStatus(ESTIMATION.message());

                    PrintableStopwatch watch = PrintableStopwatch.createStarted();
                    GCEstimationResult estimation = estimateCompactionGain(full);
                    sufficientEstimatedGain = estimation.isGcNeeded();
                    String gcLog = estimation.getGcLog();
                    if (sufficientEstimatedGain) {
                        gcListener.info("estimation completed in {}. {}", watch, gcLog);
                    } else {
                        gcListener.skipped("estimation completed in {}. {}", watch, gcLog);
                    }
                }

                if (sufficientEstimatedGain) {
                    try (GCMemoryBarrier ignored = new GCMemoryBarrier(sufficientMemory, gcListener, gcOptions)) {
                        if (gcOptions.isPaused()) {
                            gcListener.skipped("compaction paused");
                        } else if (!sufficientMemory.get()) {
                            gcListener.skipped("compaction skipped. Not enough memory");
                        } else {
                            CompactionResult compactionResult = compact.get();
                            if (compactionResult.isSuccess()) {
                                lastSuccessfullGC = System.currentTimeMillis();
                            } else {
                                gcListener.info("cleaning up after failed compaction");
                            }
                            fileReaper.add(cleanup(compactionResult));
                        }
                    }
                }
            } finally {
                compactionMonitor.finished();
                gcListener.updateStatus(IDLE.message());
            }
        }

        /**
         * Estimated compaction gain. The result will be undefined if stopped through
         * the passed {@code stop} signal.
         * @return compaction gain estimate
         */
        GCEstimationResult estimateCompactionGain(boolean full) {
            return new SizeDeltaGcEstimation(gcOptions.getGcSizeDeltaEstimation(), gcJournal, tarFiles.size(), full).estimate();
        }

        @Nonnull
        private CompactionResult compactionAborted(@Nonnull GCGeneration generation) {
            gcListener.compactionFailed(generation);
            return CompactionResult.aborted(getGcGeneration(), generation);
        }

        @Nonnull
        private CompactionResult compactionSucceeded(
                @Nonnull GCType gcType,
                @Nonnull GCGeneration generation,
                @Nonnull RecordId compactedRootId) {
            gcListener.compactionSucceeded(generation);
            return CompactionResult.succeeded(gcType, generation, gcOptions, compactedRootId);
        }

        @CheckForNull
        private SegmentNodeState getBase() {
            String root = gcJournal.read().getRoot();
            RecordId rootId = RecordId.fromString(tracker, root);
            if (RecordId.NULL.equals(rootId)) {
                return null;
            }
            try {
                SegmentNodeState node = segmentReader.readNode(rootId);
                node.getPropertyCount();  // Resilience: fail early with a SNFE if the segment is not there
                return node;
            } catch (SegmentNotFoundException snfe) {
                gcListener.error("base state " + rootId + " is not accessible", snfe);
                return null;
            }
        }

        synchronized CompactionResult compactFull() {
            gcListener.info("running full compaction");
            return compact(FULL, EMPTY_NODE, getGcGeneration().nextFull());
        }

        synchronized CompactionResult compactTail() {
            gcListener.info("running tail compaction");
            SegmentNodeState base = getBase();
            if (base != null) {
                return compact(TAIL, base, getGcGeneration().nextTail());
            }
            gcListener.info("no base state available, running full compaction instead");
            return compact(FULL, EMPTY_NODE, getGcGeneration().nextFull());
        }

        private CompactionResult compact(
                @Nonnull GCType gcType,
                @Nonnull NodeState base,
                @Nonnull GCGeneration newGeneration) {
            try {
                PrintableStopwatch watch = PrintableStopwatch.createStarted();
                gcListener.info("compaction started, gc options={}, current generation={}, new generation={}",
                                gcOptions, getHead().getRecordId().getSegment().getGcGeneration(), newGeneration);
                gcListener.updateStatus(COMPACTION.message());

                GCJournalEntry gcEntry = gcJournal.read();
                long initialSize = size();

                SegmentWriter writer = defaultSegmentWriterBuilder("c")
                        .with(cacheManager)
                        .withGeneration(newGeneration)
                        .withoutWriterPool()
                        .build(FileStore.this);

                CancelCompactionSupplier cancel = new CancelCompactionSupplier(FileStore.this);

                compactionMonitor = new GCNodeWriteMonitor(gcOptions.getGcLogInterval(), gcListener);
                compactionMonitor.init(gcEntry.getRepoSize(), gcEntry.getNodes(), initialSize);

                CheckpointCompactor compactor = new CheckpointCompactor(gcListener,
                    segmentReader, writer, getBlobStore(), cancel, compactionMonitor);

                SegmentNodeState head = getHead();
                SegmentNodeState compacted = compactor.compact(base, head, base);
                if (compacted == null) {
                    gcListener.warn("compaction cancelled: {}.", cancel);
                    return compactionAborted(newGeneration);
                }

                gcListener.info("compaction cycle 0 completed in {}. Compacted {} to {}",
                    watch, head.getRecordId(), compacted.getRecordId());

                int cycles = 0;
                boolean success = false;
                SegmentNodeState previousHead = head;
                while (cycles < gcOptions.getRetryCount() &&
                        !(success = revisions.setHead(previousHead.getRecordId(), compacted.getRecordId(), EXPEDITE_OPTION))) {
                    // Some other concurrent changes have been made.
                    // Rebase (and compact) those changes on top of the
                    // compacted state before retrying to set the head.
                    cycles++;
                    gcListener.info("compaction detected concurrent commits while compacting. " +
                            "Compacting these commits. Cycle {} of {}",
                        cycles, gcOptions.getRetryCount());
                    gcListener.updateStatus(COMPACTION_RETRY.message() + cycles);
                    PrintableStopwatch cycleWatch = PrintableStopwatch.createStarted();

                    head = getHead();
                    compacted = compactor.compact(previousHead, head, compacted);
                    if (compacted == null) {
                        gcListener.warn("compaction cancelled: {}.", cancel);
                        return compactionAborted(newGeneration);
                    }

                    gcListener.info("compaction cycle {} completed in {}. Compacted {} against {} to {}",
                        cycles, cycleWatch, head.getRecordId(), previousHead.getRecordId(), compacted.getRecordId());
                    previousHead = head;
                }

                if (!success) {
                    gcListener.info("compaction gave up compacting concurrent commits after {} cycles.",
                        cycles);
                    int forceTimeout = gcOptions.getForceTimeout();
                    if (forceTimeout > 0) {
                        gcListener.info("trying to force compact remaining commits for {} seconds. " +
                                "Concurrent commits to the store will be blocked.",
                            forceTimeout);
                        gcListener.updateStatus(COMPACTION_FORCE_COMPACT.message());
                        PrintableStopwatch forceWatch = PrintableStopwatch.createStarted();

                        cycles++;
                        cancel.timeOutAfter(forceTimeout, SECONDS);
                        compacted = forceCompact(previousHead, compacted, compactor);
                        success = compacted != null;
                        if (success) {
                            gcListener.info("compaction succeeded to force compact remaining commits " +
                                "after {}.", forceWatch);
                        } else {
                            if (cancel.get()) {
                                gcListener.warn("compaction failed to force compact remaining commits " +
                                        "after {}. Compaction was cancelled: {}.",
                                    forceWatch, cancel);
                            } else {
                                gcListener.warn("compaction failed to force compact remaining commits. " +
                                        "after {}. Could not acquire exclusive access to the node store.",
                                    forceWatch);
                            }
                        }
                    }
                }

                if (success) {
                    // Update type of the last compaction before calling methods that could throw an exception.
                    lastCompactionType = gcType;
                    writer.flush();
                    flush();
                    gcListener.info("compaction succeeded in {}, after {} cycles", watch, cycles);
                    return compactionSucceeded(gcType, newGeneration, compacted.getRecordId());
                } else {
                    gcListener.info("compaction failed after {}, and {} cycles", watch, cycles);
                    return compactionAborted(newGeneration);
                }
            } catch (InterruptedException e) {
                gcListener.error("compaction interrupted", e);
                currentThread().interrupt();
                return compactionAborted(newGeneration);
            } catch (IOException e) {
                gcListener.error("compaction encountered an error", e);
                return compactionAborted(newGeneration);
            }
        }

        private SegmentNodeState forceCompact(
                @Nonnull final NodeState base,
                @Nonnull final NodeState onto,
                @Nonnull final CheckpointCompactor compactor)
        throws InterruptedException {
            RecordId compactedId = revisions.setHead(new Function<RecordId, RecordId>() {
                @Nullable
                @Override
                public RecordId apply(RecordId headId) {
                    try {
                        long t0 = currentTimeMillis();
                        SegmentNodeState after = compactor.compact(
                            base, segmentReader.readNode(headId), onto);
                        if (after == null) {
                            gcListener.info("compaction cancelled after {} seconds",
                                (currentTimeMillis() - t0) / 1000);
                            return null;
                        } else {
                            return after.getRecordId();
                        }
                    } catch (IOException e) {
                        gcListener.error("error during forced compaction.", e);
                        return null;
                    }
                }
            }, timeout(gcOptions.getForceTimeout(), SECONDS));
            return compactedId != null
                ? segmentReader.readNode(compactedId)
                : null;
        }

        private CleanupContext newCleanupContext(Predicate<GCGeneration> old) {
            return new CleanupContext() {

                private boolean isUnreferencedBulkSegment(UUID id, boolean referenced) {
                    return !isDataSegmentId(id.getLeastSignificantBits()) && !referenced;
                }

                private boolean isOldDataSegment(UUID id, GCGeneration generation) {
                    return isDataSegmentId(id.getLeastSignificantBits()) && old.apply(generation);
                }

                @Override
                public Collection<UUID> initialReferences() {
                    Set<UUID> references = newHashSet();
                    for (SegmentId id : tracker.getReferencedSegmentIds()) {
                        if (id.isBulkSegmentId()) {
                            references.add(id.asUUID());
                        }
                    }
                    return references;
                }

                @Override
                public boolean shouldReclaim(UUID id, GCGeneration generation, boolean referenced) {
                    return isUnreferencedBulkSegment(id, referenced) || isOldDataSegment(id, generation);
                }

                @Override
                public boolean shouldFollow(UUID from, UUID to) {
                    return !isDataSegmentId(to.getLeastSignificantBits());
                }

            };
        }

        /**
         * Cleanup segments whose generation matches the reclaim predicate determined by
         * the {@link #lastCompactionType last successful compaction}.
         * @return list of files to be removed
         * @throws IOException
         */
        @Nonnull
        synchronized List<File> cleanup() throws IOException {
            return cleanup(CompactionResult.skipped(
                lastCompactionType,
                getGcGeneration(),
                garbageCollector.gcOptions,
                revisions.getHead()
            ));
        }

        /**
         * Cleanup segments whose generation matches the {@link CompactionResult#reclaimer()} predicate.
         * @return list of files to be removed
         * @throws IOException
         */
        @Nonnull
        private List<File> cleanup(@Nonnull CompactionResult compactionResult)
            throws IOException {
            PrintableStopwatch watch = PrintableStopwatch.createStarted();

            gcListener.info("cleanup started using reclaimer {}", compactionResult.reclaimer());
            gcListener.updateStatus(CLEANUP.message());
            segmentCache.clear();

            // Suggest to the JVM that now would be a good time
            // to clear stale weak references in the SegmentTracker
            System.gc();

            CleanupResult cleanupResult = tarFiles.cleanup(newCleanupContext(compactionResult.reclaimer()));
            if (cleanupResult.isInterrupted()) {
                gcListener.info("cleanup interrupted");
            }
            tracker.clearSegmentIdTables(cleanupResult.getReclaimedSegmentIds(), compactionResult.gcInfo());
            gcListener.info("cleanup marking files for deletion: {}", toFileNames(cleanupResult.getRemovableFiles()));

            long finalSize = size();
            long reclaimedSize = cleanupResult.getReclaimedSize();
            stats.reclaimed(reclaimedSize);
            gcJournal.persist(reclaimedSize, finalSize, getGcGeneration(),
                    compactionMonitor.getCompactedNodes(),
                    compactionResult.getCompactedRootId().toString10());
            gcListener.cleaned(reclaimedSize, finalSize);
            gcListener.info(
                "cleanup completed in {}. Post cleanup size is {} and space reclaimed {}.",
                watch,
                newPrintableBytes(finalSize),
                newPrintableBytes(reclaimedSize));
            return cleanupResult.getRemovableFiles();
        }

        private String toFileNames(@Nonnull List<File> files) {
            if (files.isEmpty()) {
                return "none";
            } else {
                return Joiner.on(",").join(files);
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
        synchronized void collectBlobReferences(Consumer<String> collector) throws IOException {
            segmentWriter.flush();
            tarFiles.collectBlobReferences(collector,
                    newOldReclaimer(lastCompactionType, getGcGeneration(), gcOptions.getRetainedGenerations()));
        }

        void cancel() {
            cancelled = true;
        }

        /**
         * Represents the cancellation policy for the compaction phase. If the disk
         * space was considered insufficient at least once during compaction (or if
         * the space was never sufficient to begin with), compaction is considered
         * canceled. Furthermore when the file store is shutting down, compaction is
         * considered canceled.
         * Finally the cancellation can be triggered by a timeout that can be set
         * at any time.
         */
        private class CancelCompactionSupplier implements Supplier<Boolean> {
            private final FileStore store;

            private String reason;
            private volatile long baseLine;
            private volatile long deadline;

            public CancelCompactionSupplier(@Nonnull FileStore store) {
                cancelled = false;
                this.store = store;
            }

            /**
             * Set a timeout for cancellation. Setting a different timeout cancels
             * a previous one that did not yet elapse. Setting a timeout after
             * cancellation took place has no effect.
             */
            public void timeOutAfter(final long duration, @Nonnull final TimeUnit unit) {
                baseLine = currentTimeMillis();
                deadline = baseLine + MILLISECONDS.convert(duration, unit);
            }

            @Override
            public Boolean get() {
                // The outOfDiskSpace and shutdown flags can only transition from
                // false (their initial values), to true. Once true, there should
                // be no way to go back.
                if (!store.sufficientDiskSpace.get()) {
                    reason = "Not enough disk space";
                    return true;
                }
                if (!store.sufficientMemory.get()) {
                    reason = "Not enough memory";
                    return true;
                }
                if (store.shutDown.isShutDown()) {
                    reason = "The FileStore is shutting down";
                    return true;
                }
                if (cancelled) {
                    reason = "Cancelled by user";
                    return true;
                }
                if (deadline > 0 && currentTimeMillis() > deadline) {
                    long dt = SECONDS.convert(currentTimeMillis() - baseLine, MILLISECONDS);
                    reason = "Timeout after " + dt + " seconds";
                    return true;
                }
                return false;
            }

            @Override
            public String toString() { return reason; }
        }
    }

    /**
     * Instances of this class represent the result from a compaction. Either
     * {@link #succeeded(GCType, GCGeneration, SegmentGCOptions, RecordId) succeeded},
     * {@link #aborted(GCGeneration, GCGeneration) aborted} or {@link
     * #skipped(GCType, GCGeneration, SegmentGCOptions, RecordId)}  skipped}.
     */
    private abstract static class CompactionResult {
        @Nonnull
        private final GCGeneration currentGeneration;

        protected CompactionResult(@Nonnull GCGeneration currentGeneration) {
            this.currentGeneration = currentGeneration;
        }

        /**
         * Result of a succeeded compaction.
         * @param gcType            the type of the succeeded compaction operation
         * @param newGeneration     the generation successfully created by compaction
         * @param gcOptions         the current GC options used by compaction
         * @param compactedRootId   the record id of the root created by compaction
         */
        static CompactionResult succeeded(
                @Nonnull GCType gcType,
                @Nonnull GCGeneration newGeneration,
                @Nonnull final SegmentGCOptions gcOptions,
                @Nonnull final RecordId compactedRootId) {
            return new CompactionResult(newGeneration) {
                @Override
                Predicate<GCGeneration> reclaimer() {
                    return newOldReclaimer(gcType, newGeneration, gcOptions.getRetainedGenerations());
                }

                @Override
                boolean isSuccess() {
                    return true;
                }

                @Override
                RecordId getCompactedRootId() {
                    return compactedRootId;
                }
            };
        }

        /**
         * Result of an aborted compaction.
         * @param currentGeneration  the current generation of the store
         * @param failedGeneration   the generation that compaction attempted to create
         */
        static CompactionResult aborted(
                @Nonnull GCGeneration currentGeneration,
                @Nonnull final GCGeneration failedGeneration) {
            return new CompactionResult(currentGeneration) {
                @Override
                Predicate<GCGeneration> reclaimer() {
                    return Reclaimers.newExactReclaimer(failedGeneration);
                }

                @Override
                boolean isSuccess() {
                    return false;
                }
            };
        }

        /**
         * Result serving as a placeholder for a compaction that was skipped.
         * @param lastGCType         type of the most recent gc operation. {@link GCType#FULL} if none.
         * @param currentGeneration  the current generation of the store
         * @param gcOptions          the current GC options used by compaction
         */
        static CompactionResult skipped(
                @Nonnull GCType lastGCType,
                @Nonnull GCGeneration currentGeneration,
                @Nonnull final SegmentGCOptions gcOptions,
                @Nonnull final RecordId compactedRootId) {
            return new CompactionResult(currentGeneration) {
                @Override
                Predicate<GCGeneration> reclaimer() {
                    return Reclaimers.newOldReclaimer(lastGCType, currentGeneration, gcOptions.getRetainedGenerations());
                }

                @Override
                boolean isSuccess() {
                    return true;
                }

                @Override
                RecordId getCompactedRootId() {
                    return compactedRootId;
                }
            };
        }

        /**
         * @return  a predicate determining which segments to
         *          {@link GarbageCollector#cleanup(CompactionResult) clean up} for
         *          the given compaction result.
         */
        abstract Predicate<GCGeneration> reclaimer();

        /**
         * @return  {@code true} for {@link #succeeded(GCType, GCGeneration, SegmentGCOptions, RecordId) succeeded}
         *          and {@link #skipped(GCType, GCGeneration, SegmentGCOptions, RecordId) skipped}, {@code false} otherwise.
         */
        abstract boolean isSuccess();

        /**
         * @return  the record id of the compacted root on {@link #isSuccess() success},
         *          {@link RecordId#NULL} otherwise.
         */
        RecordId getCompactedRootId() {
            return RecordId.NULL;
        }

        /**
         * @return  a diagnostic message describing the outcome of this compaction.
         */
        String gcInfo() {
            return  "gc-count=" + GC_COUNT +
                    ",gc-status=" + (isSuccess() ? "success" : "failed") +
                    ",store-generation=" + currentGeneration +
                    ",reclaim-predicate=" + reclaimer();
        }

    }

}
