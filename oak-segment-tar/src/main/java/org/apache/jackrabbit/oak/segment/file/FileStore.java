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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Integer.getInteger;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.CLEANUP;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION_FORCE_COMPACT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION_RETRY;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.ESTIMATION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.IDLE;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.EXPEDITE_OPTION;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.timeout;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
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
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;
import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles.CleanupResult;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
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

    private final FileLock lock;

    private TarRevisions revisions;

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

    /**
     * Flag signalling shutdown of the file store
     */
    private volatile boolean shutdown;

    private final FileStoreStats stats;

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
                builder.getCacheManager(),
                builder.getStatsProvider());

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
        this.stats.init(this.tarFiles.size());

        this.snfeListener = builder.getSnfeListener();

        fileStoreScheduler.scheduleAtFixedRate(
                format("TarMK flush [%s]", directory), 5, SECONDS,
                new Runnable() {
                    @Override
                    public void run() {
                        if (shutdown) {
                            return;
                        }
                        try {
                            flush();
                        } catch (IOException e) {
                            log.warn("Failed to flush the TarMK at {}",
                                    directory, e);
                        }
                    }
                });
        fileStoreScheduler.scheduleAtFixedRate(
                format("TarMK filer reaper [%s]", directory), 5, SECONDS,
                new Runnable() {
                    @Override
                    public void run() {
                        fileReaper.reap();
                    }
                });
        fileStoreScheduler.scheduleAtFixedRate(
                format("TarMK disk space check [%s]", directory), 1, MINUTES,
                new Runnable() {
                    final SegmentGCOptions gcOptions = builder.getGcOptions();

                    @Override
                    public void run() {
                        checkDiskSpace(gcOptions);
                    }
                });
        log.info("TarMK opened: {} (mmap={})", directory, memoryMapping);
        log.debug("TAR files: {}", tarFiles);
    }

    FileStore bind(TarRevisions revisions) throws IOException {
        this.revisions = revisions;
        this.revisions.bind(this, tracker, initialNode());
        return this;
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
            try {
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
        return tarFiles.size();
    }

    public int readerCount(){
        return tarFiles.readerCount();
    }

    public FileStoreStats getStats() {
        return stats;
    }

    public void flush() throws IOException {
        if (revisions == null) {
            return;
        }
        revisions.flush(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                segmentWriter.flush();
                tarFiles.flush();
                stats.flushed();
                return null;
            }
        });
    }

    /**
     * Run full garbage collection: estimation, compaction, cleanup.
     */
    public void fullGC() throws IOException {
        garbageCollector.runFull();
    }

    /**
     * Run tail garbage collection.
     */
    public void tailGC() throws IOException {
        garbageCollector.runTail();
    }

    /**
     * Run the compaction gain estimation process.
     * @return
     */
    public GCEstimation estimateCompactionGain() {
        return garbageCollector.estimateCompactionGain();
    }

    /**
     * Copy every referenced record in data (non-bulk) segments. Bulk segments
     * are fully kept (they are only removed in cleanup, if there is no
     * reference to them).
     * @return {@code true} on success, {@code false} otherwise.
     */
    public boolean compactFull() {
        return garbageCollector.compactFull().isSuccess();
    }

    public boolean compactTail() {
        return garbageCollector.compactTail().isSuccess();
    }

    /**
     * Run garbage collection on the segment level: reclaim those data segments
     * that are from an old segment generation and those bulk segments that are not
     * reachable anymore.
     * Those tar files that shrink by at least 25% are rewritten to a new tar generation
     * skipping the reclaimed segments.
     */
    public void cleanup() throws IOException {
        CompactionResult compactionResult = CompactionResult.skipped(
                getGcGeneration(),
                garbageCollector.gcOptions,
                revisions.getHead());
        fileReaper.add(garbageCollector.cleanup(compactionResult));
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
        garbageCollector.collectBlobReferences(collector);
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
        return segmentWriter;
    }

    @Override
    @Nonnull
    public TarRevisions getRevisions() {
        return revisions;
    }

    @Override
    public void close() {
        // Flag the store as shutting / shut down
        shutdown = true;

        // avoid deadlocks by closing (and joining) the background
        // thread before acquiring the synchronization lock
        fileStoreScheduler.close();

        try {
            flush();
        } catch (IOException e) {
            log.warn("Unable to flush the store", e);
        }

        Closer closer = Closer.create();
        closer.register(revisions);
        if (lock != null) {
            try {
                lock.release();
            } catch (IOException e) {
                log.warn("Unable to release the file lock", e);
            }
        }
        closer.register(lockFile);
        closer.register(tarFiles);
        closeAndLogOnFail(closer);

        // Try removing pending files in case the scheduler didn't have a chance to run yet
        fileReaper.reap();
        System.gc(); // for any memory-mappings that are no longer used

        log.info("TarMK closed: {}", directory);
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        return tarFiles.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    @Override
    @Nonnull
    public Segment readSegment(final SegmentId id) {
        try {
            return segmentCache.getSegment(id, new Callable<Segment>() {
                @Override
                public Segment call() throws Exception {
                    return readSegmentUncached(tarFiles, id);
                }
            });
        } catch (ExecutionException e) {
            SegmentNotFoundException snfe = asSegmentNotFoundException(e, id);
            snfeListener.notify(id, snfe);
            throw snfe;
        }
    }

    @Override
    public void writeSegment(SegmentId id, byte[] buffer, int offset, int length) throws IOException {
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
        private final GCListener gcListener;

        @Nonnull
        private final GCJournal gcJournal;

        @Nonnull
        private final WriterCacheManager cacheManager;

        @Nonnull
        private final StatisticsProvider statisticsProvider;

        @Nonnull
        private GCNodeWriteMonitor compactionMonitor = GCNodeWriteMonitor.EMPTY;

        private volatile boolean cancelled;

        /**
         * Timestamp of the last time {@link #fullGC()} or {@link #tailGC()} was
         * successfully invoked. 0 if never.
         */
        private long lastSuccessfullGC;

        GarbageCollector(
                @Nonnull SegmentGCOptions gcOptions,
                @Nonnull GCListener gcListener,
                @Nonnull GCJournal gcJournal,
                @Nonnull WriterCacheManager cacheManager,
                @Nonnull StatisticsProvider statisticsProvider) {
            this.gcOptions = gcOptions;
            this.gcListener = gcListener;
            this.gcJournal = gcJournal;
            this.cacheManager = cacheManager;
            this.statisticsProvider = statisticsProvider;
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
            run(this::compactFull);
        }

        synchronized void runTail() throws IOException {
            run(this::compactTail);
        }

        private void run(Supplier<CompactionResult> compact) throws IOException {
            try {
                gcListener.info("TarMK GC #{}: started", GC_COUNT.incrementAndGet());

                long dt = System.currentTimeMillis() - lastSuccessfullGC;
                if (dt < GC_BACKOFF) {
                    gcListener.skipped("TarMK GC #{}: skipping garbage collection as it already ran " +
                            "less than {} hours ago ({} s).", GC_COUNT, GC_BACKOFF/3600000, dt/1000);
                    return;
                }

                boolean sufficientEstimatedGain = true;
                if (gcOptions.isEstimationDisabled()) {
                    gcListener.info("TarMK GC #{}: estimation skipped because it was explicitly disabled", GC_COUNT);
                } else if (gcOptions.isPaused()) {
                    gcListener.info("TarMK GC #{}: estimation skipped because compaction is paused", GC_COUNT);
                } else {
                    gcListener.info("TarMK GC #{}: estimation started", GC_COUNT);
                    gcListener.updateStatus(ESTIMATION.message());
                    
                    Stopwatch watch = Stopwatch.createStarted();
                    GCEstimation estimate = estimateCompactionGain();
                    sufficientEstimatedGain = estimate.gcNeeded();
                    String gcLog = estimate.gcLog();
                    if (sufficientEstimatedGain) {
                        gcListener.info(
                                "TarMK GC #{}: estimation completed in {} ({} ms). {}",
                                GC_COUNT, watch, watch.elapsed(MILLISECONDS), gcLog);
                    } else {
                        gcListener.skipped(
                                "TarMK GC #{}: estimation completed in {} ({} ms). {}",
                                GC_COUNT, watch, watch.elapsed(MILLISECONDS), gcLog);
                    }
                }
    
                if (sufficientEstimatedGain) {
                    try (GCMemoryBarrier gcMemoryBarrier = new GCMemoryBarrier(
                            sufficientMemory, gcListener, GC_COUNT.get(), gcOptions))
                    {
                        if (gcOptions.isPaused()) {
                            gcListener.skipped("TarMK GC #{}: compaction paused", GC_COUNT);
                        } else if (!sufficientMemory.get()) {
                            gcListener.skipped("TarMK GC #{}: compaction skipped. Not enough memory", GC_COUNT);
                        } else {
                            CompactionResult compactionResult = compact.get();
                            if (compactionResult.isSuccess()) {
                                lastSuccessfullGC = System.currentTimeMillis();
                            } else {
                                gcListener.info("TarMK GC #{}: cleaning up after failed compaction", GC_COUNT);
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
        synchronized GCEstimation estimateCompactionGain() {
            return new SizeDeltaGcEstimation(gcOptions, gcJournal,
                    stats.getApproximateSize());
        }

        @Nonnull
        private CompactionResult compactionAborted(@Nonnull GCGeneration generation) {
            gcListener.compactionFailed(generation);
            return CompactionResult.aborted(getGcGeneration(), generation);
        }

        @Nonnull
        private CompactionResult compactionSucceeded(@Nonnull GCGeneration generation, @Nonnull RecordId compactedRootId) {
            gcListener.compactionSucceeded(generation);
            return CompactionResult.succeeded(generation, gcOptions, compactedRootId);
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
                gcListener.error("TarMK GC #" + GC_COUNT + ": Base state " + rootId + " is not accessible", snfe);
                return null;
            }
        }

        synchronized CompactionResult compactFull() {
            gcListener.info("TarMK GC #{}: running full compaction", GC_COUNT);
            return compact(null, getGcGeneration().nextFull());
        }

        synchronized CompactionResult compactTail() {
            gcListener.info("TarMK GC #{}: running tail compaction", GC_COUNT);
            SegmentNodeState base = getBase();
            if (base != null) {
                return compact(base, getGcGeneration().nextTail());
            }
            gcListener.info("TarMK GC #{}: no base state available, running full compaction instead", GC_COUNT);
            return compact(null, getGcGeneration().nextFull());
        }

        private CompactionResult compact(SegmentNodeState base, GCGeneration newGeneration) {
            try {
                Stopwatch watch = Stopwatch.createStarted();
                gcListener.info("TarMK GC #{}: compaction started, gc options={}", GC_COUNT, gcOptions);
                gcListener.updateStatus(COMPACTION.message());

                GCJournalEntry gcEntry = gcJournal.read();
                long initialSize = size();
                compactionMonitor = new GCNodeWriteMonitor(gcOptions.getGcLogInterval(), gcListener);
                compactionMonitor.init(GC_COUNT.get(), gcEntry.getRepoSize(), gcEntry.getNodes(), initialSize);

                SegmentNodeState before = getHead();
                CancelCompactionSupplier cancel = new CancelCompactionSupplier(FileStore.this);
                SegmentWriter writer = defaultSegmentWriterBuilder("c")
                        .with(cacheManager
                                .withAccessTracking("COMPACT", statisticsProvider))
                        .withGeneration(newGeneration)
                        .withoutWriterPool()
                        .build(FileStore.this);
                Compactor compactor = new Compactor(
                        segmentReader, writer, getBlobStore(), cancel, compactionMonitor);

                SegmentNodeState after = compact(base, before, compactor, writer);
                if (after == null) {
                    gcListener.warn("TarMK GC #{}: compaction cancelled: {}.", GC_COUNT, cancel);
                    return compactionAborted(newGeneration);
                }

                gcListener.info("TarMK GC #{}: compaction cycle 0 completed in {} ({} ms). Compacted {} to {}",
                        GC_COUNT, watch, watch.elapsed(MILLISECONDS), before.getRecordId(), after.getRecordId());

                int cycles = 0;
                boolean success = false;
                while (cycles < gcOptions.getRetryCount() &&
                        !(success = revisions.setHead(before.getRecordId(), after.getRecordId(), EXPEDITE_OPTION))) {
                    // Some other concurrent changes have been made.
                    // Rebase (and compact) those changes on top of the
                    // compacted state before retrying to set the head.
                    cycles++;
                    gcListener.info("TarMK GC #{}: compaction detected concurrent commits while compacting. " +
                                    "Compacting these commits. Cycle {} of {}",
                            GC_COUNT, cycles, gcOptions.getRetryCount());
                    gcListener.updateStatus(COMPACTION_RETRY.message() + cycles);
                    Stopwatch cycleWatch = Stopwatch.createStarted();
                    
                    SegmentNodeState head = getHead();
                    after = compact(after, head, compactor, writer);
                    if (after == null) {
                        gcListener.warn("TarMK GC #{}: compaction cancelled: {}.", GC_COUNT, cancel);
                        return compactionAborted(newGeneration);
                    }

                    gcListener.info("TarMK GC #{}: compaction cycle {} completed in {} ({} ms). Compacted {} against {} to {}",
                            GC_COUNT, cycles, cycleWatch, cycleWatch.elapsed(MILLISECONDS),
                            head.getRecordId(), before.getRecordId(), after.getRecordId());
                    before = head;
                }

                if (!success) {
                    gcListener.info("TarMK GC #{}: compaction gave up compacting concurrent commits after {} cycles.",
                            GC_COUNT, cycles);
                    int forceTimeout = gcOptions.getForceTimeout();
                    if (forceTimeout > 0) {
                        gcListener.info("TarMK GC #{}: trying to force compact remaining commits for {} seconds. " +
                                "Concurrent commits to the store will be blocked.",
                                GC_COUNT, forceTimeout);
                        gcListener.updateStatus(COMPACTION_FORCE_COMPACT.message());
                        Stopwatch forceWatch = Stopwatch.createStarted();
                        
                        cycles++;
                        cancel.timeOutAfter(forceTimeout, SECONDS);
                        after = forceCompact(after, compactor, writer);
                        success = after != null;
                        if (success) {
                            gcListener.info("TarMK GC #{}: compaction succeeded to force compact remaining commits " +
                                            "after {} ({} ms).",
                                            GC_COUNT, forceWatch, forceWatch.elapsed(MILLISECONDS));
                        } else {
                            if (cancel.get()) {
                                gcListener.warn("TarMK GC #{}: compaction failed to force compact remaining commits " +
                                        "after {} ({} ms). Compaction was cancelled: {}.",
                                        GC_COUNT, forceWatch, forceWatch.elapsed(MILLISECONDS), cancel);
                            } else {
                                gcListener.warn("TarMK GC #{}: compaction failed to force compact remaining commits. " +
                                        "after {} ({} ms). Most likely compaction didn't get exclusive access to the store.",
                                        GC_COUNT, forceWatch, forceWatch.elapsed(MILLISECONDS));
                            }
                        }
                    }
                }

                if (success) {
                    writer.flush();
                    gcListener.info("TarMK GC #{}: compaction succeeded in {} ({} ms), after {} cycles",
                            GC_COUNT, watch, watch.elapsed(MILLISECONDS), cycles);
                    return compactionSucceeded(newGeneration, after.getRecordId());
                } else {
                    gcListener.info("TarMK GC #{}: compaction failed after {} ({} ms), and {} cycles",
                            GC_COUNT, watch, watch.elapsed(MILLISECONDS), cycles);
                    return compactionAborted(newGeneration);
                }
            } catch (InterruptedException e) {
                gcListener.error("TarMK GC #" + GC_COUNT + ": compaction interrupted", e);
                currentThread().interrupt();
                return compactionAborted(newGeneration);
            } catch (IOException e) {
                gcListener.error("TarMK GC #" + GC_COUNT + ": compaction encountered an error", e);
                return compactionAborted(newGeneration);
            }
        }

        /**
         * Compact {@code uncompacted} on top of an optional {@code base}.
         * @param base         the base state to compact onto or {@code null} for an empty state.
         * @param uncompacted  the uncompacted state to compact
         * @param compactor    the compactor for creating the new generation of the
         *                     uncompacted state.
         * @param writer       the segment writer used by {@code compactor} for writing to the
         *                     new generation.
         * @return  compacted clone of {@code uncompacted} or null if cancelled.
         * @throws IOException
         */
        @CheckForNull
        private SegmentNodeState compact(
                @Nullable SegmentNodeState base,
                @Nonnull SegmentNodeState uncompacted,
                @Nonnull Compactor compactor,
                @Nonnull SegmentWriter writer)
        throws IOException {
            // Collect a chronologically ordered list of roots for the base and the uncompacted
            // state. This list consists of all checkpoints followed by the root.
            LinkedHashMap<String, NodeState> baseRoots = collectRoots(base);
            LinkedHashMap<String, NodeState> uncompactedRoots = collectRoots(uncompacted);

            // Compact the list of uncompacted roots to a list of compacted roots.
            LinkedHashMap<String, NodeState> compactedRoots = compact(baseRoots, uncompactedRoots, compactor);
            if (compactedRoots == null) {
                return null;
            }

            // Build a compacted super root by replacing the uncompacted roots with
            // the compacted ones in the original node.
            SegmentNodeBuilder builder = uncompacted.builder();
            for (Entry<String, NodeState> compactedRoot : compactedRoots.entrySet()) {
                String path = compactedRoot.getKey();
                NodeState state = compactedRoot.getValue();
                NodeBuilder childBuilder = getChild(builder, getParentPath(path));
                childBuilder.setChildNode(getName(path), state);
            }

            // Use the segment writer of the *new generation* to persist the compacted super root.
            RecordId nodeId = writer.writeNode(builder.getNodeState(), uncompacted.getStableIdBytes());
            return new SegmentNodeState(segmentReader, segmentWriter, getBlobStore(), nodeId);
        }

        /**
         * Compact a list of uncompacted roots on top of base roots of the same key or
         * an empty node if none.
         */
        @CheckForNull
        private LinkedHashMap<String, NodeState> compact(
                @Nonnull LinkedHashMap<String, NodeState> baseRoots,
                @Nonnull LinkedHashMap<String, NodeState> uncompactedRoots,
                @Nonnull Compactor compactor)
        throws IOException {
            NodeState onto = baseRoots.get("root");
            NodeState previous = onto;
            LinkedHashMap<String, NodeState> compactedRoots = newLinkedHashMap();
            for (Entry<String, NodeState> uncompactedRoot : uncompactedRoots.entrySet()) {
                String path = uncompactedRoot.getKey();
                NodeState state = uncompactedRoot.getValue();
                NodeState compacted;
                if (onto == null) {
                    compacted = compactor.compact(state);
                } else {
                    compacted = compactor.compact(previous, state, onto);
                }
                if (compacted == null) {
                    return null;
                }
                previous = state;
                onto = compacted;
                compactedRoots.put(path, compacted);
            }
            return compactedRoots;
        }

        /**
         * Collect a chronologically ordered list of roots for the base and the uncompacted
         * state from a {@code superRoot} . This list consists of all checkpoints followed by
         * the root.
         */
        @Nonnull
        private LinkedHashMap<String, NodeState> collectRoots(@Nullable SegmentNodeState superRoot) {
            LinkedHashMap<String, NodeState> roots = newLinkedHashMap();
            if (superRoot != null) {
                List<ChildNodeEntry> checkpoints = newArrayList(
                        superRoot.getChildNode("checkpoints").getChildNodeEntries());

                checkpoints.sort((cne1, cne2) -> {
                    long c1 = cne1.getNodeState().getLong("created");
                    long c2 = cne2.getNodeState().getLong("created");
                    return Long.compare(c1, c2);
                });

                for (ChildNodeEntry checkpoint : checkpoints) {
                    roots.put("checkpoints/" + checkpoint.getName() + "/root",
                            checkpoint.getNodeState().getChildNode("root"));
                }
                roots.put("root", superRoot.getChildNode("root"));
            }
            return roots;
        }

        @Nonnull
        private NodeBuilder getChild(NodeBuilder builder, String path) {
            for (String name : elements(path)) {
                builder = builder.getChildNode(name);
            }
            return builder;
        }

        private SegmentNodeState forceCompact(
                @Nonnull final SegmentNodeState base,
                @Nonnull final Compactor compactor,
                @Nonnull SegmentWriter writer)
        throws InterruptedException {
            RecordId compactedId = revisions.setHead(new Function<RecordId, RecordId>() {
                @Nullable
                @Override
                public RecordId apply(RecordId headId) {
                    try {
                        long t0 = currentTimeMillis();
                        SegmentNodeState after = compact(
                               base, segmentReader.readNode(headId), compactor, writer);
                        if (after == null) {
                            gcListener.info("TarMK GC #{}: compaction cancelled after {} seconds",
                                    GC_COUNT, (currentTimeMillis() - t0) / 1000);
                            return null;
                        } else {
                            return after.getRecordId();
                        }
                    } catch (IOException e) {
                        gcListener.error("TarMK GC #{" + GC_COUNT + "}: Error during forced compaction.", e);
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
         * Cleanup segments whose generation matches the {@link CompactionResult#reclaimer()} predicate.
         * @return list of files to be removed
         * @throws IOException
         */
        @Nonnull
        private List<File> cleanup(@Nonnull CompactionResult compactionResult)
        throws IOException {
            Stopwatch watch = Stopwatch.createStarted();

            gcListener.info("TarMK GC #{}: cleanup started.", GC_COUNT);
            gcListener.updateStatus(CLEANUP.message());
            segmentCache.clear();

            // Suggest to the JVM that now would be a good time
            // to clear stale weak references in the SegmentTracker
            System.gc();

            CleanupResult cleanupResult = tarFiles.cleanup(newCleanupContext(compactionResult.reclaimer()));
            if (cleanupResult.isInterrupted()) {
                gcListener.info("TarMK GC #{}: cleanup interrupted", GC_COUNT);
            }
            tracker.clearSegmentIdTables(cleanupResult.getReclaimedSegmentIds(), compactionResult.gcInfo());
            gcListener.info("TarMK GC #{}: cleanup marking files for deletion: {}", GC_COUNT, toFileNames(cleanupResult.getRemovableFiles()));

            long finalSize = size();
            long reclaimedSize = cleanupResult.getReclaimedSize();
            stats.reclaimed(reclaimedSize);
            gcJournal.persist(reclaimedSize, finalSize, getGcGeneration(),
                    compactionMonitor.getCompactedNodes(),
                    compactionResult.getCompactedRootId().toString10());
            gcListener.cleaned(reclaimedSize, finalSize);
            gcListener.info("TarMK GC #{}: cleanup completed in {} ({} ms). Post cleanup size is {} ({} bytes)" +
                            " and space reclaimed {} ({} bytes).",
                    GC_COUNT, watch, watch.elapsed(MILLISECONDS),
                    humanReadableByteCount(finalSize), finalSize,
                    humanReadableByteCount(reclaimedSize), reclaimedSize);
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
                    Reclaimers.newOldReclaimer(getGcGeneration(), gcOptions.getRetainedGenerations()));
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
                deadline = currentTimeMillis() + MILLISECONDS.convert(duration, unit);
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
                if (store.shutdown) {
                    reason = "The FileStore is shutting down";
                    return true;
                }
                if (cancelled) {
                    reason = "Cancelled by user";
                    return true;
                }
                if (deadline > 0 && currentTimeMillis() > deadline) {
                    reason = "Timeout after " + deadline/1000 + " seconds";
                    return true;
                }
                return false;
            }

            @Override
            public String toString() { return reason; }
        }
    }

    /**
     * Instances of this class represent the result from a compaction.
     * Either {@link #succeeded(GCGeneration, SegmentGCOptions, RecordId) succeeded},
     * {@link #aborted(GCGeneration, GCGeneration) aborted} or {@link #skipped(GCGeneration, SegmentGCOptions) skipped}.
     */
    private abstract static class CompactionResult {
        @Nonnull
        private final GCGeneration currentGeneration;

        protected CompactionResult(@Nonnull GCGeneration currentGeneration) {
            this.currentGeneration = currentGeneration;
        }

        /**
         * Result of a succeeded compaction.
         * @param newGeneration     the generation successfully created by compaction
         * @param gcOptions         the current GC options used by compaction
         * @param compactedRootId   the record id of the root created by compaction
         */
        static CompactionResult succeeded(
                @Nonnull GCGeneration newGeneration,
                @Nonnull final SegmentGCOptions gcOptions,
                @Nonnull final RecordId compactedRootId) {
            return new CompactionResult(newGeneration) {
                @Override
                Predicate<GCGeneration> reclaimer() {
                    return Reclaimers.newOldReclaimer(newGeneration, gcOptions.getRetainedGenerations());
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
         * @param currentGeneration  the current generation of the store
         * @param gcOptions         the current GC options used by compaction
         */
        static CompactionResult skipped(
                @Nonnull GCGeneration currentGeneration,
                @Nonnull final SegmentGCOptions gcOptions,
                @Nonnull final RecordId compactedRootId) {
            return new CompactionResult(currentGeneration) {
                @Override
                Predicate<GCGeneration> reclaimer() {
                    return Reclaimers.newOldReclaimer(currentGeneration, gcOptions.getRetainedGenerations());
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
         * @return  {@code true} for {@link #succeeded(GCGeneration, SegmentGCOptions, RecordId) succeeded}
         *          and {@link #skipped(GCGeneration, SegmentGCOptions, RecordId) skipped}, {@code false} otherwise.
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
