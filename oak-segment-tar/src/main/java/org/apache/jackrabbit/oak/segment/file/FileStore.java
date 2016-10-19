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
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.ByteBuffer.wrap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentWriterBuilder.segmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.GCListener.Status.FAILURE;
import static org.apache.jackrabbit.oak.segment.file.GCListener.Status.SUCCESS;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.EXPEDITE_OPTION;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.timeout;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.Segment.RecordConsumer;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentBufferWriter;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdTable;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.WriterCacheManager.Default;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The storage implementation for tar files.
 */
public class FileStore extends AbstractFileStore {

    private static final Logger log = LoggerFactory.getLogger(FileStore.class);

    private static final int MB = 1024 * 1024;

    static final String LOCK_FILE_NAME = "repo.lock";

    /**
     * GC counter for logging purposes
     */
    private static final AtomicLong GC_COUNT = new AtomicLong(0);

    @Nonnull
    private final SegmentWriter segmentWriter;

    private final int maxFileSize;

    @Nonnull
    private final GarbageCollector garbageCollector;

    private volatile List<TarReader> readers;

    private volatile TarWriter tarWriter;

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
     * Flag signalling shutdown of the file store
     */
    private volatile boolean shutdown;

    private final ReadWriteLock fileStoreLock = new ReentrantReadWriteLock();

    private final FileStoreStats stats;

    FileStore(final FileStoreBuilder builder) throws InvalidFileStoreVersionException, IOException {
        super(builder);

        lockFile = new RandomAccessFile(new File(directory, LOCK_FILE_NAME), "rw");
        try {
            lock = lockFile.getChannel().lock();
        } catch (OverlappingFileLockException ex) {
            throw new IllegalStateException(directory.getAbsolutePath()
                    + " is in use by another store.", ex);
        }

        this.segmentWriter = segmentWriterBuilder("sys")
                .withGeneration(new Supplier<Integer>() {
                    @Override
                    public Integer get() {
                        return getGcGeneration();
                    }
                })
                .withWriterPool()
                .with(builder.getCacheManager())
                .build(this);
        this.maxFileSize = builder.getMaxFileSize() * MB;
        this.garbageCollector = new GarbageCollector(builder.getGcOptions(), builder.getGcListener(), new GCJournal(directory));

        Map<Integer, Map<Character, File>> map = collectFiles(directory);

        Manifest manifest = Manifest.empty();

        if (map.size() > 0) {
            manifest = checkManifest(openManifest());
        }

        saveManifest(manifest);

        this.readers = newArrayListWithCapacity(map.size());
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);
        for (int i = indices.length - 1; i >= 0; i--) {
            readers.add(TarReader.open(map.get(indices[i]), memoryMapping, recovery));
        }
        this.stats = new FileStoreStats(builder.getStatsProvider(), this, size());

        int writeNumber = 0;
        if (indices.length > 0) {
            writeNumber = indices[indices.length - 1] + 1;
        }
        this.tarWriter = new TarWriter(directory, stats, writeNumber);

        fileStoreScheduler.scheduleAtFixedRate(
                format("TarMK flush [%s]", directory), 5, SECONDS,
                new Runnable() {
                    @Override
                    public void run() {
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
                    SegmentGCOptions gcOptions = builder.getGcOptions();

                    @Override
                    public void run() {
                        checkDiskSpace(gcOptions);
                    }
                });
        log.info("TarMK opened: {} (mmap={})", directory, memoryMapping);
        log.debug("TarMK readers {}", this.readers);
    }

    FileStore bind(TarRevisions revisions) throws IOException {
        this.revisions = revisions;
        this.revisions.bind(this, initialNode());
        return this;
    }

    private void saveManifest(Manifest manifest) throws IOException {
        manifest.setStoreVersion(CURRENT_STORE_VERSION);
        manifest.save(getManifestFile());
    }

    @Nonnull
    private Supplier<RecordId> initialNode() {
        return new Supplier<RecordId>() {
            @Override
            public RecordId get() {
                try {
                    SegmentWriter writer = segmentWriterBuilder("init").build(FileStore.this);
                    NodeBuilder builder = EMPTY_NODE.builder();
                    builder.setChildNode("root", EMPTY_NODE);
                    SegmentNodeState node = writer.writeNode(builder.getNodeState());
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

    private int getGcGeneration() {
        return revisions.getHead().getSegmentId().getGcGeneration();
    }

    @CheckForNull
    public CacheStatsMBean getStringDeduplicationCacheStats() {
        return segmentWriter.getStringCacheStats();
    }

    @CheckForNull
    public CacheStatsMBean getTemplateDeduplicationCacheStats() {
        return segmentWriter.getTemplateCacheStats();
    }

    @CheckForNull
    public CacheStatsMBean getNodeDeduplicationCacheStats() {
        return segmentWriter.getNodeCacheStats();
    }

    /**
     * @return  a runnable for running garbage collection
     */
    public Runnable getGCRunner() {
        return new SafeRunnable(format("TarMK revision gc [%s]", directory), new Runnable() {
            @Override
            public void run() {
                try {
                    gc();
                } catch (IOException e) {
                    log.error("Error running revision garbage collection", e);
                }
            }
        });
    }

    /**
     * @return the size of this store. This method shouldn't be called from
     * a very tight loop as it contents with the {@link #fileStoreLock}.
     */
    private long size() {
        List<TarReader> readersSnapshot = null;
        long writeFileSnapshotSize = 0;

        fileStoreLock.readLock().lock();
        try {
            readersSnapshot = ImmutableList.copyOf(readers);
            writeFileSnapshotSize = tarWriter != null ? tarWriter.fileLength() : 0;
        } finally {
            fileStoreLock.readLock().unlock();
        }

        long size = writeFileSnapshotSize;
        for (TarReader reader : readersSnapshot) {
            size += reader.size();
        }

        return size;
    }

    public int readerCount(){
        fileStoreLock.readLock().lock();
        try {
            return readers.size();
        } finally {
            fileStoreLock.readLock().unlock();
        }
    }

    /**
     * Returns the number of segments in this TarMK instance.
     *
     * @return number of segments
     */
    private int count() {
        fileStoreLock.readLock().lock();
        try {
            int count = 0;
            if (tarWriter != null) {
                count += tarWriter.count();
            }
            for (TarReader reader : readers) {
                count += reader.count();
            }
            return count;
        } finally {
            fileStoreLock.readLock().unlock();
        }
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
                tarWriter.flush();
                stats.flushed();

                return null;
            }
        });
    }

    /**
     * Run garbage collection: estimation, compaction, cleanup
     * @throws IOException
     */
    public void gc() throws IOException {
        garbageCollector.run();
    }

    /**
     * Run the compaction gain estimation process.
     * @return
     */
    public GCEstimation estimateCompactionGain() {
        return garbageCollector.estimateCompactionGain(Suppliers.ofInstance(false));
    }

    /**
     * Copy every referenced record in data (non-bulk) segments. Bulk segments
     * are fully kept (they are only removed in cleanup, if there is no
     * reference to them).
     * @return {@code true} on success, {@code false} otherwise.
     */
    public boolean compact() throws IOException {
        return garbageCollector.compact() > 0;
    }

    /**
     * Run garbage collection on the segment level: reclaim those data segments
     * that are from an old segment generation and those bulk segments that are not
     * reachable anymore.
     * Those tar files that shrink by at least 25% are rewritten to a new tar generation
     * skipping the reclaimed segments.
     */
    public void cleanup() throws IOException {
        garbageCollector.cleanup();
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
    public void collectBlobReferences(ReferenceCollector collector) throws IOException {
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
            revisions.close();
            fileStoreLock.writeLock().lock();
            try {
                closeAndLogOnFail(tarWriter);

                List<TarReader> list = readers;
                readers = newArrayList();
                for (TarReader reader : list) {
                    closeAndLogOnFail(reader);
                }

                if (lock != null) {
                    lock.release();
                }
                closeAndLogOnFail(lockFile);
            } finally {
                fileStoreLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to close the TarMK at " + directory, e);
        }

        // Try removing pending files in case the scheduler didn't have a chance to run yet
        fileReaper.reap();
        System.gc(); // for any memory-mappings that are no longer used

        log.info("TarMK closed: {}", directory);
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        return containsSegment(msb, lsb);
    }

    private boolean containsSegment(long msb, long lsb) {
        for (TarReader reader : readers) {
            if (reader.containsEntry(msb, lsb)) {
                return true;
            }
        }

        if (tarWriter != null) {
            fileStoreLock.readLock().lock();
            try {
                if (tarWriter.containsEntry(msb, lsb)) {
                    return true;
                }
            } finally {
                fileStoreLock.readLock().unlock();
            }
        }

        // the writer might have switched to a new file,
        // so we need to re-check the readers
        for (TarReader reader : readers) {
            if (reader.containsEntry(msb, lsb)) {
                return true;
            }
        }

        return false;
    }

    @Override
    @Nonnull
    public Segment readSegment(final SegmentId id) {
        try {
            return segmentCache.getSegment(id, new Callable<Segment>() {
                @Override
                public Segment call() throws Exception {
                    long msb = id.getMostSignificantBits();
                    long lsb = id.getLeastSignificantBits();

                    for (TarReader reader : readers) {
                        try {
                            if (reader.isClosed()) {
                                // Cleanup might already have closed the file.
                                // The segment should be available from another file.
                                log.debug("Skipping closed tar file {}", reader);
                                continue;
                            }

                            ByteBuffer buffer = reader.readEntry(msb, lsb);
                            if (buffer != null) {
                                return new Segment(FileStore.this, segmentReader, id, buffer);
                            }
                        } catch (IOException e) {
                            log.warn("Failed to read from tar file {}", reader, e);
                        }
                    }

                    if (tarWriter != null) {
                        fileStoreLock.readLock().lock();
                        try {
                            try {
                                ByteBuffer buffer = tarWriter.readEntry(msb, lsb);
                                if (buffer != null) {
                                    return new Segment(FileStore.this, segmentReader, id, buffer);
                                }
                            } catch (IOException e) {
                                log.warn("Failed to read from tar file {}", tarWriter, e);
                            }
                        } finally {
                            fileStoreLock.readLock().unlock();
                        }
                    }

                    // the writer might have switched to a new file,
                    // so we need to re-check the readers
                    for (TarReader reader : readers) {
                        try {
                            if (reader.isClosed()) {
                                // Cleanup might already have closed the file.
                                // The segment should be available from another file.
                                log.info("Skipping closed tar file {}", reader);
                                continue;
                            }

                            ByteBuffer buffer = reader.readEntry(msb, lsb);
                            if (buffer != null) {
                                return new Segment(FileStore.this, segmentReader, id, buffer);
                            }
                        } catch (IOException e) {
                            log.warn("Failed to read from tar file {}", reader, e);
                        }
                    }

                    throw new SegmentNotFoundException(id);
                }
            });
        } catch (ExecutionException e) {
            throw e.getCause() instanceof SegmentNotFoundException
                ? (SegmentNotFoundException) e.getCause()
                : new SegmentNotFoundException(id, e);
        }
    }

    @Override
    public void writeSegment(SegmentId id, byte[] buffer, int offset, int length) throws IOException {
        Segment segment = null;

        // If the segment is a data segment, create a new instance of Segment to
        // access some internal information stored in the segment and to store
        // in an in-memory cache for later use.

        if (id.isDataSegmentId()) {
            ByteBuffer data;

            if (offset > 4096) {
                data = ByteBuffer.allocate(length);
                data.put(buffer, offset, length);
                data.rewind();
            } else {
                data = ByteBuffer.wrap(buffer, offset, length);
            }

            segment = new Segment(this, segmentReader, id, data);
        }

        fileStoreLock.writeLock().lock();
        try {
            int generation = Segment.getGcGeneration(wrap(buffer, offset, length), id.asUUID());

            // Flush the segment to disk

            long size = tarWriter.writeEntry(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits(),
                    buffer,
                    offset,
                    length,
                    generation
            );

            // If the segment is a data segment, update the graph before
            // (potentially) flushing the TAR file.

            if (segment != null) {
                populateTarGraph(segment, tarWriter);
                populateTarBinaryReferences(segment, tarWriter);
            }

            // Close the TAR file if the size exceeds the maximum.

            if (size >= maxFileSize) {
                newWriter();
            }
        } finally {
            fileStoreLock.writeLock().unlock();
        }

        // Keep this data segment in memory as it's likely to be accessed soon.

        if (segment != null) {
            segmentCache.putSegment(segment);
        }
    }

    /**
     * Switch to a new tar writer.
     * This method may only be called when holding the write lock of {@link #fileStoreLock}
     * @throws IOException
     */
    private void newWriter() throws IOException {
        TarWriter newWriter = tarWriter.createNextGeneration();
        if (newWriter != tarWriter) {
            File writeFile = tarWriter.getFile();
            List<TarReader> list =
                    newArrayListWithCapacity(1 + readers.size());
            list.add(TarReader.open(writeFile, memoryMapping));
            list.addAll(readers);
            readers = list;
            tarWriter = newWriter;
        }
    }

    private void checkDiskSpace(SegmentGCOptions gcOptions) {
        long repositoryDiskSpace = size();
        long availableDiskSpace = directory.getFreeSpace();
        boolean updated = gcOptions.isDiskSpaceSufficient(repositoryDiskSpace, availableDiskSpace);
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

        private volatile boolean cancelled;

        GarbageCollector(@Nonnull SegmentGCOptions gcOptions,
                         @Nonnull GCListener gcListener,
                         @Nonnull GCJournal gcJournal) {
            this.gcOptions = gcOptions;
            this.gcListener = gcListener;
            this.gcJournal = gcJournal;
        }

        synchronized void run() throws IOException {
            gcListener.info("TarMK GC #{}: started", GC_COUNT.incrementAndGet());
            Stopwatch watch = Stopwatch.createStarted();

            int gainThreshold = gcOptions.getGainThreshold();
            boolean sufficientEstimatedGain = true;
            if (gainThreshold <= 0) {
                gcListener.info("TarMK GC #{}: estimation skipped because gain threshold value ({} <= 0)",
                        GC_COUNT, gainThreshold);
            } else if (gcOptions.isPaused()) {
                gcListener.info("TarMK GC #{}: estimation skipped because compaction is paused", GC_COUNT);
            } else {
                gcListener.info("TarMK GC #{}: estimation started", GC_COUNT);
                Supplier<Boolean> cancel = new CancelCompactionSupplier(FileStore.this);
                GCEstimation estimate = estimateCompactionGain(cancel);
                if (cancel.get()) {
                    gcListener.info("TarMK GC #{}: estimation interrupted: {}. Skipping compaction.", GC_COUNT, cancel);
                    return;
                }

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
                if (!gcOptions.isPaused()) {
                    logAndClear(segmentWriter.getNodeWriteTimeStats(), segmentWriter.getNodeCompactTimeStats());
                    log(segmentWriter.getNodeCacheOccupancyInfo());
                    int gen = compact();
                    if (gen > 0) {
                        fileReaper.add(cleanupOldGenerations(gen));
                    } else if (gen < 0) {
                        gcListener.info("TarMK GC #{}: cleaning up after failed compaction", GC_COUNT);
                        fileReaper.add(cleanupGeneration(-gen));
                    }
                    logAndClear(segmentWriter.getNodeWriteTimeStats(), segmentWriter.getNodeCompactTimeStats());
                    log(segmentWriter.getNodeCacheOccupancyInfo());
                } else {
                    gcListener.skipped("TarMK GC #{}: compaction paused", GC_COUNT);
                }
            }
        }

        /**
         * Estimated compaction gain. The result will be undefined if stopped through
         * the passed {@code stop} signal.
         * @param stop  signal for stopping the estimation process.
         * @return compaction gain estimate
         */
        synchronized GCEstimation estimateCompactionGain(Supplier<Boolean> stop) {
            if (gcOptions.isGcSizeDeltaEstimation()) {
                SizeDeltaGcEstimation e = new SizeDeltaGcEstimation(gcOptions,
                        gcJournal, stats.getApproximateSize());
                return e;
            }

            CompactionGainEstimate estimate = new CompactionGainEstimate(getHead(),
                    count(), stop, gcOptions.getGainThreshold());
            fileStoreLock.readLock().lock();
            try {
                for (TarReader reader : readers) {
                    reader.accept(estimate);
                    if (stop.get()) {
                        break;
                    }
                }
            } finally {
                fileStoreLock.readLock().unlock();
            }
            return estimate;
        }

        private void logAndClear(
                @Nonnull DescriptiveStatistics nodeWriteTimeStats,
                @Nonnull DescriptiveStatistics nodeCompactTimeStats) {
            log.info("Node write time statistics (ns) {}", toString(nodeWriteTimeStats));
            log.info("Node compact time statistics (ns) {}", toString(nodeCompactTimeStats));
            nodeWriteTimeStats.clear();
            nodeCompactTimeStats.clear();
        }

        private void log(@CheckForNull String nodeCacheOccupancyInfo) {
            if (nodeCacheOccupancyInfo != null) {
                log.info("NodeCache occupancy: {}", nodeCacheOccupancyInfo);
            }
        }

        private String toString(DescriptiveStatistics statistics) {
            DecimalFormat sci = new DecimalFormat("##0.0E0");
            DecimalFormatSymbols symbols = sci.getDecimalFormatSymbols();
            symbols.setNaN("NaN");
            symbols.setInfinity("Inf");
            sci.setDecimalFormatSymbols(symbols);
            return "min=" + sci.format(statistics.getMin()) +
                    ", 10%=" + sci.format(statistics.getPercentile(10.0)) +
                    ", 50%=" + sci.format(statistics.getPercentile(50.0)) +
                    ", 90%=" + sci.format(statistics.getPercentile(90.0)) +
                    ", max=" + sci.format(statistics.getMax()) +
                    ", mean=" + sci.format(statistics.getMean()) +
                    ", stdev=" + sci.format(statistics.getStandardDeviation()) +
                    ", N=" + sci.format(statistics.getN());
        }

        synchronized int compact() throws IOException {
            try {
                Stopwatch watch = Stopwatch.createStarted();
                gcListener.info("TarMK GC #{}: compaction started, gc options={}", GC_COUNT, gcOptions);

                SegmentNodeState before = getHead();
                final int newGeneration = getGcGeneration() + 1;
                SegmentBufferWriter bufferWriter = new SegmentBufferWriter(FileStore.this, tracker, segmentReader, "c", newGeneration);
                Supplier<Boolean> cancel = new CancelCompactionSupplier(FileStore.this);
                SegmentNodeState after = compact(bufferWriter, before, cancel);
                if (after == null) {
                    gcListener.info("TarMK GC #{}: compaction cancelled: {}.", GC_COUNT, cancel);
                    return 0;
                }

                gcListener.info("TarMK GC #{}: compacted {} to {}",
                        GC_COUNT, before.getRecordId(), after.getRecordId());

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
                    SegmentNodeState head = getHead();
                    after = compact(bufferWriter, head, cancel);
                    if (after == null) {
                        gcListener.info("TarMK GC #{}: compaction cancelled: {}.", GC_COUNT, cancel);
                        return 0;
                    }

                    gcListener.info("TarMK GC #{}: compacted {} against {} to {}",
                            GC_COUNT, head.getRecordId(), before.getRecordId(), after.getRecordId());
                    before = head;
                }

                if (!success) {
                    gcListener.info("TarMK GC #{}: compaction gave up compacting concurrent commits after {} cycles.",
                            GC_COUNT, cycles);
                    int forceTimeout = gcOptions.getForceTimeout();
                    if (forceTimeout > 0) {
                        gcListener.info("TarMK GC #{}: trying to force compact remaining commits for {} seconds",
                                GC_COUNT, forceTimeout);
                        cycles++;
                        success = forceCompact(bufferWriter, or(cancel, timeOut(forceTimeout, SECONDS)));
                        if (!success) {
                            if (cancel.get()) {
                                gcListener.warn("TarMK GC #{}: compaction failed to force compact remaining commits. " +
                                        "Compaction was cancelled: {}.", GC_COUNT, cancel);
                            } else {
                                gcListener.warn("TarMK GC #{}: compaction failed to force compact remaining commits. " +
                                        "Most likely compaction didn't get exclusive access to the store.", GC_COUNT);
                            }
                        }
                    }
                }

                if (success) {
                    gcListener.compacted(SUCCESS, newGeneration);
                    gcListener.info("TarMK GC #{}: compaction succeeded in {} ({} ms), after {} cycles",
                            GC_COUNT, watch, watch.elapsed(MILLISECONDS), cycles);
                    return newGeneration;
                } else {
                    gcListener.compacted(FAILURE, newGeneration);
                    gcListener.info("TarMK GC #{}: compaction failed after {} ({} ms), and {} cycles",
                            GC_COUNT, watch, watch.elapsed(MILLISECONDS), cycles);
                    return -newGeneration;
                }
            } catch (InterruptedException e) {
                gcListener.error("TarMK GC #" + GC_COUNT + ": compaction interrupted", e);
                currentThread().interrupt();
                return 0;
            } catch (Exception e) {
                gcListener.error("TarMK GC #" + GC_COUNT + ": compaction encountered an error", e);
                return 0;
            }
        }

        /**
         * @param duration
         * @param unit
         * @return  {@code Supplier} instance which returns true once the time specified in
         * {@code duration} and {@code unit} has passed.
         */
        private Supplier<Boolean> timeOut(final long duration, @Nonnull final TimeUnit unit) {
            return new Supplier<Boolean>() {
                long deadline = currentTimeMillis() + MILLISECONDS.convert(duration, unit);
                @Override
                public Boolean get() {
                    return currentTimeMillis() > deadline;
                }
            };
        }

        /**
         * @param supplier1
         * @param supplier2
         * @return {@code Supplier} instance that returns {@code true} iff {@code supplier1} returns
         * {@code true} or otherwise {@code supplier2} returns {@code true}.
         */
        private Supplier<Boolean> or(
                @Nonnull Supplier<Boolean> supplier1,
                @Nonnull Supplier<Boolean> supplier2) {
            if (supplier1.get()) {
                return Suppliers.ofInstance(true);
            } else {
                return supplier2;
            }
        }

        private SegmentNodeState compact(SegmentBufferWriter bufferWriter, NodeState head,
                                         Supplier<Boolean> cancel)
        throws IOException {
            if (gcOptions.isOffline()) {
                BlobStore blobStore = getBlobStore();
                SegmentWriter writer = new SegmentWriter(FileStore.this, segmentReader, blobStore, new Default(), bufferWriter);
                return new Compactor(segmentReader, writer, blobStore, cancel, gcOptions)
                        .compact(EMPTY_NODE, head, EMPTY_NODE);
            } else {
                return segmentWriter.writeNode(head, bufferWriter, cancel);
            }
        }

        private boolean forceCompact(@Nonnull final SegmentBufferWriter bufferWriter,
                                     @Nonnull final Supplier<Boolean> cancel)
        throws InterruptedException {
            return revisions.
                    setHead(new Function<RecordId, RecordId>() {
                                @Nullable
                                @Override
                                public RecordId apply(RecordId base) {
                                    try {
                                        long t0 = currentTimeMillis();
                                        SegmentNodeState after = compact(bufferWriter,
                                                segmentReader.readNode(base), cancel);
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
                            },
                            timeout(gcOptions.getForceTimeout(), SECONDS));
        }

        synchronized void cleanup() throws IOException {
            fileReaper.add(cleanupOldGenerations(getGcGeneration()));
        }

        /**
         * Cleanup segments that are from an old generation. That segments whose generation
         * is {@code gcGeneration - SegmentGCOptions.getRetainedGenerations()} or older.
         * @param gcGeneration
         * @return list of files to be removed
         * @throws IOException
         */
        private List<File> cleanupOldGenerations(int gcGeneration) throws IOException {
            final int reclaimGeneration = gcGeneration - gcOptions.getRetainedGenerations();

            Predicate<Integer> reclaimPredicate = new Predicate<Integer>() {
                @Override
                public boolean apply(Integer generation) {
                    return generation <= reclaimGeneration;
                }
            };
            return cleanup(reclaimPredicate,
                    "gc-count=" + GC_COUNT +
                            ",gc-status=success" +
                            ",store-generation=" + gcGeneration +
                            ",reclaim-predicate=(generation<=" + reclaimGeneration + ")");
        }

        /**
         * Cleanup segments whose generation matches the {@code reclaimGeneration} predicate.
         * @param reclaimGeneration
         * @param gcInfo  gc information to be passed to {@link SegmentIdTable#clearSegmentIdTables(Set, String)}
         * @return list of files to be removed
         * @throws IOException
         */
        private List<File> cleanup(
                @Nonnull Predicate<Integer> reclaimGeneration,
                @Nonnull String gcInfo)
        throws IOException {
            Stopwatch watch = Stopwatch.createStarted();
            Set<UUID> bulkRefs = newHashSet();
            Map<TarReader, TarReader> cleaned = newLinkedHashMap();

            long initialSize = 0;
            fileStoreLock.writeLock().lock();
            try {
                gcListener.info("TarMK GC #{}: cleanup started.", GC_COUNT);

                newWriter();
                segmentCache.clear();

                // Suggest to the JVM that now would be a good time
                // to clear stale weak references in the SegmentTracker
                System.gc();

                collectBulkReferences(bulkRefs);

                for (TarReader reader : readers) {
                    cleaned.put(reader, reader);
                    initialSize += reader.size();
                }
            } finally {
                fileStoreLock.writeLock().unlock();
            }

            gcListener.info("TarMK GC #{}: current repository size is {} ({} bytes)",
                    GC_COUNT, humanReadableByteCount(initialSize), initialSize);

            Set<UUID> reclaim = newHashSet();
            for (TarReader reader : cleaned.keySet()) {
                reader.mark(bulkRefs, reclaim, reclaimGeneration);
                log.info("{}: size of bulk references/reclaim set {}/{}",
                        reader, bulkRefs.size(), reclaim.size());
                if (shutdown) {
                    gcListener.info("TarMK GC #{}: cleanup interrupted", GC_COUNT);
                    break;
                }
            }
            Set<UUID> reclaimed = newHashSet();
            for (TarReader reader : cleaned.keySet()) {
                cleaned.put(reader, reader.sweep(reclaim, reclaimed));
                if (shutdown) {
                    gcListener.info("TarMK GC #{}: cleanup interrupted", GC_COUNT);
                    break;
                }
            }

            // it doesn't account for concurrent commits that might have happened
            long afterCleanupSize = 0;

            List<TarReader> oldReaders = newArrayList();
            fileStoreLock.writeLock().lock();
            try {
                // Replace current list of reader with the cleaned readers taking care not to lose
                // any new reader that might have come in through concurrent calls to newWriter()
                List<TarReader> sweptReaders = newArrayList();
                for (TarReader reader : readers) {
                    if (cleaned.containsKey(reader)) {
                        TarReader newReader = cleaned.get(reader);
                        if (newReader != null) {
                            sweptReaders.add(newReader);
                            afterCleanupSize += newReader.size();
                        }
                        // if these two differ, the former represents the swept version of the latter
                        if (newReader != reader) {
                            oldReaders.add(reader);
                        }
                    } else {
                        sweptReaders.add(reader);
                    }
                }
                readers = sweptReaders;
            } finally {
                fileStoreLock.writeLock().unlock();
            }
            tracker.clearSegmentIdTables(reclaimed, gcInfo);

            // Close old readers *after* setting readers to the new readers to avoid accessing
            // a closed reader from readSegment()
            LinkedList<File> toRemove = newLinkedList();
            for (TarReader oldReader : oldReaders) {
                closeAndLogOnFail(oldReader);
                File file = oldReader.getFile();
                gcListener.info("TarMK GC #{}: cleanup marking file for deletion: {}", GC_COUNT, file.getName());
                toRemove.addLast(file);
            }

            long finalSize = size();
            long reclaimedSize = initialSize - afterCleanupSize;
            stats.reclaimed(reclaimedSize);
            gcJournal.persist(reclaimedSize, finalSize);
            gcListener.cleaned(reclaimedSize, finalSize);
            gcListener.info("TarMK GC #{}: cleanup completed in {} ({} ms). Post cleanup size is {} ({} bytes)" +
                            " and space reclaimed {} ({} bytes).",
                    GC_COUNT, watch, watch.elapsed(MILLISECONDS),
                    humanReadableByteCount(finalSize), finalSize,
                    humanReadableByteCount(reclaimedSize), reclaimedSize);
            return toRemove;
        }

        private void collectBulkReferences(Set<UUID> bulkRefs) {
            Set<UUID> refs = newHashSet();
            for (SegmentId id : tracker.getReferencedSegmentIds()) {
                refs.add(id.asUUID());
            }
            tarWriter.collectReferences(refs);
            for (UUID ref : refs) {
                if (!isDataSegmentId(ref.getLeastSignificantBits())) {
                    bulkRefs.add(ref);
                }
            }
        }

        /**
         * Cleanup segments of the given generation {@code gcGeneration}.
         * @param gcGeneration
         * @return list of files to be removed
         * @throws IOException
         */
        private List<File> cleanupGeneration(final int gcGeneration) throws IOException {
            Predicate<Integer> cleanupPredicate = new Predicate<Integer>() {
                @Override
                public boolean apply(Integer generation) {
                    return generation == gcGeneration;
                }
            };
            return cleanup(cleanupPredicate,
                    "gc-count=" + GC_COUNT +
                            ",gc-status=failed" +
                            ",store-generation=" + (gcGeneration - 1) +
                            ",reclaim-predicate=(generation==" + gcGeneration + ")");
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
        synchronized void collectBlobReferences(ReferenceCollector collector) throws IOException {
            segmentWriter.flush();
            List<TarReader> tarReaders = newArrayList();
            fileStoreLock.writeLock().lock();
            try {
                newWriter();
                tarReaders.addAll(FileStore.this.readers);
            } finally {
                fileStoreLock.writeLock().unlock();
            }

            int minGeneration = getGcGeneration() - gcOptions.getRetainedGenerations() + 1;
            for (TarReader tarReader : tarReaders) {
                tarReader.collectBlobReferences(collector, minGeneration);
            }
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
         */
        private class CancelCompactionSupplier implements Supplier<Boolean> {
            private final FileStore store;

            private String reason;

            public CancelCompactionSupplier(@Nonnull FileStore store) {
                cancelled = false;
                this.store = store;
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
                if (store.shutdown) {
                    reason = "The FileStore is shutting down";
                    return true;
                }
                if (cancelled) {
                    reason = "Cancelled by user";
                    return true;
                }
                return false;
            }

            @Override
            public String toString() { return reason; }
        }
    }

}
