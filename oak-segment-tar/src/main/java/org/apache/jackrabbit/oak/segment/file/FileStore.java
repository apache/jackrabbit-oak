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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.nio.ByteBuffer.wrap;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentWriterBuilder.segmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.GCListener.Status.FAILURE;
import static org.apache.jackrabbit.oak.segment.file.GCListener.Status.SUCCESS;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.timeout;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.segment.BinaryReferenceConsumer;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentBufferWriter;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentGraph.SegmentGraphVisitor;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
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
public class FileStore implements SegmentStore, Closeable {
    private static final Logger log = LoggerFactory.getLogger(FileStore.class);

    private static final int MB = 1024 * 1024;

    private static final Pattern FILE_NAME_PATTERN =
            Pattern.compile("(data|bulk)((0|[1-9][0-9]*)[0-9]{4})([a-z])?.tar");

    private static final String FILE_NAME_FORMAT = "data%05d%s.tar";

    private static final String LOCK_FILE_NAME = "repo.lock";

    /**
     * GC counter for logging purposes
     */
    private static final AtomicLong GC_COUNT = new AtomicLong(0);

    static final boolean MEMORY_MAPPING_DEFAULT =
            "64".equals(System.getProperty("sun.arch.data.model", "32"));

    @Nonnull
    private final SegmentTracker tracker;

    @Nonnull
    private final SegmentWriter segmentWriter;

    @Nonnull
    private final CachingSegmentReader segmentReader;

    @Nonnull
    private final BinaryReferenceConsumer binaryReferenceConsumer;

    private final File directory;

    private final BlobStore blobStore;

    private final int maxFileSize;

    private final boolean memoryMapping;

    private volatile List<TarReader> readers;

    private int writeNumber;

    private volatile File writeFile;

    private volatile TarWriter tarWriter;

    private final RandomAccessFile lockFile;

    private final FileLock lock;

    @Nonnull
    private final TarRevisions revisions;

    /**
     * The background flush thread. Automatically flushes the TarMK state
     * once every five seconds.
     */
    private final PeriodicOperation flushOperation;

    /**
     * The background compaction thread. Compacts the TarMK contents whenever
     * triggered by the {@link #gc()} method.
     */
    private final TriggeredOperation compactionOperation;

    /**
     * This background thread periodically asks the {@code SegmentGCOptions}
     * to compare the approximate size of the repository with the available disk
     * space. The result of this comparison is stored in the state of this
     * {@code FileStore}.
     */
    private final PeriodicOperation diskSpaceOperation;

    private final SegmentGCOptions gcOptions;

    /**
     * Flag to request revision cleanup during the next flush.
     */
    private final AtomicBoolean cleanupNeeded = new AtomicBoolean(false);

    /**
     * List of old tar file generations that are waiting to be removed. They can
     * not be removed immediately, because they first need to be closed, and the
     * JVM needs to release the memory mapped file references.
     */
    private final List<File> pendingRemove = newLinkedList();

    /**
     * {@code GcListener} listening to this instance's gc progress
     */
    private final GCListener gcListener;

    /**
     * Represents the approximate size on disk of the repository.
     */
    private final AtomicLong approximateSize;

    /**
     * This flag is periodically updated by calling the {@code SegmentGCOptions}
     * at regular intervals.
     */
    private final AtomicBoolean sufficientDiskSpace;

    /**
     * Flag signalling shutdown of the file store
     */
    private volatile boolean shutdown;

    private final ReadWriteLock fileStoreLock = new ReentrantReadWriteLock();

    private final FileStoreStats stats;

    @Nonnull
    private final SegmentCache segmentCache;

    private final SegmentIdFactory segmentIdFactory = new SegmentIdFactory() {

        @Override
        @Nonnull
        public SegmentId newSegmentId(long msb, long lsb) {
            return new SegmentId(FileStore.this, msb, lsb);
        }

    };

    // FIXME OAK-4450: Properly split the FileStore into read-only and r/w variants
    FileStore(FileStoreBuilder builder, final boolean readOnly) throws IOException {
        this.tracker = new SegmentTracker();
        this.revisions = builder.getRevisions();
        this.blobStore = builder.getBlobStore();
        this.segmentCache = new SegmentCache(builder.getSegmentCacheSize());
        this.segmentReader = new CachingSegmentReader(new Supplier<SegmentWriter>() {
            @Override
            public SegmentWriter get() {
                return segmentWriter;
            }
        }, blobStore, builder.getStringCacheSize(), builder.getTemplateCacheSize());

        this.binaryReferenceConsumer = new BinaryReferenceConsumer() {

            @Override
            public void consume(int generation, UUID segmentId, String binaryReference) {
                fileStoreLock.writeLock().lock();
                try {
                    tarWriter.addBinaryReference(generation, segmentId, binaryReference);
                } finally {
                    fileStoreLock.writeLock().unlock();
                }
            }

        };

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
        this.directory = builder.getDirectory();
        this.maxFileSize = builder.getMaxFileSize() * MB;
        this.memoryMapping = builder.getMemoryMapping();
        this.gcListener = builder.getGcListener();
        this.gcOptions = builder.getGcOptions();

        Map<Integer, Map<Character, File>> map = collectFiles(directory);
        this.readers = newArrayListWithCapacity(map.size());
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);
        for (int i = indices.length - 1; i >= 0; i--) {
            if (!readOnly) {
                readers.add(TarReader.open(map.get(indices[i]), memoryMapping));
            } else {
                // only try to read-only recover the latest file as that might
                // be the *only* one still being accessed by a writer
                boolean recover = i == indices.length - 1;
                readers.add(TarReader.openRO(map.get(indices[i]),
                        memoryMapping, recover));
            }
        }

        long initialSize = size();
        this.approximateSize = new AtomicLong(initialSize);
        this.stats = new FileStoreStats(builder.getStatsProvider(), this, initialSize);

        if (!readOnly) {
            if (indices.length > 0) {
                this.writeNumber = indices[indices.length - 1] + 1;
            } else {
                this.writeNumber = 0;
            }
            this.writeFile = new File(directory, format(
                    FILE_NAME_FORMAT, writeNumber, "a"));
            this.tarWriter = new TarWriter(writeFile, stats);
        }

        if (!readOnly) {
            lockFile = new RandomAccessFile(new File(directory, LOCK_FILE_NAME), "rw");
            lock = lockFile.getChannel().lock();
        } else {
            lockFile = null;
            lock = null;
        }

        // FIXME OAK-4621: External invocation of background operations
        // The following background operations are historically part of
        // the implementation of the FileStore, but they should better be
        // scheduled and invoked by an external agent. The code deploying the
        // FileStore might have better insights on when and how these background
        // operations should be invoked. See also OAK-3468.

        flushOperation = new PeriodicOperation(format("TarMK flush thread [%s]", directory), 5, SECONDS, new Runnable() {

            @Override
            public void run() {
                try {
                    flush();
                } catch (IOException e) {
                    log.warn("Failed to flush the TarMK at {}", directory, e);
                }
            }

        });

        compactionOperation = new TriggeredOperation(format("TarMK compaction thread [%s]", directory), new Runnable() {

            @Override
            public void run() {
                try {
                    maybeCompact(true);
                } catch (IOException e) {
                    log.error("Error running compaction", e);
                }
            }

        });

        diskSpaceOperation = new PeriodicOperation(format("TarMK disk space check [%s]", directory), 1, MINUTES, new Runnable() {

            @Override
            public void run() {
                checkDiskSpace();
            }

        });

        if (!readOnly) {
            flushOperation.start();
            diskSpaceOperation.start();
        }

        sufficientDiskSpace = new AtomicBoolean(true);

        if (readOnly) {
            log.info("TarMK ReadOnly opened: {} (mmap={})", directory,
                    memoryMapping);
        } else {
            log.info("TarMK opened: {} (mmap={})", directory, memoryMapping);
        }
        log.debug("TarMK readers {}", this.readers);
    }

    FileStore bind(TarRevisions revisions) throws IOException {
        revisions.bind(this, initialNode());
        return this;
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
        return revisions.getHead().getSegment().getGcGeneration();
    }

    @Nonnull
    public CacheStatsMBean getSegmentCacheStats() {
        return segmentCache.getCacheStats();
    }

    @Nonnull
    public CacheStatsMBean getStringCacheStats() {
        return segmentReader.getStringCacheStats();
    }

    @Nonnull
    public CacheStatsMBean getTemplateCacheStats() {
        return segmentReader.getTemplateCacheStats();
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

    public void maybeCompact(boolean cleanup) throws IOException {
        gcListener.info("TarMK GC #{}: started", GC_COUNT.incrementAndGet());

        Runtime runtime = Runtime.getRuntime();
        long avail = runtime.totalMemory() - runtime.freeMemory();
        // FIXME OAK-4281: Rework memory estimation for compaction
        // What value should we use for delta?
        long delta = 0;
        long needed = delta * gcOptions.getMemoryThreshold();
        if (needed >= avail) {
            gcListener.skipped(
                    "TarMK GC #{}: not enough available memory {} ({} bytes), needed {} ({} bytes)," +
                    " last merge delta {} ({} bytes), so skipping compaction for now",
                    GC_COUNT,
                    humanReadableByteCount(avail), avail,
                    humanReadableByteCount(needed), needed,
                    humanReadableByteCount(delta), delta);
            if (cleanup) {
                cleanupNeeded.set(!gcOptions.isPaused());
            }
        }

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
            Supplier<Boolean> shutdown = newShutdownSignal();
            CompactionGainEstimate estimate = estimateCompactionGain(shutdown);
            if (shutdown.get()) {
                gcListener.info("TarMK GC #{}: estimation interrupted. Skipping compaction.", GC_COUNT);
            }

            long gain = estimate.estimateCompactionGain();
            sufficientEstimatedGain = gain >= gainThreshold;
            if (sufficientEstimatedGain) {
                gcListener.info(
                    "TarMK GC #{}: estimation completed in {} ({} ms). " +
                    "Gain is {}% or {}/{} ({}/{} bytes), so running compaction",
                        GC_COUNT, watch, watch.elapsed(MILLISECONDS), gain,
                        humanReadableByteCount(estimate.getReachableSize()), humanReadableByteCount(estimate.getTotalSize()),
                        estimate.getReachableSize(), estimate.getTotalSize());
            } else {
                if (estimate.getTotalSize() == 0) {
                    gcListener.skipped(
                            "TarMK GC #{}: estimation completed in {} ({} ms). " +
                            "Skipping compaction for now as repository consists of a single tar file only",
                            GC_COUNT, watch, watch.elapsed(MILLISECONDS));
                } else {
                    gcListener.skipped(
                        "TarMK GC #{}: estimation completed in {} ({} ms). " +
                        "Gain is {}% or {}/{} ({}/{} bytes), so skipping compaction for now",
                            GC_COUNT, watch, watch.elapsed(MILLISECONDS), gain,
                            humanReadableByteCount(estimate.getReachableSize()), humanReadableByteCount(estimate.getTotalSize()),
                            estimate.getReachableSize(), estimate.getTotalSize());
                }
            }
        }

        if (sufficientEstimatedGain) {
            if (!gcOptions.isPaused()) {
                logAndClear(segmentWriter.getNodeWriteTimeStats(), segmentWriter.getNodeCompactTimeStats());
                if (compact()) {
                    cleanupNeeded.set(cleanup);
                }
                logAndClear(segmentWriter.getNodeWriteTimeStats(), segmentWriter.getNodeCompactTimeStats());
            } else {
                gcListener.skipped("TarMK GC #{}: compaction paused", GC_COUNT);
            }
        }
    }

    private static void logAndClear(
            @Nonnull DescriptiveStatistics nodeWriteTimeStats,
            @Nonnull DescriptiveStatistics nodeCompactTimeStats) {
        log.info("Node write time statistics (ns) {}", toString(nodeWriteTimeStats));
        log.info("Node compact time statistics (ns) {}", toString(nodeCompactTimeStats));
        nodeWriteTimeStats.clear();
        nodeCompactTimeStats.clear();
    }

    private static String toString(DescriptiveStatistics statistics) {
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

    static Map<Integer, Map<Character, File>> collectFiles(File directory) {
        Map<Integer, Map<Character, File>> dataFiles = newHashMap();
        Map<Integer, File> bulkFiles = newHashMap();

        for (File file : directory.listFiles()) {
            Matcher matcher = FILE_NAME_PATTERN.matcher(file.getName());
            if (matcher.matches()) {
                Integer index = Integer.parseInt(matcher.group(2));
                if ("data".equals(matcher.group(1))) {
                    Map<Character, File> files = dataFiles.get(index);
                    if (files == null) {
                        files = newHashMap();
                        dataFiles.put(index, files);
                    }
                    Character generation = 'a';
                    if (matcher.group(4) != null) {
                        generation = matcher.group(4).charAt(0);
                    }
                    checkState(files.put(generation, file) == null);
                } else {
                    checkState(bulkFiles.put(index, file) == null);
                }
            }
        }

        if (!bulkFiles.isEmpty()) {
            log.info("Upgrading TarMK file names in {}", directory);

            if (!dataFiles.isEmpty()) {
                // first put all the data segments at the end of the list
                Integer[] indices =
                        dataFiles.keySet().toArray(new Integer[dataFiles.size()]);
                Arrays.sort(indices);
                int position = Math.max(
                        indices[indices.length - 1] + 1,
                        bulkFiles.size());
                for (Integer index : indices) {
                    Map<Character, File> files = dataFiles.remove(index);
                    Integer newIndex = position++;
                    for (Character generation : newHashSet(files.keySet())) {
                        File file = files.get(generation);
                        File newFile = new File(
                                directory,
                                format(FILE_NAME_FORMAT, newIndex, generation));
                        log.info("Renaming {} to {}", file, newFile);
                        file.renameTo(newFile);
                        files.put(generation, newFile);
                    }
                    dataFiles.put(newIndex, files);
                }
            }

            // then add all the bulk segments at the beginning of the list
            Integer[] indices =
                    bulkFiles.keySet().toArray(new Integer[bulkFiles.size()]);
            Arrays.sort(indices);
            int position = 0;
            for (Integer index : indices) {
                File file = bulkFiles.remove(index);
                Integer newIndex = position++;
                File newFile = new File(
                        directory, format(FILE_NAME_FORMAT, newIndex, "a"));
                log.info("Renaming {} to {}", file, newFile);
                file.renameTo(newFile);
                dataFiles.put(newIndex, singletonMap('a', newFile));
            }
        }

        return dataFiles;
    }

    public final long size() {
        fileStoreLock.readLock().lock();
        try {
            long size = writeFile != null ? writeFile.length() : 0;
            for (TarReader reader : readers) {
                size += reader.size();
            }
            return size;
        } finally {
            fileStoreLock.readLock().unlock();
        }
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

    /**
     * Estimated compaction gain. The result will be undefined if stopped through
     * the passed {@code stop} signal.
     * @param stop  signal for stopping the estimation process.
     * @return compaction gain estimate
     */
    CompactionGainEstimate estimateCompactionGain(Supplier<Boolean> stop) {
        CompactionGainEstimate estimate = new CompactionGainEstimate(getHead(), count(), stop);
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

    public FileStoreStats getStats() {
        return stats;
    }

    public void flush() throws IOException {
        revisions.flush(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                segmentWriter.flush();
                tarWriter.flush();
                return null;
            }
        });

        if (cleanupNeeded.getAndSet(false)) {
            // FIXME OAK-4138: Decouple revision cleanup from the flush thread
            pendingRemove.addAll(cleanup());
        }

        // FIXME OAK-4138 Decouple revision cleanup from the flush thread: instead of synchronizing, skip flush if already in progress
        // remove all obsolete tar generations
        synchronized (pendingRemove) {
            Iterator<File> iterator = pendingRemove.iterator();
            while (iterator.hasNext()) {
                File file = iterator.next();
                log.debug("TarMK GC: Attempting to remove old file {}", file);
                if (!file.exists() || file.delete()) {
                    log.debug("TarMK GC: Removed old file {}", file);
                    iterator.remove();
                } else {
                    log.warn("TarMK GC: Failed to remove old file {}. Will retry later.", file);
                }
            }
        }
    }

    /**
     * Run garbage collection on the segment level: reclaim those data segments
     * that are from an old segment generation and those bulk segments that are not
     * reachable anymore.
     * Those tar files that shrink by at least 25% are rewritten to a new tar generation
     * skipping the reclaimed segments.
     */
    public List<File> cleanup() throws IOException {
        int gcGeneration = getGcGeneration();
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

    private List<File> cleanup(
            @Nonnull Predicate<Integer> reclaimGeneration,
            @Nonnull String gcInfo)
    throws IOException {
        Stopwatch watch = Stopwatch.createStarted();
        long initialSize = size();
        Set<UUID> bulkRefs = newHashSet();
        Map<TarReader, TarReader> cleaned = newLinkedHashMap();

        fileStoreLock.writeLock().lock();
        try {
            gcListener.info("TarMK GC #{}: cleanup started. Current repository size is {} ({} bytes)",
                    GC_COUNT, humanReadableByteCount(initialSize), initialSize);

            newWriter();
            segmentCache.clear();

            // Suggest to the JVM that now would be a good time
            // to clear stale weak references in the SegmentTracker
            System.gc();

            for (SegmentId id : tracker.getReferencedSegmentIds()) {
                if (!isDataSegmentId(id.getLeastSignificantBits())) {
                    bulkRefs.add(id.asUUID());
                }
            }
            for (TarReader reader : readers) {
                cleaned.put(reader, reader);
            }
        } finally {
            fileStoreLock.writeLock().unlock();
        }

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

        List<TarReader> oldReaders = newArrayList();
        fileStoreLock.writeLock().lock();
        try {
            // Replace current list of reader with the cleaned readers taking care not to lose
            // any new reader that might have come in through concurrent calls to newWriter()
            List<TarReader> newReaders = newArrayList();
            for (TarReader reader : readers) {
                if (cleaned.containsKey(reader)) {
                    TarReader newReader = cleaned.get(reader);
                    if (newReader != null) {
                        newReaders.add(newReader);
                    }
                    if (newReader != reader) {
                        oldReaders.add(reader);
                    }
                } else {
                    newReaders.add(reader);
                }
            }
            readers = newReaders;
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
        approximateSize.set(finalSize);
        stats.reclaimed(initialSize - finalSize);
        // FIXME OAK-4106: Reclaimed size reported by FileStore.cleanup is off
        gcListener.cleaned(initialSize - finalSize, finalSize);
        gcListener.info("TarMK GC #{}: cleanup completed in {} ({} ms). Post cleanup size is {} ({} bytes)" +
                " and space reclaimed {} ({} bytes).",
                GC_COUNT, watch, watch.elapsed(MILLISECONDS),
                humanReadableByteCount(finalSize), finalSize,
                humanReadableByteCount(initialSize - finalSize), initialSize - finalSize);
        return toRemove;
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
        segmentWriter.flush();
        List<TarReader> tarReaders = newArrayList();
        fileStoreLock.writeLock().lock();
        try {
            newWriter();
            tarReaders.addAll(this.readers);
        } finally {
            fileStoreLock.writeLock().unlock();
        }

        int minGeneration = getGcGeneration() - gcOptions.getRetainedGenerations() + 1;
        for (TarReader tarReader : tarReaders) {
            tarReader.collectBlobReferences(collector, minGeneration);
        }
    }

    /**
     * Returns the cancellation policy for the compaction phase. If the disk
     * space was considered insufficient at least once during compaction (or if
     * the space was never sufficient to begin with), compaction is considered
     * canceled.
     * Furthermore when the file store is shutting down, compaction is considered
     * canceled.
     *
     * @return a flag indicating if compaction should be canceled.
     */
    private Supplier<Boolean> newCancelCompactionCondition() {
        return new Supplier<Boolean>() {

            private boolean outOfDiskSpace;
            private boolean shutdown;

            @Override
            public Boolean get() {

                // The outOfDiskSpace and shutdown flags can only transition from false (their initial
                // values), to true. Once true, there should be no way to go back.
                if (!sufficientDiskSpace.get()) {
                    outOfDiskSpace = true;
                }
                if (FileStore.this.shutdown) {
                    this.shutdown = true;
                }

                return shutdown || outOfDiskSpace;
            }

            @Override
            public String toString() {
                if (outOfDiskSpace) {
                    return "Not enough disk space available";
                } else if (shutdown) {
                    return "FileStore shutdown request received";
                } else {
                    return "";
                }
            }
        };
    }

    /**
     * Returns a signal indication the file store shutting down.
     * @return  a shutdown signal
     */
    private Supplier<Boolean> newShutdownSignal() {
        return new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                return shutdown;
            }
        };
    }

    /**
     * Copy every referenced record in data (non-bulk) segments. Bulk segments
     * are fully kept (they are only removed in cleanup, if there is no
     * reference to them).
     * @return {@code true} if compaction succeeded, {@code false} otherwise.
     */
    public boolean compact() throws IOException {
        gcListener.info("TarMK GC #{}: compaction started, gc options={}", GC_COUNT, gcOptions);
        Stopwatch watch = Stopwatch.createStarted();

        SegmentNodeState before = getHead();
        long existing = before.getChildNode(SegmentNodeStore.CHECKPOINTS)
                .getChildNodeCount(Long.MAX_VALUE);
        if (existing > 1) {
            // FIXME OAK-4371: Overly zealous warning about checkpoints on compaction
            // Make the number of checkpoints configurable above which the warning should be issued?
            gcListener.warn(
                    "TarMK GC #{}: compaction found {} checkpoints, you might need to run checkpoint cleanup",
                    GC_COUNT, existing);
        }

        final int newGeneration = getGcGeneration() + 1;
        SegmentBufferWriter bufferWriter = new SegmentBufferWriter(this, tracker, segmentReader, "c", newGeneration);
        Supplier<Boolean> cancel = newCancelCompactionCondition();
        SegmentNodeState after = compact(bufferWriter, before, cancel);
        if (after == null) {
            gcListener.info("TarMK GC #{}: compaction cancelled.", GC_COUNT);
            return false;
        }

        gcListener.info("TarMK GC #{}: compacted {} to {}",
                GC_COUNT, before.getRecordId(), after.getRecordId());

        try {
            int cycles = 0;
            boolean success = false;
            while (cycles < gcOptions.getRetryCount() &&
                    !(success = revisions.setHead(before.getRecordId(), after.getRecordId()))) {
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
                    gcListener.info("TarMK GC #{}: compaction cancelled.", GC_COUNT);
                    return false;
                }

                gcListener.info("TarMK GC #{}: compacted {} against {} to {}",
                        GC_COUNT, head.getRecordId(), before.getRecordId(), after.getRecordId());
                before = head;
            }

            if (!success) {
                gcListener.info("TarMK GC #{}: compaction gave up compacting concurrent commits after {} cycles.",
                        GC_COUNT, cycles);
                if (gcOptions.getForceAfterFail()) {
                    gcListener.info("TarMK GC #{}: compaction force compacting remaining commits", GC_COUNT);
                    cycles++;
                    success = forceCompact(bufferWriter, cancel);
                    if (!success) {
                        gcListener.warn("TarMK GC #{}: compaction failed to force compact remaining commits. " +
                            "Most likely compaction didn't get exclusive access to the store or was " +
                            "prematurely cancelled.", GC_COUNT);
                    }
                }
            }

            if (success) {
                gcListener.compacted(SUCCESS, newGeneration);
                gcListener.info("TarMK GC #{}: compaction succeeded in {} ({} ms), after {} cycles",
                        GC_COUNT, watch, watch.elapsed(MILLISECONDS), cycles);
                return true;
            } else {
                gcListener.info("TarMK GC #{}: cleaning up after failed compaction", GC_COUNT);

                Predicate<Integer> cleanupPredicate = new Predicate<Integer>() {
                    @Override
                    public boolean apply(Integer generation) {
                        return generation == newGeneration;
                    }
                };
                cleanup(cleanupPredicate,
                    "gc-count=" + GC_COUNT +
                    ",gc-status=failed" +
                    ",store-generation=" + (newGeneration - 1) +
                    ",reclaim-predicate=(generation==" + newGeneration + ")");

                gcListener.compacted(FAILURE, newGeneration);
                gcListener.info("TarMK GC #{}: compaction failed after {} ({} ms), and {} cycles",
                        GC_COUNT, watch, watch.elapsed(MILLISECONDS), cycles);
                return false;
            }
        } catch (InterruptedException e) {
            gcListener.error("TarMK GC #" + GC_COUNT + ": compaction interrupted", e);
            currentThread().interrupt();
            return false;
        } catch (Exception e) {
            gcListener.error("TarMK GC #" + GC_COUNT + ": compaction encountered an error", e);
            return false;
        }
    }

    private SegmentNodeState compact(SegmentBufferWriter bufferWriter, NodeState head,
                                     Supplier<Boolean> cancel)
    throws IOException {
        if (gcOptions.isOffline()) {
            SegmentWriter writer = new SegmentWriter(this, segmentReader, blobStore, new Default(), bufferWriter, binaryReferenceConsumer);
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
                        SegmentNodeState after = compact(bufferWriter,
                                segmentReader.readNode(base), cancel);
                        if (after == null) {
                            gcListener.info("TarMK GC #{}: compaction cancelled.", GC_COUNT);
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
            timeout(gcOptions.getLockWaitTime(), SECONDS));
    }

    public Iterable<SegmentId> getSegmentIds() {
        fileStoreLock.readLock().lock();
        try {
            List<SegmentId> ids = newArrayList();
            if (tarWriter != null) {
                for (UUID uuid : tarWriter.getUUIDs()) {
                    long msb = uuid.getMostSignificantBits();
                    long lsb = uuid.getLeastSignificantBits();
                    ids.add(newSegmentId(msb, lsb));
                }
            }
            for (TarReader reader : readers) {
                for (UUID uuid : reader.getUUIDs()) {
                    long msb = uuid.getMostSignificantBits();
                    long lsb = uuid.getLeastSignificantBits();
                    ids.add(newSegmentId(msb, lsb));
                }
            }
            return ids;
        } finally {
            fileStoreLock.readLock().unlock();
        }
    }

    @Nonnull
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Nonnull
    public SegmentWriter getWriter() {
        return segmentWriter;
    }

    @Nonnull
    public SegmentReader getReader() {
        return segmentReader;
    }

    @Nonnull
    public BinaryReferenceConsumer getBinaryReferenceConsumer() {
        return binaryReferenceConsumer;
    }

    @Nonnull
    public TarRevisions getRevisions() {
        return revisions;
    }

    /**
     * Convenience method for accessing the root node for the current head.
     * This is equivalent to
     * <pre>
     * fileStore.getReader().readHeadState(fileStore.getRevisions())
     * </pre>
     * @return the current head node state
     */
    @Nonnull
    public SegmentNodeState getHead() {
        return segmentReader.readHeadState(revisions);
    }

    @Override
    public void close() {
        // Flag the store as shutting / shut down
        shutdown = true;

        // avoid deadlocks by closing (and joining) the background
        // threads before acquiring the synchronization lock

        try {
            if (compactionOperation.stop(5, SECONDS)) {
                log.debug("The compaction background thread was successfully shut down");
            } else {
                log.warn("The compaction background thread takes too long to shutdown");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            if (flushOperation.stop(5, SECONDS)) {
                log.debug("The flush background thread was successfully shut down");
            } else {
                log.warn("The flush background thread takes too long to shutdown");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            if (diskSpaceOperation.stop(5, SECONDS)) {
                log.debug("The disk space check background thread was successfully shut down");
            } else {
                log.warn("The disk space check background thread takes too long to shutdown");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

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
        fileStoreLock.writeLock().lock();
        try {
            int generation = Segment.getGcGeneration(wrap(buffer, offset, length), id.asUUID());
            long size = tarWriter.writeEntry(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits(),
                    buffer, offset, length, generation);
            if (size >= maxFileSize) {
                newWriter();
            }
            approximateSize.addAndGet(TarWriter.BLOCK_SIZE + length + TarWriter.getPaddingSize(length));
        } finally {
            fileStoreLock.writeLock().unlock();
        }

        // Keep this data segment in memory as it's likely to be accessed soon
        if (id.isDataSegmentId()) {
            ByteBuffer data;
            if (offset > 4096) {
                data = ByteBuffer.allocate(length);
                data.put(buffer, offset, length);
                data.rewind();
            } else {
                data = ByteBuffer.wrap(buffer, offset, length);
            }
            segmentCache.putSegment(new Segment(this, segmentReader, id, data));
        }
    }

    @Override
    @Nonnull
    public SegmentId newSegmentId(long msb, long lsb) {
        return tracker.newSegmentId(msb, lsb, segmentIdFactory);
    }

    @Override
    @Nonnull
    public SegmentId newBulkSegmentId() {
        return tracker.newBulkSegmentId(segmentIdFactory);
    }

    @Override
    @Nonnull
    public SegmentId newDataSegmentId() {
        return tracker.newDataSegmentId(segmentIdFactory);
    }

    /**
     * Switch to a new tar writer.
     * This method may only be called when holding the write lock of {@link #fileStoreLock}
     * @throws IOException
     */
    private void newWriter() throws IOException {
        if (tarWriter.isDirty()) {
            tarWriter.close();

            List<TarReader> list =
                    newArrayListWithCapacity(1 + readers.size());
            list.add(TarReader.open(writeFile, memoryMapping));
            list.addAll(readers);
            readers = list;

            writeNumber++;
            writeFile = new File(
                    directory,
                    format(FILE_NAME_FORMAT, writeNumber, "a"));
            tarWriter = new TarWriter(writeFile, stats);
        }
    }

    /**
     * @return  the external BlobStore (if configured) with this store, {@code null} otherwise.
     */
    @CheckForNull
    public BlobStore getBlobStore() {
        return blobStore;
    }

    /**
     * Trigger a garbage collection cycle
     */
    public void gc() {
        compactionOperation.trigger();
    }

    public Map<String, Set<UUID>> getTarReaderIndex() {
        Map<String, Set<UUID>> index = new HashMap<String, Set<UUID>>();
        for (TarReader reader : readers) {
            index.put(reader.getFile().getAbsolutePath(), reader.getUUIDs());
        }
        return index;
    }

    public Map<UUID, List<UUID>> getTarGraph(String fileName) throws IOException {
        for (TarReader reader : readers) {
            if (fileName.equals(reader.getFile().getName())) {
                Map<UUID, List<UUID>> graph = newHashMap();
                for (UUID uuid : reader.getUUIDs()) {
                    graph.put(uuid, null);
                }
                Map<UUID, List<UUID>> g = reader.getGraph(false);
                if (g != null) {
                    graph.putAll(g);
                }
                return graph;
            }
        }
        return emptyMap();
    }

    private void checkDiskSpace() {
        long repositoryDiskSpace = approximateSize.get();
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

    /**
     * A read only {@link FileStore} implementation that supports
     * going back to old revisions.
     * <p>
     * All write methods are no-ops.
     */
    public static class ReadOnlyStore extends FileStore {
        private RecordId currentHead;

        ReadOnlyStore(FileStoreBuilder builder) throws IOException {
            super(builder, true);
        }

        @Override
        ReadOnlyStore bind(@Nonnull TarRevisions revisions) throws IOException {
            revisions.bind(this, new Supplier<RecordId>() {
                @Override
                public RecordId get() {
                    throw new IllegalStateException("Cannot start readonly store from empty journal");
                }
            });
            currentHead = revisions.getHead();
            return this;
        }

        /**
         * Go to the specified {@code revision}
         *
         * @param revision
         */
        public void setRevision(String revision) {
            RecordId newHead = RecordId.fromString(this, revision);
            if (super.revisions.setHead(currentHead, newHead)) {
                currentHead = newHead;
            }
        }

        /**
         * Include the ids of all segments transitively reachable through forward references from
         * {@code referencedIds}. See OAK-3864.
         */
        private static void includeForwardReferences(Iterable<TarReader> readers, Set<UUID> referencedIds)
            throws IOException {
            Set<UUID> fRefs = newHashSet(referencedIds);
            do {
                // Add direct forward references
                for (TarReader reader : readers) {
                    reader.calculateForwardReferences(fRefs);
                    if (fRefs.isEmpty()) {
                        break;  // Optimisation: bail out if no references left
                    }
                }
                // ... as long as new forward references are found.
            } while (referencedIds.addAll(fRefs));
        }

        /**
         * Build the graph of segments reachable from an initial set of segments
         * @param roots     the initial set of segments
         * @param visitor   visitor receiving call back while following the segment graph
         * @throws IOException
         */
        public void traverseSegmentGraph(
            @Nonnull Set<UUID> roots,
            @Nonnull SegmentGraphVisitor visitor) throws IOException {

            List<TarReader> readers = super.readers;
            includeForwardReferences(readers, roots);
            for (TarReader reader : readers) {
                reader.traverseSegmentGraph(checkNotNull(roots), checkNotNull(visitor));
            }
        }

        @Override
        public void writeSegment(SegmentId id, byte[] data,
                int offset, int length) {
            throw new UnsupportedOperationException("Read Only Store");
        }

        /**
         * no-op
         */
        @Override
        public void flush() { /* nop */ }

        @Override
        public LinkedList<File> cleanup() {
            throw new UnsupportedOperationException("Read Only Store");
        }

        @Override
        public void gc() {
            throw new UnsupportedOperationException("Read Only Store");
        }

        @Override
        public boolean compact() {
            throw new UnsupportedOperationException("Read Only Store");
        }

        @Override
        public void maybeCompact(boolean cleanup) {
            throw new UnsupportedOperationException("Read Only Store");
        }
    }

    private static void closeAndLogOnFail(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ioe) {
                // ignore and log
                log.error(ioe.getMessage(), ioe);
            }
        }
    }

}
