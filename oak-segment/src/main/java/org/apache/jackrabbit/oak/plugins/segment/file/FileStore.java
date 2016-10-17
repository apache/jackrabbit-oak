/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.CompactionMap.sum;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.NO_COMPACTION;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.segment.CompactionMap;
import org.apache.jackrabbit.oak.plugins.segment.Compactor;
import org.apache.jackrabbit.oak.plugins.segment.PersistedCompactionMap;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentGraph.SegmentGraphVisitor;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.plugins.segment.SegmentVersion;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The storage implementation for tar files.
 */
public class FileStore implements SegmentStore {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(FileStore.class);

    private static final int MB = 1024 * 1024;

    private static final Pattern FILE_NAME_PATTERN =
            Pattern.compile("(data|bulk)((0|[1-9][0-9]*)[0-9]{4})([a-z])?.tar");

    private static final String FILE_NAME_FORMAT = "data%05d%s.tar";

    private static final String JOURNAL_FILE_NAME = "journal.log";

    private static final String LOCK_FILE_NAME = "repo.lock";

    private static final String MANIFEST_FILE_NAME = "manifest";

    /**
     * GC counter for logging purposes
     */
    private static final AtomicLong gcCount = new AtomicLong(0);

    static final boolean MEMORY_MAPPING_DEFAULT =
            "64".equals(System.getProperty("sun.arch.data.model", "32"));

    private final SegmentTracker tracker;

    private final File directory;

    private final BlobStore blobStore;

    private final int maxFileSize;

    private final boolean memoryMapping;

    private volatile List<TarReader> readers;

    private int writeNumber;

    private File writeFile;

    private TarWriter writer;

    private final RandomAccessFile journalFile;

    private final RandomAccessFile lockFile;

    private final FileLock lock;

    /**
     * The latest head state.
     */
    private final AtomicReference<RecordId> head;

    /**
     * The persisted head of the root journal, used to determine whether the
     * latest {@link #head} value should be written to the disk.
     */
    private final AtomicReference<RecordId> persistedHead;

    /**
     * The background flush thread. Automatically flushes the TarMK state
     * once every five seconds.
     */
    private final BackgroundThread flushThread;

    /**
     * The background compaction thread. Compacts the TarMK contents whenever
     * triggered by the {@link #gc()} method.
     */
    private final BackgroundThread compactionThread;

    /**
     * This background thread periodically asks the {@code CompactionStrategy}
     * to compare the approximate size of the repository with the available disk
     * space. The result of this comparison is stored in the state of this
     * {@code FileStore}.
     */
    private final BackgroundThread diskSpaceThread;

    private CompactionStrategy compactionStrategy = NO_COMPACTION;

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
     * Version of the segment storage format.
     */
    private final SegmentVersion version;

    /**
     * {@code GCMonitor} monitoring this instance's gc progress
     */
    private final GCMonitor gcMonitor;

    /**
     * Represents the approximate size on disk of the repository.
     */
    private final AtomicLong approximateSize;

    /**
     * This flag is periodically updated by calling the {@code
     * CompactionStrategy} at regular intervals.
     */
    private final AtomicBoolean sufficientDiskSpace;

    /**
     * Flag signalling shutdown of the file store
     */
    private volatile boolean shutdown;

    private final ReadWriteLock fileStoreLock = new ReentrantReadWriteLock();

    private final FileStoreStats stats;

    /**
     * Create a new instance of a {@link Builder} for a file store.
     * @param directory  directory where the tar files are stored
     * @return a new {@link Builder} instance.
     */
    @Nonnull
    public static Builder builder(@Nonnull File directory) {
        return new Builder(checkNotNull(directory));
    }

    /**
     * Builder for creating {@link FileStore} instances.
     */
    public static class Builder {

        private final File directory;

        private BlobStore blobStore;   // null ->  store blobs inline

        private NodeState root = EMPTY_NODE;

        private int maxFileSize = 256;

        private int cacheSize;   // 0 -> DEFAULT_MEMORY_CACHE_SIZE

        private boolean memoryMapping;

        private final LoggingGCMonitor gcMonitor = new LoggingGCMonitor();

        private StatisticsProvider statsProvider = StatisticsProvider.NOOP;

        private SegmentVersion version = SegmentVersion.LATEST_VERSION;

        private Builder(File directory) {
            this.directory = directory;
        }

        /**
         * Specify the {@link BlobStore}.
         * @param blobStore
         * @return this instance
         */
        @Nonnull
        public Builder withBlobStore(@Nonnull BlobStore blobStore) {
            this.blobStore = checkNotNull(blobStore);
            return this;
        }

        /**
         * Specify the initial root node state for the file store
         * @param root
         * @return this instance
         */
        @Nonnull
        public Builder withRoot(@Nonnull NodeState root) {
            this.root = checkNotNull(root);
            return this;
        }

        /**
         * Maximal size of the generated tar files in MB.
         * @param maxFileSize
         * @return this instance
         */
        @Nonnull
        public Builder withMaxFileSize(int maxFileSize) {
            this.maxFileSize = maxFileSize;
            return this;
        }

        /**
         * Size of the cache in MB.
         * @param cacheSize
         * @return this instance
         */
        @Nonnull
        public Builder withCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        /**
         * Turn caching off
         * @return this instance
         */
        @Nonnull
        public Builder withNoCache() {
            this.cacheSize = -1;
            return this;
        }

        /**
         * Turn memory mapping on or off
         * @param memoryMapping
         * @return this instance
         */
        @Nonnull
        public Builder withMemoryMapping(boolean memoryMapping) {
            this.memoryMapping = memoryMapping;
            return this;
        }

        /**
         * Set memory mapping to the default value based on OS properties
         * @return this instance
         */
        @Nonnull
        public Builder withDefaultMemoryMapping() {
            this.memoryMapping = MEMORY_MAPPING_DEFAULT;
            return this;
        }

        /**
         * {@link GCMonitor} for monitoring this files store's gc process.
         * @param gcMonitor
         * @return this instance
         */
        @Nonnull
        public Builder withGCMonitor(@Nonnull GCMonitor gcMonitor) {
            this.gcMonitor.delegatee = checkNotNull(gcMonitor);
            return this;
        }

        /**
         * {@link StatisticsProvider} for collecting statistics related to FileStore
         * @param statisticsProvider
         * @return this instance
         */
        @Nonnull
        public Builder withStatisticsProvider(@Nonnull StatisticsProvider statisticsProvider) {
            this.statsProvider = checkNotNull(statisticsProvider);
            return this;
        }

        public Builder withSegmentVersion(SegmentVersion version) {
            this.version = checkNotNull(version);
            return this;
        }

        /**
         * Create a new {@link FileStore} instance with the settings specified in this
         * builder. If none of the {@code with} methods have been called before calling
         * this method, a file store with the following default settings is returned:
         * <ul>
         * <li>blob store: inline</li>
         * <li>root: empty node</li>
         * <li>max file size: 256MB</li>
         * <li>cache size: 256MB</li>
         * <li>memory mapping: on for 64 bit JVMs off otherwise</li>
         * <li>whiteboard: none. No {@link GCMonitor} tracking</li>
         * <li>statsProvider: StatisticsProvider.NOOP</li>
         * </ul>
         *
         * @return a new file store instance
         * @throws IOException
         */
        @Nonnull
        public FileStore build() throws IOException, InvalidFileStoreVersionException {
            return new FileStore(this, false);
        }

        public ReadOnlyStore buildReadOnly() throws IOException, InvalidFileStoreVersionException {
            return new ReadOnlyStore(this);
        }

    }

    private FileStore(Builder builder, boolean readOnly) throws IOException, InvalidFileStoreVersionException {
        this.version = builder.version;

        if (readOnly) {
            checkNotNull(builder.directory);
            checkState(builder.directory.exists() && builder.directory.isDirectory());
        } else {
            checkNotNull(builder.directory).mkdirs();
        }

        if (builder.cacheSize < 0) {
            this.tracker = new SegmentTracker(this, 0, version);
        } else if (builder.cacheSize > 0) {
            this.tracker = new SegmentTracker(this, builder.cacheSize, version);
        } else {
            this.tracker = new SegmentTracker(this, version);
        }
        this.blobStore = builder.blobStore;
        this.directory = builder.directory;
        this.maxFileSize = builder.maxFileSize * MB;
        this.memoryMapping = builder.memoryMapping;
        this.gcMonitor = builder.gcMonitor;

        Map<Integer, Map<Character, File>> map = collectFiles(directory);

        File manifest = new File(directory, MANIFEST_FILE_NAME);

        if (map.size() > 0) {
            if (manifest.exists()) {
                throw new InvalidFileStoreVersionException();
            } else {
                log.debug("The store folder is non empty and does not have manifest file");
            }
        }

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
        this.stats = new FileStoreStats(builder.statsProvider, this, initialSize);

        if (!readOnly) {
            if (indices.length > 0) {
                this.writeNumber = indices[indices.length - 1] + 1;
            } else {
                this.writeNumber = 0;
            }
            this.writeFile = new File(directory, String.format(
                    FILE_NAME_FORMAT, writeNumber, "a"));
            this.writer = new TarWriter(writeFile, stats);
        }

        if (readOnly) {
            journalFile = new RandomAccessFile(new File(directory,
                    JOURNAL_FILE_NAME), "r");
        } else {
            journalFile = new RandomAccessFile(new File(directory,
                    JOURNAL_FILE_NAME), "rw");
        }

        RecordId id = null;
        JournalReader journalReader = new JournalReader(new File(directory, JOURNAL_FILE_NAME));
        try {
            Iterator<String> heads = journalReader.iterator();
            while (id == null && heads.hasNext()) {
                String head = heads.next();
                try {
                    RecordId last = RecordId.fromString(tracker, head);
                    SegmentId segmentId = last.getSegmentId();
                    if (containsSegment(
                            segmentId.getMostSignificantBits(),
                            segmentId.getLeastSignificantBits())) {
                        id = last;
                    } else {
                        log.warn("Unable to access revision {}, rewinding...", last);
                    }
                } catch (IllegalArgumentException e) {
                    log.warn("Skipping invalid record id {}", head);
                }
            }
        } finally {
            journalReader.close();
        }

        journalFile.seek(journalFile.length());

        if (!readOnly) {
            lockFile = new RandomAccessFile(
                    new File(directory, LOCK_FILE_NAME), "rw");
            lock = lockFile.getChannel().lock();
        } else {
            lockFile = null;
            lock = null;
        }

        if (id != null) {
            head = new AtomicReference<RecordId>(id);
            persistedHead = new AtomicReference<RecordId>(id);
        } else {
            NodeBuilder nodeBuilder = EMPTY_NODE.builder();
            nodeBuilder.setChildNode("root", builder.root);
            head = new AtomicReference<RecordId>(tracker.getWriter().writeNode(
                    nodeBuilder.getNodeState()).getRecordId());
            persistedHead = new AtomicReference<RecordId>(null);
        }

        if (!readOnly) {
            flushThread = BackgroundThread.run(
                    "TarMK flush thread [" + directory + "]", 5000, // 5s interval
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                flush();
                            } catch (IOException e) {
                                log.warn("Failed to flush the TarMK at" +
                                        directory, e);
                            }
                        }
                    });
            compactionThread = BackgroundThread.run(
                    "TarMK compaction thread [" + directory + "]", -1,
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                maybeCompact(true);
                            } catch (IOException e) {
                                log.error("Error running compaction", e);
                            }
                        }
                    });

            diskSpaceThread = BackgroundThread.run(
                    "TarMK disk space check [" + directory + "]", MINUTES.toMillis(1), new Runnable() {

                @Override
                public void run() {
                    checkDiskSpace();
                }

            });
        } else {
            flushThread = null;
            compactionThread = null;
            diskSpaceThread = null;
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

    public boolean maybeCompact(boolean cleanup) throws IOException {
        gcMonitor.info("TarMK GC #{}: started", gcCount.incrementAndGet());

        Runtime runtime = Runtime.getRuntime();
        long avail = runtime.totalMemory() - runtime.freeMemory();
        long[] weights = tracker.getCompactionMap().getEstimatedWeights();
        long delta = weights.length > 0
            ? weights[0]
            : 0;
        long needed = delta * compactionStrategy.getMemoryThreshold();
        if (needed >= avail) {
            gcMonitor.skipped(
                    "TarMK GC #{}: not enough available memory {} ({} bytes), needed {} ({} bytes)," +
                    " last merge delta {} ({} bytes), so skipping compaction for now",
                    gcCount,
                    humanReadableByteCount(avail), avail,
                    humanReadableByteCount(needed), needed,
                    humanReadableByteCount(delta), delta);
            if (cleanup) {
                cleanupNeeded.set(!compactionStrategy.isPaused());
            }
            return false;
        }

        Stopwatch watch = Stopwatch.createStarted();
        compactionStrategy.setCompactionStart(System.currentTimeMillis());
        boolean compacted = false;

        long offset = compactionStrategy.getPersistCompactionMap()
            ? sum(tracker.getCompactionMap().getRecordCounts()) * PersistedCompactionMap.BYTES_PER_ENTRY
            : 0;

        byte gainThreshold = compactionStrategy.getGainThreshold();
        boolean runCompaction = true;
        if (gainThreshold <= 0) {
            gcMonitor.info("TarMK GC #{}: estimation skipped because gain threshold value ({} <= 0)", gcCount,
                gainThreshold);
        } else if (compactionStrategy.isPaused()) {
            gcMonitor.info("TarMK GC #{}: estimation skipped because compaction is paused", gcCount);
        } else {
            gcMonitor.info("TarMK GC #{}: estimation started", gcCount);
            Supplier<Boolean> shutdown = newShutdownSignal();
            CompactionGainEstimate estimate = estimateCompactionGain(shutdown);
            if (shutdown.get()) {
                gcMonitor.info("TarMK GC #{}: estimation interrupted. Skipping compaction.", gcCount);
                return false;
            }

            long gain = estimate.estimateCompactionGain(offset);
            runCompaction = gain >= gainThreshold;
            if (runCompaction) {
                gcMonitor.info(
                    "TarMK GC #{}: estimation completed in {} ({} ms). " +
                    "Gain is {}% or {}/{} ({}/{} bytes), so running compaction",
                        gcCount, watch, watch.elapsed(MILLISECONDS), gain,
                        humanReadableByteCount(estimate.getReachableSize()), humanReadableByteCount(estimate.getTotalSize()),
                        estimate.getReachableSize(), estimate.getTotalSize());
            } else {
                if (estimate.getTotalSize() == 0) {
                    gcMonitor.skipped(
                            "TarMK GC #{}: estimation completed in {} ({} ms). " +
                            "Skipping compaction for now as repository consists of a single tar file only",
                            gcCount, watch, watch.elapsed(MILLISECONDS));
                } else {
                    gcMonitor.skipped(
                        "TarMK GC #{}: estimation completed in {} ({} ms). " +
                        "Gain is {}% or {}/{} ({}/{} bytes), so skipping compaction for now",
                            gcCount, watch, watch.elapsed(MILLISECONDS), gain,
                            humanReadableByteCount(estimate.getReachableSize()), humanReadableByteCount(estimate.getTotalSize()),
                            estimate.getReachableSize(), estimate.getTotalSize());
                }
            }
        }

        if (runCompaction) {
            if (!compactionStrategy.isPaused()) {
                compact();
                compacted = true;
            } else {
                gcMonitor.skipped("TarMK GC #{}: compaction paused", gcCount);
            }
        }
        if (cleanup) {
            cleanupNeeded.set(!compactionStrategy.isPaused());
        }
        return compacted;
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

    public long size() {
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
            if (writer != null) {
                count += writer.count();
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
        flush(cleanupNeeded.getAndSet(false));
    }

    public void flush(boolean cleanup) throws IOException {
        synchronized (persistedHead) {
            RecordId before = persistedHead.get();
            RecordId after = head.get();

            if (cleanup || !after.equals(before)) {
                // needs to happen outside the synchronization block below to
                // avoid a deadlock with another thread flushing the writer
                tracker.getWriter().flush();

                // needs to happen outside the synchronization block below to
                // prevent the flush from stopping concurrent reads and writes
                writer.flush();

                fileStoreLock.writeLock().lock();
                try {
                    log.debug("TarMK journal update {} -> {}", before, after);
                    journalFile.writeBytes(after.toString10() + " root " + System.currentTimeMillis()+"\n");
                    journalFile.getChannel().force(false);
                    persistedHead.set(after);
                } finally {
                    fileStoreLock.writeLock().unlock();
                }

                // Needs to happen outside the synchronization block above to
                // prevent the flush from stopping concurrent reads and writes
                // by the persisted compaction map. See OAK-3264
                if (cleanup) {
                    // Explicitly give up reference to the previous root state
                    // otherwise they would block cleanup. See OAK-3347
                    before = null;
                    after = null;
                    pendingRemove.addAll(cleanup());
                }
            }

            // remove all obsolete tar generations
            Iterator<File> iterator = pendingRemove.iterator();
            while (iterator.hasNext()) {
                File file = iterator.next();
                log.debug("TarMK GC: Attempting to remove old file {}",
                        file);
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
     * Runs garbage collection on the segment level, which could write new
     * generations of tar files. It checks which segments are still reachable,
     * and throws away those that are not.
     * <p>
     * A new generation of a tar file is created (and segments are only
     * discarded) if doing so releases more than 25% of the space in a tar file.
     */
    public List<File> cleanup() throws IOException {
        Stopwatch watch = Stopwatch.createStarted();
        long initialSize = size();
        Set<UUID> referencedIds = newHashSet();
        Map<TarReader, TarReader> cleaned = newLinkedHashMap();

        fileStoreLock.writeLock().lock();
        try {
            gcMonitor.info("TarMK GC #{}: cleanup started. Current repository size is {} ({} bytes)",
                    gcCount, humanReadableByteCount(initialSize), initialSize);

            newWriter();
            tracker.clearCache();

            // Suggest to the JVM that now would be a good time
            // to clear stale weak references in the SegmentTracker
            System.gc();

            for (SegmentId id : tracker.getReferencedSegmentIds()) {
                referencedIds.add(id.asUUID());
            }
            writer.collectReferences(referencedIds);
            for (TarReader reader : readers) {
                cleaned.put(reader, reader);
            }
        } finally {
            fileStoreLock.writeLock().unlock();
        }

        // Do actual cleanup outside of the lock to prevent blocking
        // concurrent writers for a long time
        includeForwardReferences(cleaned.keySet(), referencedIds);
        LinkedList<File> toRemove = newLinkedList();
        Set<UUID> cleanedIds = newHashSet();
        for (TarReader reader : cleaned.keySet()) {
            cleaned.put(reader, reader.cleanup(referencedIds, cleanedIds));
            if (shutdown) {
                gcMonitor.info("TarMK GC #{}: cleanup interrupted", gcCount);
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

        // Close old readers *after* setting readers to the new readers to avoid accessing
        // a closed reader from readSegment()
        for (TarReader oldReader : oldReaders) {
            closeAndLogOnFail(oldReader);
            File file = oldReader.getFile();
            gcMonitor.info("TarMK GC #{}: cleanup marking file for deletion: {}", gcCount, file.getName());
            toRemove.addLast(file);
        }

        CompactionMap cm = tracker.getCompactionMap();
        cm.remove(cleanedIds);
        long finalSize = size();
        approximateSize.set(finalSize);
        stats.reclaimed(initialSize - finalSize);
        gcMonitor.cleaned(initialSize - finalSize, finalSize);
        gcMonitor.info("TarMK GC #{}: cleanup completed in {} ({} ms). Post cleanup size is {} ({} bytes)" +
                " and space reclaimed {} ({} bytes). Compaction map weight/depth is {}/{} ({} bytes/{}).",
                gcCount, watch, watch.elapsed(MILLISECONDS),
                humanReadableByteCount(finalSize), finalSize,
                humanReadableByteCount(initialSize - finalSize), initialSize - finalSize,
                humanReadableByteCount(sum(cm.getEstimatedWeights())), cm.getDepth(),
                sum(cm.getEstimatedWeights()), cm.getDepth());
        return toRemove;
    }

    /**
     * Include the ids of all segments transitively reachable through forward references from
     * {@code referencedIds}. See OAK-3864.
     */
    private void includeForwardReferences(Iterable<TarReader> readers, Set<UUID> referencedIds)
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
            if (!fRefs.isEmpty()) {
                gcMonitor.info("TarMK GC #{}: cleanup found {} forward references", gcCount, fRefs.size());
                log.debug("TarMK GC #{}: cleanup found forward references to {}", gcCount, fRefs);
            }
            // ... as long as new forward references are found.
        } while (referencedIds.addAll(fRefs));
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
     */
    public void compact() throws IOException {
        checkState(!compactionStrategy.equals(NO_COMPACTION),
                "You must set a compactionStrategy before calling compact");
        gcMonitor.info("TarMK GC #{}: compaction started, strategy={}", gcCount, compactionStrategy);
        Stopwatch watch = Stopwatch.createStarted();
        Supplier<Boolean> compactionCanceled = newCancelCompactionCondition();
        Compactor compactor = new Compactor(tracker, compactionStrategy, compactionCanceled);
        SegmentNodeState before = getHead();
        long existing = before.getChildNode(SegmentNodeStore.CHECKPOINTS)
                .getChildNodeCount(Long.MAX_VALUE);
        if (existing > 1) {
            gcMonitor.warn(
                    "TarMK GC #{}: compaction found {} checkpoints, you might need to run checkpoint cleanup",
                    gcCount, existing);
        }

        SegmentNodeState after = compactor.compact(EMPTY_NODE, before, EMPTY_NODE);
        gcMonitor.info("TarMK GC #{}: compacted {} to {}",
            gcCount, before.getRecordId(), after.getRecordId());

        if (compactionCanceled.get()) {
            gcMonitor.warn("TarMK GC #{}: compaction canceled: {}", gcCount, compactionCanceled);
            return;
        }

        Callable<Boolean> setHead = new SetHead(before, after, compactor);
        try {
            int cycles = 0;
            boolean success = false;
            while(cycles++ < compactionStrategy.getRetryCount()
                    && !(success = compactionStrategy.compacted(setHead))) {
                // Some other concurrent changes have been made.
                // Rebase (and compact) those changes on top of the
                // compacted state before retrying to set the head.
                gcMonitor.info("TarMK GC #{}: compaction detected concurrent commits while compacting. " +
                        "Compacting these commits. Cycle {}", gcCount, cycles);
                SegmentNodeState head = getHead();
                after = compactor.compact(before, head, after);
                gcMonitor.info("TarMK GC #{}: compacted {} against {} to {}",
                    gcCount, head.getRecordId(), before.getRecordId(), after.getRecordId());

                if (compactionCanceled.get()) {
                    gcMonitor.warn("TarMK GC #{}: compaction canceled: {}", gcCount, compactionCanceled);
                    return;
                }

                before = head;
                setHead = new SetHead(head, after, compactor);
            }
            if (!success) {
                gcMonitor.info("TarMK GC #{}: compaction gave up compacting concurrent commits after {} cycles.",
                        gcCount, cycles - 1);
                if (compactionStrategy.getForceAfterFail()) {
                    gcMonitor.info("TarMK GC #{}: compaction force compacting remaining commits", gcCount);
                    if (!forceCompact(before, after, compactor)) {
                        gcMonitor.warn("TarMK GC #{}: compaction failed to force compact remaining commits. " +
                                "Most likely compaction didn't get exclusive access to the store.", gcCount);
                    }
                }
            }

            gcMonitor.info("TarMK GC #{}: compaction completed in {} ({} ms), after {} cycles",
                    gcCount, watch, watch.elapsed(MILLISECONDS), cycles - 1);
        } catch (Exception e) {
            gcMonitor.error("TarMK GC #" + gcCount + ": compaction encountered an error", e);
        }
    }

    private boolean forceCompact(final NodeState before, final SegmentNodeState onto, final Compactor compactor) throws Exception {
        return compactionStrategy.compacted(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return new SetHead(getHead(), compactor.compact(before, getHead(), onto), compactor).call();
            }
        });
    }

    public Iterable<SegmentId> getSegmentIds() {
        fileStoreLock.readLock().lock();
        try {
            List<SegmentId> ids = newArrayList();
            if (writer != null) {
                for (UUID uuid : writer.getUUIDs()) {
                    ids.add(tracker.getSegmentId(
                            uuid.getMostSignificantBits(),
                            uuid.getLeastSignificantBits()));
                }
            }
            for (TarReader reader : readers) {
                for (UUID uuid : reader.getUUIDs()) {
                    ids.add(tracker.getSegmentId(
                            uuid.getMostSignificantBits(),
                            uuid.getLeastSignificantBits()));
                }
            }
            return ids;
        } finally {
            fileStoreLock.readLock().unlock();
        }
    }

    @Override
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Override
    public SegmentNodeState getHead() {
        return new SegmentNodeState(head.get());
    }

    @Override
    public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        RecordId id = this.head.get();
        return id.equals(base.getRecordId())
                && this.head.compareAndSet(id, head.getRecordId());
    }

    @Override
    public void close() {
        // Flag the store as shutting / shut down
        shutdown = true;

        // avoid deadlocks by closing (and joining) the background
        // threads before acquiring the synchronization lock
        closeAndLogOnFail(compactionThread);
        closeAndLogOnFail(flushThread);
        closeAndLogOnFail(diskSpaceThread);
        try {
            flush();
            tracker.getWriter().dropCache();
            fileStoreLock.writeLock().lock();
            try {
                closeAndLogOnFail(writer);

                List<TarReader> list = readers;
                readers = newArrayList();
                for (TarReader reader : list) {
                    closeAndLogOnFail(reader);
                }

                if (lock != null) {
                    lock.release();
                }
                closeAndLogOnFail(lockFile);
                closeAndLogOnFail(journalFile);
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
        if (id.getTracker() == tracker) {
            return true;
        }

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

        if (writer != null) {
            fileStoreLock.readLock().lock();
            try {
                if (writer.containsEntry(msb, lsb)) {
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
    public Segment readSegment(SegmentId id) {
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
                    return new Segment(tracker, id, buffer);
                }
            } catch (IOException e) {
                log.warn("Failed to read from tar file " + reader, e);
            }
        }

        if (writer != null) {
            fileStoreLock.readLock().lock();
            try {
                try {
                    ByteBuffer buffer = writer.readEntry(msb, lsb);
                    if (buffer != null) {
                        return new Segment(tracker, id, buffer);
                    }
                } catch (IOException e) {
                    log.warn("Failed to read from tar file " + writer, e);
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
                    return new Segment(tracker, id, buffer);
                }
            } catch (IOException e) {
                log.warn("Failed to read from tar file " + reader, e);
            }
        }

        throw new SegmentNotFoundException(id);
    }

    @Override
    public void writeSegment(SegmentId id, byte[] data, int offset, int length) throws IOException {
        fileStoreLock.writeLock().lock();
        try {
            long size = writer.writeEntry(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits(),
                    data, offset, length);
            if (size >= maxFileSize) {
                newWriter();
            }
            approximateSize.addAndGet(TarWriter.BLOCK_SIZE + length + TarWriter.getPaddingSize(length));
        } finally {
            fileStoreLock.writeLock().unlock();
        }
    }

    private void newWriter() throws IOException {
        if (writer.isDirty()) {
            writer.close();

            List<TarReader> list =
                    newArrayListWithCapacity(1 + readers.size());
            list.add(TarReader.open(writeFile, memoryMapping));
            list.addAll(readers);
            readers = list;

            writeNumber++;
            writeFile = new File(
                    directory,
                    String.format(FILE_NAME_FORMAT, writeNumber, "a"));
            writer = new TarWriter(writeFile, stats);
        }
    }

    @Override
    public Blob readBlob(String blobId) {
        if (blobStore != null) {
            return new BlobStoreBlob(blobStore, blobId);
        }
        throw new IllegalStateException("Attempt to read external blob with blobId [" + blobId + "] " +
                "without specifying BlobStore");
    }

    @Override
    public BlobStore getBlobStore() {
        return blobStore;
    }

    @Override
    public void gc() {
        if (compactionStrategy == NO_COMPACTION) {
            log.warn("Call to gc while compaction strategy set to {}. ", NO_COMPACTION);
        }
        compactionThread.trigger();
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
                Map<UUID, List<UUID>> g = reader.getGraph();
                if (g != null) {
                    graph.putAll(g);
                }
                return graph;
            }
        }
        return emptyMap();
    }

    public FileStore setCompactionStrategy(CompactionStrategy strategy) {
        this.compactionStrategy = strategy;
        log.info("Compaction strategy set to: {}", strategy);
        return this;
    }

    private void setRevision(String rootRevision) {
        fileStoreLock.writeLock().lock();
        try {
            RecordId id = RecordId.fromString(tracker, rootRevision);
            head.set(id);
            persistedHead.set(id);
        } finally {
            fileStoreLock.writeLock().unlock();
        }
    }

    private void checkDiskSpace() {
        long repositoryDiskSpace = approximateSize.get();
        long availableDiskSpace = directory.getFreeSpace();
        boolean updated = compactionStrategy.isDiskSpaceSufficient(repositoryDiskSpace, availableDiskSpace);
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

        private ReadOnlyStore(Builder builder) throws IOException, InvalidFileStoreVersionException {
            super(builder, true);
        }

        /**
         * Go to the specified {@code revision}
         *
         * @param revision
         */
        public void setRevision(String revision) {
            super.setRevision(revision);
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
            super.includeForwardReferences(readers, roots);
            for (TarReader reader : readers) {
                reader.traverseSegmentGraph(checkNotNull(roots), checkNotNull(visitor));
            }
        }

        @Override
        public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
            throw new UnsupportedOperationException("Read Only Store");
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
        public void compact() {
            throw new UnsupportedOperationException("Read Only Store");
        }

        @Override
        public boolean maybeCompact(boolean cleanup) {
            throw new UnsupportedOperationException("Read Only Store");
        }
    }

    private class SetHead implements Callable<Boolean> {
        private final SegmentNodeState before;
        private final SegmentNodeState after;
        private final Compactor compactor;

        public SetHead(SegmentNodeState before, SegmentNodeState after, Compactor compactor) {
            this.before = before;
            this.after = after;
            this.compactor = compactor;
        }

        @Override
        public Boolean call() throws Exception {
            // When used in conjunction with the SegmentNodeStore, this method
            // needs to be called inside the commitSemaphore as doing otherwise
            // might result in mixed segments. See OAK-2192.
            if (setHead(before, after)) {
                tracker.setCompactionMap(compactor.getCompactionMap());

                // Drop the SegmentWriter caches and flush any existing state
                // in an attempt to prevent new references to old pre-compacted
                // content. TODO: There should be a cleaner way to do this. (implement GCMonitor!?)
                tracker.getWriter().dropCache();
                tracker.getWriter().flush();

                gcMonitor.compacted();
                tracker.clearSegmentIdTables(compactionStrategy);
                return true;
            } else {
                return false;
            }
        }
    }

    public SegmentVersion getVersion() {
        return version;
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

    private static class LoggingGCMonitor implements GCMonitor {
        public GCMonitor delegatee = GCMonitor.EMPTY;

        @Override
        public void info(String message, Object... arguments) {
            log.info(message, arguments);
            delegatee.info(message, arguments);
        }

        @Override
        public void warn(String message, Object... arguments) {
            log.warn(message, arguments);
            delegatee.warn(message, arguments);
        }

        @Override
        public void error(String message, Exception exception) {
            delegatee.error(message, exception);
        }

        @Override
        public void skipped(String reason, Object... arguments) {
            log.info(reason, arguments);
            delegatee.skipped(reason, arguments);
        }

        @Override
        public void compacted() {
            delegatee.compacted();
        }

        @Override
        public void cleaned(long reclaimedSize, long currentSize) {
            delegatee.cleaned(reclaimedSize, currentSize);
        }
    }
}
