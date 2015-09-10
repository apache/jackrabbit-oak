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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.segment.CompactionMap;
import org.apache.jackrabbit.oak.plugins.segment.Compactor;
import org.apache.jackrabbit.oak.plugins.segment.PersistedCompactionMap;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.plugins.segment.SegmentVersion;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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
    private final LinkedList<File> toBeRemoved = newLinkedList();

    /**
     * Version of the segment storage format.
     */
    private final SegmentVersion version = SegmentVersion.V_11;

    /**
     * {@code GCMonitor} monitoring this instance's gc progress
     */
    private final GCMonitor gcMonitor;

    /**
     * Create a new instance of a {@link Builder} for a file store.
     * @param directory  directory where the tar files are stored
     * @return a new {@link Builder} instance.
     */
    @Nonnull
    public static Builder newFileStore(@Nonnull File directory) {
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
         * </ul>
         *
         * @return a new file store instance
         * @throws IOException
         */
        @Nonnull
        public FileStore create() throws IOException {
            return new FileStore(
                    blobStore, directory, root, maxFileSize, cacheSize, memoryMapping, gcMonitor, false);
        }
    }

    @Deprecated
    public FileStore(BlobStore blobStore, File directory, int maxFileSizeMB, boolean memoryMapping)
            throws IOException {
        this(blobStore, directory, EMPTY_NODE, maxFileSizeMB, 0, memoryMapping, GCMonitor.EMPTY, false);
    }

    @Deprecated
    public FileStore(File directory, int maxFileSizeMB, boolean memoryMapping)
            throws IOException {
        this(null, directory, maxFileSizeMB, memoryMapping);
    }

    @Deprecated
    public FileStore(File directory, int maxFileSizeMB)
            throws IOException {
        this(null, directory, maxFileSizeMB, MEMORY_MAPPING_DEFAULT);
    }

    @Deprecated
    public FileStore(File directory, int maxFileSizeMB, int cacheSizeMB,
            boolean memoryMapping) throws IOException {
        this(null, directory, EMPTY_NODE, maxFileSizeMB, cacheSizeMB, memoryMapping, GCMonitor.EMPTY, false);
    }

    @Deprecated
    FileStore(File directory, NodeState initial, int maxFileSize) throws IOException {
        this(null, directory, initial, maxFileSize, -1, MEMORY_MAPPING_DEFAULT, GCMonitor.EMPTY, false);
    }

    @Deprecated
    public FileStore(
            BlobStore blobStore, final File directory, NodeState initial, int maxFileSizeMB,
            int cacheSizeMB, boolean memoryMapping) throws IOException {
        this(blobStore, directory, initial, maxFileSizeMB, cacheSizeMB, memoryMapping, GCMonitor.EMPTY, false);
    }

    private FileStore(
            BlobStore blobStore, final File directory, NodeState initial, int maxFileSizeMB,
            int cacheSizeMB, boolean memoryMapping, GCMonitor gcMonitor, boolean readonly)
            throws IOException {

        if (readonly) {
            checkNotNull(directory);
            checkState(directory.exists() && directory.isDirectory());
        } else {
            checkNotNull(directory).mkdirs();
        }

        if (cacheSizeMB < 0) {
            this.tracker = new SegmentTracker(this, 0, getVersion());
        } else if (cacheSizeMB > 0) {
            this.tracker = new SegmentTracker(this, cacheSizeMB, getVersion());
        } else {
            this.tracker = new SegmentTracker(this, getVersion());
        }
        this.blobStore = blobStore;
        this.directory = directory;
        this.maxFileSize = maxFileSizeMB * MB;
        this.memoryMapping = memoryMapping;
        this.gcMonitor = gcMonitor;

        if (readonly) {
            journalFile = new RandomAccessFile(new File(directory,
                    JOURNAL_FILE_NAME), "r");
        } else {
            journalFile = new RandomAccessFile(new File(directory,
                    JOURNAL_FILE_NAME), "rw");
        }

        Map<Integer, Map<Character, File>> map = collectFiles(directory);
        this.readers = newArrayListWithCapacity(map.size());
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);
        for (int i = indices.length - 1; i >= 0; i--) {
            if (!readonly) {
                readers.add(TarReader.open(map.get(indices[i]), memoryMapping));
            } else {
                // only try to read-only recover the latest file as that might
                // be the *only* one still being accessed by a writer
                boolean recover = i == indices.length - 1;
                readers.add(TarReader.openRO(map.get(indices[i]),
                        memoryMapping, recover));
            }
        }

        if (!readonly) {
            if (indices.length > 0) {
                this.writeNumber = indices[indices.length - 1] + 1;
            } else {
                this.writeNumber = 0;
            }
            this.writeFile = new File(directory, String.format(
                    FILE_NAME_FORMAT, writeNumber, "a"));
            this.writer = new TarWriter(writeFile);
        }

        RecordId id = null;
        JournalReader journalReader = new JournalReader(new File(directory, JOURNAL_FILE_NAME));
        try {
            Iterator<String> heads = journalReader.iterator();
            while (id == null && heads.hasNext()) {
                RecordId last = RecordId.fromString(tracker, heads.next());
                SegmentId segmentId = last.getSegmentId();
                if (containsSegment(
                        segmentId.getMostSignificantBits(),
                        segmentId.getLeastSignificantBits())) {
                    id = last;
                } else {
                    log.warn("Unable to access revision {}, rewinding...", last);
                }
            }
        } finally {
            journalReader.close();
        }

        journalFile.seek(journalFile.length());

        if (!readonly) {
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
            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setChildNode("root", initial);
            head = new AtomicReference<RecordId>(tracker.getWriter().writeNode(
                    builder.getNodeState()).getRecordId());
            persistedHead = new AtomicReference<RecordId>(null);
        }

        if (!readonly) {
            this.flushThread = new BackgroundThread(
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
            this.compactionThread = new BackgroundThread(
                    "TarMK compaction thread [" + directory + "]", -1,
                    new Runnable() {
                        @Override
                        public void run() {
                            maybeCompact(true);
                        }
                    });
        } else {
            this.flushThread = null;
            this.compactionThread = null;
        }

        if (readonly) {
            log.info("TarMK ReadOnly opened: {} (mmap={})", directory,
                    memoryMapping);
        } else {
            log.info("TarMK opened: {} (mmap={})", directory, memoryMapping);
        }
    }

    public boolean maybeCompact(boolean cleanup) {
        log.info("TarMK compaction started");

        Runtime runtime = Runtime.getRuntime();
        long avail = runtime.totalMemory() - runtime.freeMemory();
        long[] weights = tracker.getCompactionMap().getEstimatedWeights();
        long delta = weights.length > 0
            ? weights[0]
            : 0;
        long needed = delta * compactionStrategy.getMemoryThreshold();
        if (needed >= avail) {
            gcMonitor.skipped(
                    "Not enough available memory {}, needed {}, last merge delta {}, so skipping compaction for now",
                    humanReadableByteCount(avail),
                    humanReadableByteCount(needed),
                    humanReadableByteCount(delta));
            if (cleanup) {
                cleanupNeeded.set(true);
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
        if (gainThreshold > 0) {
            CompactionGainEstimate estimate = estimateCompactionGain();
            long gain = estimate.estimateCompactionGain(offset);
            runCompaction = gain >= gainThreshold;
            if (runCompaction) {
                gcMonitor.info(
                    "Estimated compaction in {}, gain is {}% ({}/{}) or ({}/{}), so running compaction",
                    watch, gain, estimate.getReachableSize(), estimate.getTotalSize(),
                    humanReadableByteCount(estimate.getReachableSize()), humanReadableByteCount(estimate.getTotalSize()));
            } else {
                if (estimate.getTotalSize() == 0) {
                    gcMonitor.skipped(
                        "Estimated compaction in {}. Skipping compaction for now as repository consists " +
                        "of a single tar file only", watch);
                } else {
                    gcMonitor.skipped(
                        "Estimated compaction in {}, gain is {}% ({}/{}) or ({}/{}), so skipping compaction for now",
                        watch, gain, estimate.getReachableSize(), estimate.getTotalSize(),
                        humanReadableByteCount(estimate.getReachableSize()), humanReadableByteCount(estimate.getTotalSize()));
                }
            }
        } else {
            gcMonitor.info("Compaction estimation is skipped due to threshold value ({}). Running compaction",
                gainThreshold);
        }

        if (runCompaction) {
            if (!compactionStrategy.isPaused()) {
                compact();
                compacted = true;
            } else {
                gcMonitor.skipped("TarMK compaction paused");
            }
        }
        if (cleanup) {
            cleanupNeeded.set(true);
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

    public synchronized long size() {
        long size = writeFile.length();
        for (TarReader reader : readers) {
            size += reader.size();
        }
        return size;
    }

    /**
     * Returns the number of segments in this TarMK instance.
     *
     * @return number of segments
     */
    private synchronized int count() {
        int count = 0;
        if (writer != null) {
            count += writer.count();
        }
        for (TarReader reader : readers) {
            count += reader.count();
        }
        return count;
    }

    CompactionGainEstimate estimateCompactionGain() {
        CompactionGainEstimate estimate = new CompactionGainEstimate(getHead(),
                count());
        synchronized (this) {
            for (TarReader reader : readers) {
                reader.accept(estimate);
            }
        }
        return estimate;
    }

    public void flush() throws IOException {
        synchronized (persistedHead) {
            RecordId before = persistedHead.get();
            RecordId after = head.get();
            boolean cleanup = cleanupNeeded.getAndSet(false);
            if (cleanup || !after.equals(before)) {
                // needs to happen outside the synchronization block below to
                // avoid a deadlock with another thread flushing the writer
                tracker.getWriter().flush();

                // needs to happen outside the synchronization block below to
                // prevent the flush from stopping concurrent reads and writes
                writer.flush();

                synchronized (this) {
                    log.debug("TarMK journal update {} -> {}", before, after);
                    journalFile.writeBytes(after.toString10() + " root\n");
                    journalFile.getChannel().force(false);
                    persistedHead.set(after);
                }

                // Needs to happen outside the synchronization block above to
                // prevent the flush from stopping concurrent reads and writes
                // by the persisted compaction map. See OAK-3264
                if (cleanup) {
                    // Explicitly give up reference to the previous root state
                    // otherwise they would block cleanup. See OAK-3347
                    before = null;
                    after = null;
                    cleanup();
                }
            }
            synchronized (this) {
                // remove all obsolete tar generations
                Iterator<File> iterator = toBeRemoved.iterator();
                while (iterator.hasNext()) {
                    File file = iterator.next();
                    log.debug("TarMK GC: Attempting to remove old file {}",
                            file);
                    if (!file.exists() || file.delete()) {
                        log.debug("TarMK GC: Removed old file {}", file);
                        iterator.remove();
                    }
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
    public void cleanup() throws IOException {
        Stopwatch watch = Stopwatch.createStarted();
        long initialSize = size();
        CompactionMap cm = tracker.getCompactionMap();
        Set<UUID> cleanedIds = newHashSet();

        synchronized (this) {
            gcMonitor.info("TarMK revision cleanup started. Current repository size {}",
                    humanReadableByteCount(initialSize));

            newWriter();
            tracker.clearCache();

            // Suggest to the JVM that now would be a good time
            // to clear stale weak references in the SegmentTracker
            System.gc();

            Set<UUID> ids = newHashSet();
            for (SegmentId id : tracker.getReferencedSegmentIds()) {
                ids.add(new UUID(
                        id.getMostSignificantBits(),
                        id.getLeastSignificantBits()));
            }
            writer.collectReferences(ids);

            List<TarReader> list = newArrayListWithCapacity(readers.size());
            for (TarReader reader : readers) {
                TarReader cleaned = reader.cleanup(ids, cm, cleanedIds);
                if (cleaned == reader) {
                    list.add(reader);
                } else {
                    if (cleaned != null) {
                        list.add(cleaned);
                    }
                    closeAndLogOnFail(reader);
                    File file = reader.getFile();
                    gcMonitor.info("TarMK revision cleanup reclaiming {}", file.getName());
                    toBeRemoved.addLast(file);
                }
            }
            readers = list;
        }

        // Do this outside sync to avoid deadlock with SegmentId.getSegment(). See OAK-3179
        cm.remove(cleanedIds);
        long finalSize = size();
        gcMonitor.cleaned(initialSize - finalSize, finalSize);
        gcMonitor.info("TarMK revision cleanup completed in {}. Post cleanup size is {} " +
                "and space reclaimed {}. Compaction map weight/depth is {}/{}.", watch,
                humanReadableByteCount(finalSize),
                humanReadableByteCount(initialSize - finalSize),
                humanReadableByteCount(sum(cm.getEstimatedWeights())),
                cm.getDepth());
    }

    /**
     * @return  a new {@link SegmentWriter} instance for writing to this store.
     */
    public SegmentWriter createSegmentWriter() {
        return new SegmentWriter(this, tracker, getVersion());
    }

    /**
     * Copy every referenced record in data (non-bulk) segments. Bulk segments
     * are fully kept (they are only removed in cleanup, if there is no
     * reference to them).
     */
    public void compact() {
        checkArgument(!compactionStrategy.equals(NO_COMPACTION),
                "You must set a compactionStrategy before calling compact");
        gcMonitor.info("TarMK compaction running, strategy={}", compactionStrategy);

        long start = System.currentTimeMillis();
        Compactor compactor = new Compactor(this, compactionStrategy);
        SegmentNodeState before = getHead();
        long existing = before.getChildNode(SegmentNodeStore.CHECKPOINTS)
                .getChildNodeCount(Long.MAX_VALUE);
        if (existing > 1) {
            gcMonitor.warn(
                    "TarMK compaction found {} checkpoints, you might need to run checkpoint cleanup",
                    existing);
        }

        SegmentNodeState after = compactor.compact(EMPTY_NODE, before, EMPTY_NODE);

        Callable<Boolean> setHead = new SetHead(before, after, compactor);
        try {
            int cycles = 0;
            boolean success = false;
            while(cycles++ < compactionStrategy.getRetryCount()
                    && !(success = compactionStrategy.compacted(setHead))) {
                // Some other concurrent changes have been made.
                // Rebase (and compact) those changes on top of the
                // compacted state before retrying to set the head.
                gcMonitor.info("TarMK compaction detected concurrent commits while compacting. " +
                        "Compacting these commits. Cycle {}", cycles);
                SegmentNodeState head = getHead();
                after = compactor.compact(before, head, after);
                before = head;
                setHead = new SetHead(head, after, compactor);
            }
            if (!success) {
                gcMonitor.info("TarMK compaction gave up compacting concurrent commits after " +
                        "{} cycles.", cycles - 1);
                if (compactionStrategy.getForceAfterFail()) {
                    gcMonitor.info("TarMK compaction force compacting remaining commits");
                    if (!forceCompact(before, after, compactor)) {
                        gcMonitor.warn("TarMK compaction failed to force compact remaining commits. " +
                                "Most likely compaction didn't get exclusive access to the store.");
                    }
                }
            }

            gcMonitor.info("TarMK compaction completed after {} cycles in {}ms",
                    cycles - 1, System.currentTimeMillis() - start);
        } catch (Exception e) {
            gcMonitor.error("Error while running TarMK compaction", e);
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

    public synchronized Iterable<SegmentId> getSegmentIds() {
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
        // avoid deadlocks by closing (and joining) the background
        // threads before acquiring the synchronization lock
        closeAndLogOnFail(compactionThread);
        closeAndLogOnFail(flushThread);
        synchronized (this) {
            try {
                flush();

                closeAndLogOnFail(writer);
                tracker.getWriter().dropCache();

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
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to close the TarMK at " + directory, e);
            }
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
            synchronized (this) {
                if (writer.containsEntry(msb, lsb)) {
                    return true;
                }
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
            synchronized (this) {
                try {
                    ByteBuffer buffer = writer.readEntry(msb, lsb);
                    if (buffer != null) {
                        return new Segment(tracker, id, buffer);
                    }
                } catch (IOException e) {
                    log.warn("Failed to read from tar file " + writer, e);
                }
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
    public synchronized void writeSegment(
            SegmentId id, byte[] data, int offset, int length) {
        try {
            long size = writer.writeEntry(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits(),
                    data, offset, length);
            if (size >= maxFileSize) {
                newWriter();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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
            writer = new TarWriter(writeFile);
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
                Map<UUID, List<UUID>> graph = Maps.newHashMap();
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

    private synchronized void setRevision(String rootRevision) {
        RecordId id = RecordId.fromString(tracker, rootRevision);
        head.set(id);
        persistedHead.set(id);
    }

    /**
     * A read only {@link FileStore} implementation that supports
     * going back to old revisions.
     * <p>
     * All write methods are no-ops.
     */
    public static class ReadOnlyStore extends FileStore {
        public ReadOnlyStore(File directory) throws IOException {
            super(null, directory, EMPTY_NODE, -1, 0, MEMORY_MAPPING_DEFAULT,
                    GCMonitor.EMPTY, true);
        }

        /**
         * Go to the specified {@code revision}
         *
         * @param revision
         */
        public synchronized void setRevision(String revision) {
            super.setRevision(revision);
        }

        @Override
        public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
            throw new UnsupportedOperationException("Read Only Store");
        }

        @Override
        public synchronized void writeSegment(SegmentId id, byte[] data,
                int offset, int length) {
            throw new UnsupportedOperationException("Read Only Store");
        }

        /**
         * no-op
         */
        @Override
        public void flush() { /* nop */ }

        @Override
        public synchronized void cleanup() {
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

                CompactionMap cm = tracker.getCompactionMap();
                gcMonitor.compacted(cm.getSegmentCounts(), cm.getRecordCounts(), cm.getEstimatedWeights());
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
        public void compacted(long[] segmentCounts, long[] recordCounts, long[] compactionMapWeights) {
            delegatee.compacted(segmentCounts, recordCounts, compactionMapWeights);
        }

        @Override
        public void cleaned(long reclaimedSize, long currentSize) {
            delegatee.cleaned(reclaimedSize, currentSize);
        }
    }
}
