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
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory.isBulkSegmentId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

public class FileStore implements SegmentStore {

    private static final Logger log = LoggerFactory.getLogger(FileStore.class);

    private static final int MB = 1024 * 1024;

    private static final int DEFAULT_MEMORY_CACHE_SIZE = 256;

    private static final String FILE_NAME_FORMAT = "%s%05d.tar";

    private static final String JOURNAL_FILE_NAME = "journal.log";

    private final SegmentIdFactory factory = new SegmentIdFactory();

    private final SegmentWriter writer = new SegmentWriter(this, factory);

    protected final Cache<UUID, Segment> segments;

    /**
     * Identifiers of the segments that are currently being loaded.
     */
    private final Set<UUID> currentlyLoading = newHashSet();

    /**
     * Number of threads that are currently waiting for segments to be loaded.
     * Used to avoid extra {@link #notifyAll()} calls when nobody is waiting.
     */
    private int currentlyWaiting = 0;

    private final File directory;

    private final int maxFileSize;

    private final boolean memoryMapping;

    private final List<TarFile> bulkFiles = newCopyOnWriteArrayList();

    private final List<TarFile> dataFiles = newCopyOnWriteArrayList();

    private final RandomAccessFile journalFile;

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
    private final Thread flushThread;

    /**
     * Synchronization aid used by the background flush thread to stop itself
     * as soon as the {@link #close()} method is called.
     */
    private final CountDownLatch timeToClose = new CountDownLatch(1);

    public FileStore(File directory, int maxFileSizeMB, boolean memoryMapping)
            throws IOException {
        this(directory, EMPTY_NODE, maxFileSizeMB, DEFAULT_MEMORY_CACHE_SIZE, memoryMapping);
    }

    public FileStore(File directory, int maxFileSizeMB, int cacheSizeMB,
            boolean memoryMapping) throws IOException {
        this(directory, EMPTY_NODE, maxFileSizeMB, cacheSizeMB, memoryMapping);
    }

    public FileStore(
            final File directory, NodeState initial, int maxFileSizeMB,
            int cacheSizeMB, boolean memoryMapping) throws IOException {
        checkNotNull(directory).mkdirs();
        this.directory = directory;
        this.maxFileSize = maxFileSizeMB * MB;
        this.memoryMapping = memoryMapping;

        if (memoryMapping) {
            // let the OS take care of caching
            this.segments = null;
        } else {
            this.segments = CacheLIRS.newBuilder()
                    .weigher(Segment.WEIGHER)
                    .maximumWeight(cacheSizeMB * MB)
                    .build();
        }

        for (int i = 0; true; i++) {
            String name = String.format(FILE_NAME_FORMAT, "bulk", i);
            File file = new File(directory, name);
            if (file.isFile()) {
                bulkFiles.add(new TarFile(file, maxFileSize, memoryMapping));
            } else {
                break;
            }
        }

        for (int i = 0; true; i++) {
            String name = String.format(FILE_NAME_FORMAT, "data", i);
            File file = new File(directory, name);
            if (file.isFile()) {
                dataFiles.add(new TarFile(file, maxFileSize, memoryMapping));
            } else {
                break;
            }
        }

        journalFile = new RandomAccessFile(
                new File(directory, JOURNAL_FILE_NAME), "rw");

        RecordId id = null;
        String line = journalFile.readLine();
        while (line != null) {
            int space = line.indexOf(' ');
            if (space != -1) {
                id = RecordId.fromString(line.substring(0, space));
            }
            line = journalFile.readLine();
        }

        if (id != null) {
            head = new AtomicReference<RecordId>(id);
            persistedHead = new AtomicReference<RecordId>(id);
        } else {
            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setChildNode("root", initial);
            head = new AtomicReference<RecordId>(
                    getWriter().writeNode(builder.getNodeState()).getRecordId());
            persistedHead = new AtomicReference<RecordId>(null);
        }

        this.flushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    timeToClose.await(1, SECONDS);
                    while (timeToClose.getCount() > 0) {
                        try {
                            flush();
                        } catch (IOException e) {
                            log.warn("Failed to flush the TarMK at" +
                                    directory, e);
                        }
                        timeToClose.await(5, SECONDS);
                    }
                } catch (InterruptedException e) {
                    log.warn("TarMK flush thread interrupted");
                }
            }
        });
        flushThread.setName("TarMK flush thread: " + directory);
        flushThread.setDaemon(true);
        flushThread.setPriority(Thread.MIN_PRIORITY);
        flushThread.start();
    }

    public void flush() throws IOException {
        synchronized (persistedHead) {
            RecordId before = persistedHead.get();
            RecordId after = head.get();
            if (!after.equals(before)) {
                // needs to happen outside the synchronization block below to
                // avoid a deadlock with another thread flushing the writer
                getWriter().flush();

                synchronized (this) {
                    for (TarFile file : bulkFiles) {
                        file.flush();
                    }
                    for (TarFile file : dataFiles) {
                        file.flush();
                    }
                    journalFile.writeBytes(after + " root\n");
                    journalFile.getChannel().force(false);
                    persistedHead.set(after);
                }
            }
        }

        if (segments != null) {
            for (Segment segment : segments.asMap().values().toArray(new Segment[0])) {
                segment.dropOldCacheEntries();
            }
        }
    }

    public Iterable<UUID> getSegmentIds() {
        List<UUID> ids = newArrayList();
        for (TarFile file : dataFiles) {
            ids.addAll(file.getUUIDs());
        }
        for (TarFile file : bulkFiles) {
            ids.addAll(file.getUUIDs());
        }
        return ids;
    }

    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    @Override
    public SegmentNodeState getHead() {
        return new SegmentNodeState(getWriter().getDummySegment(), head.get());
    }

    @Override
    public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        RecordId id = this.head.get();
        return id.equals(base.getRecordId())
                && this.head.compareAndSet(id, head.getRecordId());
    }

    @Override
    public void close() {
        try {
            // avoid deadlocks while joining the flush thread
            timeToClose.countDown();
            try {
                flushThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while joining the TarMK flush thread", e);
            }

            synchronized (this) {
                if (segments != null) {
                    synchronized (segments) {
                        while (!currentlyLoading.isEmpty()) {
                            try {
                                segments.wait(); // for concurrent loads to finish
                            } catch (InterruptedException e) {
                                throw new RuntimeException("Interrupted", e);
                            }
                        }
                        segments.invalidateAll();
                    }
                }

                flush();

                journalFile.close();

                for (TarFile file : bulkFiles) {
                    file.close();
                }
                bulkFiles.clear();
                for (TarFile file : dataFiles) {
                    file.close();
                }
                dataFiles.clear();

                System.gc(); // for any memory-mappings that are no longer used
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to close the TarMK at " + directory, e);
        }
    }

    @Override
    public Segment readSegment(UUID id) {
        if (isBulkSegmentId(id)) {
            return loadSegment(id, bulkFiles);
        }

        Segment segment = getWriter().getCurrentSegment(id);
        if (segment != null) {
            return segment;
        }

        if (segments == null) {
            // no in-memory cache, load the segment directly
            return loadSegment(id, dataFiles);
        }

        synchronized (segments) {
            // check if the segment is already cached
            segment = segments.getIfPresent(id);
            // ... or currently being loaded
            while (segment == null && currentlyLoading.contains(id)) {
                currentlyWaiting++;
                try {
                    segments.wait(); // for another thread to load the segment
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                } finally {
                    currentlyWaiting--;
                }
                segment = segments.getIfPresent(id);
            }
            if (segment != null) {
                // found the segment in the cache
                return segment;
            }
            // not yet cached, so start let others know that we're loading it
            currentlyLoading.add(id);
        }

        try {
            segment = loadSegment(id, dataFiles);
        } finally {
            synchronized (segments) {
                if (segment != null) {
                    segments.put(id, segment);
                }
                currentlyLoading.remove(id);
                if (currentlyWaiting > 0) {
                    segments.notifyAll();
                }
            }
        }

        return segment;
    }

    protected Segment loadSegment(UUID id, List<TarFile> files) {
        for (TarFile file : files) {
            try {
                ByteBuffer buffer = file.readEntry(id);
                if (buffer != null) {
                    return new Segment(this, factory, id, buffer);
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to access file " + file, e);
            }
        }

        throw new IllegalStateException("Segment " + id + " not found");
    }

    @Override
    public synchronized void writeSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        // select whether to write a data or a bulk segment
        List<TarFile> files = dataFiles;
        String base = "data";
        if (isBulkSegmentId(segmentId)) {
            files = bulkFiles;
            base = "bulk";
        }

        try {
            if (files.isEmpty() || !files.get(files.size() - 1).writeEntry(
                    segmentId, data, offset, length)) {
                String name = String.format(FILE_NAME_FORMAT, base, files.size());
                File file = new File(directory, name);
                TarFile last = new TarFile(file, maxFileSize, memoryMapping);
                checkState(last.writeEntry(segmentId, data, offset, length));
                files.add(last);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Blob readBlob(String reference) {
        return new FileBlob(reference); // FIXME: proper reference lookup
    }

}