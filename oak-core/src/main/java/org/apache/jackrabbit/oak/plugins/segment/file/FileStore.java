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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

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
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStore implements SegmentStore {

    private static final Logger log = LoggerFactory.getLogger(FileStore.class);

    private static final int MB = 1024 * 1024;

    private static final String FILE_NAME_FORMAT = "%s%05d.tar";

    private static final String JOURNAL_FILE_NAME = "journal.log";

    private static final boolean MEMORY_MAPPING_DEFAULT =
            "64".equals(System.getProperty("sun.arch.data.model", "32"));

    private final SegmentTracker tracker;

    private final File directory;

    private final BlobStore blobStore;

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

    public FileStore(BlobStore blobStore, File directory, int maxFileSizeMB, boolean memoryMapping)
            throws IOException {
        this(blobStore, directory, EMPTY_NODE, maxFileSizeMB, 0, memoryMapping);
    }

    public FileStore(File directory, int maxFileSizeMB, boolean memoryMapping)
            throws IOException {
        this(null, directory, maxFileSizeMB, memoryMapping);
    }

    public FileStore(File directory, int maxFileSizeMB)
            throws IOException {
        this(null, directory, maxFileSizeMB, MEMORY_MAPPING_DEFAULT);
    }

    public FileStore(File directory, int maxFileSizeMB, int cacheSizeMB,
            boolean memoryMapping) throws IOException {
        this(null, directory, EMPTY_NODE, maxFileSizeMB, cacheSizeMB, memoryMapping);
    }

    public FileStore(
            BlobStore blobStore, final File directory, NodeState initial,
            int maxFileSizeMB, int cacheSizeMB, boolean memoryMapping)
            throws IOException {
        checkNotNull(directory).mkdirs();
        if (cacheSizeMB > 0) {
            this.tracker = new SegmentTracker(this, cacheSizeMB);
        } else {
            this.tracker = new SegmentTracker(this);
        }
        this.blobStore = blobStore;
        this.directory = directory;
        this.maxFileSize = maxFileSizeMB * MB;
        this.memoryMapping = memoryMapping;

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
                id = RecordId.fromString(tracker, line.substring(0, space));
            }
            line = journalFile.readLine();
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
                tracker.getWriter().flush();

                synchronized (this) {
                    boolean success = true;
                    for (TarFile file : bulkFiles) {
                        success = success && file.flush();
                    }
                    for (TarFile file : dataFiles) {
                        success = success && file.flush();
                    }
                    if (!success) {
                        log.warn("Failed to sync one ore more tar files with"
                                + " the underlying file system, possibly because of"
                                + " http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6539707."
                                + " Will retry later.");
                    }
                    journalFile.writeBytes(after + " root\n");
                    journalFile.getChannel().force(false);
                    persistedHead.set(after);
                }
            }
        }
    }

    public Iterable<SegmentId> getSegmentIds() {
        List<SegmentId> ids = newArrayList();
        for (TarFile file : dataFiles) {
            for (UUID uuid : file.getUUIDs()) {
                ids.add(tracker.getSegmentId(
                        uuid.getMostSignificantBits(),
                        uuid.getLeastSignificantBits()));
            }
        }
        for (TarFile file : bulkFiles) {
            for (UUID uuid : file.getUUIDs()) {
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
    public boolean containsSegment(SegmentId id) {
        if (id.getTracker() == tracker) {
            return true;
        } else if (id.isDataSegmentId()) {
            return containsSegment(id, dataFiles);
        } else {
            return containsSegment(id, bulkFiles);
        }
    }

    @Override
    public Segment readSegment(SegmentId id) {
        if (id.isBulkSegmentId()) {
            return loadSegment(id, bulkFiles);
        } else {
            return loadSegment(id, dataFiles);
        }
    }

    private boolean containsSegment(SegmentId id, List<TarFile> files) {
        UUID uuid = new UUID(
                id.getMostSignificantBits(),
                id.getLeastSignificantBits());
        for (TarFile file : files) {
            try {
                ByteBuffer buffer = file.readEntry(uuid);
                if (buffer != null) {
                    return true;
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to access file " + file, e);
            }
        }
        return false;
    }

    private Segment loadSegment(SegmentId id, List<TarFile> files) {
        UUID uuid = new UUID(
                id.getMostSignificantBits(),
                id.getLeastSignificantBits());
        for (TarFile file : files) {
            try {
                ByteBuffer buffer = file.readEntry(uuid);
                if (buffer != null) {
                    return new Segment(tracker, id, buffer);
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
            SegmentId id, byte[] data, int offset, int length) {
        // select whether to write a data or a bulk segment
        List<TarFile> files = dataFiles;
        String base = "data";
        if (id.isBulkSegmentId()) {
            files = bulkFiles;
            base = "bulk";
        }

        try {
            UUID uuid = new UUID(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits());
            if (files.isEmpty() || !files.get(files.size() - 1).writeEntry(
                    uuid, data, offset, length)) {
                String name = String.format(FILE_NAME_FORMAT, base, files.size());
                File file = new File(directory, name);
                TarFile last = new TarFile(file, maxFileSize, memoryMapping);
                checkState(last.writeEntry(uuid, data, offset, length));
                files.add(last);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Blob readBlob(String reference) {
        if(blobStore != null){
            return new BlobStoreBlob(blobStore, reference);
        }
        throw new IllegalStateException("Attempt to read external reference ["+reference+"] " +
                "without specifying BlobStore");
    }

    @Override
    public BlobStore getBlobStore() {
        return blobStore;
    }

    @Override
    public void gc() {
        System.gc();
        Set<SegmentId> ids = tracker.getReferencedSegmentIds();
        // TODO reclaim unreferenced segments
    }
}