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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newHashMap;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.oak.plugins.segment.Journal;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.Template;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class FileStore implements SegmentStore {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(FileStore.class);

    private static final long SEGMENT_MAGIC = 0x4f616b0a527845ddL;

    private static final long JOURNAL_MAGIC = 0xdf36544212c0cb24L;

    private static final long PADDING_MAGIC = 0x786da7779516c12L;

    private static final String JOURNALS_UUID = new UUID(0, 0).toString();

    private static final String PADDING_UUID = new UUID(-1, -1).toString();

    private static final long FILE_SIZE = 256 * 1024 * 1024;

    private static final String FILE_NAME_FORMAT = "data%05d.tar";

    private static final int SEGMENT_SIZE = 0x200; // 512

    private static final byte[] PADDING_BYTES = new byte[SEGMENT_SIZE];

    private class SegmentReference {

        private final UUID id;

        private final ByteBuffer buffer;

        private final int position;

        SegmentReference(UUID id, ByteBuffer buffer, int position) {
            this.id = id;
            this.buffer = checkNotNull(buffer);
            this.position = position;
        }

        UUID getSegmentId() {
            return id;
        }

        Segment getSegment() {
            ByteBuffer ro = buffer.asReadOnlyBuffer();
            ro.position(position);

            int length = ro.getInt();
            int count = ro.getInt();

            checkState(id.equals(new UUID(ro.getLong(), ro.getLong())));

            Collection<UUID> referencedIds = newArrayListWithCapacity(count);
            for (int i = 0; i < count; i++) {
                SegmentReference reference =
                        references.get(new UUID(ro.getLong(), ro.getLong()));
                checkState(reference != null);
                referencedIds.add(reference.getSegmentId());
            }

            ro.limit(ro.position() + length);
            return new Segment(FileStore.this, id, ro.slice(), referencedIds,
                    Collections.<String, RecordId>emptyMap(),
                    Collections.<Template, RecordId>emptyMap());
        }

    }

    private final File directory;

    private final LinkedList<MappedByteBuffer> files = newLinkedList();

    private final Map<String, Journal> journals = newHashMap();

    private final Map<UUID, SegmentReference> references = newConcurrentMap();

    private final Cache<UUID, Segment> segments =
            CacheBuilder.newBuilder().maximumSize(1000).build();

    public FileStore(File directory, NodeState root) throws IOException {
        // http://www.oracle.com/technetwork/java/hotspotfaq-138619.html#64bit_detection
        if ("32".equals(System.getProperty("sun.arch.data.model"))) {
            log.warn("TarMK will only work with small repositories"
                    + " in a 32 bit JVM. Consider switching to a 64 bit JVM.");
        }

        checkNotNull(directory).mkdirs();
        this.directory = directory;

        for (int i = 0; true; i++) {
            String name = String.format(FILE_NAME_FORMAT, i);
            File file = new File(directory, name);
            if (file.isFile()) {
                RandomAccessFile f = new RandomAccessFile(file, "rw");
                try {
                    files.add(f.getChannel().map(READ_WRITE, 0, FILE_SIZE));
                } finally {
                    f.close();
                }
            } else {
                break;
            }
        }

        for (MappedByteBuffer file : files) {
            loadSegments(file);
        }

        if (!journals.containsKey("root")) {
            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setChildNode("root", root);
            journals.put("root", new FileJournal(this, builder.getNodeState()));
        }
    }

    public FileStore(File directory) throws IOException {
        this(directory, EMPTY_NODE);
    }

    public FileStore(String directory) throws IOException {
        this(new File(directory));
    }

    public synchronized void close() {
        if (!files.isEmpty()) {
            files.getLast().force();
        }
        files.clear();
        references.clear();
        segments.invalidateAll();
        segments.cleanUp();
        System.gc();
    }

    private void loadSegments(MappedByteBuffer ro) throws IOException {
        while (ro.remaining() >= 4 * SEGMENT_SIZE) {
            // skip tar header and get the magic bytes; TODO: verify?
            long magic = ro.getLong(ro.position() + SEGMENT_SIZE);
            if (magic == SEGMENT_MAGIC) {
                int position = ro.position() + SEGMENT_SIZE + 8;
                int length = ro.getInt(position);
                int count = ro.getInt(position + 4);
                UUID id = new UUID(
                        ro.getLong(position + 8),
                        ro.getLong(position + 16));
                references.put(id, new SegmentReference(id, ro, position));

                // advance to next entry in the file
                position += 24 + count * 16 + length;
                ro.position((position + 0x1ff) & ~0x1ff);
            } else if (magic == JOURNAL_MAGIC) {
                ro.position(ro.position() + SEGMENT_SIZE + 8);

                int count = ro.getInt();
                for (int i = 0; i < count; i++) {
                    byte[] n = new byte[ro.getInt()];
                    ro.get(n);
                    SegmentReference reference = references.get(
                            new UUID(ro.getLong(), ro.getLong()));
                    checkState(reference != null);
                    SegmentNodeState h = new SegmentNodeState(this, new RecordId(
                            reference.getSegmentId(), ro.getInt()));
                    journals.put(
                            new String(n, UTF_8),
                            new FileJournal(this, h));
                }

                // advance to next entry in the file
                ro.position((ro.position() + 0x1ff) & ~0x1ff);
            } else {
                // last entry encountered
                return;
            }
        }
    }

    @Override
    public synchronized Journal getJournal(final String name) {
        Journal journal = journals.get(name);
        if (journal == null) {
            journal = new FileJournal(this, "root");
            journals.put(name, journal);
        }
        return journal;
    }

    @Override
    public Segment readSegment(final UUID id) {
        try {
            return segments.get(id, new Callable<Segment>() {
                @Override
                public Segment call() throws Exception {
                    SegmentReference reference = references.get(id);
                    checkState(reference != null);
                    return reference.getSegment();
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to load segment " + id, e);
        }
    }

    @Override
    public synchronized void createSegment(
            UUID segmentId, byte[] data, int offset, int length,
            Collection<UUID> referencedSegmentIds,
            Map<String, RecordId> strings, Map<Template, RecordId> templates) {
        int size = 8 + 4 + 4 + 16 + 16 * referencedSegmentIds.size() + length;

        MappedByteBuffer rw = prepare(size);

        rw.put(createTarHeader(segmentId.toString(), size));
        rw.putLong(SEGMENT_MAGIC);
        int position = rw.position();

        rw.putInt(length);
        rw.putInt(referencedSegmentIds.size());
        rw.putLong(segmentId.getMostSignificantBits());
        rw.putLong(segmentId.getLeastSignificantBits());
        for (UUID referencedSegmentId : referencedSegmentIds) {
            rw.putLong(referencedSegmentId.getMostSignificantBits());
            rw.putLong(referencedSegmentId.getLeastSignificantBits());
        }

        ByteBuffer ro = rw.asReadOnlyBuffer();
        ro.limit(ro.position() + length);
        ro = ro.slice();

        rw.put(data, offset, length);

        int n = rw.position() % SEGMENT_SIZE;
        if (n > 0) {
            rw.put(PADDING_BYTES, 0, SEGMENT_SIZE - n);
        }

        SegmentReference previous = references.put(
                segmentId, new SegmentReference(segmentId, rw, position));
        checkState(previous == null);

        segments.put(segmentId, new Segment(
                this, segmentId, ro, referencedSegmentIds, strings, templates));
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        if (references.remove(segmentId) == null) {
            throw new IllegalStateException("Missing segment: " + segmentId);
        }
        segments.invalidate(segmentId);
    }

    synchronized void writeJournals() {
        int size = 8 + 4;
        for (String name : journals.keySet()) {
            size += 4 + name.getBytes(UTF_8).length + 16 + 4;
        }

        MappedByteBuffer rw = prepare(size);

        rw.put(createTarHeader(JOURNALS_UUID, size));

        rw.putLong(JOURNAL_MAGIC);
        rw.putInt(journals.size());
        for (Map.Entry<String, Journal> entry : journals.entrySet()) {
            byte[] name = entry.getKey().getBytes(UTF_8);
            rw.putInt(name.length);
            rw.put(name);
            RecordId head = entry.getValue().getHead();
            rw.putLong(head.getSegmentId().getMostSignificantBits());
            rw.putLong(head.getSegmentId().getLeastSignificantBits());
            rw.putInt(head.getOffset());
        }

        int n = rw.position() % SEGMENT_SIZE;
        if (n > 0) {
            rw.put(PADDING_BYTES, 0, SEGMENT_SIZE - n);
        }
    }

    private synchronized MappedByteBuffer prepare(int size) {
        if (!files.isEmpty()) {
            MappedByteBuffer last = files.getLast();

            // Check if there's still enough room in the last open file
            int segments = (size + SEGMENT_SIZE - 1) / SEGMENT_SIZE;
            if ((1 + segments + 2) * SEGMENT_SIZE <= last.remaining()) {
                return last;
            }

            // No more room, so finish the last file with padding as needed
            if (last.remaining() >= 3 * SEGMENT_SIZE) {
                // Add a padding entry to avoid problems during reopening
                last.put(createTarHeader(
                        PADDING_UUID,
                        last.remaining() - 3 * SEGMENT_SIZE));
                if (last.remaining() > 2 * SEGMENT_SIZE) {
                    last.putLong(PADDING_MAGIC);
                    last.put(PADDING_BYTES, 0, SEGMENT_SIZE - 8);
                }
            }
            while (last.remaining() > 0) {
                last.put(PADDING_BYTES);
            }
            last.force();
        }

        // Need to start a new file
        try {
            String name = String.format(FILE_NAME_FORMAT, files.size());
            File file = new File(directory, name);
            RandomAccessFile f = new RandomAccessFile(file, "rw");
            try {
                MappedByteBuffer buffer =
                        f.getChannel().map(READ_WRITE, 0, FILE_SIZE);
                files.add(buffer);
                return buffer;
            } finally {
                f.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to create a new segment", e);
        }
    }

    private static byte[] createTarHeader(String name, int length) {
        byte[] header = new byte[SEGMENT_SIZE];

        // File name
        byte[] n = name.getBytes(UTF_8);
        System.arraycopy(n, 0, header, 0, n.length);

        // File mode
        System.arraycopy(
                String.format("%07o", 0400).getBytes(UTF_8), 0,
                header, 100, 7);

        // User's numeric user ID
        System.arraycopy(
                String.format("%07o", 0).getBytes(UTF_8), 0,
                header, 108, 7);

        // Group's numeric user ID
        System.arraycopy(
                String.format("%07o", 0).getBytes(UTF_8), 0,
                header, 116, 7);

        // File size in bytes (octal basis)
        System.arraycopy(
                String.format("%011o", length).getBytes(UTF_8), 0,
                header, 124, 11);

        // Last modification time in numeric Unix time format (octal)
        long time = System.currentTimeMillis() / 1000;
        System.arraycopy(
                String.format("%011o", time).getBytes(UTF_8), 0,
                header, 136, 11);

        // Checksum for header record
        System.arraycopy(
                new byte[] { ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ' }, 0,
                header, 148, 8);

        // Type flag
        header[156] = '0';

        // Compute checksum
        int checksum = 0;
        for (int i = 0; i < header.length; i++) {
            checksum += header[i] & 0xff;
        }
        System.arraycopy(
                String.format("%06o", checksum).getBytes(UTF_8), 0,
                header, 148, 6);
        header[154] = 0;

        return header;
    }

}
