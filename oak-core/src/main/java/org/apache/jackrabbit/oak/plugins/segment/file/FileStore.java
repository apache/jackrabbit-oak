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
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.jackrabbit.oak.plugins.segment.Journal;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.Template;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryJournal;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Maps;

public class FileStore implements SegmentStore {

    private static final long MAGIC_BYTES = 0x4f616b0a527845ddL;

    private static final long FILE_SIZE = 256 * 1024 * 1024;

    private static final String FILE_NAME_FORMAT = "data%05d.tar";

    private final Map<String, Journal> journals = Maps.newHashMap();

    private final File directory;
    private int index;

    private MappedByteBuffer rw;
    private ByteBuffer ro;

    private final ConcurrentMap<UUID, Segment> segments =
            Maps.newConcurrentMap();

    public FileStore(File directory, NodeState root) throws IOException {
        checkNotNull(directory).mkdirs();
        this.directory = directory;
        this.index = 0;
        while (loadSegments()) {
            this.index++;
        }

        journals.put("root", new MemoryJournal(this, root));
    }

    public FileStore(File directory) throws IOException {
        this(directory, EMPTY_NODE);
    }

    public FileStore(String directory) throws IOException {
        this(new File(directory));
    }

    public void close() {
        rw.force();

        segments.clear();
        rw = null;
        ro = null;

        System.gc();
    }

    private boolean loadSegments() throws IOException {
        String name = String.format(FILE_NAME_FORMAT, index);
        File file = new File(directory, name);
        long size = FILE_SIZE;
        if (file.isFile()) {
            size = file.length();
        }
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        try {
            rw = f.getChannel().map(READ_WRITE, 0, size);
            ro = rw.asReadOnlyBuffer();

            while (ro.remaining() >= 4 * 0x200) {
                // skip tar header and get the magic bytes; TODO: verify?
                long magic = ro.getLong(ro.position() + 0x200);
                if (magic == MAGIC_BYTES) {
                    ro.position(ro.position() + 0x200 + 8);

                    int length = ro.getInt();
                    int count = ro.getInt();

                    UUID segmentId = new UUID(ro.getLong(), ro.getLong());
                    Collection<UUID> referencedSegmentIds =
                            newArrayListWithCapacity(count);
                    for (int i = 0; i < count; i++) {
                        referencedSegmentIds.add(
                                new UUID(ro.getLong(), ro.getLong()));
                    }

                    ro.limit(ro.position() + length);
                    ByteBuffer data = ro.slice();
                    ro.limit(rw.limit());

                    Segment segment = new Segment(
                            this, segmentId, data, referencedSegmentIds,
                            Collections.<String, RecordId>emptyMap(),
                            Collections.<Template, RecordId>emptyMap());
                    segments.put(segmentId, segment);

                    // advance to next entry in the file
                    ro.position((ro.position() + length + 0x1ff) & ~0x1ff);
                } else {
                    // still space for more segments: position the write
                    // buffer at this point and return false to stop looking
                    // for more entries
                    rw.position(ro.position());
                    return false;
                }
            }
            return true;
        } finally {
            f.close();
        }
    }

    @Override
    public synchronized Journal getJournal(final String name) {
        Journal journal = journals.get(name);
        if (journal == null) {
            journal = new MemoryJournal(this, "root");
            journals.put(name, journal);
        }
        return journal;
    }

    @Override
    public Segment readSegment(UUID id) {
        Segment segment = segments.get(id);
        if (segment != null) {
            return segment;
        } else {
            throw new IllegalArgumentException("Segment not found: " + id);
        }
    }

    @Override
    public synchronized void createSegment(
            UUID segmentId, byte[] data, int offset, int length,
            Collection<UUID> referencedSegmentIds,
            Map<String, RecordId> strings, Map<Template, RecordId> templates) {
        int size = 8 + 4 + 4 + 16 + 16 * referencedSegmentIds.size() + length;

        if (0x200 + ((size + 0x1ff) & ~0x1ff) + 2 * 0x200 > rw.remaining()) {
            rw.force();
            String name = String.format(FILE_NAME_FORMAT, ++index);
            File file = new File(directory, name);
            try {
                RandomAccessFile f = new RandomAccessFile(file, "rw");
                try {
                    rw = f.getChannel().map(READ_WRITE, 0, FILE_SIZE);
                    ro = rw.asReadOnlyBuffer();
                } finally {
                    f.close();
                }
            } catch (IOException e) {
                throw new RuntimeException("Unable to create a new segment", e);
            }
        }

        rw.put(createTarHeader(segmentId.toString(), size));

        rw.putLong(MAGIC_BYTES);
        rw.putInt(length);
        rw.putInt(referencedSegmentIds.size());
        rw.putLong(segmentId.getMostSignificantBits());
        rw.putLong(segmentId.getLeastSignificantBits());
        for (UUID referencedSegmentId : referencedSegmentIds) {
            rw.putLong(referencedSegmentId.getMostSignificantBits());
            rw.putLong(referencedSegmentId.getLeastSignificantBits());
        }

        ro.position(rw.position());
        rw.put(data, offset, length);
        ro.limit(rw.position());
        ByteBuffer buffer = ro.slice();
        ro.limit(rw.limit());

        rw.position((rw.position() + 0x1ff) & ~0x1ff);
        ro.position(rw.position());

        Segment segment = new Segment(
                this, segmentId, buffer,
                referencedSegmentIds, strings, templates);
        checkState(segments.put(segmentId, segment) == null);
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        if (segments.remove(segmentId) == null) {
            throw new IllegalStateException("Missing segment: " + segmentId);
        }
    }

    private static byte[] createTarHeader(String name, int length) {
        byte[] header = new byte[] {
                // File name
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0,
                // File mode
                '0', '0', '0', '0', '4', '0', '0', 0,
                // User's numeric user ID
                '0', '0', '0', '0', '0', '0', '0', 0,
                // Group's numeric user ID
                '0', '0', '0', '0', '0', '0', '0', 0,
                // File size in bytes (octal basis)
                '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',  0,
                // Last modification time in numeric Unix time format (octal)
                '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',  0,
                // Checksum for header record
                ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
                // Type flag
                '0',
                // Name of linked file
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0,
                // unused
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };

        byte[] n = name.getBytes(UTF_8);
        System.arraycopy(n, 0, header, 0, n.length);

        byte[] l = Integer.toOctalString(length).getBytes(UTF_8);
        System.arraycopy(l, 0, header, 124 + 11 - l.length, l.length);

        long time = System.currentTimeMillis() / 1000;
        byte[] t = Long.toOctalString(time).getBytes(UTF_8);
        System.arraycopy(t, 0, header, 136 + 11 - t.length, t.length);

        int checksum = 0;
        for (int i = 0; i < header.length; i++) {
            checksum += header[i] & 0xff;
        }
        byte[] c = Integer.toOctalString(checksum).getBytes(UTF_8);
        System.arraycopy(c, 0, header, 148 + 6 - c.length, c.length);
        header[148 + 6] = 0;

        return header;
    }

}
