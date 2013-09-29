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
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.segment.AbstractStore;
import org.apache.jackrabbit.oak.plugins.segment.Journal;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentCache;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class FileStore extends AbstractStore {

    private static final long SEGMENT_MAGIC = 0x4f616b0a527845ddL;

    private static final long JOURNAL_MAGIC = 0xdf36544212c0cb24L;

    static final UUID JOURNALS_UUID = new UUID(0, 0);

    private static final String FILE_NAME_FORMAT = "data%05d.tar";

    private final File directory;

    private final int maxFileSize;

    private final boolean memoryMapping;

    private final LinkedList<TarFile> files = newLinkedList();

    private final Map<String, Journal> journals = newHashMap();

    private final SegmentCache cache = SegmentCache.create();

    private final SegmentWriter writer = new SegmentWriter(this);

    public FileStore(File directory, int maxFileSize, boolean memoryMapping)
            throws IOException {
        checkNotNull(directory).mkdirs();
        this.directory = directory;
        this.maxFileSize = maxFileSize;
        this.memoryMapping = memoryMapping;

        for (int i = 0; true; i++) {
            String name = String.format(FILE_NAME_FORMAT, i);
            File file = new File(directory, name);
            if (file.isFile()) {
                files.add(new TarFile(file, maxFileSize, memoryMapping));
            } else {
                break;
            }
        }

        Segment segment = writer.getDummySegment();
        for (TarFile tar : files) {
            ByteBuffer buffer = tar.readEntry(JOURNALS_UUID);
            if (buffer != null) {
                checkState(JOURNAL_MAGIC == buffer.getLong());
                int count = buffer.getInt();
                for (int i = 0; i < count; i++) {
                    byte[] b = new byte[buffer.getInt()];
                    buffer.get(b);
                    String name = new String(b, UTF_8);
                    RecordId recordId = new RecordId(
                            new UUID(buffer.getLong(), buffer.getLong()),
                            buffer.getInt());
                    journals.put(name, new FileJournal(
                            this, new SegmentNodeState(segment, recordId)));
                }
            }
        }

        if (!journals.containsKey("root")) {
            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setChildNode("root", EMPTY_NODE);
            journals.put("root", new FileJournal(this, builder.getNodeState()));
        }
    }

    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    @Override
    public synchronized void close() {
        try {
            for (TarFile file : files) {
                file.close();
            }
            files.clear();
            cache.clear();
            System.gc(); // for any memory-mappings that are no longer used
        } catch (Exception e) {
            throw new RuntimeException(e);
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
            Segment segment = writer.getCurrentSegment(id);
            if (segment == null) {
                segment = cache.getSegment(id, new Callable<Segment>() {
                    @Override
                    public Segment call() throws Exception {
                        return loadSegment(id);
                    }
                });
            }
            return segment;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Segment loadSegment(UUID id) throws Exception {
        for (TarFile file : files) {
            ByteBuffer buffer = file.readEntry(id);
            if (buffer != null) {
                checkState(SEGMENT_MAGIC == buffer.getLong());
                int length = buffer.getInt();
                int count = buffer.getInt();

                checkState(id.equals(new UUID(
                        buffer.getLong(), buffer.getLong())));

                List<UUID> referencedIds = newArrayListWithCapacity(count);
                for (int i = 0; i < count; i++) {
                    referencedIds.add(new UUID(
                            buffer.getLong(), buffer.getLong()));
                }

                buffer.limit(buffer.position() + length);
                return new Segment(
                        FileStore.this, id,
                        buffer.slice(), referencedIds);
            }
        }

        throw new IllegalStateException("Segment " + id + " not found");
    }

    @Override
    public synchronized void writeSegment(
            UUID segmentId, byte[] data, int offset, int length,
            List<UUID> referencedSegmentIds) {
        int size = 8 + 4 + 4 + 16 + 16 * referencedSegmentIds.size() + length;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putLong(SEGMENT_MAGIC);
        buffer.putInt(length);
        buffer.putInt(referencedSegmentIds.size());
        buffer.putLong(segmentId.getMostSignificantBits());
        buffer.putLong(segmentId.getLeastSignificantBits());
        for (UUID referencedSegmentId : referencedSegmentIds) {
            buffer.putLong(referencedSegmentId.getMostSignificantBits());
            buffer.putLong(referencedSegmentId.getLeastSignificantBits());
        }

        int pos = buffer.position();
        buffer.put(data, offset, length);

        try {
            writeEntry(segmentId, buffer.array());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        buffer.position(pos);
    }

    private void writeEntry(UUID segmentId, byte[] buffer)
            throws IOException {
        if (files.isEmpty() || !files.getLast().writeEntry(
                segmentId, buffer, 0, buffer.length)) {
            String name = String.format(FILE_NAME_FORMAT, files.size());
            File file = new File(directory, name);
            TarFile last = new TarFile(file, maxFileSize, memoryMapping);
            checkState(last.writeEntry(segmentId, buffer, 0, buffer.length));
            files.add(last);
        }
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        // TODO: implement
        try {
            cache.removeSegment(segmentId);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    synchronized void writeJournals() throws IOException {
        int size = 8 + 4;
        for (String name : journals.keySet()) {
            size += 4 + name.getBytes(UTF_8).length + 16 + 4;
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(JOURNAL_MAGIC);
        buffer.putInt(journals.size());
        for (Map.Entry<String, Journal> entry : journals.entrySet()) {
            byte[] name = entry.getKey().getBytes(UTF_8);
            buffer.putInt(name.length);
            buffer.put(name);
            RecordId head = entry.getValue().getHead();
            buffer.putLong(head.getSegmentId().getMostSignificantBits());
            buffer.putLong(head.getSegmentId().getLeastSignificantBits());
            buffer.putInt(head.getOffset());
        }

        writeEntry(JOURNALS_UUID, buffer.array());
    }

}
