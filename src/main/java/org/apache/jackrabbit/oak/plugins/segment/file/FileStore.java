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
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.segment.AbstractStore;
import org.apache.jackrabbit.oak.plugins.segment.Journal;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class FileStore extends AbstractStore {

    private static final int DEFAULT_MEMORY_CACHE_SIZE = 1 << 28; // 256MB

    private static final long JOURNAL_MAGIC = 0xdf36544212c0cb24L;

    static final UUID JOURNALS_UUID = new UUID(0, 0);

    private static final String FILE_NAME_FORMAT = "%s%05d.tar";

    private final File directory;

    private final int maxFileSize;

    private final boolean memoryMapping;

    private final LinkedList<TarFile> bulkFiles = newLinkedList();

    private final LinkedList<TarFile> treeFiles = newLinkedList();

    private final Map<String, Journal> journals = newHashMap();

    public FileStore(File directory, int maxFileSize, boolean memoryMapping)
            throws IOException {
        this(directory, maxFileSize, DEFAULT_MEMORY_CACHE_SIZE, memoryMapping);
    }

    public FileStore(File directory, int maxFileSize, int cacheSize,
            boolean memoryMapping) throws IOException {
        super(cacheSize);
        checkNotNull(directory).mkdirs();
        this.directory = directory;
        this.maxFileSize = maxFileSize;
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
                treeFiles.add(new TarFile(file, maxFileSize, memoryMapping));
            } else {
                break;
            }
        }

        Segment segment = getWriter().getDummySegment();
        for (TarFile tar : treeFiles) {
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
    public synchronized void close() {
        try {
            super.close();
            for (TarFile file : bulkFiles) {
                file.close();
            }
            bulkFiles.clear();
            for (TarFile file : treeFiles) {
                file.close();
            }
            treeFiles.clear();
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
    protected Segment loadSegment(UUID id) throws Exception {
        for (TarFile file : treeFiles) {
            ByteBuffer buffer = file.readEntry(id);
            if (buffer != null) {
                return new Segment(FileStore.this, id, buffer);
            }
        }
        for (TarFile file : bulkFiles) {
            ByteBuffer buffer = file.readEntry(id);
            if (buffer != null) {
                return new Segment(FileStore.this, id, buffer);
            }
        }
        throw new IllegalStateException("Segment " + id + " not found");
    }

    @Override
    public synchronized void writeSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        try {
            writeEntry(segmentId, data, offset, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeEntry(
            UUID segmentId, byte[] buffer, int offset, int length)
            throws IOException {
        LinkedList<TarFile> files = treeFiles;
        String base = "data";
        if (length == Segment.MAX_SEGMENT_SIZE) {
            files = bulkFiles;
            base = "bulk";
        }
        if (files.isEmpty() || !files.getLast().writeEntry(
                segmentId, buffer, offset, length)) {
            String name = String.format(FILE_NAME_FORMAT, base, files.size());
            File file = new File(directory, name);
            TarFile last = new TarFile(file, maxFileSize, memoryMapping);
            checkState(last.writeEntry(segmentId, buffer, offset, length));
            files.add(last);
        }
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        // TODO: implement
        super.deleteSegment(segmentId);
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

        writeEntry(JOURNALS_UUID, buffer.array(), 0, size);
    }

}
