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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentVersion;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class FileStoreProcBackend implements Backend {

    private final AbstractFileStore fileStore;

    private final SegmentNodeStorePersistence persistence;

    private final SegmentArchiveManager archiveManager;

    FileStoreProcBackend(AbstractFileStore fileStore, SegmentNodeStorePersistence persistence) throws IOException {
        this.fileStore = fileStore;
        this.persistence = persistence;
        this.archiveManager = persistence.createArchiveManager(true, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter());
    }

    @Override
    public boolean tarExists(String name) {
        try {
            return archiveManager.listArchives().contains(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Long> getTarSize(String name) {
        try (SegmentArchiveReader reader = archiveManager.open(name)) {
            return Optional.ofNullable(reader).map(SegmentArchiveReader::length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> getTarNames() {
        try {
            return archiveManager.listArchives();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean segmentExists(String name, String segmentId) {
        try (SegmentArchiveReader reader = archiveManager.open(name)) {
            if (reader == null) {
                return false;
            }
            return segmentExists(reader, UUID.fromString(segmentId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean segmentExists(SegmentArchiveReader reader, UUID id) {
        return reader.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    @Override
    public Iterable<String> getSegmentIds(String name) {
        try (SegmentArchiveReader reader = archiveManager.open(name)) {
            if (reader == null) {
                return Collections.emptyList();
            }
            return getSegmentIds(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterable<String> getSegmentIds(SegmentArchiveReader reader) {
        List<String> ids = new ArrayList<>();
        for (SegmentArchiveEntry entry : reader.listSegments()) {
            ids.add(new UUID(entry.getMsb(), entry.getLsb()).toString());
        }
        return ids;
    }

    private Optional<org.apache.jackrabbit.oak.segment.Segment> readSegment(String id) {
        return readSegment(UUID.fromString(id));
    }

    private Optional<org.apache.jackrabbit.oak.segment.Segment> readSegment(UUID id) {
        return readSegment(fileStore.getSegmentIdProvider().newSegmentId(
            id.getMostSignificantBits(),
            id.getLeastSignificantBits()
        ));
    }

    private Optional<org.apache.jackrabbit.oak.segment.Segment> readSegment(SegmentId id) {
        try {
            return Optional.of(fileStore.readSegment(id));
        } catch (SegmentNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Segment> getSegment(String id) {
        return readSegment(id).map(segment -> new Segment() {

            @Override
            public int getGeneration() {
                return segment.getGcGeneration().getGeneration();
            }

            @Override
            public int getFullGeneration() {
                return segment.getGcGeneration().getFullGeneration();
            }

            @Override
            public boolean isCompacted() {
                return segment.getGcGeneration().isCompacted();
            }

            @Override
            public int getLength() {
                return segment.size();
            }

            @Override
            public int getVersion() {
                return SegmentVersion.asByte(segment.getSegmentVersion());
            }

            @Override
            public boolean isDataSegment() {
                return segment.getSegmentId().isDataSegmentId();
            }

            @Override
            public Optional<String> getInfo() {
                return Optional.ofNullable(segment.getSegmentInfo());
            }

        });
    }

    @Override
    public Optional<InputStream> getSegmentData(String segmentId) {
        return readSegment(segmentId).map(segment -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            try {
                segment.writeTo(out);
            } catch (IOException e) {
                return null;
            }

            return new ByteArrayInputStream(out.toByteArray());
        });
    }

    @Override
    public Optional<Iterable<String>> getSegmentReferences(String segmentId) {
        return readSegment(segmentId).map(segment -> {
            List<String> references = new ArrayList<>(segment.getReferencedSegmentIdCount());

            for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
                references.add(segment.getReferencedSegmentId(i).toString());
            }

            return references;
        });
    }

    @Override
    public Optional<Iterable<Record>> getSegmentRecords(String segmentId) {
        return readSegment(segmentId).map(segment -> {
            List<Record> records = new ArrayList<>();

            segment.forEachRecord((number, type, offset) -> {
                records.add(new Record() {

                    @Override
                    public int getNumber() {
                        return number;
                    }

                    @Override
                    public String getSegmentId() {
                        return segmentId;
                    }

                    @Override
                    public int getOffset() {
                        return offset;
                    }

                    @Override
                    public int getAddress() {
                        return segment.getAddress(offset);
                    }

                    @Override
                    public String getType() {
                        return type.name();
                    }

                    @Override
                    public Optional<NodeState> getRoot() {
                        if (RecordType.NODE == type) {
                            RecordId id = new RecordId(segment.getSegmentId(), number);
                            return Optional.of(fileStore.getReader().readNode(id));
                        } else {
                            return Optional.empty();
                        }
                    }
                });
            });

            return records;
        });
    }

    @Override
    public boolean commitExists(String handle) {
        long timestamp = Long.parseLong(handle);

        try (JournalReader reader = new JournalReader(persistence.getJournalFile())) {
            for (JournalEntry entry : iterable(reader)) {
                if (entry.getTimestamp() == timestamp) {
                    return true;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    private <T> Iterable<T> iterable(Iterator<T> i) {
        return () -> i;
    }

    @Override
    public Iterable<String> getCommitHandles() {
        try (JournalReader reader = new JournalReader(persistence.getJournalFile())) {
            List<String> handles = new ArrayList<>();
            for (JournalEntry entry : iterable(reader)) {
                handles.add(Long.toString(entry.getTimestamp()));
            }
            return handles;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Commit> getCommit(String handle) {
        JournalEntry entry;

        try (JournalReader reader = new JournalReader(persistence.getJournalFile())) {
            entry = getEntry(reader, Long.parseLong(handle));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (entry == null) {
            return Optional.empty();
        }

        return Optional.of(new Commit() {

            @Override
            public long getTimestamp() {
                return entry.getTimestamp();
            }

            @Override
            public String getRevision() {
                return entry.getRevision();
            }

            @Override
            public Optional<NodeState> getRoot() {
                RecordId id = RecordId.fromString(fileStore.getSegmentIdProvider(), entry.getRevision());
                return Optional.of(fileStore.getReader().readNode(id));
            }

        });
    }

    private JournalEntry getEntry(JournalReader reader, long timestamp) {
        for (JournalEntry entry : iterable(reader)) {
            if (entry.getTimestamp() == timestamp) {
                return entry;
            }
        }
        return null;
    }

}
