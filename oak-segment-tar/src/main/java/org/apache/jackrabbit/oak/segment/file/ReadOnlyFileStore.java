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

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A read only {@link AbstractFileStore} implementation that supports going back
 * to old revisions.
 * <p>
 * All write methods are no-ops.
 */
public class ReadOnlyFileStore extends AbstractFileStore {

    private static final Logger log = LoggerFactory
            .getLogger(ReadOnlyFileStore.class);

    private final TarFiles tarFiles;

    @Nonnull
    private final SegmentWriter writer;

    private ReadOnlyRevisions revisions;

    private RecordId currentHead;

    ReadOnlyFileStore(FileStoreBuilder builder) throws InvalidFileStoreVersionException, IOException {
        super(builder);

        newManifestChecker(directory).checkManifest();

        tarFiles = TarFiles.builder()
                .withDirectory(directory)
                .withTarRecovery(recovery)
                .withIOMonitor(ioMonitor)
                .withMemoryMapping(memoryMapping)
                .withReadOnly()
                .build();

        writer = defaultSegmentWriterBuilder("read-only").withoutCache().build(this);
        log.info("TarMK ReadOnly opened: {} (mmap={})", directory,
                memoryMapping);
    }

    ReadOnlyFileStore bind(@Nonnull ReadOnlyRevisions revisions) throws IOException {
        this.revisions = revisions;
        this.revisions.bind(this, tracker);
        currentHead = revisions.getHead();
        return this;
    }

    /**
     * Go to the specified {@code revision}
     * 
     * @param revision
     */
    public void setRevision(String revision) {
        RecordId newHead = RecordId.fromString(tracker, revision);
        if (revisions.setHead(currentHead, newHead)) {
            currentHead = newHead;
        }
    }

    @Override
    public void writeSegment(SegmentId id, byte[] data, int offset, int length) {
        throw new UnsupportedOperationException("Read Only Store");
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        return tarFiles.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    @Override
    @Nonnull
    public Segment readSegment(final SegmentId id) {
        try {
            return segmentCache.getSegment(id, new Callable<Segment>() {
                @Override
                public Segment call() throws Exception {
                    return readSegmentUncached(tarFiles, id);
                }
            });
        } catch (ExecutionException e) {
            throw asSegmentNotFoundException(e, id);
        }
    }

    @Override
    public void close() {
        Closer closer = Closer.create();
        closer.register(tarFiles);
        closer.register(revisions);
        closeAndLogOnFail(closer);
        System.gc(); // for any memory-mappings that are no longer used
        log.info("TarMK closed: {}", directory);
    }

    @Nonnull
    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    public Map<String, Set<UUID>> getTarReaderIndex() {
        return tarFiles.getIndices();
    }

    public Map<UUID, Set<UUID>> getTarGraph(String fileName) throws IOException {
        return tarFiles.getGraph(fileName);
    }

    public Iterable<SegmentId> getSegmentIds() {
        List<SegmentId> ids = new ArrayList<>();
        for (UUID id : tarFiles.getSegmentIds()) {
            long msb = id.getMostSignificantBits();
            long lsb = id.getLeastSignificantBits();
            ids.add(tracker.newSegmentId(msb, lsb));
        }
        return ids;
    }

    @Override
    public ReadOnlyRevisions getRevisions() {
        return revisions;
    }

    public Set<SegmentId> getReferencedSegmentIds() {
        return tracker.getReferencedSegmentIds();
    }
}
