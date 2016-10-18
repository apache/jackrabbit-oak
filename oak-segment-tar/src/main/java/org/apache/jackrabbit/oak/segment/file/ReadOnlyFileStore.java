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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptyMap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentGraph.SegmentGraphVisitor;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
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

    private final List<TarReader> readers;

    private ReadOnlyRevisions revisions;

    private RecordId currentHead;

    ReadOnlyFileStore(FileStoreBuilder builder)
            throws InvalidFileStoreVersionException, IOException {
        super(builder);

        Map<Integer, Map<Character, File>> map = collectFiles(directory);

        if (map.size() > 0) {
            checkManifest(openManifest());
        }

        this.readers = newArrayListWithCapacity(map.size());
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);
        for (int i = indices.length - 1; i >= 0; i--) {
            // only try to read-only recover the latest file as that might
            // be the *only* one still being accessed by a writer
            boolean recover = i == indices.length - 1;
            readers.add(TarReader.openRO(map.get(indices[i]), memoryMapping, recover, recovery));
        }
        log.info("TarMK ReadOnly opened: {} (mmap={})", directory,
                memoryMapping);
    }

    ReadOnlyFileStore bind(@Nonnull ReadOnlyRevisions revisions)
            throws IOException {
        this.revisions = revisions;
        this.revisions.bind(this);
        currentHead = revisions.getHead();
        return this;
    }

    /**
     * Go to the specified {@code revision}
     * 
     * @param revision
     */
    public void setRevision(String revision) {
        RecordId newHead = RecordId.fromString(this, revision);
        if (revisions.setHead(currentHead, newHead)) {
            currentHead = newHead;
        }
    }

    /**
     * Include the ids of all segments transitively reachable through forward
     * references from {@code referencedIds}. See OAK-3864.
     */
    private static void includeForwardReferences(Iterable<TarReader> readers,
            Set<UUID> referencedIds) throws IOException {
        Set<UUID> fRefs = newHashSet(referencedIds);
        do {
            // Add direct forward references
            for (TarReader reader : readers) {
                reader.calculateForwardReferences(fRefs);
                if (fRefs.isEmpty()) {
                    break; // Optimisation: bail out if no references left
                }
            }
            // ... as long as new forward references are found.
        } while (referencedIds.addAll(fRefs));
    }

    /**
     * Build the graph of segments reachable from an initial set of segments
     * 
     * @param roots
     *            the initial set of segments
     * @param visitor
     *            visitor receiving call back while following the segment graph
     * @throws IOException
     */
    public void traverseSegmentGraph(@Nonnull Set<UUID> roots,
            @Nonnull SegmentGraphVisitor visitor) throws IOException {

        List<TarReader> readers = this.readers;
        includeForwardReferences(readers, roots);
        for (TarReader reader : readers) {
            reader.traverseSegmentGraph(checkNotNull(roots),
                    checkNotNull(visitor));
        }
    }

    @Override
    public void writeSegment(SegmentId id, byte[] data, int offset, int length) {
        throw new UnsupportedOperationException("Read Only Store");
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        for (TarReader reader : readers) {
            if (reader.containsEntry(msb, lsb)) {
                return true;
            }
        }
        return false;
    }

    @Override
    @Nonnull
    public Segment readSegment(final SegmentId id) {
        try {
            return segmentCache.getSegment(id, new Callable<Segment>() {
                @Override
                public Segment call() throws Exception {
                    long msb = id.getMostSignificantBits();
                    long lsb = id.getLeastSignificantBits();

                    for (TarReader reader : readers) {
                        try {
                            ByteBuffer buffer = reader.readEntry(msb, lsb);
                            if (buffer != null) {
                                return new Segment(ReadOnlyFileStore.this, segmentReader, id, buffer);
                            }
                        } catch (IOException e) {
                            log.warn("Failed to read from tar file {}", reader, e);
                        }
                    }
                    throw new SegmentNotFoundException(id);
                }
            });
        } catch (ExecutionException e) {
            throw e.getCause() instanceof SegmentNotFoundException
                ? (SegmentNotFoundException) e.getCause()
                : new SegmentNotFoundException(id, e);
        }
    }

    @Override
    public void close() {
        try {
            revisions.close();
            for (TarReader reader : readers) {
                closeAndLogOnFail(reader);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close the TarMK at "
                    + directory, e);
        }
        System.gc(); // for any memory-mappings that are no longer used
        log.info("TarMK closed: {}", directory);
    }

    @Override
    public SegmentWriter getWriter() {
        return null;
    }

    public Map<String, Set<UUID>> getTarReaderIndex() {
        Map<String, Set<UUID>> index = new HashMap<String, Set<UUID>>();
        for (TarReader reader : readers) {
            index.put(reader.getFile().getAbsolutePath(), reader.getUUIDs());
        }
        return index;
    }

    public Map<UUID, List<UUID>> getTarGraph(String fileName)
            throws IOException {
        for (TarReader reader : readers) {
            if (fileName.equals(reader.getFile().getName())) {
                Map<UUID, List<UUID>> graph = newHashMap();
                for (UUID uuid : reader.getUUIDs()) {
                    graph.put(uuid, null);
                }
                Map<UUID, List<UUID>> g = reader.getGraph(false);
                if (g != null) {
                    graph.putAll(g);
                }
                return graph;
            }
        }
        return emptyMap();
    }

    public Iterable<SegmentId> getSegmentIds() {
        List<SegmentId> ids = newArrayList();
        for (TarReader reader : readers) {
            for (UUID uuid : reader.getUUIDs()) {
                long msb = uuid.getMostSignificantBits();
                long lsb = uuid.getLeastSignificantBits();
                ids.add(newSegmentId(msb, lsb));
            }
        }
        return ids;
    }

    @Override
    public ReadOnlyRevisions getRevisions() {
        return revisions;
    }
}
