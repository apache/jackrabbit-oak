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

import static org.apache.jackrabbit.oak.segment.data.SegmentData.newSegmentData;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.Segment.RecordConsumer;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.file.tar.EntryRecovery;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.IOMonitor;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.file.tar.TarRecovery;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The storage implementation for tar files.
 */
public abstract class AbstractFileStore implements SegmentStore, Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractFileStore.class);

    private static final String MANIFEST_FILE_NAME = "manifest";

    /**
     * The minimum supported store version. It is possible for an implementation
     * to support in a transparent and backwards-compatible way older versions
     * of a repository. In this case, the minimum supported store version
     * identifies the store format that can still be processed by the
     * implementation. The minimum store version has to be greater than zero and
     * less than or equal to the maximum store version.
     */
    private static final int MIN_STORE_VERSION = 1;

    /**
     * The maximum supported store version. It is possible for an implementation
     * to support in a transparent and forwards-compatible way newer version of
     * a repository. In this case, the maximum supported store version
     * identifies the store format that can still be processed by the
     * implementation. The maximum supported store version has to be greater
     * than zero and greater than or equal to the minimum store version.
     */
    private static final int MAX_STORE_VERSION = 2;

    static ManifestChecker newManifestChecker(File directory, boolean strictVersionCheck) {
        return ManifestChecker.newManifestChecker(
                new File(directory, MANIFEST_FILE_NAME),
                notEmptyDirectory(directory),
                strictVersionCheck ? MAX_STORE_VERSION : MIN_STORE_VERSION,
                MAX_STORE_VERSION
        );
    }

    private static boolean notEmptyDirectory(File path) {
        Collection<File> entries = FileUtils.listFiles(path, new String[] {"tar"}, false);
        return !entries.isEmpty();
    }

    @Nonnull
    final SegmentTracker tracker;

    @Nonnull
    final CachingSegmentReader segmentReader;

    final File directory;

    private final BlobStore blobStore;

    final boolean memoryMapping;

    @Nonnull
    final SegmentCache segmentCache;

    final TarRecovery recovery = new TarRecovery() {

        @Override
        public void recoverEntry(UUID uuid, byte[] data, EntryRecovery entryRecovery) throws IOException {
            writeSegment(uuid, data, entryRecovery);
        }

    };

    protected final IOMonitor ioMonitor;

    AbstractFileStore(final FileStoreBuilder builder) {
        this.directory = builder.getDirectory();
        this.tracker = new SegmentTracker(new SegmentIdFactory() {
            @Override @Nonnull
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(AbstractFileStore.this, msb, lsb, segmentCache::recordHit);
            }
        });
        this.blobStore = builder.getBlobStore();
        this.segmentCache = new SegmentCache(builder.getSegmentCacheSize());
        this.segmentReader = new CachingSegmentReader(this::getWriter, blobStore, builder.getStringCacheSize(), builder.getTemplateCacheSize());
        this.memoryMapping = builder.getMemoryMapping();
        this.ioMonitor = builder.getIOMonitor();
    }

    static SegmentNotFoundException asSegmentNotFoundException(ExecutionException e, SegmentId id) {
        if (e.getCause() instanceof SegmentNotFoundException) {
            return (SegmentNotFoundException) e.getCause();
        }
        return new SegmentNotFoundException(id, e);
    }

    @Nonnull
    public CacheStatsMBean getSegmentCacheStats() {
        return segmentCache.getCacheStats();
    }

    @Nonnull
    public CacheStatsMBean getStringCacheStats() {
        return segmentReader.getStringCacheStats();
    }

    @Nonnull
    public CacheStatsMBean getTemplateCacheStats() {
        return segmentReader.getTemplateCacheStats();
    }

    @Nonnull
    public abstract SegmentWriter getWriter();

    @Nonnull
    public SegmentReader getReader() {
        return segmentReader;
    }

    @Nonnull
    public SegmentIdProvider getSegmentIdProvider() {
        return tracker;
    }

    /**
     * @return the {@link Revisions} object bound to the current store.
     */
    public abstract Revisions getRevisions();

    /**
     * Convenience method for accessing the root node for the current head.
     * This is equivalent to
     * <pre>
     * fileStore.getReader().readHeadState(fileStore.getRevisions())
     * </pre>
     * @return the current head node state
     */
    @Nonnull
    public SegmentNodeState getHead() {
        return segmentReader.readHeadState(getRevisions());
    }

    /**
     * @return  the external BlobStore (if configured) with this store, {@code null} otherwise.
     */
    @CheckForNull
    public BlobStore getBlobStore() {
        return blobStore;
    }

    private void writeSegment(UUID id, byte[] data, EntryRecovery w) throws IOException {
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        GCGeneration generation = Segment.getGcGeneration(newSegmentData(buffer), id);
        w.recoverEntry(msb, lsb, data, 0, data.length, generation);
        if (SegmentId.isDataSegmentId(lsb)) {
            Segment segment = new Segment(tracker, segmentReader, tracker.newSegmentId(msb, lsb), buffer);
            populateTarGraph(segment, w);
            populateTarBinaryReferences(segment, w);
        }
    }

    private static void populateTarGraph(Segment segment, EntryRecovery w) {
        UUID from = segment.getSegmentId().asUUID();
        for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
            w.recoverGraphEdge(from, segment.getReferencedSegmentId(i));
        }
    }

    private static void populateTarBinaryReferences(final Segment segment, final EntryRecovery w) {
        final GCGeneration generation = segment.getGcGeneration();
        final UUID id = segment.getSegmentId().asUUID();
        segment.forEachRecord((number, type, offset) -> {
            if (type == RecordType.BLOB_ID) {
                w.recoverBinaryReference(generation, id, SegmentBlob.readBlobId(segment, number));
            }
        });
    }

    static Set<UUID> readReferences(Segment segment) {
        Set<UUID> references = new HashSet<>();
        for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
            references.add(segment.getReferencedSegmentId(i));
        }
        return references;
    }

    static Set<String> readBinaryReferences(final Segment segment) {
        final Set<String> binaryReferences = new HashSet<>();
        segment.forEachRecord(new RecordConsumer() {

            @Override
            public void consume(int number, RecordType type, int offset) {
                if (type == RecordType.BLOB_ID) {
                    binaryReferences.add(SegmentBlob.readBlobId(segment, number));
                }
            }

        });
        return binaryReferences;
    }

    static void closeAndLogOnFail(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ioe) {
                // ignore and log
                log.error(ioe.getMessage(), ioe);
            }
        }
    }

    Segment readSegmentUncached(TarFiles tarFiles, SegmentId id) {
        ByteBuffer buffer = tarFiles.readSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
        if (buffer == null) {
            throw new SegmentNotFoundException(id);
        }
        return new Segment(tracker, segmentReader, id, buffer);
    }

}
