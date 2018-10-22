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

import static java.lang.Integer.getInteger;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.FULL;
import static org.apache.jackrabbit.oak.segment.file.Reclaimers.newOldReclaimer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.GarbageCollectionStrategy.SuccessfulGarbageCollectionListener;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.jetbrains.annotations.NotNull;

class GarbageCollector {

    /**
     * GC counter for logging purposes
     */
    private static final AtomicInteger GC_COUNT = new AtomicInteger(0);

    /**
     * Minimal interval in milli seconds between subsequent garbage collection
     * cycles. Garbage collection invoked via full compaction will be skipped
     * unless at least the specified time has passed since its last successful
     * invocation.
     */
    private static final long GC_BACKOFF = getInteger("oak.gc.backoff", 10 * 3600 * 1000);

    @NotNull
    private final SegmentGCOptions gcOptions;

    /**
     * {@code GcListener} listening to this instance's gc progress
     */
    @NotNull
    private final PrefixedGCListener gcListener;

    @NotNull
    private final GCJournal gcJournal;

    private final AtomicBoolean sufficientMemory;

    private final FileReaper fileReaper;

    private final TarFiles tarFiles;

    private final SegmentTracker tracker;

    private final SegmentReader segmentReader;

    private final Supplier<Revisions> revisionsSupplier;

    private final BlobStore blobStore;

    private final SegmentCache segmentCache;

    private final SegmentWriter segmentWriter;

    private final FileStoreStats stats;

    private final Canceller cancel;

    private final Flusher flusher;

    private final SegmentWriterFactory segmentWriterFactory;

    private final GCNodeWriteMonitor compactionMonitor;


    /**
     * Timestamp of the last time full or tail compaction was successfully
     * invoked. 0 if never.
     */
    private long lastSuccessfullGC;

    /**
     * Last compaction type used to determine which predicate to use during
     * cleanup. Defaults to {@link SegmentGCOptions.GCType#FULL FULL}, which is
     * conservative and safe in case it does not match the real type (e.g.
     * because of a system restart).
     */
    private SegmentGCOptions.GCType lastCompactionType = FULL;

    private volatile boolean cancelRequested;

    GarbageCollector(
        SegmentGCOptions gcOptions,
        GCListener gcListener,
        GCJournal gcJournal,
        AtomicBoolean sufficientMemory,
        FileReaper fileReaper,
        TarFiles tarFiles,
        SegmentTracker tracker,
        SegmentReader segmentReader,
        Supplier<Revisions> revisionsSupplier,
        BlobStore blobStore,
        SegmentCache segmentCache,
        SegmentWriter segmentWriter,
        FileStoreStats stats,
        Canceller canceller,
        Flusher flusher,
        SegmentWriterFactory segmentWriterFactory
    ) {
        this.gcOptions = gcOptions;
        this.gcListener = new PrefixedGCListener(gcListener, GC_COUNT);
        this.gcJournal = gcJournal;
        this.sufficientMemory = sufficientMemory;
        this.fileReaper = fileReaper;
        this.tarFiles = tarFiles;
        this.tracker = tracker;
        this.segmentReader = segmentReader;
        this.revisionsSupplier = revisionsSupplier;
        this.blobStore = blobStore;
        this.segmentCache = segmentCache;
        this.segmentWriter = segmentWriter;
        this.stats = stats;
        this.cancel = canceller.withCondition("cancelled by user", () -> cancelRequested);
        this.flusher = flusher;
        this.segmentWriterFactory = segmentWriterFactory;
        this.compactionMonitor = new GCNodeWriteMonitor(gcOptions.getGcLogInterval(), gcListener);
    }

    private GCGeneration getGcGeneration() {
        return revisionsSupplier.get().getHead().getSegmentId().getGcGeneration();
    }

    GCNodeWriteMonitor getGCNodeWriteMonitor() {
        return compactionMonitor;
    }

    private GarbageCollectionStrategy.Context newGarbageCollectionContext(int gcCount) {
        return new GarbageCollectionStrategy.Context() {

            @Override
            public SegmentGCOptions getGCOptions() {
                return gcOptions;
            }

            @Override
            public GCListener getGCListener() {
                return gcListener;
            }

            @Override
            public Revisions getRevisions() {
                return revisionsSupplier.get();
            }

            @Override
            public GCJournal getGCJournal() {
                return gcJournal;
            }

            @Override
            public SegmentTracker getSegmentTracker() {
                return tracker;
            }

            @Override
            public SegmentWriterFactory getSegmentWriterFactory() {
                return segmentWriterFactory;
            }

            @Override
            public GCNodeWriteMonitor getCompactionMonitor() {
                return compactionMonitor;
            }

            @Override
            public BlobStore getBlobStore() {
                return blobStore;
            }

            @Override
            public Canceller getCanceller() {
                return cancel;
            }

            @Override
            public long getLastSuccessfulGC() {
                return lastSuccessfullGC;
            }

            @Override
            public TarFiles getTarFiles() {
                return tarFiles;
            }

            @Override
            public AtomicBoolean getSufficientMemory() {
                return sufficientMemory;
            }

            @Override
            public FileReaper getFileReaper() {
                return fileReaper;
            }

            @Override
            public SuccessfulGarbageCollectionListener getSuccessfulGarbageCollectionListener() {
                return () -> lastSuccessfullGC = System.currentTimeMillis();
            }

            @Override
            public SuccessfulCompactionListener getSuccessfulCompactionListener() {
                return type -> lastCompactionType = type;
            }

            @Override
            public Flusher getFlusher() {
                return flusher;
            }

            @Override
            public long getGCBackOff() {
                return GC_BACKOFF;
            }

            @Override
            public SegmentGCOptions.GCType getLastCompactionType() {
                return lastCompactionType;
            }

            @Override
            public int getGCCount() {
                return gcCount;
            }

            @Override
            public SegmentCache getSegmentCache() {
                return segmentCache;
            }

            @Override
            public FileStoreStats getFileStoreStats() {
                return stats;
            }

            @Override
            public SegmentReader getSegmentReader() {
                return segmentReader;
            }

        };
    }

    synchronized void run(GarbageCollectionStrategy strategy) throws IOException {
        cancelRequested = false;
        strategy.collectGarbage(newGarbageCollectionContext(GC_COUNT.incrementAndGet()));
    }

    synchronized void runFull(GarbageCollectionStrategy strategy) throws IOException {
        cancelRequested = false;
        strategy.collectFullGarbage(newGarbageCollectionContext(GC_COUNT.incrementAndGet()));
    }

    synchronized void runTail(GarbageCollectionStrategy strategy) throws IOException {
        cancelRequested = false;
        strategy.collectTailGarbage(newGarbageCollectionContext(GC_COUNT.incrementAndGet()));
    }

    synchronized CompactionResult compactFull(GarbageCollectionStrategy strategy) throws IOException {
        cancelRequested = false;
        return strategy.compactFull(newGarbageCollectionContext(GC_COUNT.get()));
    }

    synchronized CompactionResult compactTail(GarbageCollectionStrategy strategy) throws IOException {
        cancelRequested = false;
        return strategy.compactTail(newGarbageCollectionContext(GC_COUNT.get()));
    }

    synchronized List<String> cleanup(GarbageCollectionStrategy strategy) throws IOException {
        cancelRequested = false;
        return strategy.cleanup(newGarbageCollectionContext(GC_COUNT.get()));
    }

    /**
     * Finds all external blob references that are currently accessible in this
     * repository and adds them to the given collector. Useful for collecting
     * garbage in an external data store.
     * <p>
     * Note that this method only collects blob references that are already
     * stored in the repository (at the time when this method is called), so the
     * garbage collector will need some other mechanism for tracking in-memory
     * references and references stored while this method is running.
     *
     * @param collector reference collector called back for each blob reference
     *                  found
     */
    synchronized void collectBlobReferences(Consumer<String> collector) throws IOException {
        segmentWriter.flush();
        tarFiles.collectBlobReferences(collector,
            newOldReclaimer(lastCompactionType, getGcGeneration(), gcOptions.getRetainedGenerations()));
    }

    void cancel() {
        cancelRequested = true;
    }

}
