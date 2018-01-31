/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobTracker.Options;
import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;

public class ActiveDeletedBlobCollectorFactory {
    public interface ActiveDeletedBlobCollector {
        /**
         * @return an instance of {@link BlobDeletionCallback} that can be used to track deleted blobs
         */
        BlobDeletionCallback getBlobDeletionCallback();

        void purgeBlobsDeleted(long before, GarbageCollectableBlobStore blobStore);

        void cancelBlobCollection();

        void flagActiveDeletionUnsafe(boolean toFlag);

        boolean isActiveDeletionUnsafe();
    }

    public static ActiveDeletedBlobCollector NOOP = new ActiveDeletedBlobCollector() {
        private volatile boolean activeDeletionUnsafe = false;

        @Override
        public BlobDeletionCallback getBlobDeletionCallback() {
            return BlobDeletionCallback.NOOP;
        }

        @Override
        public void purgeBlobsDeleted(long before, GarbageCollectableBlobStore blobStore) {

        }

        @Override
        public void cancelBlobCollection() {

        }

        @Override
        public void flagActiveDeletionUnsafe(boolean toFlag) {
            activeDeletionUnsafe = toFlag;
        }

        @Override
        public boolean isActiveDeletionUnsafe() {
            return activeDeletionUnsafe;
        }
    };

    public interface BlobDeletionCallback extends IndexCommitCallback {
        /**
         * Tracks deleted blobs. From the pov of this interface, blobId is an opaque string
         * that needs to be tracked.
         * @param blobId blobId representing deleted blob. In theory, it has nothing to do with
         *               blobs though.
         * @param ids Information that can be useful for debugging - this is not used for purging
         *            blobs.
         */
        void deleted(String blobId, Iterable<String> ids);

        boolean isMarkingForActiveDeletionUnsafe();

        BlobDeletionCallback NOOP = new BlobDeletionCallback() {
            @Override
            public void deleted(String blobId, Iterable<String> ids) {
            }

            @Override
            public void commitProgress(IndexProgress indexProgress) {
            }

            @Override
            public boolean isMarkingForActiveDeletionUnsafe() {
                return ActiveDeletedBlobCollectorFactory.NOOP.isActiveDeletionUnsafe();
            }
        };
    }

    public static ActiveDeletedBlobCollector newInstance(@Nonnull File rootDirectory,
                                                         ExecutorService executorService) {
        try {
            FileUtils.forceMkdir(rootDirectory);
        } catch (IOException ioe) {
            ActiveDeletedBlobCollectorImpl.LOG.warn("Disabling active blob collector as we couldn't not create folder: "
                    + rootDirectory, ioe);
            return NOOP;
        }
        if(!rootDirectory.canRead() || !rootDirectory.canWrite() || !rootDirectory.canExecute()) {
            ActiveDeletedBlobCollectorImpl.LOG.warn("Insufficient access in directory - {}. Disabling active blob collector",
                    rootDirectory);
            return NOOP;
        }
        return new ActiveDeletedBlobCollectorImpl(rootDirectory, executorService);
    }

    /**
     * Blob collector which takes *no* guarantees about checking whether the
     * blob might be referred by paths other than one for which it is notified
     * due deleted blob
     */
    static class ActiveDeletedBlobCollectorImpl implements ActiveDeletedBlobCollector {
        private static PerfLogger PERF_LOG = new PerfLogger(
                LoggerFactory.getLogger(ActiveDeletedBlobCollectorImpl.class.getName() + ".perf"));
        private static Logger LOG = LoggerFactory.getLogger(ActiveDeletedBlobCollectorImpl.class.getName());

        private final Clock clock;

        private final File rootDirectory;

        private final ExecutorService executorService;

        private volatile boolean cancelled;
        private volatile boolean activeDeletionUnsafe = false;


        private static final String BLOB_FILE_PATTERN_PREFIX = "blobs-";
        private static final String BLOB_FILE_PATTERN_SUFFIX = ".txt";
        private static final String BLOB_FILE_PATTERN = BLOB_FILE_PATTERN_PREFIX + "%s" + BLOB_FILE_PATTERN_SUFFIX;
        private static final IOFileFilter blobFileNameFilter = new RegexFileFilter("blobs-.*\\.txt");

        private final BlockingQueue<BlobIdInfoStruct> deletedBlobs;
        private final DeletedBlobsFileWriter deletedBlobsFileWriter;

        /**
         * @param rootDirectory directory that may be used by this instance to
         *                      keep temporary data (e.g. reported deleted blob-ids).
         * @param executorService executor service to asynchronously flush deleted blobs
         *                        to a file.
         */
        ActiveDeletedBlobCollectorImpl(@Nonnull File rootDirectory, @Nonnull ExecutorService executorService) {
            this(Clock.SIMPLE, rootDirectory, executorService);
        }

        ActiveDeletedBlobCollectorImpl(Clock clock, @Nonnull File rootDirectory,
                                       @Nonnull ExecutorService executorService) {
            this.clock = clock;
            this.rootDirectory = rootDirectory;
            this.executorService = executorService;
            this.deletedBlobs = new LinkedBlockingQueue<>(100000);
            this.deletedBlobsFileWriter = new DeletedBlobsFileWriter();
        }

        /**
         * Purges blobs form blob-store which were tracked earlier to deleted.
         * @param before only purge blobs which were deleted before this timestamps
         * @param blobStore used to purge blobs/chunks
         */
        public void purgeBlobsDeleted(long before, @Nonnull GarbageCollectableBlobStore blobStore) {
            cancelled = false;
            long start = clock.getTime();
            LOG.info("Starting purge of blobs deleted before {}", before);
            long numBlobsDeleted = 0;
            long numChunksDeleted = 0;

            File idTempDeleteFile = null;
            BufferedWriter idTempDeleteWriter = null;
            // If blob store support blob tracking
            boolean blobIdsTracked = blobStore instanceof BlobTrackingStore;

            if (blobIdsTracked) {
                try {
                    idTempDeleteFile = File.createTempFile("idTempDelete", null, rootDirectory);
                    idTempDeleteWriter = Files.newWriter(idTempDeleteFile, Charsets.UTF_8);
                } catch (Exception e) {
                    LOG.warn("Unable to open a writer to a temp file, will ignore tracker sync");
                    blobIdsTracked = false;
                }
            }

            long lastCheckedBlobTimestamp = readLastCheckedBlobTimestamp();
            long lastDeletedBlobTimestamp = lastCheckedBlobTimestamp;
            String currInUseFileName = deletedBlobsFileWriter.inUseFileName;
            deletedBlobsFileWriter.releaseInUseFile();
            for (File deletedBlobListFile : FileUtils.listFiles(rootDirectory, blobFileNameFilter, null)) {
                if (cancelled) {
                    break;
                }
                if (deletedBlobListFile.getName().equals(deletedBlobsFileWriter.inUseFileName)) {
                    continue;
                }
                LOG.debug("Purging blobs from {}", deletedBlobListFile);
                long timestamp;
                try {
                    timestamp = getTimestampFromBlobFileName(deletedBlobListFile.getName());
                } catch (IllegalArgumentException iae) {
                    LOG.warn("Couldn't extract timestamp from filename - " + deletedBlobListFile, iae);
                    continue;
                }
                if (timestamp < before) {
                    LineIterator blobLineIter = null;
                    try {
                        blobLineIter = FileUtils.lineIterator(deletedBlobListFile);
                        while (blobLineIter.hasNext()) {
                            if (cancelled) {
                                break;
                            }
                            String deletedBlobLine = blobLineIter.next();

                            String[] parsedDeletedBlobIdLine = deletedBlobLine.split("\\|", 3);
                            if (parsedDeletedBlobIdLine.length != 3) {
                                LOG.warn("Unparseable line ({}) in file {}. It won't be retried.",
                                        parsedDeletedBlobIdLine, deletedBlobListFile);
                            } else {
                                String deletedBlobId = parsedDeletedBlobIdLine[0];
                                try {
                                    long blobDeletionTimestamp = Long.valueOf(parsedDeletedBlobIdLine[1]);

                                    if (blobDeletionTimestamp < lastCheckedBlobTimestamp) {
                                        continue;
                                    }

                                    if (blobDeletionTimestamp >= before) {
                                        break;
                                    }

                                    lastDeletedBlobTimestamp = Math.max(lastDeletedBlobTimestamp, blobDeletionTimestamp);

                                    List<String> chunkIds = Lists.newArrayList(blobStore.resolveChunks(deletedBlobId));
                                    if (chunkIds.size() > 0) {
                                        long deleted = blobStore.countDeleteChunks(chunkIds, 0);
                                        if (deleted < 1) {
                                            LOG.warn("Blob {} in file {} not deleted", deletedBlobId, deletedBlobListFile);
                                        } else {
                                            numBlobsDeleted++;
                                            numChunksDeleted += deleted;

                                            if (blobIdsTracked) {
                                                // Save deleted chunkIds to a temporary file
                                                for (String id : chunkIds) {
                                                    FileIOUtils.writeAsLine(idTempDeleteWriter, id, true);
                                                }
                                            }
                                        }
                                    }
                                } catch (NumberFormatException nfe) {
                                    LOG.warn("Couldn't parse blobTimestamp(" + parsedDeletedBlobIdLine[1] +
                                            "). deletedBlobLine - " + deletedBlobLine +
                                            "; file - " + deletedBlobListFile.getName(), nfe);
                                } catch (DataStoreException dse) {
                                    LOG.debug("Exception occurred while attempting to delete blob " + deletedBlobId, dse);
                                } catch (Exception e) {
                                    LOG.warn("Exception occurred while attempting to delete blob " + deletedBlobId, e);
                                }
                            }
                        }
                    } catch (IOException ioe) {
                        //log error and continue
                        LOG.warn("Couldn't read deleted blob list file - " + deletedBlobListFile, ioe);
                    } finally {
                        LineIterator.closeQuietly(blobLineIter);
                    }

                    // OAK-6314 revealed that blobs appended might not be immediately available. So, we'd skip
                    // the file that was being processed when purge started - next cycle would re-process and
                    // delete
                    if (!deletedBlobListFile.getName().equals(currInUseFileName)) {
                        if (!deletedBlobListFile.delete()) {
                            LOG.warn("File {} couldn't be deleted while all blobs listed in it have been purged", deletedBlobListFile);
                        } else {
                            LOG.debug("File {} deleted", deletedBlobListFile);
                        }
                    }
                } else {
                    LOG.debug("Skipping {} as its timestamp is newer than {}", deletedBlobListFile.getName(), before);
                }
            }

            long startBlobTrackerSyncTime = clock.getTime();
            // Synchronize deleted blob ids with the blob id tracker
            try {
                Closeables.close(idTempDeleteWriter, true);

                if (blobIdsTracked && numBlobsDeleted > 0) {
                    BlobTracker tracker = ((BlobTrackingStore) blobStore).getTracker();
                    if (tracker != null) {
                        tracker.remove(idTempDeleteFile, Options.ACTIVE_DELETION);
                    }
                }
            } catch(Exception e) {
                LOG.warn("Error refreshing tracked blob ids", e);
            }
            long endBlobTrackerSyncTime = clock.getTime();
            LOG.info("Synchronizing changes with blob tracker took {} ms", endBlobTrackerSyncTime - startBlobTrackerSyncTime);

            if (cancelled) {
                LOG.info("Deletion run cancelled by user");
            }
            long end = clock.getTime();
            LOG.info("Deleted {} blobs contained in {} chunks in {} ms", numBlobsDeleted, numChunksDeleted, end - start);
            writeOutLastCheckedBlobTimestamp(lastDeletedBlobTimestamp);
        }

        @Override
        public void cancelBlobCollection() {
            cancelled = true;
        }

        @Override
        public void flagActiveDeletionUnsafe(boolean toFlag) {
            activeDeletionUnsafe = toFlag;
        }

        @Override
        public boolean isActiveDeletionUnsafe() {
            return activeDeletionUnsafe;
        }

        private long readLastCheckedBlobTimestamp() {
            File blobCollectorInfoFile = new File(rootDirectory, "collection-info.txt");
            if (!blobCollectorInfoFile.exists()) {
                LOG.debug("Couldn't read last checked blob timestamp (file not found). Would do a bit more scan");
                return -1;
            }
            InputStream is = null;
            Properties p;
            try {
                is = new BufferedInputStream(new FileInputStream(blobCollectorInfoFile));
                p = new Properties();
                p.load(is);
            } catch (IOException e) {
                LOG.warn("Couldn't read last checked blob timestamp from {} ... would do a bit more scan",
                        blobCollectorInfoFile, e);
                return -1;
            } finally {
                org.apache.commons.io.IOUtils.closeQuietly(is);
            }

            String resString = p.getProperty("last-checked-blob-timestamp");
            if (resString == null) {
                LOG.warn("Couldn't fine last checked blob timestamp property in collection-info.txt");
                return -1;
            }

            try {
                return Long.valueOf(resString);
            } catch (NumberFormatException nfe) {
                LOG.warn("Couldn't read last checked blob timestamp '" + resString + "' as long", nfe);
                return -1;
            }
        }

        private void writeOutLastCheckedBlobTimestamp(long timestamp) {
            Properties p = new Properties();
            p.setProperty("last-checked-blob-timestamp", String.valueOf(timestamp));
            File blobCollectorInfoFile = new File(rootDirectory, "collection-info.txt");
            OutputStream os = null;
            try {
                os = new BufferedOutputStream(new FileOutputStream(blobCollectorInfoFile));
                p.store(os, "Last checked blob timestamp");
            } catch (IOException e) {
                LOG.warn("Couldn't write out last checked blob timestamp(" + timestamp + ")", e);
            } finally {
                org.apache.commons.io.IOUtils.closeQuietly(os);
            }

        }

        public BlobDeletionCallback getBlobDeletionCallback() throws IllegalStateException {
            return new DeletedBlobCollector();
        }

        static long getTimestampFromBlobFileName(String filename) throws IllegalArgumentException {
            checkArgument(filename.startsWith(BLOB_FILE_PATTERN_PREFIX),
                    "Filename(%s) must start with %s", filename, BLOB_FILE_PATTERN_PREFIX);
            checkArgument(filename.endsWith(BLOB_FILE_PATTERN_SUFFIX),
                    "Filename(%s) must end with %s", filename, BLOB_FILE_PATTERN_SUFFIX);
            String timestampStr = filename.substring(
                    BLOB_FILE_PATTERN_PREFIX.length(),
                    filename.length() - BLOB_FILE_PATTERN_SUFFIX.length());

            return Long.parseLong(timestampStr);
        }

        private void addDeletedBlobs(Collection<BlobIdInfoStruct> deletedBlobs) {
            int addedForFlush = 0;
            for (BlobIdInfoStruct info : deletedBlobs) {
                try {
                    if (!this.deletedBlobs.offer(info, 1, TimeUnit.SECONDS)) {
                        LOG.warn("Timed out while offer-ing {} into queue.", info);
                    }
                    if (LOG.isDebugEnabled()) {
                        addedForFlush++;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while adding " + info, e);
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Added {} (out of {} tried) to be flushed. QSize: {}",
                        addedForFlush, deletedBlobs.size(), this.deletedBlobs.size());
            }
            deletedBlobsFileWriter.scheduleFileFlushIfNeeded();
        }

        private class DeletedBlobsFileWriter implements Runnable {
            private final AtomicBoolean fileFlushScheduled = new AtomicBoolean(false);

            private volatile String inUseFileName = null;

            private synchronized void flushDeletedBlobs() {
                List<BlobIdInfoStruct> localDeletedBlobs = new LinkedList<>();
                deletedBlobs.drainTo(localDeletedBlobs);
                if (localDeletedBlobs.size() > 0) {
                    File outFile = new File(rootDirectory, getBlobFileName());
                    try {
                        long start = PERF_LOG.start();
                        FileUtils.writeLines(outFile, localDeletedBlobs, true);
                        PERF_LOG.end(start, 1, "Flushing deleted blobs");
                    } catch (IOException e) {
                        LOG.error("Couldn't write out to " + outFile, e);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Flushed {} blobs to {}", localDeletedBlobs.size(), outFile.getName());
                    }
                }
            }

            private void scheduleFileFlushIfNeeded() {
                if (fileFlushScheduled.compareAndSet(false, true)) {
                    executorService.submit(this);
                }
            }

            private synchronized void releaseInUseFile() {
                inUseFileName = null;
            }

            @Override
            public void run() {
                flushDeletedBlobs();
                fileFlushScheduled.set(false);
            }

            private String getBlobFileName() {
                if (inUseFileName == null) {
                    inUseFileName = String.format(BLOB_FILE_PATTERN, clock.getTime());
                }
                return inUseFileName;
            }
        }

        /**
         * This implementation would track deleted blobs and then pass them onto
         * {@link ActiveDeletedBlobCollectorImpl} on a successful commit
         */
        private class DeletedBlobCollector implements BlobDeletionCallback {
            List<BlobIdInfoStruct> deletedBlobs = new ArrayList<>();

            @Override
            public void deleted(String blobId, Iterable<String> ids) {
                deletedBlobs.add(new BlobIdInfoStruct(blobId, ids));
            }

            @Override
            public void commitProgress(IndexProgress indexProgress) {
                if (indexProgress != IndexProgress.COMMIT_SUCCEDED && indexProgress != IndexProgress.COMMIT_FAILED) {
                    LOG.debug("We only care for commit success/failure");
                    return;
                }
                if (indexProgress == IndexProgress.COMMIT_SUCCEDED) {
                    addDeletedBlobs(deletedBlobs);
                }

                deletedBlobs.clear();
            }

            @Override
            public boolean isMarkingForActiveDeletionUnsafe() {
                return activeDeletionUnsafe;
            }
        }

        private class BlobIdInfoStruct {
            final String blobId;
            final Iterable<String> ids;

            BlobIdInfoStruct(String blobId, Iterable<String> ids) {
                this.blobId = blobId;
                this.ids = ids;
            }

            @Override
            public String toString() {
                return String.format("%s|%s|%s", blobId, clock.getTime(), Joiner.on("|").join(ids));
            }
        }
    }
}
