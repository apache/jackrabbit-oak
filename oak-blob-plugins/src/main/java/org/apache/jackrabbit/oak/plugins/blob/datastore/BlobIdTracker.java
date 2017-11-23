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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.commons.FileIOUtils.FileLineDifferenceIterator;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.Files.fileTreeTraverser;
import static com.google.common.io.Files.move;
import static com.google.common.io.Files.newWriter;
import static java.io.File.createTempFile;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.synchronizedList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.copyFile;
import static org.apache.commons.io.FileUtils.forceDelete;
import static org.apache.commons.io.FileUtils.forceMkdir;
import static org.apache.commons.io.FileUtils.lineIterator;
import static org.apache.commons.io.FileUtils.touch;
import static org.apache.commons.io.FilenameUtils.concat;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.BurnOnCloseFileIterator.wrap;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.append;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.copy;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.sort;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker.BlobIdStore.Type.GENERATION;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker.BlobIdStore.Type.IN_PROCESS;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker.BlobIdStore.Type.REFS;


/**
 * Tracks the blob ids available or added in the blob store using the {@link BlobIdStore} .
 *
 */
public class BlobIdTracker implements Closeable, BlobTracker {
    private static final Logger LOG = LoggerFactory.getLogger(BlobIdTracker.class);

    /**
     * System property to skip tracker. If set will skip:
     * * Snapshots (No-op)
     * * Retrieve (return empty)
     * * Add (No-op)
     */
    private final boolean SKIP_TRACKER = Boolean.getBoolean("oak.datastore.skipTracker");

    private static final String datastoreMeta = "blobids";
    private static final String fileNamePrefix = "blob";
    private static final String mergedFileSuffix = ".refs";
    private static final String snapshotMarkerSuffix = ".snapshot";

    /* Local instance identifier */
    private final String instanceId = randomUUID().toString();

    private final SharedDataStore datastore;
    private final long snapshotInterval;
    private final ActiveDeletionTracker deleteTracker;

    protected BlobIdStore store;

    private final ScheduledExecutorService scheduler;

    private String prefix;

    private File rootDir;

    public BlobIdTracker(String path, String repositoryId, long snapshotIntervalSecs, SharedDataStore datastore)
        throws IOException {
        this(path, repositoryId, newSingleThreadScheduledExecutor(), snapshotIntervalSecs, snapshotIntervalSecs,
            datastore);
    }

    public BlobIdTracker(String path, String repositoryId, ScheduledExecutorService scheduler, long snapshotDelaySecs,
        long snapshotIntervalSecs, SharedDataStore datastore) throws IOException {
        String root = concat(path, datastoreMeta);
        this.rootDir = new File(root);
        this.datastore = datastore;
        this.scheduler = scheduler;
        this.snapshotInterval = SECONDS.toMillis(snapshotIntervalSecs);
        try {
            forceMkdir(rootDir);
            prefix = fileNamePrefix + "-" + repositoryId;
            this.store = new BlobIdStore(rootDir, prefix);
            scheduler.scheduleAtFixedRate(new SnapshotJob(), SECONDS.toMillis(snapshotDelaySecs),
                SECONDS.toMillis(snapshotIntervalSecs), MILLISECONDS);
            this.deleteTracker = new ActiveDeletionTracker(rootDir, prefix);
        } catch (IOException e) {
            LOG.error("Error initializing blob tracker", e);
            close();
            throw e;
        }
    }

    public ActiveDeletionTracker getDeleteTracker() {
        return deleteTracker;
    }

    @Override public void remove(File recs, Options options) throws IOException {
        if (options == Options.ACTIVE_DELETION) {
            get();
            deleteTracker.track(recs);
        }
        store.removeRecords(recs);
        snapshot(true);
    }

    @Override public void remove(File recs) throws IOException {
        store.removeRecords(recs);
        snapshot(true);
    }

    @Override public void remove(Iterator<String> recs) throws IOException {
        store.removeRecords(recs);
        snapshot(true);
    }

    @Override public void add(String id) throws IOException {
        if (!SKIP_TRACKER) {
            store.addRecord(id);
        }
    }

    @Override public void add(Iterator<String> recs) throws IOException {
        if (!SKIP_TRACKER) {
            store.addRecords(recs);
        }
    }

    @Override public void add(File recs) throws IOException {
        if (!SKIP_TRACKER) {
            store.addRecords(recs);
        }
    }

    /**
     * Retrieves all the reference files available in the DataStore and merges
     * them to the local store and then returns an iterator over it.
     * This way the ids returned are as recent as the snapshots taken on all
     * instances/repositories connected to the DataStore.
     * <p>
     * The iterator returned ia a Closeable instance and should be closed by calling #close().
     *
     * @return iterator over all the blob ids available
     * @throws IOException
     */
    @Override public Iterator<String> get() throws IOException {
        try {
            if (!SKIP_TRACKER) {
                globalMerge();
                return store.getRecords();
            }
            return Iterators.emptyIterator();
        } catch (IOException e) {
            LOG.error("Error in retrieving blob records iterator", e);
            throw e;
        }
    }

    @Override public File get(String path) throws IOException {
        if (!SKIP_TRACKER) {
            globalMerge();
            return store.getRecords(path);
        }
        return new File(path);
    }

    /**
     * Retrieves and merges all the blob id records available in the DataStore from different
     * instances sharing the DataStore (cluster nodes/different repositories).
     *
     * @throws IOException
     */
    private void globalMerge() throws IOException {
        try {
            Stopwatch watch = Stopwatch.createStarted();
            LOG.trace("Retrieving all blob id files available form the DataStore");
            // Download all the blob reference records from the data store
            Iterable<DataRecord> refRecords = datastore.getAllMetadataRecords(fileNamePrefix);

            // Download all the corresponding files for the records
            List<File> refFiles = newArrayList(transform(refRecords, new Function<DataRecord, File>() {
                @Override public File apply(DataRecord input) {
                    InputStream inputStream = null;
                    try {
                        inputStream = input.getStream();
                        return copy(inputStream);
                    } catch (Exception e) {
                        LOG.warn("Error copying data store file locally {}", input.getIdentifier(), e);
                    } finally {
                        closeQuietly(inputStream);
                    }
                    return null;
                }
            }));
            LOG.info("Retrieved all blob id files in [{}]", watch.elapsed(TimeUnit.MILLISECONDS));

            // Merge all the downloaded files in to the local store
            watch = Stopwatch.createStarted();
            store.merge(refFiles, true);
            LOG.info("Merged all retrieved blob id files in [{}]", watch.elapsed(TimeUnit.MILLISECONDS));

            // Remove all the data store records as they have been merged
            watch = Stopwatch.createStarted();
            for (DataRecord rec : refRecords) {
                datastore.deleteMetadataRecord(rec.getIdentifier().toString());
                LOG.debug("Deleted metadata record {}", rec.getIdentifier().toString());
            }
            LOG.info("Deleted all blob id metadata files in [{}]", watch.elapsed(TimeUnit.MILLISECONDS));
        } catch (IOException e) {
            LOG.error("Error in merging blob records iterator from the data store", e);
            throw e;
        }
    }

    private void snapshot() throws IOException {
        snapshot(false);
    }

    /**
     * Takes a snapshot on the tracker.
     * to other cluster nodes/repositories connected to the DataStore.
     *
     * @throws IOException
     */
    private void snapshot(boolean skipStoreSnapshot) throws IOException {
        try {
            if (!SKIP_TRACKER) {
                Stopwatch watch = Stopwatch.createStarted();

                if (!skipStoreSnapshot) {
                    store.snapshot();
                    LOG.debug("Completed snapshot in [{}]", watch.elapsed(TimeUnit.MILLISECONDS));
                }

                watch = Stopwatch.createStarted();
                File recs = store.getBlobRecordsFile();
                datastore.addMetadataRecord(recs, (prefix + instanceId + System.currentTimeMillis() + mergedFileSuffix));
                LOG.info("Added blob id metadata record in DataStore in [{}]", watch.elapsed(TimeUnit.MILLISECONDS));

                try {
                    forceDelete(recs);
                    LOG.info("Deleted blob record file after snapshot and upload {}", recs);

                    // Update the timestamp for the snapshot marker
                    touch(getSnapshotMarkerFile());
                    LOG.info("Updated snapshot marker");
                } catch (IOException e) {
                    LOG.debug("Failed to in cleaning up {}", recs, e);
                }
            }
        } catch (Exception e) {
            LOG.error("Error taking snapshot", e);
            throw new IOException("Snapshot error", e);
        }
    }

    private File getSnapshotMarkerFile() {
        File snapshotMarker = new File(rootDir, prefix + snapshotMarkerSuffix);
        return snapshotMarker;
    }

    /**
     * Closes the tracker and the underlying store.
     *
     * @throws IOException
     */
    @Override public void close() throws IOException {
        store.close();
        new ExecutorCloser(scheduler).close();
    }

    /**
     * Tracking any active deletions  store for managing the blob reference
     */
    public static class ActiveDeletionTracker {
        /* Suffix for tracking file */
        private static final String DEL_SUFFIX = ".del";

        /* deletion tracking file */
        private File delFile;

        public static final String DELIM = ",";

        /* Lock for operations on the active deletions file */
        private final ReentrantLock lock;

        private static final Function<String, String> transformer = new Function<String, String>() {
            @Nullable
            @Override
            public String apply(@Nullable String input) {
                if (input != null) {
                    return input.split(DELIM)[0];
                }
                return "";
            }};

        ActiveDeletionTracker(File rootDir, String prefix) throws IOException {
            delFile = new File(rootDir, prefix + DEL_SUFFIX);
            touch(delFile);
            lock = new ReentrantLock();
        }

        /**
         * Adds the ids in the file provided to the tracked deletions.
         * @param recs the deleted ids to track
         */
        public void track(File recs) throws IOException {
            lock.lock();
            try {
                append(Lists.newArrayList(recs), delFile, false);
                sort(delFile);
            } finally {
                lock.unlock();
            }
        }

        public File retrieve(String path) throws IOException {
            File copiedRecsFile = new File(path);
            try {
                copyFile(delFile, copiedRecsFile);
                return copiedRecsFile;
            } catch (IOException e) {
                LOG.error("Error in retrieving active deletions file", e);
                throw e;
            }
        }

        /**
         * Remove ids given by the file in parameter from the deletions being tracked.
         *
         * @param recs the sorted file containing ids to be removed from tracker
         */
        public void reconcile(File recs) throws IOException {
            lock.lock();
            try {
                // Remove and spool the remaining ids into a temp file
                File toBeRemoved = createTempFile("toBeRemoved", null);
                File removed = createTempFile("removed", null);

                FileLineDifferenceIterator toBeRemovedIterator = null;
                FileLineDifferenceIterator removeIterator = null;
                try {
                    // Gather all records which are not required to be tracked anymore
                    toBeRemovedIterator = new FileLineDifferenceIterator(recs, delFile, null);
                    writeStrings(toBeRemovedIterator, toBeRemoved, false);

                    // Remove records not to be tracked
                    removeIterator = new FileLineDifferenceIterator(toBeRemoved, delFile, null);
                    writeStrings(removeIterator, removed, false);
                } finally {
                    if (toBeRemovedIterator != null) {
                        toBeRemovedIterator.close();
                    }

                    if (removeIterator != null) {
                        removeIterator.close();
                    }

                    if (toBeRemoved != null) {
                        toBeRemoved.delete();
                    }
                }

                move(removed, delFile);
                LOG.trace("removed active delete records");
            } finally {
                lock.unlock();
            }
        }

        /**
         * Return any ids not existing in the deletions being tracked from the ids in file parameter.
         *
         * @param recs the file to search for ids existing in the deletions here
         * @return
         */
        public Iterator<String> filter(File recs) throws IOException {
            return new FileLineDifferenceIterator(delFile, recs, transformer);
        }
    }

    /**
     * Local store for managing the blob reference
     */
    static class BlobIdStore implements Closeable {
        /* Suffix for a snapshot generation file */
        private static final String genFileNameSuffix = ".gen";

        /* Suffix for in process file */
        private static final String workingCopySuffix = ".process";

        /* The current writer where all blob ids are being appended */
        private BufferedWriter writer;

        /* In-process file */
        private File processFile;

        /* All available generations that need to be merged */
        private final List<File> generations;

        private final File rootDir;

        private final String prefix;

        /* Lock for operations on references file */
        private final ReentrantLock refLock;

        /* Lock for snapshot */
        private final ReentrantLock snapshotLock;

        BlobIdStore(File rootDir, String prefix) throws IOException {
            this.rootDir = rootDir;
            this.prefix = prefix;
            this.refLock = new ReentrantLock();
            this.snapshotLock = new ReentrantLock();

            // Retrieve the process file if it exists
            processFile =
                fileTreeTraverser().breadthFirstTraversal(rootDir).firstMatch(IN_PROCESS.filter())
                    .orNull();

            // Get the List of all generations available.
            generations = synchronizedList(newArrayList(
                fileTreeTraverser().breadthFirstTraversal(rootDir).filter(GENERATION.filter())));

            // Close/rename any existing in process
            nextGeneration();
        }

        /**
         * Add a blob id to the tracking file.
         *
         * @param id id to track
         * @throws IOException
         */
        protected synchronized void addRecord(String id) throws IOException {
            writer.append(id);
            writer.newLine();
            writer.flush();
            LOG.debug("Added record {}", id);
        }

        /**
         * Returns an iterator on the tracked blob ids.
         *
         * @return record iterator
         * @throws IOException
         */
        protected Iterator<String> getRecords() throws IOException {
            try {
                // Get a temp file path
                String path = createTempFile("temp", null).getAbsolutePath();
                return wrap(lineIterator(getRecords(path)), new File(path));
            } catch (IOException e) {
                LOG.error("Error in retrieving blob records iterator", e);
                throw e;
            }
        }

        /**
         * Returns a file with the tracked blob ids.
         *
         * @param path path of the file
         * @return File containing tracked file ids
         * @throws IOException
         */
        protected File getRecords(String path) throws IOException {
            refLock.lock();
            File copiedRecsFile = new File(path);
            try {
                copyFile(getBlobRecordsFile(), copiedRecsFile);
                return copiedRecsFile;
            } catch (IOException e) {
                LOG.error("Error in retrieving blob records file", e);
                throw e;
            } finally {
                refLock.unlock();
            }
        }

        /**
         * Returns the blob references file
         *
         * @return blob reference file
         * @throws IOException
         */
        protected File getBlobRecordsFile() throws IOException {
            File refs = new File(rootDir, prefix + REFS.getFileNameSuffix());
            if (!refs.exists()) {
                LOG.debug("File created {}", refs.createNewFile());
            }
            return refs;
        }

        /**
         * Merges the given files with the references file and deletes the files.
         *
         * @param refFiles files to merge
         * @param doSort whether to sort while merging
         * @throws IOException
         */
        protected void merge(List<File> refFiles, boolean doSort) throws IOException {
            refLock.lock();
            try {
                if (refFiles != null && !refFiles.isEmpty()) {
                    File merged = new File(rootDir, prefix + REFS.getFileNameSuffix());
                    append(refFiles, merged, true);
                    LOG.debug("Merged files into references {}", refFiles);
                    // Clear the references as not needed
                    refFiles.clear();
                }
                if (doSort) {
                    sort(getBlobRecordsFile());
                }
            } finally {
                refLock.unlock();
            }
        }

        /**
         * Removes ids obtained by the given iterator from the tracked references.
         * The iterator has to be closed by the caller.
         *
         * @param recs iterator over records to remove
         * @throws IOException
         */
        protected void removeRecords(Iterator<String> recs) throws IOException {
            // Spool the ids to be deleted into a file and sort
            File deleted = createTempFile("deleted", null);
            writeStrings(recs, deleted, false);
            removeRecords(deleted);
            LOG.trace("Removed records");
        }

        /**
         * Removes ids obtained by the given file from the tracked references.
         * File is deleted before returning.
         *
         * @param recs file of records to delete
         * @throws IOException
         */
        protected void removeRecords(File recs) throws IOException {
            // do a snapshot
            snapshot();

            refLock.lock();
            try {
                sort(getBlobRecordsFile());
                sort(recs);
                LOG.trace("Sorted files");

                // Remove and spool the remaining ids into a temp file
                File temp = createTempFile("sorted", null);
                FileLineDifferenceIterator iterator = null;
                try {
                    iterator = new FileLineDifferenceIterator(recs, getBlobRecordsFile(), null);
                    writeStrings(iterator, temp, false);
                } finally {
                    if (iterator != null) {
                        iterator.close();
                    }
                }

                File blobRecs = getBlobRecordsFile();
                move(temp, blobRecs);
                LOG.trace("removed records");
            } finally {
                refLock.unlock();
                try {
                    forceDelete(recs);
                } catch (IOException e) {
                    LOG.debug("Failed to delete file {}", recs, e);
                }
            }
        }

        /**
         * Opens a new generation file and a writer over it.
         *
         * @throws IOException
         */
        private synchronized void nextGeneration() throws IOException {
            close();

            processFile = new File(rootDir, prefix + IN_PROCESS.getFileNameSuffix());
            writer = newWriter(processFile, UTF_8);
            LOG.info("Created new process file and writer over {} ", processFile.getAbsolutePath());
        }

        /**
         * Adds all the ids backed by the iterator into the references.
         * Closing the iterator is the responsibility of the caller.
         *
         * @param recs iterator over records to add
         * @throws IOException
         */
        protected void addRecords(Iterator<String> recs) throws IOException {
            // Spool contents into a temp file
            File added = createTempFile("added", null);
            writeStrings(recs, added, false);
            // Merge the file with the references
            merge(Lists.newArrayList(added), false);
        }

        /**
         * Merges the contents of the file into the references file.
         *
         * @param recs File of records to add
         * @throws IOException
         */
        protected void addRecords(File recs) throws IOException {
            // Merge the file with the references
            merge(Lists.newArrayList(recs), false);
        }

        /**
         * Snapshots the local store, starts a new generation for writing
         * and merges all generations available.
         *
         * @throws IOException
         */
        protected void snapshot() throws IOException {
            snapshotLock.lock();
            try {
                nextGeneration();
                merge(generations, false);
            } finally {
                snapshotLock.unlock();
            }
        }

        @Override
        public synchronized void close() {
            closeQuietly(writer);
            LOG.info("Closed writer");

            // Convert the process file to a generational file
            if (processFile != null) {
                File renamed = new File(removeExtension(processFile.getAbsolutePath()));
                boolean success = processFile.renameTo(renamed);
                LOG.debug("File renamed {}", success);
                if (success) {
                    generations.add(renamed);
                    LOG.info("Process file renamed to {}", renamed.getAbsolutePath());
                } else {
                    LOG.trace("Trying a copy file operation");
                    try {
                        if (renamed.createNewFile()) {
                            Files.copy(processFile, renamed);
                            generations.add(renamed);
                            LOG.info("{} File copied to {}", processFile.getAbsolutePath(),
                                renamed.getAbsolutePath());
                        }
                    } catch (Exception e) {
                        LOG.warn("Unable to copy process file to corresponding gen file. Some"
                            + " elements may be missed", e);
                    }
                }
            }
        }

        /**
         * Different types of files
         */
        enum Type {
            IN_PROCESS {
                @Override String getFileNameSuffix() {
                    return "." + currentTimeMillis() + genFileNameSuffix + workingCopySuffix;
                }

                @Override Predicate<File> filter() {
                    return new Predicate<File>() {
                        @Override public boolean apply(File input) {
                            return input.getName().endsWith(workingCopySuffix) && input.getName().startsWith(fileNamePrefix);
                        }
                    };
                }
            },
            GENERATION {
                @Override String getFileNameSuffix() {
                    return "." + currentTimeMillis() + genFileNameSuffix;
                }

                @Override Predicate<File> filter() {
                    return new Predicate<File>() {
                        @Override public boolean apply(File input) {
                            return input.getName().startsWith(fileNamePrefix)
                                && input.getName().contains(genFileNameSuffix)
                                && !input.getName().endsWith(workingCopySuffix);
                        }
                    };
                }
            },
            REFS {
                @Override String getFileNameSuffix() {
                    return mergedFileSuffix;
                }

                @Override Predicate<File> filter() {
                    return new Predicate<File>() {
                        @Override public boolean apply(File input) {
                            return input.getName().endsWith(mergedFileSuffix)
                                && input.getName().startsWith(fileNamePrefix);
                        }
                    };
                }
            };

            /**
             * Returns the file name suffix according to the type.
             *
             * @return file name suffix
             */
            String getFileNameSuffix() {
                return "";
            }

            /**
             * Returns the predicate to filter files in the root directory according to the type.
             *
             * @return a predicate to select particular file types
             */
            Predicate<File> filter() {
                return alwaysTrue();
            }
        }
    }

    /**
     * Job which calls the snapshot on the tracker.
     */
    class SnapshotJob implements Runnable {
        private long interval;
        private Clock clock;

        public SnapshotJob() {
            this.interval = snapshotInterval;
            this.clock = Clock.SIMPLE;
        }

        public SnapshotJob(long millis, Clock clock) {
            this.interval = millis;
            this.clock = clock;
        }

        @Override
        public void run() {
            if (!skip()) {
                try {
                    snapshot();
                    LOG.info("Finished taking snapshot");
                } catch(Exception e){
                    LOG.warn("Failure in taking snapshot", e);
                }
            } else {
                LOG.info("Skipping scheduled snapshot as it last executed within {} seconds",
                    MILLISECONDS.toSeconds(interval));
            }
        }

        private boolean skip() {
            File snapshotMarker = getSnapshotMarkerFile();
            if (snapshotMarker.exists() &&
                (snapshotMarker.lastModified() > (clock.getTime() - interval))) {
                return true;
            }
            return false;
        }
    }
}
