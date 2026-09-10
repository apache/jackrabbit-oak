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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies index files from the datastore to the local disk, caching them for reuse across
 * reader reopens. This is a read-only port of {@code oak-lucene}'s {@code IndexCopier} for
 * Lucene 9's {@link Directory} API: it keeps only the read path (backed by
 * {@link CopyOnReadDirectory}, added in a later task on top of this class) and drops the
 * write path ({@code CopyOnWriteDirectory}, {@code wrapForWrite}, reindex-time buffering),
 * which this module does not need.
 * <p>
 * Named differently from legacy's {@code IndexCopier} to avoid ambiguity should both classes
 * ever be imported side by side (e.g. during review/comparison).
 */
public class LuceneNgIndexCopier implements Closeable {

    /**
     * Files which only ever live remotely and are never copied to local disk.
     */
    public static final Set<String> REMOTE_ONLY = Set.of("segments.gen");

    private static final int MAX_FAILURE_ENTRIES = 10000;
    private static final String WORK_DIR_NAME = "indexWriterDir";

    private static final Logger log = LoggerFactory.getLogger(LuceneNgIndexCopier.class);

    private final Executor executor;
    private final File indexWorkDir;

    private final AtomicInteger readerLocalReadCount = new AtomicInteger();
    private final AtomicInteger readerRemoteReadCount = new AtomicInteger();
    private final AtomicInteger invalidFileCount = new AtomicInteger();
    private final AtomicInteger deletedFileCount = new AtomicInteger();
    private final AtomicInteger scheduledForCopyCount = new AtomicInteger();
    private final AtomicInteger copyInProgressCount = new AtomicInteger();
    private final AtomicInteger maxCopyInProgressCount = new AtomicInteger();
    private final AtomicInteger maxScheduledForCopyCount = new AtomicInteger();
    private final AtomicInteger downloadCount = new AtomicInteger();
    private final AtomicLong downloadSize = new AtomicLong();
    private final AtomicLong downloadTime = new AtomicLong();
    // Genuine compile dependency of deleteFile()/successfullyDeleted() and of
    // DeleteOldDirOnClose below (both are in scope for the read path) - not part of the
    // upload/CopyOnReadStatsMBean write-path bookkeeping this port otherwise drops.
    private final AtomicLong garbageCollectedSize = new AtomicLong();

    private final Lock copyCompletionLock = new ReentrantLock();
    private final Condition notCopyingCondition = copyCompletionLock.newCondition();

    private final Map<String, String> indexPathVersionMapping = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LocalIndexFile> failedToDeleteFiles = new ConcurrentHashMap<>();
    private final Set<LocalIndexFile> copyInProgressFiles = Collections.newSetFromMap(new ConcurrentHashMap<LocalIndexFile, Boolean>());
    private final boolean prefetchEnabled;
    private volatile boolean closed;
    private final IndexRootDirectory indexRootDirectory;
    private final Set<String> validatedIndexPaths = SetUtils.newConcurrentHashSet();
    private final IndexSanityChecker.IndexSanityStatistics indexSanityStatistics = new IndexSanityChecker.IndexSanityStatistics();

    public LuceneNgIndexCopier(Executor executor, File indexRootDir, boolean prefetchEnabled) throws IOException {
        this.executor = executor;
        this.prefetchEnabled = prefetchEnabled;
        this.indexWorkDir = initializerWorkDir(indexRootDir);
        this.indexRootDirectory = new IndexRootDirectory(indexRootDir);
    }

    public Directory wrapForRead(String indexPath, IndexDefinition definition,
                                 OakDirectory remote, String dirName) throws IOException {
        File localDir = getIndexDir(definition, indexPath, dirName);
        Directory local = createLocalDirForIndexReader(indexPath, definition, dirName, localDir);
        checkIntegrity(indexPath, local, localDir, remote);
        return new CopyOnReadDirectory(this, remote, local, localDir, prefetchEnabled, indexPath, executor);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * @return the root directory under which index files are copied locally, as passed to the constructor.
     */
    public File getIndexRootDir() {
        return indexRootDirectory.getIndexRootDir();
    }

    public File getIndexDir(IndexDefinition definition, String indexPath, String dirName) throws IOException {
        return indexRootDirectory.getIndexDir(definition, indexPath, dirName);
    }

    protected Directory createLocalDirForIndexReader(String indexPath, IndexDefinition definition, String dirName,
                                                      File localDir) throws IOException {
        Directory result = FSDirectory.open(localDir.toPath());
        String newPath = localDir.getAbsolutePath();
        String oldPath = indexPathVersionMapping.put(createIndexPathKey(indexPath, dirName), newPath);
        if (!newPath.equals(oldPath) && oldPath != null) {
            result = new DeleteOldDirOnClose(result, new File(oldPath));
        }
        return result;
    }

    private void checkIntegrity(String indexPath, Directory local, File localDir, OakDirectory remote) throws IOException {
        if (validatedIndexPaths.contains(indexPath)) {
            return;
        }

        //The integrity check needs to be done for the very first time at startup when
        //a directory gets created as at that time it can be ensured that there is no
        //work in progress files, no memory mapping issue etc
        //Also at this time its required that state in local dir should exactly same as
        //one in remote dir
        synchronized (validatedIndexPaths) {
            new IndexSanityChecker(indexPath, local, localDir, remote).check(indexSanityStatistics);
            validatedIndexPaths.add(indexPath);
        }
    }

    /**
     * Creates the workDir. If it exists then it is cleaned
     *
     * @param indexRootDir root directory under which all indexing related files are managed
     * @return work directory. Always empty
     */
    private static File initializerWorkDir(File indexRootDir) throws IOException {
        File workDir = new File(indexRootDir, WORK_DIR_NAME);
        FileUtils.deleteDirectory(workDir);
        Validate.checkState(workDir.mkdirs(), "Cannot create directory %s", workDir);
        return workDir;
    }

    /**
     * Create a unique key based on indexPath and dirName used under that path
     */
    private static String createIndexPathKey(String indexPath, String dirName) {
        return indexPath.concat(dirName);
    }

    boolean deleteFile(Directory dir, File localDir, String fileName, boolean copiedFromRemote) {
        LocalIndexFile file = new LocalIndexFile(dir, fileName, localFileLength(localDir, fileName), copiedFromRemote);
        boolean successFullyDeleted = false;
        try {
            boolean fileExisted = existsLocally(localDir, fileName);
            if (fileExisted) {
                dir.deleteFile(fileName);
            }
            successfullyDeleted(file, fileExisted);
            successFullyDeleted = true;
        } catch (IOException e) {
            failedToDelete(file);
            log.debug("Error occurred while removing deleted file {} from Local {}. " +
                    "Attempt would be made to delete it on next run ", fileName, dir, e);
        }
        return successFullyDeleted;
    }

    /**
     * Package-visible: shared by CopyOnReadDirectory (Part B2) and IndexSanityChecker,
     * both of which only ever check existence/length on the local, File-backed side.
     */
    static boolean existsLocally(File localDir, String fileName) {
        return new File(localDir, fileName).isFile();
    }

    /** Mirrors legacy DirectoryUtils.getFileLength's "-1 if it can't be determined" contract. */
    static long localFileLength(File localDir, String fileName) {
        File f = new File(localDir, fileName);
        return f.isFile() ? f.length() : -1;
    }

    private void failedToDelete(LocalIndexFile file) {
        //Limit the size on best effort basis
        if (failedToDeleteFiles.size() < MAX_FAILURE_ENTRIES) {
            LocalIndexFile failedToDeleteFile = failedToDeleteFiles.putIfAbsent(file.getKey(), file);
            if (failedToDeleteFile == null) {
                failedToDeleteFile = file;
            }
            failedToDeleteFile.incrementAttemptToDelete();
        } else {
            long garbageSize = 0;
            for (LocalIndexFile pending : failedToDeleteFiles.values()) {
                garbageSize += pending.getSize();
            }
            log.warn("Not able to delete {}. Currently more than {} file with total size {} bytes are pending delete.",
                    file.deleteLog(), failedToDeleteFiles.size(), garbageSize);
        }
    }

    private void successfullyDeleted(LocalIndexFile file, boolean fileExisted) {
        LocalIndexFile failedToDeleteFile = failedToDeleteFiles.remove(file.getKey());
        if (failedToDeleteFile != null) {
            log.debug("Deleted : {}", failedToDeleteFile.deleteLog());
        }

        if (fileExisted) {
            garbageCollectedSize.addAndGet(file.getSize());
            deletedFileCount.incrementAndGet();
        }
    }

    /**
     * This method would return the latest modification timestamp from the set of file{@code names}
     * on the file system.
     * The parameter {@code localDir} is expected to be an instance of {@link FSDirectory} (or wrapped one in
     * {@link FilterDirectory}. If this assumption doesn't hold, the method would return -1.
     * Each of file names are expected to be existing in {@code localDir}. If this fails the method shall return -1.
     * In case of any error while computing modified timestamps on the file system, the method shall return -1.
     * @param names file names to evaluate on local FS
     * @param localDir {@link Directory} implementation to be used to get the files
     * @return latest timestamp or -1 (with logs) in case of any doubt
     */
    public static long getNewestLocalFSTimestampFor(Set<String> names, Directory localDir) {
        File localFSDir = LocalIndexFile.getFSDir(localDir);

        if (localFSDir == null) {
            log.warn("Couldn't get FSDirectory instance for {}.", localDir);
            return -1;
        }

        long maxTS = 0L;
        for (String name : names) {
            File f = new File(localFSDir, name);

            if (!f.exists()) {
                log.warn("File {} doesn't exist in {}", name, localFSDir);
                return -1;
            }

            long modTS = f.lastModified();
            if (modTS == 0L) {
                log.warn("Couldn't get lastModification timestamp for {} in {}", name, localFSDir);
                return -1;
            }

            if (modTS > maxTS) {
                maxTS = modTS;
            }
        }

        return maxTS;
    }

    /**
     * @param name file name to evaluate on local FS
     * @param localDir {@link Directory} implementation to be used to get the file
     * @param millis timestamp to compare file's modified timestamp against
     * @return {@code true} if file referred to be {@code name} is modified before {@code millis}; false otherwise
     */
    public static boolean isFileModifiedBefore(String name, Directory localDir, long millis) {
        File localFSDir = LocalIndexFile.getFSDir(localDir);

        if (localFSDir == null) {
            log.warn("Couldn't get FSDirectory instance for {}.", localDir);
            return false;
        }

        File f = new File(localFSDir, name);
        if (!f.exists()) {
            log.warn("File {} doesn't exist in {}", name, localFSDir);
            return false;
        }

        long modTS = f.lastModified();
        if (modTS == 0L) {
            log.warn("Couldn't get lastModification timestamp for {} in {}", name, localFSDir);
            return false;
        }

        return modTS < millis;
    }

    long startCopy(LocalIndexFile file) {
        updateMaxInProgress(copyInProgressCount.incrementAndGet());
        copyInProgressFiles.add(file);
        return System.currentTimeMillis();
    }

    boolean isCopyInProgress(LocalIndexFile file) {
        return copyInProgressFiles.contains(file);
    }

    /**
     * Waits for maximum of {@code timeoutMillis} while checking if {@code file} isn't being copied already.
     * The method can return before {@code timeoutMillis} if it got interrupted. So, if required then the
     * caller should check using {@code isCopyInProgress} and wait again.
     * @param file
     * @param timeoutMillis
     */
    void waitForCopyCompletion(LocalIndexFile file, long timeoutMillis) {
        long localLength = file.actualSize();
        long lastLocalLength = localLength;

        boolean notCopying = !isCopyInProgress(file);
        while (!notCopying) {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            copyCompletionLock.lock();
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Checking for copy completion of {} - {}", file.getKey(), file.copyLog());
                }
                while (isCopyInProgress(file)) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        // timeout
                        break;
                    }
                    notCopyingCondition.awaitNanos(remaining);
                }
                notCopying = !isCopyInProgress(file);
            } catch (InterruptedException e) {
                // ignore and reset interrupt flag
                Thread.currentThread().interrupt();
            } finally {
                copyCompletionLock.unlock();
            }

            localLength = file.actualSize();

            // Break out if local file length hasn't changed since last we checked.
            // Do note that our assumption is that our monitor would return false only on timeout.
            // BUT that's not true and the monitor could be interrupted as well. We are ignoring that
            // gotcha explicitly as we don't really want to over complicate here for a rare race of
            // concurrent copying and avoid reading from remote.
            if (localLength <= lastLocalLength) {
                log.warn("Breaking out of waiting for copy to finish as current local length ({})" +
                                " hasn't increased from {}",
                        localLength, lastLocalLength);
                break;
            }
            lastLocalLength = localLength;
        }
    }

    void doneCopy(LocalIndexFile file, long start) {
        copyCompletionLock.lock();
        try {
            copyInProgressFiles.remove(file);
            // wake up any threads waiting in waitForCopyCompletion(...)
            notCopyingCondition.signalAll();
        } finally {
            copyCompletionLock.unlock();
        }
        copyInProgressCount.decrementAndGet();

        if (file.isCopyFromRemote()) {
            downloadTime.addAndGet(System.currentTimeMillis() - start);
            downloadSize.addAndGet(file.getSize());
            downloadCount.incrementAndGet();
        }
    }

    private void updateMaxScheduled(int val) {
        synchronized (maxScheduledForCopyCount) {
            int current = maxScheduledForCopyCount.get();
            if (val > current) {
                maxScheduledForCopyCount.set(val);
            }
        }
    }

    private void updateMaxInProgress(int val) {
        synchronized (maxCopyInProgressCount) {
            int current = maxCopyInProgressCount.get();
            if (val > current) {
                maxCopyInProgressCount.set(val);
            }
        }
    }

    private class DeleteOldDirOnClose extends FilterDirectory {
        private final File oldIndexDir;

        protected DeleteOldDirOnClose(Directory in, File oldIndexDir) {
            super(in);
            this.oldIndexDir = oldIndexDir;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                //Clean out the local dir irrespective of any error occurring upon
                //close in wrapped directory
                try {
                    long totalDeletedSize = FileUtils.sizeOf(oldIndexDir);
                    FileUtils.deleteDirectory(oldIndexDir);
                    totalDeletedSize += indexRootDirectory.gcEmptyDirs(oldIndexDir);
                    garbageCollectedSize.addAndGet(totalDeletedSize);
                    log.debug("Removed old index content from {} ", oldIndexDir);
                } catch (IOException e) {
                    log.warn("Not able to remove old version of copied index at {}", oldIndexDir, e);
                }
            }
        }

        @Override
        public String toString() {
            return "DeleteOldDirOnClose wrapper for " + getDelegate();
        }
    }

    //~------------------------------------------< Stats Collection >

    void scheduledForCopy() {
        updateMaxScheduled(scheduledForCopyCount.incrementAndGet());
    }

    void copyDone() {
        scheduledForCopyCount.decrementAndGet();
    }

    void readFromRemote(boolean reader) {
        if (reader) {
            readerRemoteReadCount.incrementAndGet();
        }
    }

    void readFromLocal(boolean reader) {
        if (reader) {
            readerLocalReadCount.incrementAndGet();
        }
    }

    void foundInvalidFile() {
        invalidFileCount.incrementAndGet();
    }

    public int getReaderLocalReadCount() {
        return readerLocalReadCount.get();
    }

    public int getReaderRemoteReadCount() {
        return readerRemoteReadCount.get();
    }

    public int getInvalidFileCount() {
        return invalidFileCount.get();
    }

    public int getDownloadCount() {
        return downloadCount.get();
    }

    public long getDownloadSize() {
        return downloadSize.get();
    }

    public long getDownloadTime() {
        return downloadTime.get();
    }
}
