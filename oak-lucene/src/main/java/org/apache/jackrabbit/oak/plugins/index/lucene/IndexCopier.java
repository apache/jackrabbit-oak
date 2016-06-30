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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.concurrent.NotifyingFutureTask;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

public class IndexCopier implements CopyOnReadStatsMBean, Closeable {
    private static final Set<String> REMOTE_ONLY = ImmutableSet.of("segments.gen");
    private static final int MAX_FAILURE_ENTRIES = 10000;
    private static final AtomicInteger UNIQUE_COUNTER = new AtomicInteger();
    private static final String WORK_DIR_NAME = "indexWriterDir";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final PerfLogger PERF_LOGGER = new PerfLogger(LoggerFactory.getLogger(log.getName() + ".perf"));
    private final Executor executor;
    private final File indexWorkDir;

    private final AtomicInteger readerLocalReadCount = new AtomicInteger();
    private final AtomicInteger writerLocalReadCount = new AtomicInteger();
    private final AtomicInteger readerRemoteReadCount = new AtomicInteger();
    private final AtomicInteger writerRemoteReadCount = new AtomicInteger();
    private final AtomicInteger invalidFileCount = new AtomicInteger();
    private final AtomicInteger deletedFileCount = new AtomicInteger();
    private final AtomicInteger scheduledForCopyCount = new AtomicInteger();
    private final AtomicInteger copyInProgressCount = new AtomicInteger();
    private final AtomicInteger maxCopyInProgressCount = new AtomicInteger();
    private final AtomicInteger maxScheduledForCopyCount = new AtomicInteger();
    private final AtomicInteger uploadCount = new AtomicInteger();
    private final AtomicInteger downloadCount = new AtomicInteger();
    private final AtomicLong copyInProgressSize = new AtomicLong();
    private final AtomicLong downloadSize = new AtomicLong();
    private final AtomicLong uploadSize = new AtomicLong();
    private final AtomicLong garbageCollectedSize = new AtomicLong();
    private final AtomicLong skippedFromUploadSize = new AtomicLong();
    private final AtomicLong downloadTime = new AtomicLong();
    private final AtomicLong uploadTime = new AtomicLong();


    private final Map<String, String> indexPathMapping = newConcurrentMap();
    private final Map<String, Set<String>> sharedWorkingSetMap = newHashMap();
    private final Map<String, String> indexPathVersionMapping = newConcurrentMap();
    private final ConcurrentMap<String, LocalIndexFile> failedToDeleteFiles = newConcurrentMap();
    private final Set<LocalIndexFile> copyInProgressFiles = Collections.newSetFromMap(new ConcurrentHashMap<LocalIndexFile, Boolean>());
    private final boolean prefetchEnabled;
    private volatile boolean closed;
    private final IndexRootDirectory indexRootDirectory;

    public IndexCopier(Executor executor, File indexRootDir) throws IOException {
        this(executor, indexRootDir, false);
    }

    public IndexCopier(Executor executor, File indexRootDir, boolean prefetchEnabled) throws IOException {
        this.executor = executor;
        this.prefetchEnabled = prefetchEnabled;
        this.indexWorkDir = initializerWorkDir(indexRootDir);
        this.indexRootDirectory = new IndexRootDirectory(indexRootDir);
    }

    public Directory wrapForRead(String indexPath, IndexDefinition definition,
            Directory remote) throws IOException {
        Directory local = createLocalDirForIndexReader(indexPath, definition);
        return new CopyOnReadDirectory(remote, local, prefetchEnabled, indexPath, getSharedWorkingSet(indexPath));
    }

    public Directory wrapForWrite(IndexDefinition definition, Directory remote, boolean reindexMode) throws IOException {
        Directory local = createLocalDirForIndexWriter(definition);
        return new CopyOnWriteDirectory(remote, local, reindexMode,
                getIndexPathForLogging(definition), getSharedWorkingSet(definition.getIndexPathFromConfig()));
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }

    File getIndexWorkDir() {
        return indexWorkDir;
    }

    IndexRootDirectory getIndexRootDirectory() {
        return indexRootDirectory;
    }

    protected Directory createLocalDirForIndexWriter(IndexDefinition definition) throws IOException {
        String indexPath = definition.getIndexPathFromConfig();
        File indexWriterDir = getIndexDir(definition, indexPath);

        //By design indexing in Oak is single threaded so Lucene locking
        //can be disabled
        Directory dir = FSDirectory.open(indexWriterDir, NoLockFactory.getNoLockFactory());

        log.debug("IndexWriter would use {}", indexWriterDir);
        return dir;
    }

    protected Directory createLocalDirForIndexReader(String indexPath, IndexDefinition definition) throws IOException {
        File indexDir = getIndexDir(definition, indexPath);
        Directory result = FSDirectory.open(indexDir);

        String newPath = indexDir.getAbsolutePath();
        //TODO Account for type of path also
        String oldPath = indexPathVersionMapping.put(indexPath, newPath);
        if (!newPath.equals(oldPath) && oldPath != null) {
            result = new DeleteOldDirOnClose(result, new File(oldPath));
        }
        return result;
    }

    public File getIndexDir(IndexDefinition definition, String indexPath) throws IOException {
        return indexRootDirectory.getIndexDir(definition, indexPath);
    }

    Map<String, LocalIndexFile> getFailedToDeleteFiles() {
        return Collections.unmodifiableMap(failedToDeleteFiles);
    }

    private void failedToDelete(LocalIndexFile file){
        //Limit the size on best effort basis
        if (failedToDeleteFiles.size() < MAX_FAILURE_ENTRIES) {
            LocalIndexFile failedToDeleteFile = failedToDeleteFiles.putIfAbsent(file.getKey(), file);
            if (failedToDeleteFile == null){
                failedToDeleteFile = file;
            }
            failedToDeleteFile.incrementAttemptToDelete();
        } else {
            log.warn("Not able to delete {}. Currently more than {} file with total size {} are pending delete.",
                    file.deleteLog(), failedToDeleteFiles.size(), getGarbageSize());
        }
    }

    private void successfullyDeleted(LocalIndexFile file, boolean fileExisted){
        LocalIndexFile failedToDeleteFile = failedToDeleteFiles.remove(file.getKey());
        if (failedToDeleteFile != null){
            log.debug("Deleted : {}", failedToDeleteFile.deleteLog());
        }

        if (fileExisted){
            garbageCollectedSize.addAndGet(file.size);
            deletedFileCount.incrementAndGet();
        }
    }

    /**
     * Provide the corresponding shared state to enable COW inform COR
     * about new files it is creating while indexing. This would allow COR to ignore
     * such files while determining the deletion candidates.
     *
     * @param defn index definition for which the directory is being created
     * @return a set to maintain the state of new files being created by the COW Directory
     */
    private Set<String> getSharedWorkingSet(String indexPath){
        Set<String> sharedSet;
        synchronized (sharedWorkingSetMap){
            sharedSet = sharedWorkingSetMap.get(indexPath);
            if (sharedSet == null){
                sharedSet = Sets.newConcurrentHashSet();
                sharedWorkingSetMap.put(indexPath, sharedSet);
            }
        }
        return sharedSet;
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
        checkState(workDir.mkdirs(), "Cannot create directory %s", workDir);
        return workDir;
    }

    private static String getIndexPathForLogging(IndexDefinition defn){
        String indexPath = defn.getIndexPathFromConfig();
        if (indexPath == null){
            return "UNKNOWN";
        }
        return indexPath;
    }

    /**
     * Directory implementation which lazily copies the index files from a
     * remote directory in background.
     */
    class CopyOnReadDirectory extends FilterDirectory {
        private final Directory remote;
        private final Directory local;
        private final String indexPath;

        private final ConcurrentMap<String, CORFileReference> files = newConcurrentMap();
        /**
         * Set of fileNames bound to current local dir. It is updated with any new file
         * which gets added by this directory
         */
        private final Set<String> localFileNames = Sets.newConcurrentHashSet();

        public CopyOnReadDirectory(Directory remote, Directory local, boolean prefetch,
                                   String indexPath, Set<String> sharedWorkingSet) throws IOException {
            super(remote);
            this.remote = remote;
            this.local = local;
            this.indexPath = indexPath;

            this.localFileNames.addAll(Arrays.asList(local.listAll()));
            //Remove files which are being worked upon by COW
            this.localFileNames.removeAll(sharedWorkingSet);

            if (prefetch) {
                prefetchIndexFiles();
            }
        }

        @Override
        public void deleteFile(String name) throws IOException {
            throw new UnsupportedOperationException("Cannot delete in a ReadOnly directory");
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            throw new UnsupportedOperationException("Cannot write in a ReadOnly directory");
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (REMOTE_ONLY.contains(name)) {
                log.trace("[{}] opening remote only file {}", indexPath, name);
                return remote.openInput(name, context);
            }

            CORFileReference ref = files.get(name);
            if (ref != null) {
                if (ref.isLocalValid()) {
                    log.trace("[{}] opening existing local file {}", indexPath, name);
                    return files.get(name).openLocalInput(context);
                } else {
                    readerRemoteReadCount.incrementAndGet();
                    log.trace(
                            "[{}] opening existing remote file as local version is not valid {}",
                            indexPath, name);
                    return remote.openInput(name, context);
                }
            }

            //If file does not exist then just delegate to remote and not
            //schedule a copy task
            if (!remote.fileExists(name)){
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Looking for non existent file {}. Current known files {}",
                            indexPath, name, Arrays.toString(remote.listAll()));
                }
                return remote.openInput(name, context);
            }

            CORFileReference toPut = new CORFileReference(name);
            CORFileReference old = files.putIfAbsent(name, toPut);
            if (old == null) {
                log.trace("[{}] scheduled local copy for {}", indexPath, name);
                copy(toPut);
            }

            //If immediate executor is used the result would be ready right away
            if (toPut.isLocalValid()) {
                log.trace("[{}] opening new local file {}", indexPath, name);
                return toPut.openLocalInput(context);
            }

            log.trace("[{}] opening new remote file {}", indexPath, name);
            readerRemoteReadCount.incrementAndGet();
            return remote.openInput(name, context);
        }

        Directory getLocal() {
            return local;
        }

        private void copy(final CORFileReference reference) {
            updateMaxScheduled(scheduledForCopyCount.incrementAndGet());
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    scheduledForCopyCount.decrementAndGet();
                    copyFilesToLocal(reference, true, true);
                }
            });
        }

        private void prefetchIndexFiles() throws IOException {
            long start = PERF_LOGGER.start();
            long totalSize = 0;
            int copyCount = 0;
            List<String> copiedFileNames = Lists.newArrayList();
            for (String name : remote.listAll()) {
                if (REMOTE_ONLY.contains(name)) {
                    continue;
                }
                CORFileReference fileRef = new CORFileReference(name);
                files.putIfAbsent(name, fileRef);
                long fileSize = copyFilesToLocal(fileRef, false, false);
                if (fileSize > 0) {
                    copyCount++;
                    totalSize += fileSize;
                    copiedFileNames.add(name);
                }
            }

            local.sync(copiedFileNames);
            PERF_LOGGER.end(start, -1, "[{}] Copied {} files totaling {}", indexPath, copyCount, humanReadableByteCount(totalSize));
        }

        private long copyFilesToLocal(CORFileReference reference, boolean sync, boolean logDuration) {
            String name = reference.name;
            boolean success = false;
            boolean copyAttempted = false;
            long fileSize = 0;
            try {
                if (!local.fileExists(name)) {
                    long perfStart = -1;
                    if (logDuration) {
                        perfStart = PERF_LOGGER.start();
                    }

                    fileSize = remote.fileLength(name);
                    LocalIndexFile file = new LocalIndexFile(local, name, fileSize, true);
                    long start = startCopy(file);
                    copyAttempted = true;

                    remote.copy(local, name, name, IOContext.READ);
                    reference.markValid();

                    if (sync) {
                        local.sync(Collections.singleton(name));
                    }

                    doneCopy(file, start);
                    if (logDuration) {
                        PERF_LOGGER.end(perfStart, 0,
                                "[{}] Copied file {} of size {}", indexPath,
                                name, humanReadableByteCount(fileSize));
                    }
                } else {
                    long localLength = local.fileLength(name);
                    long remoteLength = remote.fileLength(name);

                    //Do a simple consistency check. Ideally Lucene index files are never
                    //updated but still do a check if the copy is consistent
                    if (localLength != remoteLength) {
                        log.warn("[{}] Found local copy for {} in {} but size of local {} differs from remote {}. " +
                                        "Content would be read from remote file only",
                                indexPath, name, local, localLength, remoteLength);
                        invalidFileCount.incrementAndGet();
                    } else {
                        reference.markValid();
                        log.trace("[{}] found local copy of file {}",
                                indexPath, name);
                    }
                }
                success = true;
            } catch (IOException e) {
                //TODO In case of exception there would not be any other attempt
                //to download the file. Look into support for retry
                log.warn("[{}] Error occurred while copying file [{}] from {} to {}", indexPath, name, remote, local, e);
            } finally {
                if (copyAttempted && !success){
                    try {
                        if (local.fileExists(name)) {
                            local.deleteFile(name);
                        }
                    } catch (IOException e) {
                        log.warn("[{}] Error occurred while deleting corrupted file [{}] from [{}]", indexPath, name, local, e);
                    }
                }
            }
            return fileSize;
        }

        /**
         * On close file which are not present in remote are removed from local.
         * CopyOnReadDir is opened at different revisions of the index state
         *
         * CDir1 - V1
         * CDir2 - V2
         *
         * Its possible that two different IndexSearcher are opened at same local
         * directory but pinned to different revisions. So while removing it must
         * be ensured that any currently opened IndexSearcher does not get affected.
         * The way IndexSearchers get created in IndexTracker it ensures that new searcher
         * pinned to newer revision gets opened first and then existing ones are closed.
         *
         *
         * @throws IOException
         */
        @Override
        public void close() throws IOException {
            //Always remove old index file on close as it ensures that
            //no other IndexSearcher are opened with previous revision of Index due to
            //way IndexTracker closes IndexNode. At max there would be only two IndexNode
            //opened pinned to different revision of same Lucene index
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try{
                        removeDeletedFiles();
                    } catch (IOException e) {
                        log.warn(
                                "[{}] Error occurred while removing deleted files from Local {}, Remote {}",
                                indexPath, local, remote, e);
                    }

                    try {
                        //This would also remove old index files if current
                        //directory was based on newerRevision as local would
                        //be of type DeleteOldDirOnClose
                        local.close();
                        remote.close();
                    } catch (IOException e) {
                        log.warn(
                                "[{}] Error occurred while closing directory ",
                                indexPath, e);
                    }
                }
            });
        }

        @Override
        public String toString() {
            return String.format("[COR] Local %s, Remote %s", local, remote);
        }

        private void removeDeletedFiles() throws IOException {
            //Files present in dest but not present in source have to be deleted
            Set<String> filesToBeDeleted = Sets.difference(
                    ImmutableSet.copyOf(localFileNames),
                    ImmutableSet.copyOf(remote.listAll())
            );

            Set<String> failedToDelete = Sets.newHashSet();

            for (String fileName : filesToBeDeleted) {
                boolean deleted = IndexCopier.this.deleteFile(local, fileName, true);
                if (!deleted){
                    failedToDelete.add(fileName);
                }
            }

            filesToBeDeleted = new HashSet<String>(filesToBeDeleted);
            filesToBeDeleted.removeAll(failedToDelete);
            if(!filesToBeDeleted.isEmpty()) {
                log.debug(
                        "[{}] Following files have been removed from Lucene index directory {}",
                        indexPath, filesToBeDeleted);
            }
        }

        private class CORFileReference {
            final String name;
            private volatile boolean valid;

            private CORFileReference(String name) {
                this.name = name;
            }

            boolean isLocalValid(){
                return valid;
            }

            IndexInput openLocalInput( IOContext context) throws IOException {
                readerLocalReadCount.incrementAndGet();
                return local.openInput(name, context);
            }

            void markValid(){
                this.valid = true;
                localFileNames.add(name);
            }
        }
    }

    private class CopyOnWriteDirectory extends FilterDirectory {
        /**
         * Signal for the background thread to stop processing changes.
         */
        private final Callable<Void> STOP = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                return null;
            }
        };
        private final Directory remote;
        private final Directory local;
        private final ConcurrentMap<String, COWFileReference> fileMap = newConcurrentMap();
        private final Set<String> deletedFilesLocal = Sets.newConcurrentHashSet();
        private final Set<String> skippedFiles = Sets.newConcurrentHashSet();

        private final BlockingQueue<Callable<Void>> queue = new LinkedBlockingQueue<Callable<Void>>();
        private final AtomicReference<Throwable> errorInCopy = new AtomicReference<Throwable>();
        private final CountDownLatch copyDone = new CountDownLatch(1);
        private final boolean reindexMode;
        private final String indexPathForLogging;
        private final Set<String> sharedWorkingSet;

        /**
         * Current background task
         */
        private volatile NotifyingFutureTask currentTask =  NotifyingFutureTask.completed();

        /**
         * Completion handler: set the current task to the next task and schedules that one
         * on the background thread.
         */
        private final Runnable completionHandler = new Runnable() {
            Callable<Void> task = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        Callable<Void> task = queue.poll();
                        if (task != null && task != STOP) {
                            if (errorInCopy.get() != null) {
                                log.trace("[COW][{}] Skipping task {} as some exception occurred in previous run",
                                        indexPathForLogging, task);
                            } else {
                                task.call();
                            }
                            currentTask.onComplete(completionHandler);
                        }

                        //Signal that all tasks completed
                        if (task == STOP){
                            copyDone.countDown();
                        }
                    } catch (Throwable t) {
                        errorInCopy.set(t);
                        log.debug("[COW][{}] Error occurred while copying files. Further processing would " +
                                "be skipped", indexPathForLogging, t);
                        currentTask.onComplete(completionHandler);
                    }
                    return null;
                }
            };

            @Override
            public void run() {
                currentTask = new NotifyingFutureTask(task);
                try {
                    executor.execute(currentTask);
                } catch (RejectedExecutionException e){
                    checkIfClosed(false);
                    throw e;
                }
            }
        };

        public CopyOnWriteDirectory(Directory remote, Directory local, boolean reindexMode,
                                    String indexPathForLogging, Set<String> sharedWorkingSet) throws IOException {
            super(local);
            this.remote = remote;
            this.local = local;
            this.indexPathForLogging = indexPathForLogging;
            this.reindexMode = reindexMode;
            this.sharedWorkingSet = sharedWorkingSet;
            initialize();
        }

        @Override
        public String[] listAll() throws IOException {
            return Iterables.toArray(fileMap.keySet(), String.class);
        }

        @Override
        public boolean fileExists(String name) throws IOException {
            return fileMap.containsKey(name);
        }

        @Override
        public void deleteFile(String name) throws IOException {
            log.trace("[COW][{}] Deleted file {}", indexPathForLogging, name);
            COWFileReference ref = fileMap.remove(name);
            if (ref != null) {
                ref.delete();
            }
        }

        @Override
        public long fileLength(String name) throws IOException {
            COWFileReference ref = fileMap.get(name);
            if (ref == null) {
                throw new FileNotFoundException(name);
            }
            return ref.fileLength();
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            COWFileReference ref = fileMap.remove(name);
            if (ref != null) {
                ref.delete();
            }
            ref = new COWLocalFileReference(name);
            fileMap.put(name, ref);
            sharedWorkingSet.add(name);
            return ref.createOutput(context);
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            for (String name : names){
                COWFileReference file = fileMap.get(name);
                if (file != null){
                    file.sync();
                }
            }
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            COWFileReference ref = fileMap.get(name);
            if (ref == null) {
                throw new FileNotFoundException(name);
            }
            return ref.openInput(context);
        }

        @Override
        public void close() throws IOException {
            int pendingCopies = queue.size();
            addTask(STOP);

            //Wait for all pending copy task to finish
            try {
                long start = PERF_LOGGER.start();

                //Loop untill queue finished or IndexCopier
                //found to be closed. Doing it with timeout to
                //prevent any bug causing the thread to wait indefinitely
                while (!copyDone.await(10, TimeUnit.SECONDS)) {
                    if (closed) {
                        throw new IndexCopierClosedException("IndexCopier found to be closed " +
                                "while processing copy task for" + remote.toString());
                    }
                }
                PERF_LOGGER.end(start, -1, "[COW][{}] Completed pending copying task {}", indexPathForLogging, pendingCopies);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }

            Throwable t = errorInCopy.get();
            if (t != null){
                throw new IOException("Error occurred while copying files for " + indexPathForLogging, t);
            }

            //Sanity check
            checkArgument(queue.isEmpty(), "Copy queue still " +
                    "has pending task left [%d]. %s", queue.size(), queue);

            long skippedFilesSize = getSkippedFilesSize();

            for (String fileName : deletedFilesLocal){
                deleteLocalFile(fileName);
            }

            skippedFromUploadSize.addAndGet(skippedFilesSize);

            String msg = "[COW][{}] CopyOnWrite stats : Skipped copying {} files with total size {}";
            if ((reindexMode && skippedFilesSize > 0) || skippedFilesSize > 10 * FileUtils.ONE_MB){
                log.info(msg, indexPathForLogging, skippedFiles.size(), humanReadableByteCount(skippedFilesSize));
            } else {
                log.debug(msg,indexPathForLogging, skippedFiles.size(), humanReadableByteCount(skippedFilesSize));
            }

            if (log.isTraceEnabled()){
                log.trace("[COW][{}] File listing - Upon completion {}", indexPathForLogging, Arrays.toString(remote.listAll()));
            }

            local.close();
            remote.close();
            sharedWorkingSet.clear();
        }

        @Override
        public String toString() {
            return String.format("[COW][%s] Local %s, Remote %s", indexPathForLogging, local, remote);
        }

        private long getSkippedFilesSize() {
            long size = 0;
            for (String name : skippedFiles){
                try{
                    if (local.fileExists(name)){
                        size += local.fileLength(name);
                    }
                } catch (Exception ignore){

                }
            }
            return size;
        }

        private void deleteLocalFile(String fileName) {
            IndexCopier.this.deleteFile(local, fileName, false);
        }

        private void initialize() throws IOException {
            for (String name : remote.listAll()) {
                fileMap.put(name, new COWRemoteFileReference(name));
            }

            if (log.isTraceEnabled()){
                log.trace("[COW][{}] File listing - At start {}", indexPathForLogging, Arrays.toString(remote.listAll()));
            }
        }

        private void addCopyTask(final String name){
            updateMaxScheduled(scheduledForCopyCount.incrementAndGet());
            addTask(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    scheduledForCopyCount.decrementAndGet();
                    if (deletedFilesLocal.contains(name)){
                        skippedFiles.add(name);
                        log.trace("[COW][{}] Skip copying of deleted file {}", indexPathForLogging, name);
                        return null;
                    }
                    long fileSize = local.fileLength(name);
                    LocalIndexFile file = new LocalIndexFile(local, name, fileSize, false);
                    long perfStart = PERF_LOGGER.start();
                    long start = startCopy(file);

                    local.copy(remote, name, name, IOContext.DEFAULT);

                    doneCopy(file, start);
                    PERF_LOGGER.end(perfStart, 0, "[COW][{}] Copied to remote {} -- size: {}",
                        indexPathForLogging, name, IOUtils.humanReadableByteCount(fileSize));
                    return null;
                }

                @Override
                public String toString() {
                    return "Copy: " + name;
                }
            });
        }

        private void addDeleteTask(final String name){
            addTask(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (!skippedFiles.contains(name)) {
                        log.trace("[COW][{}] Marking as deleted {}", indexPathForLogging, name);
                        remote.deleteFile(name);
                    }
                    return null;
                }

                @Override
                public String toString() {
                    return "Delete : " + name;
                }
            });
        }

        private void addTask(Callable<Void> task){
            checkIfClosed(true);
            queue.add(task);
            currentTask.onComplete(completionHandler);
        }

        private void checkIfClosed(boolean throwException) {
            if (closed) {
                IndexCopierClosedException e = new IndexCopierClosedException("IndexCopier found to be closed " +
                        "while processing" +remote.toString());
                errorInCopy.set(e);
                copyDone.countDown();

                if (throwException) {
                    throw e;
                }
            }
        }

        private abstract class COWFileReference {
            protected final String name;

            public COWFileReference(String name) {
                this.name = name;
            }

            public abstract long fileLength() throws IOException;

            public abstract IndexInput openInput(IOContext context) throws IOException;

            public abstract IndexOutput createOutput(IOContext context) throws IOException;

            public abstract void delete() throws IOException;

            public void sync() throws IOException {

            }
        }

        private class COWRemoteFileReference extends COWFileReference {
            private boolean validLocalCopyPresent;
            private final long length;

            public COWRemoteFileReference(String name) throws IOException {
                super(name);
                this.length = remote.fileLength(name);
            }

            @Override
            public long fileLength() throws IOException {
                return length;
            }

            @Override
            public IndexInput openInput(IOContext context) throws IOException {
                checkIfLocalValid();
                if (validLocalCopyPresent && !REMOTE_ONLY.contains(name)) {
                    writerLocalReadCount.incrementAndGet();
                    return local.openInput(name, context);
                }
                writerRemoteReadCount.incrementAndGet();
                return remote.openInput(name, context);
            }

            @Override
            public IndexOutput createOutput(IOContext context) throws IOException {
                throw new UnsupportedOperationException("Cannot create output for existing remote file " + name);
            }

            @Override
            public void delete() throws IOException {
                //Remote file should not be deleted locally as it might be
                //in use by existing opened IndexSearcher. It would anyway
                //get deleted by CopyOnRead later
                //For now just record that these need to be deleted to avoid
                //potential concurrent access of the NodeBuilder
                addDeleteTask(name);
            }

            private void checkIfLocalValid() throws IOException {
                validLocalCopyPresent = local.fileExists(name)
                        && local.fileLength(name) == remote.fileLength(name);
            }
        }

        private class COWLocalFileReference extends COWFileReference {
            public COWLocalFileReference(String name) {
                super(name);
            }

            @Override
            public long fileLength() throws IOException {
                return local.fileLength(name);
            }

            @Override
            public IndexInput openInput(IOContext context) throws IOException {
                return local.openInput(name, context);
            }

            @Override
            public IndexOutput createOutput(IOContext context) throws IOException {
                log.debug("[COW][{}] Creating output {}", indexPathForLogging, name);
                return new CopyOnCloseIndexOutput(local.createOutput(name, context));
            }

            @Override
            public void delete() throws IOException {
                addDeleteTask(name);
                deletedFilesLocal.add(name);
            }

            @Override
            public void sync() throws IOException {
                local.sync(Collections.singleton(name));
            }

            /**
             * Implementation note - As we are decorating existing implementation
             * we would need to ensure that we also override methods (non abstract)
             * which might be implemented in say FSIndexInput like setLength
             */
            private class CopyOnCloseIndexOutput extends IndexOutput {
                private final IndexOutput delegate;

                public CopyOnCloseIndexOutput(IndexOutput delegate) {
                    this.delegate = delegate;
                }

                @Override
                public void flush() throws IOException {
                    delegate.flush();
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                    //Schedule this file to be copied in background
                    addCopyTask(name);
                }

                @Override
                public long getFilePointer() {
                    return delegate.getFilePointer();
                }

                @Override
                public void seek(long pos) throws IOException {
                    delegate.seek(pos);
                }

                @Override
                public long length() throws IOException {
                    return delegate.length();
                }

                @Override
                public void writeByte(byte b) throws IOException {
                    delegate.writeByte(b);
                }

                @Override
                public void writeBytes(byte[] b, int offset, int length) throws IOException {
                    delegate.writeBytes(b, offset, length);
                }

                @Override
                public void setLength(long length) throws IOException {
                    delegate.setLength(length);
                }
            }
        }
    }

    private boolean deleteFile(Directory dir, String fileName, boolean copiedFromRemote){
        LocalIndexFile file = new LocalIndexFile(dir, fileName, getFileLength(dir, fileName), copiedFromRemote);
        boolean successFullyDeleted = false;
        try {
            boolean fileExisted = false;
            if (dir.fileExists(fileName)) {
                fileExisted = true;
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

    private long startCopy(LocalIndexFile file) {
        updateMaxInProgress(copyInProgressCount.incrementAndGet());
        copyInProgressSize.addAndGet(file.size);
        copyInProgressFiles.add(file);
        return System.currentTimeMillis();
    }

    private void doneCopy(LocalIndexFile file, long start) {
        copyInProgressFiles.remove(file);
        copyInProgressCount.decrementAndGet();
        copyInProgressSize.addAndGet(-file.size);

        if(file.copyFromRemote) {
            downloadTime.addAndGet(System.currentTimeMillis() - start);
            downloadSize.addAndGet(file.size);
            downloadCount.incrementAndGet();
        } else {
            uploadSize.addAndGet(file.size);
            uploadTime.addAndGet(System.currentTimeMillis() - start);
            uploadCount.incrementAndGet();
        }

    }

    private void updateMaxScheduled(int val) {
        synchronized (maxScheduledForCopyCount){
            int current = maxScheduledForCopyCount.get();
            if (val > current){
                maxScheduledForCopyCount.set(val);
            }
        }
    }

    private void updateMaxInProgress(int val) {
        synchronized (maxCopyInProgressCount){
            int current = maxCopyInProgressCount.get();
            if (val > current){
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
                try{
                    FileUtils.deleteDirectory(oldIndexDir);
                    log.debug("Removed old index content from {} ", oldIndexDir);
                } catch (IOException e){
                    log.warn("Not able to remove old version of copied index at {}", oldIndexDir, e);
                }
            }
        }

        @Override
        public String toString() {
            return "DeleteOldDirOnClose wrapper for " + getDelegate();
        }
    }
    
    static final class LocalIndexFile {
        final File dir;
        final String name;
        final long size;
        final boolean copyFromRemote;
        private volatile int deleteAttemptCount;
        final long creationTime = System.currentTimeMillis();
        
        public LocalIndexFile(Directory dir, String fileName,
                              long size, boolean copyFromRemote){
            this.copyFromRemote = copyFromRemote;
            this.dir = getFSDir(dir);
            this.name = fileName;
            this.size = size;
        }

        public LocalIndexFile(Directory dir, String fileName){
            this(dir, fileName, getFileLength(dir, fileName), true);
        }

        public String getKey(){
            if (dir != null){
                return new File(dir, name).getAbsolutePath();
            }
            return name;
        }

        public void incrementAttemptToDelete(){
            deleteAttemptCount++;
        }

        public int getDeleteAttemptCount() {
            return deleteAttemptCount;
        }

        public String deleteLog(){
            return String.format("%s (%s, %d attempts, %d s)", name,
                    humanReadableByteCount(size), deleteAttemptCount, timeTaken());
        }

        public String copyLog(){
            return String.format("%s (%s, %1.1f%%, %s, %d s)", name,
                    humanReadableByteCount(actualSize()),
                    copyProgress(),
                    humanReadableByteCount(size), timeTaken());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LocalIndexFile localIndexFile = (LocalIndexFile) o;

            if (dir != null ? !dir.equals(localIndexFile.dir) : localIndexFile.dir != null)
                return false;
            return name.equals(localIndexFile.name);

        }

        @Override
        public int hashCode() {
            int result = dir != null ? dir.hashCode() : 0;
            result = 31 * result + name.hashCode();
            return result;
        }

        private long timeTaken(){
            return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - creationTime);
        }

        private float copyProgress(){
            return actualSize() * 1.0f / size * 100;
        }

        private long actualSize(){
            return dir != null ? new File(dir, name).length() : 0;
        }
    }

    static File getFSDir(Directory dir) {
        if (dir instanceof FilterDirectory){
            dir = ((FilterDirectory) dir).getDelegate();
        }

        if (dir instanceof FSDirectory){
            return ((FSDirectory) dir).getDirectory();
        }

        return null;
    }

    /**
     * Get the file length in best effort basis.
     * @return actual fileLength. -1 if cannot determine
     */
    private static long getFileLength(Directory dir, String fileName){
        try{
            return dir.fileLength(fileName);
        } catch (Exception e){
            return -1;
        }
    }

    //~------------------------------------------< CopyOnReadStatsMBean >

    @Override
    public TabularData getIndexPathMapping() {
        TabularDataSupport tds;
        try{
            TabularType tt = new TabularType(IndexMappingData.class.getName(),
                    "Lucene Index Stats", IndexMappingData.TYPE, new String[]{"jcrPath"});
            tds = new TabularDataSupport(tt);
            for (LocalIndexDir indexDir : indexRootDirectory.getAllLocalIndexes()){
                String size = humanReadableByteCount(indexDir.size());
                tds.put(new CompositeDataSupport(IndexMappingData.TYPE,
                        IndexMappingData.FIELD_NAMES,
                        new String[]{indexDir.getJcrPath(), indexDir.getFSPath(), size}));
            }
        } catch (OpenDataException e){
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }

    @Override
    public boolean isPrefetchEnabled() {
        return prefetchEnabled;
    }

    @Override
    public int getReaderLocalReadCount() {
        return readerLocalReadCount.get();
    }

    @Override
    public int getReaderRemoteReadCount() {
        return readerRemoteReadCount.get();
    }

    @Override
    public int getWriterLocalReadCount() {
        return writerLocalReadCount.get();
    }

    @Override
    public int getWriterRemoteReadCount() {
        return writerRemoteReadCount.get();
    }

    public int getInvalidFileCount(){
        return invalidFileCount.get();
    }

    @Override
    public String getDownloadSize() {
        return humanReadableByteCount(downloadSize.get());
    }

    @Override
    public long getDownloadTime() {
        return downloadTime.get();
    }

    @Override
    public int getDownloadCount() {
        return downloadCount.get();
    }

    @Override
    public int getUploadCount() {
        return uploadCount.get();
    }

    @Override
    public String getUploadSize() {
        return humanReadableByteCount(uploadSize.get());
    }

    @Override
    public long getUploadTime() {
        return uploadTime.get();
    }

    @Override
    public String getLocalIndexSize() {
        return humanReadableByteCount(indexRootDirectory.getSize());
    }

    @Override
    public String[] getGarbageDetails() {
        return toArray(transform(failedToDeleteFiles.values(),
                new Function<LocalIndexFile, String>() {
                    @Override
                    public String apply(LocalIndexFile input) {
                        return input.deleteLog();
                    }
                }), String.class);
    }

    @Override
    public String getGarbageSize() {
        long garbageSize = 0;
        for (LocalIndexFile failedToDeleteFile : failedToDeleteFiles.values()){
            garbageSize += failedToDeleteFile.size;
        }
        return humanReadableByteCount(garbageSize);
    }

    @Override
    public int getScheduledForCopyCount() {
        return scheduledForCopyCount.get();
    }

    @Override
    public int getCopyInProgressCount() {
        return copyInProgressCount.get();
    }

    @Override
    public String getCopyInProgressSize() {
        return humanReadableByteCount(copyInProgressSize.get());
    }

    @Override
    public int getMaxCopyInProgressCount() {
        return maxCopyInProgressCount.get();
    }

    @Override
    public int getMaxScheduledForCopyCount() {
        return maxScheduledForCopyCount.get();
    }

    public String getSkippedFromUploadSize() {
        return humanReadableByteCount(skippedFromUploadSize.get());
    }

    @Override
    public String[] getCopyInProgressDetails() {
        return toArray(transform(copyInProgressFiles,
                new Function<LocalIndexFile, String>() {
                    @Override
                    public String apply(LocalIndexFile input) {
                        return input.copyLog();
                    }
                }), String.class);
    }

    @Override
    public int getDeletedFilesCount() {
        return deletedFileCount.get();
    }

    @Override
    public String getGarbageCollectedSize() {
        return humanReadableByteCount(garbageCollectedSize.get());
    }

    private static class IndexMappingData {
        static final String[] FIELD_NAMES = new String[]{
                "jcrPath",
                "fsPath",
                "size",
        };

        static final String[] FIELD_DESCRIPTIONS = new String[]{
                "JCR Path",
                "Filesystem Path",
                "Size",
        };

        static final OpenType[] FIELD_TYPES = new OpenType[]{
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.STRING,
        };

        static final CompositeType TYPE = createCompositeType();

        static CompositeType createCompositeType() {
            try {
                return new CompositeType(
                        IndexMappingData.class.getName(),
                        "Composite data type for Index Mapping Data",
                        IndexMappingData.FIELD_NAMES,
                        IndexMappingData.FIELD_DESCRIPTIONS,
                        IndexMappingData.FIELD_TYPES);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
