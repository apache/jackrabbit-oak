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
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.stream;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

/**
 * Directory implementation which lazily copies the index files from a
 * remote directory in background.
 */
public class CopyOnReadDirectory extends FilterDirectory {
    private static final Logger log = LoggerFactory.getLogger(CopyOnReadDirectory.class);
    private static final PerfLogger PERF_LOGGER = new PerfLogger(LoggerFactory.getLogger(log.getName() + ".perf"));

    public static final String DELETE_MARGIN_MILLIS_NAME = "oak.lucene.delete.margin";
    public final long DELETE_MARGIN_MILLIS = Long.getLong(DELETE_MARGIN_MILLIS_NAME, TimeUnit.MINUTES.toMillis(5));

    private final IndexCopier indexCopier;
    private final Directory remote;
    private final Directory local;
    private final boolean prefetch;
    private final String indexPath;
    private final Executor executor;
    private final AtomicBoolean closed = new AtomicBoolean();

    // exported as package private to be useful in tests
    static final String WAIT_OTHER_COPY_SYSPROP_NAME = "cor.waitCopyMillis";

    long waitOtherCopyTimeoutMillis = Long.getLong(WAIT_OTHER_COPY_SYSPROP_NAME, TimeUnit.SECONDS.toMillis(30));

    private final ConcurrentMap<String, CORFileReference> files = new ConcurrentHashMap<>();

    public CopyOnReadDirectory(IndexCopier indexCopier, Directory remote, Directory local, boolean prefetch,
                               String indexPath, Executor executor) throws IOException {
        super(remote);
        this.indexCopier = indexCopier;
        this.executor = executor;
        this.remote = remote;
        this.local = local;
        this.prefetch = prefetch;
        this.indexPath = indexPath;

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
        if (Objects.nonNull(name) && IndexCopier.REMOTE_ONLY.contains(name)) {
            log.trace("[{}] opening remote only file {}", indexPath, name);
            return remote.openInput(name, context);
        }

        CORFileReference ref = files.get(name);
        if (ref != null) {
            if (ref.isLocalValid()) {
                log.trace("[{}] opening existing local file {}", indexPath, name);
                return files.get(name).openLocalInput(context);
            } else {
                indexCopier.readFromRemote(true);
                logRemoteAccess(
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

        logRemoteAccess("[{}] opening new remote file {}", indexPath, name);
        indexCopier.readFromRemote(true);
        return remote.openInput(name, context);
    }

    public Directory getLocal() {
        return local;
    }

    private void copy(final CORFileReference reference) {
        indexCopier.scheduledForCopy();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                indexCopier.copyDone();
                copyFilesToLocal(reference, true, true);
            }
        });
    }

    private void prefetchIndexFiles() throws IOException {
        long start = PERF_LOGGER.start();
        long totalSize = 0;
        int copyCount = 0;
        List<String> copiedFileNames = new ArrayList<>();
        for (String name : remote.listAll()) {
            if (IndexCopier.REMOTE_ONLY.contains(name)) {
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
                long start = indexCopier.startCopy(file);
                copyAttempted = true;

                remote.copy(local, name, name, IOContext.READ);
                reference.markValid();

                if (sync) {
                    local.sync(Collections.singleton(name));
                }

                indexCopier.doneCopy(file, start);
                if (logDuration) {
                    PERF_LOGGER.end(perfStart, 0,
                            "[{}] Copied file {} of size {}", indexPath,
                            name, humanReadableByteCount(fileSize));
                }
            } else {
                long remoteLength = remote.fileLength(name);

                LocalIndexFile file = new LocalIndexFile(local, name, remoteLength, true);
                // as a local file exists, attempt a wait for completion of any potential ongoing concurrent copy
                indexCopier.waitForCopyCompletion(file, waitOtherCopyTimeoutMillis);

                long localLength = local.fileLength(name);

                //Do a simple consistency check. Ideally Lucene index files are never
                //updated but still do a check if the copy is consistent
                if (localLength != remoteLength) {
                    if (!indexCopier.isCopyInProgress(file)) {
                        log.warn("[{}] Found local copy for {} in {} but size of local {} differs from remote {}. " +
                                        "Content would be read from remote file only",
                                indexPath, name, local, localLength, remoteLength);
                        indexCopier.foundInvalidFile();
                    } else {

                        logRemoteAccess("[{}] Found in progress copy of file {}. Would read from remote", indexPath, name);
                    }
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
     * Close the files _after_ the method returns (asynchronously).
     *
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
     */
    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)){
            return;
        }
        //Always remove old index file on close as it ensures that
        //no other IndexSearcher are opened with previous revision of Index due to
        //way IndexTracker closes LuceneIndexNode. At max there would be only two LuceneIndexNode
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
        Set<String> remoteFiles = stream(remote.listAll())
                .filter(name -> !IndexCopier.REMOTE_ONLY.contains(name))
                .collect(Collectors.toSet());

        long maxTS = IndexCopier.getNewestLocalFSTimestampFor(remoteFiles, local);
        if (maxTS == -1) {
            log.warn("Couldn't compute safe timestamp to delete files from {}", local);
            return;
        }

        // subtract DELETE_MARGIN_MILLIS from maxTS for safety (you can never be too careful with time)
        final long deleteBeforeTS = maxTS - DELETE_MARGIN_MILLIS;

        Set<String> filesToBeDeleted =
                // Files present locally
                CollectionUtils.toSet(local.listAll()).stream()
                // but not in my view
                .filter(name -> !remoteFiles.contains(name))
                // and also older than a safe timestamp (deleteBeforeTS)
                .filter(name -> IndexCopier.isFileModifiedBefore(name, local, deleteBeforeTS))
                // can be deleted
                .collect(Collectors.toSet())
        ;

        Set<String> failedToDelete = new HashSet<>();

        for (String fileName : filesToBeDeleted) {
            boolean deleted = indexCopier.deleteFile(local, fileName, true);
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

    private void logRemoteAccess(String format, Object o1, Object o2) {
        if (prefetch) {
            log.warn(format, o1, o2);
        } else {
            log.trace(format, o1, o2);
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
            indexCopier.readFromLocal(true);
            return local.openInput(name, context);
        }

        void markValid(){
            this.valid = true;
        }
    }
}
