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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.commons.concurrent.NotifyingFutureTask;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopierClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newConcurrentMap;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

public class CopyOnWriteDirectory extends FilterDirectory {
    private static final Logger log = LoggerFactory.getLogger(CopyOnWriteDirectory.class);
    private static final PerfLogger PERF_LOGGER = new PerfLogger(LoggerFactory.getLogger(log.getName() + ".perf"));
    private final IndexCopier indexCopier;
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
    private final Executor executor;
    private final ConcurrentMap<String, COWFileReference> fileMap = newConcurrentMap();
    private final Set<String> deletedFilesLocal = Sets.newConcurrentHashSet();
    private final Set<String> skippedFiles = Sets.newConcurrentHashSet();

    private final BlockingQueue<Callable<Void>> queue = new LinkedBlockingQueue<Callable<Void>>();
    private final AtomicReference<Throwable> errorInCopy = new AtomicReference<Throwable>();
    private final CountDownLatch copyDone = new CountDownLatch(1);
    private final boolean reindexMode;
    private final String indexPath;

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
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
            @Override
            public Void call() throws Exception {
                try {
                    Callable<Void> task = queue.poll();
                    if (task != null && task != STOP) {
                        if (errorInCopy.get() != null) {
                            log.trace("[COW][{}] Skipping task {} as some exception occurred in previous run",
                                    indexPath, task);
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
                            "be skipped", indexPath, t);
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

    public CopyOnWriteDirectory(IndexCopier indexCopier, Directory remote, Directory local, boolean reindexMode,
                                String indexPath, Executor executor) throws
            IOException {
        super(local);
        this.indexCopier = indexCopier;
        this.remote = remote;
        this.local = local;
        this.executor = executor;
        this.indexPath = indexPath;
        this.reindexMode = reindexMode;
        indexCopier.clearIndexFilesBeingWritten(indexPath);
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
        log.trace("[COW][{}] Deleted file {}", indexPath, name);
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
        indexCopier.addIndexFileBeingWritten(indexPath, name);
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
                if (indexCopier.isClosed()) {
                    throw new IndexCopierClosedException("IndexCopier found to be closed " +
                            "while processing copy task for" + remote.toString());
                }
            }
            PERF_LOGGER.end(start, -1, "[COW][{}] Completed pending copying task {}", indexPath, pendingCopies);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }

        Throwable t = errorInCopy.get();
        if (t != null){
            throw new IOException("Error occurred while copying files for " + indexPath, t);
        }

        //Sanity check
        checkArgument(queue.isEmpty(), "Copy queue still " +
                "has pending task left [%d]. %s", queue.size(), queue);

        long skippedFilesSize = getSkippedFilesSize();

        for (String fileName : deletedFilesLocal){
            deleteLocalFile(fileName);
        }

        indexCopier.skippedUpload(skippedFilesSize);

        String msg = "[COW][{}] CopyOnWrite stats : Skipped copying {} files with total size {}";
        if ((reindexMode && skippedFilesSize > 0) || skippedFilesSize > 10 * FileUtils.ONE_MB){
            log.info(msg, indexPath, skippedFiles.size(), humanReadableByteCount(skippedFilesSize));
        } else {
            log.debug(msg, indexPath, skippedFiles.size(), humanReadableByteCount(skippedFilesSize));
        }

        if (log.isTraceEnabled()){
            log.trace("[COW][{}] File listing - Upon completion {}", indexPath, Arrays.toString(remote.listAll()));
        }

        local.close();
        remote.close();
        indexCopier.clearIndexFilesBeingWritten(indexPath);
    }

    @Override
    public String toString() {
        return String.format("[COW][%s] Local %s, Remote %s", indexPath, local, remote);
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
        indexCopier.deleteFile(local, fileName, false);
    }

    private void initialize() throws IOException {
        for (String name : remote.listAll()) {
            fileMap.put(name, new COWRemoteFileReference(name));
        }

        if (log.isTraceEnabled()){
            log.trace("[COW][{}] File listing - At start {}", indexPath, Arrays.toString(remote.listAll()));
        }
    }

    private void addCopyTask(final String name){
        indexCopier.scheduledForCopy();
        addTask(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                indexCopier.copyDone();
                if (deletedFilesLocal.contains(name)){
                    skippedFiles.add(name);
                    log.trace("[COW][{}] Skip copying of deleted file {}", indexPath, name);
                    return null;
                }
                long fileSize = local.fileLength(name);
                LocalIndexFile file = new LocalIndexFile(local, name, fileSize, false);
                long perfStart = PERF_LOGGER.start();
                long start = indexCopier.startCopy(file);

                local.copy(remote, name, name, IOContext.DEFAULT);

                indexCopier.doneCopy(file, start);
                PERF_LOGGER.end(perfStart, 0, "[COW][{}] Copied to remote {} -- size: {}",
                        indexPath, name, IOUtils.humanReadableByteCount(fileSize));
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
                    log.trace("[COW][{}] Marking as deleted {}", indexPath, name);
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
        if (indexCopier.isClosed()) {
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
            if (validLocalCopyPresent && !IndexCopier.REMOTE_ONLY.contains(name)) {
                indexCopier.readFromLocal(false);
                return local.openInput(name, context);
            }
            indexCopier.readFromRemote(false);
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
            log.debug("[COW][{}] Creating output {}", indexPath, name);
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

            @SuppressWarnings("deprecation")
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
