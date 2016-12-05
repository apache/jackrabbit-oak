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
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnWriteDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexSanityChecker;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexFile;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

public class IndexCopier implements CopyOnReadStatsMBean, Closeable {
    public static final Set<String> REMOTE_ONLY = ImmutableSet.of("segments.gen");
    private static final int MAX_FAILURE_ENTRIES = 10000;
    private static final String WORK_DIR_NAME = "indexWriterDir";

    private final Logger log = LoggerFactory.getLogger(getClass());
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


    private final Map<String, Set<String>> sharedWorkingSetMap = newHashMap();
    private final Map<String, String> indexPathVersionMapping = newConcurrentMap();
    private final ConcurrentMap<String, LocalIndexFile> failedToDeleteFiles = newConcurrentMap();
    private final Set<LocalIndexFile> copyInProgressFiles = Collections.newSetFromMap(new ConcurrentHashMap<LocalIndexFile, Boolean>());
    private final boolean prefetchEnabled;
    private volatile boolean closed;
    private final IndexRootDirectory indexRootDirectory;
    private final Set<String> validatedIndexPaths = Sets.newConcurrentHashSet();

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
                                 Directory remote, String dirName) throws IOException {
        Directory local = createLocalDirForIndexReader(indexPath, definition, dirName);
        checkIntegrity(indexPath, local, remote);
        return new CopyOnReadDirectory(this, remote, local, prefetchEnabled, indexPath, executor);
    }

    public Directory wrapForWrite(IndexDefinition definition, Directory remote, boolean reindexMode, String dirName) throws IOException {
        Directory local = createLocalDirForIndexWriter(definition, dirName);
        String indexPath = definition.getIndexPath();
        checkIntegrity(indexPath, local, remote);
        return new CopyOnWriteDirectory(this, remote, local, reindexMode, indexPath, executor);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    File getIndexWorkDir() {
        return indexWorkDir;
    }

    IndexRootDirectory getIndexRootDirectory() {
        return indexRootDirectory;
    }

    protected Directory createLocalDirForIndexWriter(IndexDefinition definition, String dirName) throws IOException {
        String indexPath = definition.getIndexPath();
        File indexWriterDir = getIndexDir(definition, indexPath, dirName);

        //By design indexing in Oak is single threaded so Lucene locking
        //can be disabled
        Directory dir = FSDirectory.open(indexWriterDir, NoLockFactory.getNoLockFactory());

        log.debug("IndexWriter would use {}", indexWriterDir);
        return dir;
    }

    protected Directory createLocalDirForIndexReader(String indexPath, IndexDefinition definition, String dirName) throws IOException {
        File indexDir = getIndexDir(definition, indexPath, dirName);
        Directory result = FSDirectory.open(indexDir);

        String newPath = indexDir.getAbsolutePath();
        String oldPath = indexPathVersionMapping.put(createIndexPathKey(indexPath, dirName), newPath);
        if (!newPath.equals(oldPath) && oldPath != null) {
            result = new DeleteOldDirOnClose(result, new File(oldPath));
        }
        return result;
    }

    public File getIndexDir(IndexDefinition definition, String indexPath, String dirName) throws IOException {
        return indexRootDirectory.getIndexDir(definition, indexPath, dirName);
    }

    public void addIndexFileBeingWritten(String indexPath, String name) {
        getSharedWorkingSet(indexPath).add(name);
    }

    public void clearIndexFilesBeingWritten(String indexPath) {
        getSharedWorkingSet(indexPath).clear();
    }

    public Set<String> getIndexFilesBeingWritten(String indexPath) {
        return getSharedWorkingSet(indexPath);
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
            garbageCollectedSize.addAndGet(file.getSize());
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

    private void checkIntegrity(String indexPath, Directory local, Directory remote) throws IOException {
        if (validatedIndexPaths.contains(indexPath)){
            return;
        }

        //The integrity check needs to be done for the very first time at startup when
        //a directory gets created as at that time it can be ensured that there is no
        //work in progress files, no memory mapping issue etc
        //Also at this time its required that state in local dir should exactly same as
        //one in remote dir
        synchronized (validatedIndexPaths){
            new IndexSanityChecker(indexPath, local, remote).check();
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
        checkState(workDir.mkdirs(), "Cannot create directory %s", workDir);
        return workDir;
    }

    /**
     * Create a unique key based on indexPath and dirName used under that path
     */
    private static String createIndexPathKey(String indexPath, String dirName){
        return indexPath.concat(dirName);
    }

    public boolean deleteFile(Directory dir, String fileName, boolean copiedFromRemote){
        LocalIndexFile file = new LocalIndexFile(dir, fileName, DirectoryUtils.getFileLength(dir, fileName), copiedFromRemote);
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

    public long startCopy(LocalIndexFile file) {
        updateMaxInProgress(copyInProgressCount.incrementAndGet());
        copyInProgressSize.addAndGet(file.getSize());
        copyInProgressFiles.add(file);
        return System.currentTimeMillis();
    }

    public boolean isCopyInProgress(LocalIndexFile file) {
        return copyInProgressFiles.contains(file);
    }

    public void doneCopy(LocalIndexFile file, long start) {
        copyInProgressFiles.remove(file);
        copyInProgressCount.decrementAndGet();
        copyInProgressSize.addAndGet(-file.getSize());

        if(file.isCopyFromRemote()) {
            downloadTime.addAndGet(System.currentTimeMillis() - start);
            downloadSize.addAndGet(file.getSize());
            downloadCount.incrementAndGet();
        } else {
            uploadSize.addAndGet(file.getSize());
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
                    long totalDeletedSize = FileUtils.sizeOf(oldIndexDir);
                    FileUtils.deleteDirectory(oldIndexDir);
                    totalDeletedSize  += indexRootDirectory.gcEmptyDirs(oldIndexDir);
                    garbageCollectedSize.addAndGet(totalDeletedSize);
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

    //~------------------------------------------< Stats Collection >

    public void skippedUpload(long skippedFilesSize) {
        skippedFromUploadSize.addAndGet(skippedFilesSize);
    }

    public void scheduledForCopy() {
        updateMaxScheduled(scheduledForCopyCount.incrementAndGet());
    }

    public void copyDone(){
        scheduledForCopyCount.decrementAndGet();
    }

    public void readFromRemote(boolean reader) {
        if (reader) {
            readerRemoteReadCount.incrementAndGet();
        } else {
            writerRemoteReadCount.incrementAndGet();
        }
    }

    public void readFromLocal(boolean reader) {
        if (reader) {
            readerLocalReadCount.incrementAndGet();
        } else {
            writerLocalReadCount.incrementAndGet();
        }
    }

    public void foundInvalidFile(){
        invalidFileCount.incrementAndGet();
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
            garbageSize += failedToDeleteFile.getSize();
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
