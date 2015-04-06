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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newConcurrentMap;

class IndexCopier implements CopyOnReadStatsMBean {
    private static final Set<String> REMOTE_ONLY = ImmutableSet.of("segments.gen");
    private static final int MAX_FAILURE_ENTRIES = 10000;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Executor executor;
    private final File indexRootDir;

    private final AtomicInteger localReadCount = new AtomicInteger();
    private final AtomicInteger remoteReadCount = new AtomicInteger();
    private final AtomicInteger invalidFileCount = new AtomicInteger();
    private final AtomicInteger deletedFileCount = new AtomicInteger();
    private final AtomicInteger scheduledForCopyCount = new AtomicInteger();
    private final AtomicInteger copyInProgressCount = new AtomicInteger();
    private final AtomicInteger maxCopyInProgressCount = new AtomicInteger();
    private final AtomicInteger maxScheduledForCopyCount = new AtomicInteger();
    private final AtomicLong copyInProgressSize = new AtomicLong();
    private final AtomicLong downloadSize = new AtomicLong();
    private final AtomicLong garbageCollectedSize = new AtomicLong();
    private final AtomicLong downloadTime = new AtomicLong();


    private final Map<String, String> indexPathMapping = Maps.newConcurrentMap();
    private final Map<String, String> indexPathVersionMapping = Maps.newConcurrentMap();
    private final ConcurrentMap<String, LocalIndexFile> failedToDeleteFiles = Maps.newConcurrentMap();
    private final Set<LocalIndexFile> copyInProgressFiles = Collections.newSetFromMap(new ConcurrentHashMap<LocalIndexFile, Boolean>());

    public IndexCopier(Executor executor, File indexRootDir) {
        this.executor = executor;
        this.indexRootDir = indexRootDir;
    }

    public Directory wrap(String indexPath, IndexDefinition definition, Directory remote) throws IOException {
        Directory local = createLocalDir(indexPath, definition);
        return new CopyOnReadDirectory(remote, local);
    }

    protected Directory createLocalDir(String indexPath, IndexDefinition definition) throws IOException {
        File indexDir = getIndexDir(indexPath);
        String newVersion = String.valueOf(definition.getReindexCount());
        File versionedIndexDir = new File(indexDir, newVersion);
        if (!versionedIndexDir.exists()) {
            checkState(versionedIndexDir.mkdirs(), "Cannot create directory %s", versionedIndexDir);
        }
        indexPathMapping.put(indexPath, indexDir.getAbsolutePath());
        Directory result = FSDirectory.open(versionedIndexDir);

        String oldVersion = indexPathVersionMapping.put(indexPath, newVersion);
        if (!newVersion.equals(oldVersion) && oldVersion != null) {
            result = new DeleteOldDirOnClose(result, new File(indexDir, oldVersion));
        }
        return result;
    }

    public File getIndexDir(String indexPath) {
        String subDir = Hashing.sha256().hashString(indexPath, Charsets.UTF_8).toString();
        return new File(indexRootDir, subDir);
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
     * Directory implementation which lazily copies the index files from a
     * remote directory in background.
     */
    private class CopyOnReadDirectory extends BaseDirectory {
        private final Directory remote;
        private final Directory local;

        private final ConcurrentMap<String, FileReference> files = newConcurrentMap();

        public CopyOnReadDirectory(Directory remote, Directory local) throws IOException {
            this.remote = remote;
            this.local = local;
        }

        @Override
        public String[] listAll() throws IOException {
            return remote.listAll();
        }

        @Override
        public boolean fileExists(String name) throws IOException {
            return remote.fileExists(name);
        }

        @Override
        public void deleteFile(String name) throws IOException {
            throw new UnsupportedOperationException("Cannot delete in a ReadOnly directory");
        }

        @Override
        public long fileLength(String name) throws IOException {
            return remote.fileLength(name);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            throw new UnsupportedOperationException("Cannot write in a ReadOnly directory");
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            remote.sync(names);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (REMOTE_ONLY.contains(name)) {
                return remote.openInput(name, context);
            }

            FileReference ref = files.get(name);
            if (ref != null) {
                if (ref.isLocalValid()) {
                    return files.get(name).openLocalInput(context);
                } else {
                    remoteReadCount.incrementAndGet();
                    return remote.openInput(name, context);
                }
            }

            FileReference toPut = new FileReference(name);
            FileReference old = files.putIfAbsent(name, toPut);
            if (old == null) {
                copy(toPut);
            }

            //If immediate executor is used the result would be ready right away
            if (toPut.isLocalValid()) {
                return toPut.openLocalInput(context);
            }

            return remote.openInput(name, context);
        }

        private void copy(final FileReference reference) {
            updateMaxScheduled(scheduledForCopyCount.incrementAndGet());
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    String name = reference.name;
                    boolean success = false;
                    boolean copyAttempted = false;
                    try {
                        scheduledForCopyCount.decrementAndGet();
                        if (!local.fileExists(name)) {
                            long fileSize = remote.fileLength(name);
                            LocalIndexFile file = new LocalIndexFile(local, name, fileSize);
                            long start = startCopy(file);
                            copyAttempted = true;

                            remote.copy(local, name, name, IOContext.READ);
                            reference.markValid();

                            doneCopy(file, start);
                        } else {
                            long localLength = local.fileLength(name);
                            long remoteLength = remote.fileLength(name);

                            //Do a simple consistency check. Ideally Lucene index files are never
                            //updated but still do a check if the copy is consistent
                            if (localLength != remoteLength) {
                                log.warn("Found local copy for {} in {} but size of local {} differs from remote {}. " +
                                                "Content would be read from remote file only",
                                        name, local, localLength, remoteLength);
                                invalidFileCount.incrementAndGet();
                            } else {
                                reference.markValid();
                            }
                        }
                        success = true;
                    } catch (IOException e) {
                        //TODO In case of exception there would not be any other attempt
                        //to download the file. Look into support for retry
                        log.warn("Error occurred while copying file [{}] " +
                                "from {} to {}", name, remote, local, e);
                    } finally {
                        if (copyAttempted && !success){
                            try {
                                if (local.fileExists(name)) {
                                    local.deleteFile(name);
                                }
                            } catch (IOException e) {
                                log.warn("Error occurred while deleting corrupted file [{}] from [{}]", name, local, e);
                            }
                        }
                    }
                }
            });
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
                        log.warn("Error occurred while removing deleted files from Local {}, " +
                                "Remote {}", local, remote, e);
                    }

                    try {
                        //This would also remove old index files if current
                        //directory was based on newerRevision as local would
                        //be of type DeleteOldDirOnClose
                        local.close();
                        remote.close();
                    } catch (IOException e) {
                        log.warn("Error occurred while closing directory ", e);
                    }
                }
            });
        }

        private void removeDeletedFiles() throws IOException {
            //Files present in dest but not present in source have to be deleted
            Set<String> filesToBeDeleted = Sets.difference(
                    ImmutableSet.copyOf(local.listAll()),
                    ImmutableSet.copyOf(remote.listAll())
            );

            Set<String> failedToDelete = Sets.newHashSet();

            for (String fileName : filesToBeDeleted) {
                LocalIndexFile file = new LocalIndexFile(local, fileName);
                try {
                    boolean fileExisted = false;
                    if (local.fileExists(fileName)) {
                        fileExisted = true;
                        local.deleteFile(fileName);
                    }
                    successfullyDeleted(file, fileExisted);
                } catch (IOException e) {
                    failedToDelete.add(fileName);
                    failedToDelete(file);
                    log.debug("Error occurred while removing deleted file {} from Local {}. " +
                            "Attempt would be maid to delete it on next run ", fileName, local, e);
                }
            }

            filesToBeDeleted = new HashSet<String>(filesToBeDeleted);
            filesToBeDeleted.removeAll(failedToDelete);
            if(!filesToBeDeleted.isEmpty()) {
                log.debug("Following files have been removed from Lucene " +
                        "index directory [{}]", filesToBeDeleted);
            }
        }

        private class FileReference {
            final String name;
            private volatile boolean valid;

            private FileReference(String name) {
                this.name = name;
            }

            boolean isLocalValid(){
                return valid;
            }

            IndexInput openLocalInput( IOContext context) throws IOException {
                localReadCount.incrementAndGet();
                return local.openInput(name, context);
            }

            void markValid(){
                this.valid = true;
            }
        }
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

        downloadTime.addAndGet(System.currentTimeMillis() - start);
        downloadSize.addAndGet(file.size);
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
            try{
                FileUtils.deleteDirectory(oldIndexDir);
                log.debug("Removed old index content from {} ", oldIndexDir);
            } catch (IOException e){
                log.warn("Not able to remove old version of copied index at {}", oldIndexDir, e);
            }
            super.close();
        }
    }
    
    static final class LocalIndexFile {
        final File dir;
        final String name;
        final long size;
        private volatile int deleteAttemptCount;
        final long creationTime = System.currentTimeMillis();
        
        public LocalIndexFile(Directory dir, String fileName, long size){
            this.dir = getFSDir(dir);
            this.name = fileName;
            this.size = size;
        }

        public LocalIndexFile(Directory dir, String fileName){
            this(dir, fileName, getFileLength(dir, fileName));
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
                    IOUtils.humanReadableByteCount(size), deleteAttemptCount, timeTaken());
        }

        public String copyLog(){
            return String.format("%s (%s, %1.1f%%, %s, %d s)", name,
                    IOUtils.humanReadableByteCount(actualSize()),
                    copyProgress(),
                    IOUtils.humanReadableByteCount(size), timeTaken());
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
            for (Map.Entry<String, String> e : indexPathMapping.entrySet()){
                String size = IOUtils.humanReadableByteCount(FileUtils.sizeOfDirectory(new File(e.getValue())));
                tds.put(new CompositeDataSupport(IndexMappingData.TYPE,
                        IndexMappingData.FIELD_NAMES,
                        new String[]{e.getKey(), e.getValue(), size}));
            }
        } catch (OpenDataException e){
            throw new IllegalStateException(e);
        }
        return tds;
    }

    @Override
    public int getLocalReadCount() {
        return localReadCount.get();
    }

    @Override
    public int getRemoteReadCount() {
        return remoteReadCount.get();
    }

    public int getInvalidFileCount(){
        return invalidFileCount.get();
    }

    @Override
    public String getDownloadSize() {
        return IOUtils.humanReadableByteCount(downloadSize.get());
    }

    @Override
    public long getDownloadTime() {
        return downloadTime.get();
    }

    @Override
    public String getLocalIndexSize() {
        return IOUtils.humanReadableByteCount(FileUtils.sizeOfDirectory(indexRootDir));
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
        return IOUtils.humanReadableByteCount(garbageSize);
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
        return IOUtils.humanReadableByteCount(copyInProgressSize.get());
    }

    @Override
    public int getMaxCopyInProgressCount() {
        return maxCopyInProgressCount.get();
    }

    @Override
    public int getMaxScheduledForCopyCount() {
        return maxScheduledForCopyCount.get();
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
        return IOUtils.humanReadableByteCount(garbageCollectedSize.get());
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
