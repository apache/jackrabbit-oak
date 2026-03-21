/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.core.data;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.util.TransientFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a LRU cache used by {@link CachingDataStore}. If cache
 * size exceeds limit, this cache goes in purge mode. In purge mode any
 * operation to cache is no-op. After purge cache size would be less than
 * cachePurgeResizeFactor * maximum size.
 */
public class LocalCache {

    /**
     * Logger instance.
     */
    static final Logger LOG = LoggerFactory.getLogger(LocalCache.class);

    /**
     * The file names of the files that need to be deleted.
     */
    final Set<String> toBeDeleted = new HashSet<String>();

    /**
     * The filename Vs file size LRU cache.
     */
    LRUCache cache;

    /**
     * The directory where the files are created.
     */
    private final File directory;

    /**
     * The directory where tmp files are created.
     */
    private final File tmp;

    /**
     * If true cache is in purgeMode and not available. All operation would be
     * no-op.
     */
    private volatile boolean purgeMode;
    
    private AsyncUploadCache asyncUploadCache;
    
    private AtomicLong cacheMissCounter = new AtomicLong();
    
    private AtomicLong cacheMissDuration = new AtomicLong();
    

    /**
     * Build LRU cache of files located at 'path'. It uses lastModified property
     * of file to build LRU cache. If cache size exceeds limit size, this cache
     * goes in purge mode. In purge mode any operation to cache is no-op.
     * 
     * @param path file system path
     * @param tmpPath temporary directory used by cache.
     * @param maxSizeInBytes maximum size of cache.
     * @param cachePurgeTrigFactor factor which triggers cache to purge mode.
     * That is if current size exceed (cachePurgeTrigFactor * maxSizeInBytes), the
     * cache will go in auto-purge mode.
     * @param cachePurgeResizeFactor after cache purge size of cache will be
     * just less (cachePurgeResizeFactor * maxSizeInBytes).
     * @param asyncUploadCache {@link AsyncUploadCache}
     */
    public LocalCache(String path, String tmpPath, long maxSizeInBytes, double cachePurgeTrigFactor,
            double cachePurgeResizeFactor, AsyncUploadCache asyncUploadCache) {
        directory = new File(path);
        tmp = new File(tmpPath);
        LOG.info(
            "cachePurgeTrigFactor =[{}], cachePurgeResizeFactor =[{}],  " +
            "cachePurgeTrigFactorSize =[{}], cachePurgeResizeFactorSize =[{}]",
            new Object[] { cachePurgeTrigFactor, cachePurgeResizeFactor,
                (cachePurgeTrigFactor * maxSizeInBytes), 
                (cachePurgeResizeFactor * maxSizeInBytes) });
        cache = new LRUCache(maxSizeInBytes, cachePurgeTrigFactor, cachePurgeResizeFactor);
        this.asyncUploadCache = asyncUploadCache;
        new Thread(new CacheBuildJob()).start();
    }

    /**
     * Store an item in the cache and return the input stream. If cache is in
     * purgeMode or file doesn't exists, inputstream from a
     * {@link TransientFileFactory#createTransientFile(String, String, File)} is
     * returned. Otherwise inputStream from cached file is returned. This method
     * doesn't close the incoming inputstream.
     * 
     * @param fileName the key of cache.
     * @param in {@link InputStream}
     * @return the (new) input stream.
     */
    public InputStream store(String fileName, final InputStream in)
            throws IOException {
        fileName = fileName.replace("\\", "/");
        File f = getFile(fileName);
        long length = 0;
        if (!f.exists() || isInPurgeMode()) {
            OutputStream out = null;
            File transFile = null;
            try {
                TransientFileFactory tff = TransientFileFactory.getInstance();
                transFile = tff.createTransientFile("s3-", "tmp", tmp);
                out = new BufferedOutputStream(new FileOutputStream(transFile));
                length = IOUtils.copyLarge(in, out);
            } finally {
                IOUtils.closeQuietly(out);
            }
            // rename the file to local fs cache
            if (canAdmitFile(length)
                && (f.getParentFile().exists() || f.getParentFile().mkdirs())
                && transFile.renameTo(f) && f.exists()) {
                if (transFile.exists() && transFile.delete()) {
                    LOG.info("tmp file [{}] not deleted successfully",
                        transFile.getAbsolutePath());
                }
                transFile = null;
                LOG.debug(
                    "file [{}] doesn't exists. adding to local cache using inputstream.",
                    fileName);
                cache.put(fileName, f.length());
            } else {
                LOG.debug(
                    "file [{}] doesn't exists. returning transient file [{}].",
                    fileName, transFile.getAbsolutePath());
                f = transFile;
            }
        } else {
            if (in instanceof BackendResourceAbortable) {
                ((BackendResourceAbortable) in).abort();
            }
            f.setLastModified(System.currentTimeMillis());
            LOG.debug(
                "file [{}]  exists. adding to local cache using inputstream.",
                fileName);
            cache.put(fileName, f.length());
        }
        tryPurge();
        return new LazyFileInputStream(f);
    }

    /**
     * Store an item along with file in cache. Cache size is increased by
     * {@link File#length()} If file already exists in cache,
     * {@link File#setLastModified(long)} is updated with current time.
     * 
     * @param fileName the key of cache.
     * @param src file to be added to cache.
     */
    public File store(String fileName, final File src) {
        try {
            return store(fileName, src, false).getFile();
        } catch (IOException ioe) {
            LOG.warn("Exception in addding file [" + fileName + "] to local cache.", ioe);
        }
        return null;
    }

    /**
     * This method add file to {@link LocalCache} and tries that file can be
     * added to {@link AsyncUploadCache}. If file is added to
     * {@link AsyncUploadCache} successfully, it sets
     * {@link AsyncUploadCacheResult#setAsyncUpload(boolean)} to true.
     *
     * @param fileName name of the file.
     * @param src source file.
     * @param tryForAsyncUpload If true it tries to add fileName to
     *            {@link AsyncUploadCache}
     * @return {@link AsyncUploadCacheResult}. This method sets
     *         {@link AsyncUploadCacheResult#setAsyncUpload(boolean)} to true, if
     *         fileName is added to {@link AsyncUploadCache} successfully else
     *         it sets {@link AsyncUploadCacheResult#setAsyncUpload(boolean)} to
     *         false. {@link AsyncUploadCacheResult#getFile()} contains cached
     *         file, if it is added to {@link LocalCache} or original file.
     * @throws IOException
     */
    public AsyncUploadCacheResult store(String fileName, File src,
            boolean tryForAsyncUpload) throws IOException {
        fileName = fileName.replace("\\", "/");
        File dest = getFile(fileName);
        File parent = dest.getParentFile();
        AsyncUploadCacheResult result = new AsyncUploadCacheResult();
        result.setFile(src);
        result.setAsyncUpload(false);
        boolean destExists = false;
        if ((destExists = dest.exists())
            || (src.exists() && !dest.exists() && !src.equals(dest)
                && canAdmitFile(src.length())
                && (parent.exists() || parent.mkdirs()) && (src.renameTo(dest)))) {
            if (destExists) {
                dest.setLastModified(System.currentTimeMillis());
            }
            LOG.debug("file [{}] moved to [{}] ", src.getAbsolutePath(), dest.getAbsolutePath());
            LOG.debug(
                "file [{}]  exists= [{}] added to local cache, isLastModified [{}]",
                new Object[] { dest.getAbsolutePath(), dest.exists(),
                    destExists });
            
            cache.put(fileName, dest.length());
            result.setFile(dest);
            if (tryForAsyncUpload) {
                result.setAsyncUpload(asyncUploadCache.add(fileName).canAsyncUpload());
            }
        } else {
            LOG.info("file [{}] exists= [{}] not added to local cache.",
                fileName, destExists);
        }
        tryPurge();
        return result;
    }
    /**
     * Return the inputstream from from cache, or null if not in the cache.
     * 
     * @param fileName name of file.
     * @return  stream or null.
     */
    public InputStream getIfStored(String fileName) throws IOException {
        File file = getFileIfStored(fileName);
        return file == null ? null : new LazyFileInputStream(file);
    }

    public File getFileIfStored(String fileName) throws IOException {
        fileName = fileName.replace("\\", "/");
        File f = getFile(fileName);
        long diff = (System.currentTimeMillis() - cacheMissDuration.get()) / 1000;
        // logged at 5 minute interval minimum
        if (diff > 5 * 60) {
            LOG.info("local cache misses [{}] in [{}] sec", new Object[] {
                cacheMissCounter.getAndSet(0), diff });
            cacheMissDuration.set(System.currentTimeMillis());
        }
        
        // return file in purge mode = true and file present in asyncUploadCache
        // as asyncUploadCache's files will be not be deleted in cache purge.
        if (!f.exists() || (isInPurgeMode() && !asyncUploadCache.hasEntry(fileName, false))) {
            LOG.debug(
                "getFileIfStored returned: purgeMode=[{}], file=[{}] exists=[{}]",
                new Object[] { isInPurgeMode(), f.getAbsolutePath(), f.exists() });
            cacheMissCounter.incrementAndGet();
            return null;
        } else {
            // touch entry in LRU caches
            f.setLastModified(System.currentTimeMillis());
            cache.get(fileName);
            return f;
        }
    }

    /**
     * Delete file from cache. Size of cache is reduced by file length. The
     * method is no-op if file doesn't exist in cache.
     * 
     * @param fileName file name that need to be removed from cache.
     */
    public void delete(String fileName) {
        if (isInPurgeMode()) {
            LOG.debug("purgeMode true :delete returned");
            return;
        }
        fileName = fileName.replace("\\", "/");
        cache.remove(fileName);
    }

    /**
     * Returns length of file if exists in cache else returns null.
     * @param fileName name of the file.
     */
    public Long getFileLength(String fileName) {
        Long length = null;
        try {
            length = cache.get(fileName);
            if( length == null ) {
                File f = getFileIfStored(fileName);
                if (f != null) {
                    length = f.length();
                }
            }
        } catch (IOException ignore) {

        }
        return length;
    }

    /**
     * Close the cache. Cache maintain set of files which it was not able to
     * delete successfully. This method will an attempt to delete all
     * unsuccessful delete files.
     */
    public void close() {
        LOG.debug("close");
        deleteOldFiles();
    }

    /**
     * Check if cache can admit file of given length.
     * @param length of the file.
     * @return true if yes else return false.
     */
    private boolean canAdmitFile(final long length) {
      //order is important here
        boolean value = !isInPurgeMode() && (cache.canAdmitFile(length));
        if (!value) {
            LOG.debug("cannot admit file of length=[{}] and currentSizeInBytes=[{}] ",
                length, cache.currentSizeInBytes);
        }
        return value;
    }

    /**
     * Return true if cache is in purge mode else return false.
     */
    synchronized boolean isInPurgeMode() {
        return purgeMode;
    }

    /**
     * Set purge mode. If set to true all cache operation will be no-op. If set
     * to false, all operations to cache are available.
     * 
     * @param purgeMode purge mode
     */
    synchronized void setPurgeMode(final boolean purgeMode) {
        this.purgeMode = purgeMode;
    }

    File getFile(final String fileName) {
        return new File(directory, fileName);
    }

    private void deleteOldFiles() {
        int initialSize = toBeDeleted.size();
        int count = 0;
        for (String fileName : new ArrayList<String>(toBeDeleted)) {
            fileName = fileName.replace("\\", "/");
            if( cache.remove(fileName) != null) {
                count++;
            }
        }
        LOG.info("deleted [{}]/[{}] files.", count, initialSize);
    }

    /**
     * This method tries to delete a file. If it is not able to delete file due
     * to any reason, it add it toBeDeleted list.
     * 
     * @param fileName name of the file which will be deleted.
     * @return true if this method deletes file successfuly else return false.
     */
    boolean tryDelete(final String fileName) {
        LOG.debug("try deleting file [{}]", fileName);
        File f = getFile(fileName);
        if (f.exists() && f.delete()) {
            LOG.info("File [{}]  deleted successfully", f.getAbsolutePath());
            toBeDeleted.remove(fileName);
            while (true) {
                f = f.getParentFile();
                if (f.equals(directory) || f.list().length > 0) {
                    break;
                }
                // delete empty parent folders (except the main directory)
                f.delete();
            }
            return true;
        } else if (f.exists()) {
            LOG.info("not able to delete file [{}]", f.getAbsolutePath());
            toBeDeleted.add(fileName);
            return false;
        }
        return true;
    }

    static int maxSizeElements(final long bytes) {
        // after a CQ installation, the average item in
        // the data store is about 52 KB
        int count = (int) (bytes / 65535);
        count = Math.max(1024, count);
        count = Math.min(64 * 1024, count);
        return count;
    }
    
    /**
     * This method tries purging of local cache. It checks if local cache
     * has exceeded the defined limit then it triggers purge cache job in a
     * seperate thread.
     */
    synchronized void tryPurge() {
        if (!isInPurgeMode()
            && cache.currentSizeInBytes > cache.cachePurgeTrigSize) {
            setPurgeMode(true);
            LOG.info(
                "cache.entries = [{}], currentSizeInBytes=[{}]  exceeds cachePurgeTrigSize=[{}]",
                new Object[] { cache.size(), cache.currentSizeInBytes,
                    cache.cachePurgeTrigSize });
            new Thread(new PurgeJob()).start();
        } else {
            LOG.debug(
                "currentSizeInBytes=[{}],cachePurgeTrigSize=[{}], isInPurgeMode =[{}]",
                new Object[] { cache.currentSizeInBytes,
                    cache.cachePurgeTrigSize, isInPurgeMode() });
        }
    }

    /**
     * A LRU based extension {@link LinkedHashMap}. The key is file name and
     * value is length of file.
     */
    private class LRUCache extends LinkedHashMap<String, Long> {
        private static final long serialVersionUID = 1L;

        volatile long currentSizeInBytes;

        final long maxSizeInBytes;

        final long cachePurgeResize;
        
        final long cachePurgeTrigSize;

        LRUCache(final long maxSizeInBytes,
                final double cachePurgeTrigFactor,
                final double cachePurgeResizeFactor) {
            super(maxSizeElements(maxSizeInBytes), (float) 0.75, true);
            this.maxSizeInBytes = maxSizeInBytes;
            this.cachePurgeTrigSize = new Double(cachePurgeTrigFactor
                * maxSizeInBytes).longValue();
            this.cachePurgeResize = new Double(cachePurgeResizeFactor
                * maxSizeInBytes).longValue();
        }

        /**
         * Overridden {@link Map#remove(Object)} to delete corresponding file
         * from file system.
         */
        @Override
        public synchronized Long remove(final Object key) {
            String fileName = (String) key;
            fileName = fileName.replace("\\", "/");
            try {
                // not removing file from local cache, if there is in progress
                // async upload on it.
                if (asyncUploadCache.hasEntry(fileName, false)) {
                    LOG.info(
                        "AsyncUploadCache upload contains file [{}]. Not removing it from LocalCache.",
                        fileName);
                    return null;
                }
            } catch (IOException e) {
                LOG.debug("error: ", e);
                return null;
            }
            Long flength = null;
            if (tryDelete(fileName)) {
                flength = super.remove(key);
                if (flength != null) {
                    LOG.debug("cache entry [{}], with size [{}] removed.",
                        fileName, flength);
                    currentSizeInBytes -= flength.longValue();
                }
            } else if (!getFile(fileName).exists()) {
                // second attempt. remove from cache if file doesn't exists
                flength = super.remove(key);
                if (flength != null) {
                    LOG.debug(
                        "file not exists. cache entry [{}], with size [{}] removed.",
                        fileName, flength);
                    currentSizeInBytes -= flength.longValue();
                }
            } else {
                LOG.info("not able to remove cache entry [{}], size [{}]", key,
                    super.get(key));
            }
            return flength;
        }

        @Override
        public Long put(final String fileName, final Long value) {
            if( isInPurgeMode()) {
                LOG.debug("cache is purge mode: put is no-op");
                return null;
            }
            synchronized (this) {
                Long oldValue = cache.get(fileName);
                if (oldValue == null) {
                    long flength = value.longValue();
                    currentSizeInBytes += flength;
                    return super.put(fileName.replace("\\", "/"), value);
                }
                toBeDeleted.remove(fileName);
                return oldValue;
            }
        }
        
        @Override
        public Long get(Object key) {
            if( isInPurgeMode()) {
                LOG.debug("cache is purge mode: get is no-op");
                return null;
            }
            synchronized (this) {
                return super.get(key);
            }
        }
        
        /**
         * This method check if cache can admit file of given length. 
         * @param length length of file.
         * @return true if cache size + length is less than maxSize.
         */
        synchronized boolean canAdmitFile(final long length) {
            return cache.currentSizeInBytes + length < cache.maxSizeInBytes;
        }
    }

    /**
     * This class performs purging of local cache. It implements
     * {@link Runnable} and should be invoked in a separate thread.
     */
    private class PurgeJob implements Runnable {
        public PurgeJob() {
            // TODO Auto-generated constructor stub
        }

        /**
         * This method purges local cache till its size is less than
         * cacheResizefactor * maxSize
         */
        @Override
        public void run() {
            try {
                synchronized (cache) {
                    // first try to delete toBeDeleted files
                    int initialSize = cache.size();
                    LOG.info(" cache purge job started. initial cache entries = [{}]", initialSize);
                    for (String fileName : new ArrayList<String>(toBeDeleted)) {
                        cache.remove(fileName);
                    }
                    int skipCount = 0;
                    Iterator<Map.Entry<String, Long>> itr = cache.entrySet().iterator();
                    while (itr.hasNext()) {
                        Map.Entry<String, Long> entry = itr.next();
                        if (entry.getKey() != null) {
                            if (cache.currentSizeInBytes > cache.cachePurgeResize) {
                                if (cache.remove(entry.getKey()) != null) {
                                    itr = cache.entrySet().iterator();
                                    for (int i = 0; i < skipCount && itr.hasNext(); i++) {
                                        itr.next();
                                    }
                                } else {
                                    skipCount++;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    LOG.info(
                        " cache purge job completed: cleaned [{}] files and currentSizeInBytes = [{}]",
                        (initialSize - cache.size()), cache.currentSizeInBytes);
                }
            } catch (Exception e) {
                LOG.error("error in purge jobs:", e);
            } finally {
                setPurgeMode(false);
            }
        }
    }
    
    /**
     * This class implements {@link Runnable} interface to build LRU cache
     * asynchronously.
     */
    private class CacheBuildJob implements Runnable {

        
        public void run() {
            long startTime = System.currentTimeMillis();
            ArrayList<File> allFiles = new ArrayList<File>();
            Iterator<File> it = FileUtils.iterateFiles(directory, null, true);
            while (it.hasNext()) {
                File f = it.next();
                allFiles.add(f);
            }
            long t1 = System.currentTimeMillis();
            LOG.debug("Time taken to recursive [{}] took [{}] sec",
                allFiles.size(), ((t1 - startTime) / 1000));

            String dataStorePath = directory.getAbsolutePath();
            // convert to java path format
            dataStorePath = dataStorePath.replace("\\", "/");
            LOG.info("directoryPath = " + dataStorePath);

            String tmpPath = tmp.getAbsolutePath();
            tmpPath = tmpPath.replace("\\", "/");
            LOG.debug("tmp path [{}]", tmpPath); 
            long time = System.currentTimeMillis();
            int count = 0;
            for (File f : allFiles) {
                if (f.exists()) {
                    count++;
                    String name = f.getPath();
                    String filePath = f.getAbsolutePath();
                    // convert to java path format
                    name = name.replace("\\", "/");
                    filePath = filePath.replace("\\", "/");
                    // skipped any temp file
                    if(filePath.startsWith(tmpPath) ) {
                        LOG.info    ("tmp file [{}] skipped ", filePath);
                        continue;
                    }
                    if (filePath.startsWith(dataStorePath)) {
                        name = filePath.substring(dataStorePath.length());
                    }
                    if (name.startsWith("/") || name.startsWith("\\")) {
                        name = name.substring(1);
                    }
                    store(name, f);
                    long now = System.currentTimeMillis();
                    if (now > time + 10000) {
                        LOG.info("Processed {" + (count) + "}/{" + allFiles.size() + "}");
                        time = now;
                    }
                }
            }
            LOG.debug(
                "Processed [{}]/[{}], currentSizeInBytes = [{}], maxSizeInBytes = [{}], cache.filecount = [{}]",
                new Object[] { count, allFiles.size(),
                    cache.currentSizeInBytes, cache.maxSizeInBytes,
                    cache.size() });
            long t3 = System.currentTimeMillis();
            LOG.info("Time to build cache of  [{}] files took [{}] sec",
                allFiles.size(), ((t3 - startTime) / 1000));
        }
    }
}

