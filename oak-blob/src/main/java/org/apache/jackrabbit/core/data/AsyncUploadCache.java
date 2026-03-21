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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds all in progress uploads. This class contains two data
 * structures, one is {@link #asyncUploadMap} which is {@link Map}
 * of file path vs lastModified of upload. The second {@link #toBeDeleted} is
 * {@link Set} of upload which is marked for delete, while it is already
 * in progress. Before starting an asynchronous upload, it requires to invoke
 * {@link #add(String)} to add entry to {@link #asyncUploadMap}. After
 * asynchronous upload completes, it requires to invoke
 * {@link #remove(String)} to remove entry from
 * {@link #asyncUploadMap} Any modification to this class are immediately
 * persisted to local file system. {@link #asyncUploadMap} is persisted to /
 * {@link homeDir}/ {@link #PENDIND_UPLOAD_FILE}. {@link #toBeDeleted} is
 * persisted to / {@link homeDir}/ {@link #TO_BE_DELETED_UPLOAD_FILE}. The /
 * {@link homeDir} refer to ${rep.home}.
 */
public class AsyncUploadCache {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncUploadCache.class);

    /**
     * {@link Map} of fileName Vs lastModified to store asynchronous upload.
     */
    Map<String, Long> asyncUploadMap = new HashMap<String, Long>();

    /**
     * {@link Set} of fileName which are mark for delete during asynchronous
     * Upload.
     */
    Set<String> toBeDeleted = new HashSet<String>();

    String path;

    String homeDir;

    int asyncUploadLimit;

    private File pendingUploads;

    private File toBeDeletedUploads;

    private static final String PENDIND_UPLOAD_FILE = "async-pending-uploads.ser";

    private static final String TO_BE_DELETED_UPLOAD_FILE = "async-tobedeleted-uploads.ser";

    /**
     * This methods checks if file can be added to {@link #asyncUploadMap}. If
     * yes it adds to {@link #asyncUploadMap} and
     * {@link #serializeAsyncUploadMap()} the {@link #asyncUploadMap} to disk.
     * 
     * @return {@link AsyncUploadCacheResult} if successfully added to
     *         asynchronous uploads it sets
     *         {@link AsyncUploadCacheResult#setAsyncUpload(boolean)} to true
     *         else sets to false.
     */
    public synchronized AsyncUploadCacheResult add(String fileName)
            throws IOException {
        AsyncUploadCacheResult result = new AsyncUploadCacheResult();
        if (asyncUploadMap.entrySet().size() >= asyncUploadLimit) {
            LOG.info(
                "Async write limit [{}]  reached. File [{}] not added to async write cache.",
                asyncUploadLimit, fileName);
            LOG.debug("AsyncUploadCache size=[{}] and entries =[{}]",
                asyncUploadMap.size(), asyncUploadMap.keySet());
            result.setAsyncUpload(false);
        } else {
            long startTime = System.currentTimeMillis();
            if (toBeDeleted.remove(fileName)) {
                serializeToBeDeleted();
            }
            asyncUploadMap.put(fileName, System.currentTimeMillis());
            serializeAsyncUploadMap();
            LOG.debug("added file [{}] to asyncUploadMap upoad took [{}] sec",
                fileName, ((System.currentTimeMillis() - startTime) / 1000));
            LOG.debug("AsyncUploadCache size=[{}] and entries =[{}]",
                asyncUploadMap.size(), asyncUploadMap.keySet());
            result.setAsyncUpload(true);
        }
        return result;
    }

    /**
     * This methods removes file (if found) from {@link #asyncUploadMap}. If
     * file is found, it immediately serializes the {@link #asyncUploadMap} to
     * disk. This method sets
     * {@link AsyncUploadCacheResult#setRequiresDelete(boolean)} to true, if
     * asynchronous upload found to be in {@link #toBeDeleted} set i.e. marked
     * for delete.
     */
    public synchronized AsyncUploadCacheResult remove(String fileName)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Long retVal = asyncUploadMap.remove(fileName);
        if (retVal != null) {
            serializeAsyncUploadMap();
            LOG.debug("removed file [{}] from asyncUploadMap took [{}] sec",
                fileName, ((System.currentTimeMillis() - startTime) / 1000));
            LOG.debug("AsyncUploadCache size=[{}] and entries =[{}]",
                asyncUploadMap.size(), asyncUploadMap.keySet());
        } else {
            LOG.debug("cannot removed file [{}] from asyncUploadMap took [{}] sec. File not found.",
                fileName, ((System.currentTimeMillis() - startTime) / 1000));
            LOG.debug("AsyncUploadCache size=[{}] and entries =[{}]",
                asyncUploadMap.size(), asyncUploadMap.keySet());
        }
        AsyncUploadCacheResult result = new AsyncUploadCacheResult();
        result.setRequiresDelete(toBeDeleted.contains(fileName));
        return result;
    }

    /**
     * This methods returns the in progress asynchronous uploads which are not
     * marked for delete.
     */
    public synchronized Set<String> getAll() {
        Set<String> retVal = new HashSet<String>();
        retVal.addAll(asyncUploadMap.keySet());
        retVal.removeAll(toBeDeleted);
        return retVal;
    }

    /**
     * This methos checks if asynchronous upload is in progress for @param
     * fileName. If @param touch is true, the lastModified is updated to current
     * time.
     */
    public synchronized boolean hasEntry(String fileName, boolean touch)
            throws IOException {
        boolean contains = asyncUploadMap.containsKey(fileName)
            && !toBeDeleted.contains(fileName);
        if (touch && contains) {
            long timeStamp = System.currentTimeMillis();
            asyncUploadMap.put(fileName, timeStamp);
            serializeAsyncUploadMap();
        }
        return contains;
    }

    /**
     * Returns lastModified from {@link #asyncUploadMap} if found else returns
     * 0.
     */
    public synchronized long getLastModified(String fileName) {
        return asyncUploadMap.get(fileName) != null
            && !toBeDeleted.contains(fileName)
                ? asyncUploadMap.get(fileName)
                : 0;
    }

    /**
     * This methods deletes asynchronous upload for @param fileName if there
     * exists asynchronous upload for @param fileName.
     */
    public synchronized void delete(String fileName) throws IOException {
        boolean serialize = false;
        if (toBeDeleted.remove(fileName)) {
            serialize = true;
        }
        if (asyncUploadMap.containsKey(fileName) && toBeDeleted.add(fileName)) {
            serialize = true;
        }
        if (serialize) {
            serializeToBeDeleted();
        }
    }

    /**
     * Delete in progress asynchronous uploads which are older than @param min.
     * This method leverage lastModified stored in {@link #asyncUploadMap}
     */
    public synchronized Set<String> deleteOlderThan(long min)
            throws IOException {
        min = min - 1000;
        LOG.info("deleteOlderThan min [{}]", min);
        Set<String> deleteSet = new HashSet<String>();
        for (Map.Entry<String, Long> entry : asyncUploadMap.entrySet()) {
            if (entry.getValue() < min) {
                deleteSet.add(entry.getKey());
            }
        }
        if (deleteSet.size() > 0) {
            LOG.debug("deleteOlderThan set [{}]", deleteSet);
            toBeDeleted.addAll(deleteSet);
            serializeToBeDeleted();
        }
        return deleteSet;
    }

    /**
     * @param homeDir
     *            home directory of repository.
     * @param path
     *            path of the {@link LocalCache}
     * @param asyncUploadLimit
     *            the maximum number of asynchronous uploads
     */
    public synchronized void init(String homeDir, String path,
            int asyncUploadLimit) throws IOException, ClassNotFoundException {
        this.homeDir = homeDir;
        this.path = path;
        this.asyncUploadLimit = asyncUploadLimit;
        LOG.info(
            "AsynWriteCache:homeDir=[{}], path=[{}], asyncUploadLimit=[{}].",
            new Object[] { homeDir, path, asyncUploadLimit });
        pendingUploads = new File(homeDir + "/" + PENDIND_UPLOAD_FILE);
        toBeDeletedUploads = new File(homeDir + "/" + TO_BE_DELETED_UPLOAD_FILE);
        if (pendingUploads.exists()) {
            deserializeAsyncUploadMap();
        } else {
            pendingUploads.createNewFile();
            asyncUploadMap = new HashMap<String, Long>();
            serializeAsyncUploadMap();
        }
        
        if (toBeDeletedUploads.exists()) {
            deserializeToBeDeleted();
        } else {
            toBeDeletedUploads.createNewFile();
            asyncUploadMap = new HashMap<String, Long>();
            serializeToBeDeleted();
        }
    }

    /**
     * Reset the {@link AsyncUploadCache} to empty {@link #asyncUploadMap} and
     * {@link #toBeDeleted}
     */
    public synchronized void reset() throws IOException {
        if (!pendingUploads.exists()) {
            pendingUploads.createNewFile();
        }
        pendingUploads.createNewFile();
        asyncUploadMap = new HashMap<String, Long>();
        serializeAsyncUploadMap();

        if (!toBeDeletedUploads.exists()) {
            toBeDeletedUploads.createNewFile();
        }
        toBeDeletedUploads.createNewFile();
        toBeDeleted = new HashSet<String>();
        serializeToBeDeleted();
    }

    /**
     * Serialize {@link #asyncUploadMap} to local file system.
     */
    private synchronized void serializeAsyncUploadMap() throws IOException {

        // use buffering
        OutputStream fos = new FileOutputStream(pendingUploads);
        OutputStream buffer = new BufferedOutputStream(fos);
        ObjectOutput output = new ObjectOutputStream(buffer);
        try {
            output.writeObject(asyncUploadMap);
            output.flush();
        } finally {
            output.close();
            IOUtils.closeQuietly(buffer);
            
        }
    }

    /**
     * Deserialize {@link #asyncUploadMap} from local file system.
     */
    private synchronized void deserializeAsyncUploadMap() throws IOException,
            ClassNotFoundException {
        // use buffering
        InputStream fis = new FileInputStream(pendingUploads);
        InputStream buffer = new BufferedInputStream(fis);
        ObjectInput input = new ObjectInputStream(buffer);
        try {
            asyncUploadMap = (Map<String, Long>) input.readObject();
        } finally {
            input.close();
            IOUtils.closeQuietly(buffer);
        }
    }

    /**
     * Serialize {@link #toBeDeleted} to local file system.
     */
    private synchronized void serializeToBeDeleted() throws IOException {

        // use buffering
        OutputStream fos = new FileOutputStream(toBeDeletedUploads);
        OutputStream buffer = new BufferedOutputStream(fos);
        ObjectOutput output = new ObjectOutputStream(buffer);
        try {
            output.writeObject(toBeDeleted);
            output.flush();
        } finally {
            output.close();
            IOUtils.closeQuietly(buffer);
        }
    }

    /**
     * Deserialize {@link #toBeDeleted} from local file system.
     */
    private synchronized void deserializeToBeDeleted() throws IOException,
            ClassNotFoundException {
        // use buffering
        InputStream fis = new FileInputStream(toBeDeletedUploads);
        InputStream buffer = new BufferedInputStream(fis);
        ObjectInput input = new ObjectInputStream(buffer);
        try {
            toBeDeleted = (Set<String>) input.readObject();
        } finally {
            input.close();
            IOUtils.closeQuietly(buffer);
        }
    }
}
