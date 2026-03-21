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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A caching data store that consists of {@link LocalCache} and {@link Backend}.
 * {@link Backend} is single source of truth. All methods first try to fetch
 * information from {@link LocalCache}. If record is not available in
 * {@link LocalCache}, then it is fetched from {@link Backend} and saved to
 * {@link LocalCache} for further access. This class is designed to work without
 * {@link LocalCache} and then all information is fetched from {@link Backend}.
 * To disable {@link LocalCache} set {@link #setCacheSize(long)} to 0. *
 * Configuration:
 * 
 * <pre>
 * &lt;DataStore class="org.apache.jackrabbit.aws.ext.ds.CachingDataStore"&gt;
 * 
 *     &lt;param name="{@link #setPath(String) path}" value="/data/datastore"/&gt;
 *     &lt;param name="{@link #setConfig(String) config}" value="${rep.home}/backend.properties"/&gt;
 *     &lt;param name="{@link #setCacheSize(long) cacheSize}" value="68719476736"/&gt;
 *     &lt;param name="{@link #setSecret(String) secret}" value="123456"/&gt;
 *     &lt;param name="{@link #setCachePurgeTrigFactor(double)}" value="0.95d"/&gt;
 *     &lt;param name="{@link #setCachePurgeResizeFactor(double) cacheSize}" value="0.85d"/&gt;
 *     &lt;param name="{@link #setMinRecordLength(int) minRecordLength}" value="1024"/&gt;
 *     &lt;param name="{@link #setContinueOnAsyncUploadFailure(boolean) continueOnAsyncUploadFailure}" value="false"/&gt;
 *     &lt;param name="{@link #setConcurrentUploadsThreads(int) concurrentUploadsThreads}" value="10"/&gt;
 *     &lt;param name="{@link #setAsyncUploadLimit(int) asyncUploadLimit}" value="100"/&gt;
 *     &lt;param name="{@link #setUploadRetries(int) uploadRetries}" value="3"/&gt;
 *     &lt;param name="{@link #setTouchAsync(boolean) touchAsync}" value="false"/&gt;
 *     &lt;param name="{@link #setProactiveCaching(boolean) proactiveCaching}" value="true"/&gt;
 *     &lt;param name="{@link #setRecLengthCacheSize(int) recLengthCacheSize}" value="200"/&gt;
 * &lt;/DataStore&gt;
 * </pre>
 */
public abstract class CachingDataStore extends AbstractDataStore implements
        MultiDataStoreAware, AsyncUploadCallback, AsyncTouchCallback {

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(CachingDataStore.class);

    private static final String DS_STORE = ".DS_Store";

    /**
     * Name of the directory used for temporary files. Must be at least 3
     * characters.
     */
    private static final String TMP = "tmp";

    /**
     * All data identifiers that are currently in use are in this set until they
     * are garbage collected.
     */
    protected Map<DataIdentifier, WeakReference<DataIdentifier>> inUse = Collections.synchronizedMap(new WeakHashMap<DataIdentifier, WeakReference<DataIdentifier>>());
    
    /**
     * In memory map to hold failed asynchronous upload {@link DataIdentifier}
     * and its retry count. Once if all retries are exhausted or file is 
     * successfully uploaded, then corresponding entry is flushed from the map.
     * As all failed uploads are synchronously uploaded at startup, this map 
     * is not required to be persisted. 
     */
    protected final Map<DataIdentifier, Integer> uploadRetryMap = new ConcurrentHashMap<DataIdentifier, Integer>(5);
    
    /**
     * In memory map to hold in-progress asynchronous touch. Once touch is
     * successful corresponding entry is flushed from the map.
     */
    protected final Map<DataIdentifier, Long> asyncTouchCache = new ConcurrentHashMap<DataIdentifier, Long>(5);

    /**
     * In memory map to hold in-progress asynchronous downloads. Once
     * download is finished corresponding entry is flushed from the map.
     */
    protected final Map<DataIdentifier, Long> asyncDownloadCache = new ConcurrentHashMap<DataIdentifier, Long>(5);

    /**
     * In memory cache to hold {@link DataRecord#getLength()} against
     * {@link DataIdentifier}
     */
    protected Map<DataIdentifier, Long> recLenCache = null;

    protected Backend backend;

    /**
     * The minimum size of an object that should be stored in this data store.
     */
    private int minRecordLength = 16 * 1024;

    private String path;

    private File directory;

    private File tmpDir;

    private String secret;

    /**
     * Flag to indicate if lastModified is updated asynchronously.
     */
    private boolean touchAsync = false;

    /**
     * Flag to indicate that binary content will be cached proactively and
     * asynchronously when binary metadata is retrieved from {@link Backend}.
     */
    private boolean proactiveCaching = true;

    /**
     * The optional backend configuration.
     */
    private String config;

    /**
     * The minimum modified date. If a file is accessed (read or write) with a
     * modified date older than this value, the modified date is updated to the
     * current time.
     */
    private long minModifiedDate;

    /**
     * Cache purge trigger factor. Cache will undergo in auto-purge mode if
     * cache current size is greater than cachePurgeTrigFactor * cacheSize
     */
    private double cachePurgeTrigFactor = 0.95d;

    /**
     * Cache resize factor. After auto-purge mode, cache current size would just
     * greater than cachePurgeResizeFactor * cacheSize cacheSize
     */
    private double cachePurgeResizeFactor = 0.85d;

    /**
     * The number of bytes in the cache. The default value is 64 GB.
     */
    private long cacheSize = 64L * 1024 * 1024 * 1024;
    
    /**
     * The number of retries for failed upload.
     */
    private int uploadRetries = 3;

    /**
     * The local file system cache.
     */
    private LocalCache cache;

    /**
     * Caching holding pending uploads
     */
    private AsyncUploadCache asyncWriteCache;

    /**
     * {@link ExecutorService} to asynchronous downloads
     */
    private ExecutorService downloadExecService;

    protected abstract Backend createBackend();

    protected abstract String getMarkerFile();

    /**
     * In {@link #init(String)},it resumes all incomplete asynchronous upload
     * from {@link AsyncUploadCache} and uploads them concurrently in multiple
     * threads. It throws {@link RepositoryException}, if file is not found in
     * local cache for that asynchronous upload. As far as code is concerned, it
     * is only possible when somebody has removed files from local cache
     * manually. If there is an exception and user want to proceed with
     * inconsistencies, set parameter continueOnAsyncUploadFailure to true in
     * repository.xml. This will ignore {@link RepositoryException} and log all
     * missing files and proceed after resetting {@link AsyncUploadCache} .
     */
    private boolean continueOnAsyncUploadFailure;

    /**
     * The {@link #init(String)} methods checks for {@link #getMarkerFile()} and
     * if it doesn't exists migrates all files from fileystem to {@link Backend}
     * . This parameter governs number of threads which will upload files
     * concurrently to {@link Backend}.
     */
    private int concurrentUploadsThreads = 10;

    /**
     * This parameter limits the number of asynchronous uploads slots to
     * {@link Backend}. Once this limit is reached, further uploads to
     * {@link Backend} are synchronous, till one of asynchronous uploads
     * completes and make asynchronous uploads slot available. To disable
     * asynchronous upload, set {@link #asyncUploadLimit} parameter to 0 in
     * repository.xml. By default it is 100
     */
    private int asyncUploadLimit = 100;
    
    /**
     * Size of {@link #recLenCache}. Each entry consumes of approx 140 bytes.
     * Default total memory consumption of {@link #recLenCache} 28KB.
     */
    private int recLengthCacheSize = 200;

    /**
     * Initialized the data store. If the path is not set, &lt;repository
     * home&gt;/repository/datastore is used. This directory is automatically
     * created if it does not yet exist. During first initialization, it upload
     * all files from local datastore to backed and local datastore act as a
     * local cache.
     */
    @Override
    public void init(String homeDir) throws RepositoryException {
        try {
            if (path == null) {
                path = homeDir + "/repository/datastore";
            }
            // create tmp inside path
            tmpDir = new File(path, "tmp");
            LOG.info("path=[{}],  tmpPath=[{}]", path, tmpDir.getAbsolutePath());
            directory = new File(path);
            mkdirs(directory);
            mkdirs(new File(homeDir));

            if (!mkdirs(tmpDir)) {
                FileUtils.cleanDirectory(tmpDir);
                LOG.info("tmp=[{}] cleaned.", tmpDir.getPath());
            }
            boolean asyncWriteCacheInitStatus = true;
            try {
                asyncWriteCache = new AsyncUploadCache();
                asyncWriteCache.init(homeDir, path, asyncUploadLimit);
            } catch (Exception e) {
                LOG.warn("Failed to initialize asyncWriteCache", e);
                asyncWriteCacheInitStatus = false;
            }
            backend = createBackend();
            backend.init(this, path, config);
            String markerFileName = getMarkerFile();
            if (markerFileName != null && !"".equals(markerFileName.trim())) {
                // create marker file in homeDir to avoid deletion in cache
                // cleanup.
                File markerFile = new File(homeDir, markerFileName);
                if (!markerFile.exists()) {
                    LOG.info("load files from local cache");
                    uploadFilesFromCache();
                    try {
                        markerFile.createNewFile();
                    } catch (IOException e) {
                        throw new DataStoreException(
                            "Could not create marker file "
                                + markerFile.getAbsolutePath(), e);
                    }
                } else {
                    LOG.info("marker file = [{}] exists ",
                        markerFile.getAbsolutePath());
                    if (!asyncWriteCacheInitStatus) {
                        LOG.info("Initialization of asyncWriteCache failed. "
                            + "Re-loading all files from local cache");
                        uploadFilesFromCache();
                        asyncWriteCache.reset();
                    }
                }
            } else {
                throw new DataStoreException("Failed to intialized DataStore."
                    + " MarkerFileName is null or empty. ");
            }
            // upload any leftover async uploads to backend during last shutdown
            Set<String> fileList = asyncWriteCache.getAll();
            if (fileList != null && !fileList.isEmpty()) {
                List<String> errorFiles = new ArrayList<String>();
                LOG.info("Uploading [{}] and size=[{}] from AsyncUploadCache.",
                    fileList, fileList.size());
                long totalSize = 0;
                List<File> files = new ArrayList<File>(fileList.size());
                for (String fileName : fileList) {
                    File f = new File(path, fileName);
                    if (!f.exists()) {
                        errorFiles.add(fileName);
                        LOG.error(
                            "Cannot upload pending file [{}]. File doesn't exist.",
                            f.getAbsolutePath());
                    } else {
                        totalSize += f.length();
                        files.add(new File(path, fileName));
                    }
                }
                new FilesUploader(files, totalSize, concurrentUploadsThreads,
                    true).upload();
                if (!continueOnAsyncUploadFailure && errorFiles.size() > 0) {
                    LOG.error(
                        "Pending uploads of files [{}] failed. Files do not exist in Local cache.",
                        errorFiles);
                    LOG.error("To continue set [continueOnAsyncUploadFailure] "
                        + "to true in Datastore configuration in "
                        + "repository.xml. There would be inconsistent data "
                        + "in repository due the missing files. ");
                    throw new RepositoryException(
                        "Cannot upload async uploads from local cache. Files not found.");
                } else {
                    if (errorFiles.size() > 0) {
                        LOG.error(
                            "Pending uploads of files [{}] failed. Files do" +
                            " not exist in Local cache. Continuing as " +
                            "[continueOnAsyncUploadFailure] is set to true.",
                            errorFiles);
                    }
                    LOG.info("Reseting AsyncWrite Cache list.");
                    asyncWriteCache.reset();
                }
            }
            downloadExecService = Executors.newFixedThreadPool(5,
                new NamedThreadFactory("backend-file-download-worker"));
            cache = new LocalCache(path, tmpDir.getAbsolutePath(), cacheSize,
                cachePurgeTrigFactor, cachePurgeResizeFactor, asyncWriteCache);
            /*
             * Initialize LRU cache of size {@link #recLengthCacheSize}
             */
            recLenCache = Collections.synchronizedMap(new LinkedHashMap<DataIdentifier, Long>(
                recLengthCacheSize, 0.75f, true) {

                private static final long serialVersionUID = -8752749075395630485L;

                @Override
                protected boolean removeEldestEntry(
                                Map.Entry<DataIdentifier, Long> eldest) {
                    if (size() > recLengthCacheSize) {
                        LOG.trace("evicted from recLengthCache [{}]",
                            eldest.getKey());
                        return true;
                    }
                    return false;
                }
            });
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
    }

    /**
     * Creates a new data record in {@link Backend}. The stream is first
     * consumed and the contents are saved in a temporary file and the {@link #DIGEST}
     * message digest of the stream is calculated. If a record with the same
     * {@link #DIGEST} digest (and length) is found then it is returned. Otherwise new
     * record is created in {@link Backend} and the temporary file is moved in
     * place to {@link LocalCache}.
     * 
     * @param input
     *            binary stream
     * @return {@link CachingDataRecord}
     * @throws DataStoreException
     *             if the record could not be created.
     */
    @Override
    public DataRecord addRecord(InputStream input) throws DataStoreException {
        File temporary = null;
        long startTime = System.currentTimeMillis();
        long length = 0;
        try {
            temporary = newTemporaryFile();
            DataIdentifier tempId = new DataIdentifier(temporary.getName());
            usesIdentifier(tempId);
            // Copy the stream to the temporary file and calculate the
            // stream length and the message digest of the stream
            MessageDigest digest = MessageDigest.getInstance(DIGEST);
            OutputStream output = new DigestOutputStream(new FileOutputStream(
                temporary), digest);
            try {
                length = IOUtils.copyLarge(input, output);
            } finally {
                output.close();
            }
            long currTime = System.currentTimeMillis();
            DataIdentifier identifier = new DataIdentifier(
                encodeHexString(digest.digest()));
            LOG.debug("Digest of [{}], length =[{}] took [{}]ms ",
                new Object[] { identifier, length, (currTime - startTime) });
            String fileName = getFileName(identifier);
            AsyncUploadCacheResult result = null;
            synchronized (this) {
                usesIdentifier(identifier);
                // check if async upload is already in progress
                if (!asyncWriteCache.hasEntry(fileName, true)) {
                    result = cache.store(fileName, temporary, true);
                }
            }
            LOG.debug("storing  [{}] in localCache took [{}] ms", identifier,
                (System.currentTimeMillis() - currTime));
            if (result != null) {
                if (result.canAsyncUpload()) {
                    backend.writeAsync(identifier, result.getFile(), this);
                } else {
                    backend.write(identifier, result.getFile());
                }
            }
            // this will also make sure that
            // tempId is not garbage collected until here
            inUse.remove(tempId);
            LOG.debug("addRecord [{}] of length [{}] took [{}]ms.",
                new Object[] { identifier, length,
                    (System.currentTimeMillis() - startTime) });
            return new CachingDataRecord(this, identifier);
        } catch (NoSuchAlgorithmException e) {
            throw new DataStoreException(DIGEST + " not available", e);
        } catch (IOException e) {
            throw new DataStoreException("Could not add record", e);
        } finally {
            if (temporary != null) {
                // try to delete - but it's not a big deal if we can't
                temporary.delete();
            }
        }
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier)
                    throws DataStoreException {
        String fileName = getFileName(identifier);
        try {
            if (getLength(identifier) > -1) {
                LOG.trace("getRecord: [{}]  retrieved using getLength",
                    identifier);
                if (minModifiedDate > 0) {
                    touchInternal(identifier);
                }
                usesIdentifier(identifier);
                return new CachingDataRecord(this, identifier);
            } else if (asyncWriteCache.hasEntry(fileName, minModifiedDate > 0)) {
                LOG.trace("getRecord: [{}]  retrieved from asyncUploadmap",
                    identifier);
                usesIdentifier(identifier);
                return new CachingDataRecord(this, identifier);
            }
        } catch (IOException ioe) {
            throw new DataStoreException("error in getting record ["
                + identifier + "]", ioe);
        }
        throw new DataStoreException("Record not found: " + identifier);
    }
    
    /**
     * Get a data record for the given identifier or null it data record doesn't
     * exist in {@link Backend}
     * 
     * @param identifier identifier of record.
     * @return the {@link CachingDataRecord} or null.
     */
    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier)
                    throws DataStoreException {
        String fileName = getFileName(identifier);
        try {
            if (asyncWriteCache.hasEntry(fileName, minModifiedDate > 0)) {
                LOG.trace(
                    "getRecordIfStored: [{}]  retrieved from asyncuploadmap",
                    identifier);
                usesIdentifier(identifier);
                return new CachingDataRecord(this, identifier);
            } else if (recLenCache.containsKey(identifier)) {
                LOG.trace(
                    "getRecordIfStored: [{}]  retrieved using recLenCache",
                    identifier);
                if (minModifiedDate > 0) {
                    touchInternal(identifier);
                }
                usesIdentifier(identifier);
                return new CachingDataRecord(this, identifier);
            } else {
                try {
                    long length = backend.getLength(identifier);
                    LOG.debug(
                        "getRecordIfStored :[{}]  retrieved from backend",
                        identifier);
                    recLenCache.put(identifier, length);
                    if (minModifiedDate > 0) {
                        touchInternal(identifier);
                    }
                    usesIdentifier(identifier);
                    return new CachingDataRecord(this, identifier);
                } catch (DataStoreException ignore) {
                    LOG.warn(" getRecordIfStored: [{}]  not found", identifier);
                }

            }
        } catch (IOException ioe) {
            throw new DataStoreException(ioe);
        }
        return null;
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {
        LOG.info("minModifiedDate set to [{}]", before);
        minModifiedDate = before;
    }

    /**
     * Retrieves all identifiers from {@link Backend}.
     */
    @Override
    public Iterator<DataIdentifier> getAllIdentifiers()
            throws DataStoreException {
        Set<DataIdentifier> ids = new HashSet<DataIdentifier>();
        for (String fileName : asyncWriteCache.getAll()) {
            ids.add(getIdentifier(fileName));
        }
        Iterator<DataIdentifier> itr = backend.getAllIdentifiers();
        while (itr.hasNext()) {
            ids.add(itr.next());
        }
        return ids.iterator();
    }

    /**
     * This method deletes record from {@link Backend} and then from
     * {@link LocalCache}
     */
    @Override
    public void deleteRecord(DataIdentifier identifier)
            throws DataStoreException {
        String fileName = getFileName(identifier);
        synchronized (this) {
            try {
                // order is important here
                recLenCache.remove(identifier);
                asyncWriteCache.delete(fileName);
                backend.deleteRecord(identifier);
                cache.delete(fileName);
            } catch (IOException ioe) {
                throw new DataStoreException(ioe);
            }
        }
    }

    @Override
    public synchronized int deleteAllOlderThan(long min)
            throws DataStoreException {
        Set<DataIdentifier> diSet = backend.deleteAllOlderThan(min);
        
        // remove entries from local cache
        for (DataIdentifier identifier : diSet) {
            recLenCache.remove(identifier);
            cache.delete(getFileName(identifier));
        }
        try {
            for (String fileName : asyncWriteCache.deleteOlderThan(min)) {
                diSet.add(getIdentifier(fileName));
            }
        } catch (IOException e) {
            throw new DataStoreException(e);
        }
        LOG.info(
            "deleteAllOlderThan  exit. Deleted [{}]records. Number of records deleted [{}]",
            diSet, diSet.size());
        return diSet.size();
    }

    /**
     * Get stream of record from {@link LocalCache}. If record is not available
     * in {@link LocalCache}, this method fetches record from {@link Backend}
     * and stores it to {@link LocalCache}. Stream is then returned from cached
     * record.
     */
    InputStream getStream(DataIdentifier identifier) throws DataStoreException {
        InputStream in = null;
        try {
            String fileName = getFileName(identifier);
            InputStream cached = cache.getIfStored(fileName);
            if (cached != null) {
                return cached;
            }
            in = backend.read(identifier);
            return cache.store(fileName, in);
        } catch (IOException e) {
            throw new DataStoreException("IO Exception: " + identifier, e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    /**
     * Return lastModified of record from {@link Backend} assuming
     * {@link Backend} as a single source of truth.
     */
    public long getLastModified(DataIdentifier identifier)
            throws DataStoreException {
        String fileName = getFileName(identifier);
        long lastModified = asyncWriteCache.getLastModified(fileName);
        if (lastModified != 0) {
            LOG.trace(
                "identifier [{}], lastModified=[{}] retrireved from AsyncUploadCache ",
                identifier, lastModified);

        } else if (asyncTouchCache.get(identifier) != null) {
            lastModified = asyncTouchCache.get(identifier);
            LOG.trace(
                "identifier [{}], lastModified=[{}] retrireved from asyncTouchCache ",
                identifier, lastModified);
        } else {
            lastModified = backend.getLastModified(identifier);
            LOG.debug(
                "identifier [{}], lastModified=[{}] retrireved from backend ",
                identifier, lastModified);
            asyncDownload(identifier);
        }
        return lastModified;
    }

    /**
     * Return the length of record from {@link LocalCache} if available,
     * otherwise retrieve it from {@link Backend}.
     */
    public long getLength(final DataIdentifier identifier)
                    throws DataStoreException {
        String fileName = getFileName(identifier);

        Long length = recLenCache.get(identifier);
        if (length != null) {
            LOG.trace(" identifier [{}] length fetched from recLengthCache",
                identifier);
            return length;
        } else if ((length = cache.getFileLength(fileName)) != null) {
            LOG.trace(" identifier [{}] length fetched from local cache",
                identifier);
            recLenCache.put(identifier, length);
            return length;
        } else {
            length = backend.getLength(identifier);
            LOG.debug(" identifier [{}] length fetched from backend",
                identifier);
            recLenCache.put(identifier, length);
            asyncDownload(identifier);
            return length;
        }
    }

    @Override
    protected byte[] getOrCreateReferenceKey() throws DataStoreException {
        return secret.getBytes(StandardCharsets.UTF_8);
    }

    public Set<String> getPendingUploads() {
        return asyncWriteCache.getAll();
    }
    
    
    public void deleteFromCache(DataIdentifier identifier)
                    throws DataStoreException {
        try {
            // order is important here
            recLenCache.remove(identifier);
            String fileName = getFileName(identifier);
            asyncWriteCache.delete(fileName);
            cache.delete(fileName);
        } catch (IOException ioe) {
            throw new DataStoreException(ioe);
        }
    }
    
    @Override
    public void onSuccess(AsyncUploadResult result) {
        DataIdentifier identifier = result.getIdentifier();
        File file = result.getFile();
        String fileName = getFileName(identifier);
        try {
            LOG.debug("Upload completed for [{}]", identifier);
            // remove from failed upload map if any.
            uploadRetryMap.remove(identifier);
            AsyncUploadCacheResult cachedResult = asyncWriteCache.remove(fileName);
            if (cachedResult.doRequiresDelete()) {
                // added record already marked for delete
                deleteRecord(identifier);
            } else {
                // async upload took lot of time.
                // getRecord to touch if required.
                getRecord(identifier);
            }
        } catch (IOException ie) {
            LOG.warn("Cannot remove pending file upload. Dataidentifer [ "
                + identifier + "], file [" + file.getAbsolutePath() + "]", ie);
        } catch (DataStoreException dse) {
            LOG.warn("Cannot remove pending file upload. Dataidentifer [ "
                + identifier + "], file [" + file.getAbsolutePath() + "]", dse);
        }
    }

    @Override
    public void onFailure(AsyncUploadResult result) {
        DataIdentifier identifier = result.getIdentifier();
        File file = result.getFile();
        String fileName = getFileName(identifier);
        if (result.getException() != null) {
            LOG.warn("Async Upload failed. Dataidentifer [ " + identifier
                + "], file [" + file.getAbsolutePath() + "]",
                result.getException());
        } else {
            LOG.warn("Async Upload failed. Dataidentifer [ " + identifier
                + "], file [" + file.getAbsolutePath() + "]");
        }
        // Retry failed upload upto uploadRetries times.
        try {
            if (asyncWriteCache.hasEntry(fileName, false)) {
                synchronized (uploadRetryMap) {
                    Integer retry = uploadRetryMap.get(identifier);
                    if (retry == null) {
                        retry = new Integer(1);
                    } else {
                        retry++;
                    }
                    if (retry <= uploadRetries) {
                        uploadRetryMap.put(identifier, retry);
                        LOG.info(
                            "Retrying [{}] times failed upload for dataidentifer {}",
                            retry, identifier);
                        try {
                            backend.writeAsync(identifier, file, this);
                        } catch (DataStoreException e) {
                            LOG.warn("exception", e);
                        }
                    } else {
                        LOG.info("Retries [{}] exhausted for  dataidentifer {}.",
                            (retry - 1), identifier);
                        uploadRetryMap.remove(identifier);
                    }
                }
            }
        } catch (IOException ie) {
            LOG.warn("Cannot retry failed async file upload. Dataidentifer [ "
                + identifier + "], file [" + file.getAbsolutePath() + "]", ie);
        }
    }

    @Override
    public void onAbort(AsyncUploadResult result) {
        DataIdentifier identifier = result.getIdentifier();
        File file = result.getFile();
        String fileName = getFileName(identifier);
        try {
            // remove from failed upload map if any.
            uploadRetryMap.remove(identifier);
            asyncWriteCache.remove(fileName);
            LOG.info(
                "Async Upload Aborted. Dataidentifer [{}], file [{}] removed from AsyncCache.",
                identifier, file.getAbsolutePath());
        } catch (IOException ie) {
            LOG.warn("Cannot remove pending file upload. Dataidentifer [ "
                + identifier + "], file [" + file.getAbsolutePath() + "]", ie);
        }
    }

    
    @Override
    public void onSuccess(AsyncTouchResult result) {
        asyncTouchCache.remove(result.getIdentifier());
        LOG.debug(" Async Touch succeed. Removed [{}] from asyncTouchCache",
            result.getIdentifier());

    }
    
    @Override
    public void onFailure(AsyncTouchResult result) {
        LOG.warn(" Async Touch failed. Not removing [{}] from asyncTouchCache",
            result.getIdentifier());
        if (result.getException() != null) {
            LOG.debug(" Async Touch failed. exception", result.getException());
        }
    }
    
    @Override
    public void onAbort(AsyncTouchResult result) {
        asyncTouchCache.remove(result.getIdentifier());
        LOG.debug(" Async Touch aborted. Removed [{}] from asyncTouchCache",
            result.getIdentifier());
    }
    
    /**
     * Method to confirm that identifier can be deleted from {@link Backend}
     * 
     * @param identifier
     * @return
     */
    public boolean confirmDelete(DataIdentifier identifier) {
        if (isInUse(identifier)) {
            LOG.debug("identifier [{}] is inUse confirmDelete= false ",
                identifier);
            return false;
        }

        String fileName = getFileName(identifier);
        long lastModified = asyncWriteCache.getLastModified(fileName);
        if (lastModified != 0) {
            LOG.debug(
                "identifier [{}] is asyncWriteCache map confirmDelete= false ",
                identifier);
            return false;

        }
        if (asyncTouchCache.get(identifier) != null) {
            LOG.debug(
                "identifier [{}] is asyncTouchCache confirmDelete = false ",
                identifier);
            return false;
        }

        return true;
    }
    
    /**
     * Internal method to touch identifier in @link {@link Backend}. if
     * {@link #touchAsync}, the record is updated asynchronously.
     * 
     * @param identifier
     * @throws DataStoreException
     */
    private void touchInternal(DataIdentifier identifier)
            throws DataStoreException {

        if (touchAsync) {
            Long lastModified = asyncTouchCache.put(identifier,
                System.currentTimeMillis());

            if (lastModified == null) {
                LOG.debug("Async touching [{}] ", identifier);
                backend.touchAsync(identifier, minModifiedDate, this);
            } else {
                LOG.debug( "Touched in asyncTouchMap [{}]", identifier);
            }
                
        } else {
            backend.touch(identifier, minModifiedDate);
        }
    }
    
    /**
     * Invoke {@link #getStream(DataIdentifier)} asynchronously to cache binary
     * asynchronously.
     */
    private void asyncDownload(final DataIdentifier identifier) {
        if (proactiveCaching
            && cacheSize != 0
            && asyncDownloadCache.put(identifier, System.currentTimeMillis()) == null) {
            downloadExecService.execute(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    InputStream input = null;
                    try {
                        LOG.trace("Async download [{}] started.", identifier);
                        input = getStream(identifier);
                    } catch (RepositoryException re) {
                        // ignore exception
                    } finally {
                        asyncDownloadCache.remove(identifier);
                        IOUtils.closeQuietly(input);
                        LOG.debug("Async download [{}] completed in [{}] ms.",
                            identifier,
                            (System.currentTimeMillis() - startTime));
                    }
                }
            });
        }
    }

    /**
     * Returns a unique temporary file to be used for creating a new data
     * record.
     */
    private File newTemporaryFile() throws IOException {
        return File.createTempFile(TMP, null, tmpDir);
    }

    /**
     * Load files from {@link LocalCache} to {@link Backend}.
     */
    private void uploadFilesFromCache() throws RepositoryException {
        ArrayList<File> files = new ArrayList<File>();
        listRecursive(files, directory);
        long totalSize = 0;
        for (File f : files) {
            totalSize += f.length();
        }
        if (files.size() > 0) {
            if (concurrentUploadsThreads > 1) {
                new FilesUploader(files, totalSize, concurrentUploadsThreads,
                    false).upload();
            } else {
                uploadFilesInSingleThread(files, totalSize);
            }
        }
    }

    private void uploadFilesInSingleThread(List<File> files, long totalSize)
            throws RepositoryException {
        long startTime = System.currentTimeMillis();
        LOG.info("Upload:  [{}] files in single thread.", files.size());
        long currentCount = 0;
        long currentSize = 0;
        long time = System.currentTimeMillis();
        for (File f : files) {
            String name = f.getName();
            LOG.debug("upload file [{}] ", name);
            if (!name.startsWith(TMP) && !name.endsWith(DS_STORE)
                && f.length() > 0) {
                uploadFileToBackEnd(f, false);
            }
            currentSize += f.length();
            currentCount++;
            long now = System.currentTimeMillis();
            if (now > time + 5000) {
                LOG.info("Uploaded:  [{}/{}] files, [{}/{}] size data",
                    new Object[] { currentCount, files.size(), currentSize,
                        totalSize });
                time = now;
            }
        }
        long endTime = System.currentTimeMillis();
        LOG.info(
            "Uploaded:  [{}/{}] files, [{}/{}] size data, time taken = [{}] sec",
            new Object[] { currentCount, files.size(), currentSize, totalSize,
                ((endTime - startTime) / 1000) });
    }

    /**
     * Traverse recursively and populate list with files.
     */
    private static void listRecursive(List<File> list, File file) {
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    listRecursive(list, f);
                } else {
                    list.add(f);
                }
            }
        }
    }

    /**
     * Upload file from {@link LocalCache} to {@link Backend}.
     * 
     * @param f
     *            file to uploaded.
     * @throws DataStoreException
     */
    private void uploadFileToBackEnd(File f, boolean updateAsyncUploadCache)
            throws DataStoreException {
        try {
            DataIdentifier identifier = new DataIdentifier(f.getName());
            usesIdentifier(identifier);
            if (!backend.exists(identifier)) {
                backend.write(identifier, f);
            }
            if (updateAsyncUploadCache) {
                String fileName = getFileName(identifier);
                asyncWriteCache.remove(fileName);
            }
            LOG.debug("uploaded [{}]", f.getName());
        } catch (IOException ioe) {
            throw new DataStoreException(ioe);
        }
    }

    /**
     * Derive file name from identifier.
     */
    private static String getFileName(DataIdentifier identifier) {
        String name = identifier.toString();
        return getFileName(name);
    }

    private static String getFileName(String name) {
        return name.substring(0, 2) + "/" + name.substring(2, 4) + "/"
            + name.substring(4, 6) + "/" + name;
    }

    private static DataIdentifier getIdentifier(String fileName) {
        return new DataIdentifier(
            fileName.substring(fileName.lastIndexOf("/") + 1));
    }

    private void usesIdentifier(DataIdentifier identifier) {
        inUse.put(identifier, new WeakReference<DataIdentifier>(identifier));
    }

    private static boolean mkdirs(File dir) throws IOException {
        if (dir.exists()) {
            if (dir.isFile()) {
                throw new IOException("Can not create a directory "
                    + "because a file exists with the same name: "
                    + dir.getAbsolutePath());
            }
            return false;
        }
        boolean created = dir.mkdirs();
        if (!created) {
            throw new IOException("Could not create directory: "
                + dir.getAbsolutePath());
        }
        return created;
    }

    @Override
    public void clearInUse() {
        inUse.clear();
    }

    public boolean isInUse(DataIdentifier identifier) {
        return inUse.containsKey(identifier);
    }

    @Override
    public void close() throws DataStoreException {
        cache.close();
        backend.close();
        downloadExecService.shutdown();
    }

    /**
     * Setter for configuration based secret
     * 
     * @param secret
     *            the secret used to sign reference binaries
     */
    public void setSecret(String secret) {
        this.secret = secret;
    }

    /**
     * Set the minimum object length.
     * 
     * @param minRecordLength
     *            the length
     */
    public void setMinRecordLength(int minRecordLength) {
        this.minRecordLength = minRecordLength;
    }

    /**
     * Return mininum object length.
     */
    @Override
    public int getMinRecordLength() {
        return minRecordLength;
    }

    /**
     * Return path of configuration properties.
     * 
     * @return path of configuration properties.
     */
    public String getConfig() {
        return config;
    }

    /**
     * Set the configuration properties path.
     * 
     * @param config
     *            path of configuration properties.
     */
    public void setConfig(String config) {
        this.config = config;
    }

    /**
     * @return size of {@link LocalCache}.
     */
    public long getCacheSize() {
        return cacheSize;
    }

    /**
     * Set size of {@link LocalCache}.
     * 
     * @param cacheSize
     *            size of {@link LocalCache}.
     */
    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    /**
     * @return path of {@link LocalCache}.
     */
    public String getPath() {
        return path;
    }

    /**
     * Set path of {@link LocalCache}.
     * 
     * @param path
     *            of {@link LocalCache}.
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * @return Purge trigger factor of {@link LocalCache}.
     */
    public double getCachePurgeTrigFactor() {
        return cachePurgeTrigFactor;
    }

    /**
     * Set purge trigger factor of {@link LocalCache}.
     * 
     * @param cachePurgeTrigFactor
     *            purge trigger factor.
     */
    public void setCachePurgeTrigFactor(double cachePurgeTrigFactor) {
        this.cachePurgeTrigFactor = cachePurgeTrigFactor;
    }

    /**
     * @return Purge resize factor of {@link LocalCache}.
     */
    public double getCachePurgeResizeFactor() {
        return cachePurgeResizeFactor;
    }

    /**
     * Set purge resize factor of {@link LocalCache}.
     * 
     * @param cachePurgeResizeFactor
     *            purge resize factor.
     */
    public void setCachePurgeResizeFactor(double cachePurgeResizeFactor) {
        this.cachePurgeResizeFactor = cachePurgeResizeFactor;
    }

    public int getConcurrentUploadsThreads() {
        return concurrentUploadsThreads;
    }

    public void setConcurrentUploadsThreads(int concurrentUploadsThreads) {
        this.concurrentUploadsThreads = concurrentUploadsThreads;
    }

    public int getAsyncUploadLimit() {
        return asyncUploadLimit;
    }

    public void setAsyncUploadLimit(int asyncUploadLimit) {
        this.asyncUploadLimit = asyncUploadLimit;
    }

    public boolean isContinueOnAsyncUploadFailure() {
        return continueOnAsyncUploadFailure;
    }

    public void setContinueOnAsyncUploadFailure(
            boolean continueOnAsyncUploadFailure) {
        this.continueOnAsyncUploadFailure = continueOnAsyncUploadFailure;
    }
    
    public int getUploadRetries() {
        return uploadRetries;
    }

    public void setUploadRetries(int uploadRetries) {
        this.uploadRetries = uploadRetries;
    }

    public void setTouchAsync(boolean touchAsync) {
        this.touchAsync = touchAsync;
    }

    public void setProactiveCaching(boolean proactiveCaching) {
        this.proactiveCaching = proactiveCaching;
    }
    
    public void setRecLengthCacheSize(int recLengthCacheSize) {
        this.recLengthCacheSize = recLengthCacheSize;
    }

    public Backend getBackend() {
        return backend;
    }

    /**
     * This class initiates files upload in multiple threads to backend.
     */
    private class FilesUploader {
        final List<File> files;

        final long totalSize;

        volatile AtomicInteger currentCount = new AtomicInteger();

        volatile AtomicLong currentSize = new AtomicLong();

        volatile AtomicBoolean exceptionRaised = new AtomicBoolean();

        DataStoreException exception;

        final int threads;

        final boolean updateAsyncCache;

        FilesUploader(List<File> files, long totalSize, int threads,
                boolean updateAsyncCache) {
            super();
            this.files = files;
            this.threads = threads;
            this.totalSize = totalSize;
            this.updateAsyncCache = updateAsyncCache;
        }

        void addCurrentCount(int delta) {
            currentCount.addAndGet(delta);
        }

        void addCurrentSize(long delta) {
            currentSize.addAndGet(delta);
        }

        synchronized void setException(DataStoreException exception) {
            exceptionRaised.getAndSet(true);
            this.exception = exception;
        }

        boolean isExceptionRaised() {
            return exceptionRaised.get();
        }

        void logProgress() {
            LOG.info("Uploaded:  [{}/{}] files, [{}/{}] size data",
                new Object[] { currentCount, files.size(), currentSize,
                    totalSize });
        }

        void upload() throws DataStoreException {
            long startTime = System.currentTimeMillis();
            LOG.info(" Uploading [{}] using [{}] threads.", files.size(), threads);
            ExecutorService executor = Executors.newFixedThreadPool(threads,
                new NamedThreadFactory("backend-file-upload-worker"));
            int partitionSize = files.size() / (threads);
            int startIndex = 0;
            int endIndex = partitionSize;
            for (int i = 1; i <= threads; i++) {
                List<File> partitionFileList = Collections.unmodifiableList(files.subList(
                    startIndex, endIndex));
                FileUploaderThread fut = new FileUploaderThread(
                    partitionFileList, startIndex, endIndex, this,
                    updateAsyncCache);
                executor.execute(fut);

                startIndex = endIndex;
                if (i == (threads - 1)) {
                    endIndex = files.size();
                } else {
                    endIndex = startIndex + partitionSize;
                }
            }
            // This will make the executor accept no new threads
            // and finish all existing threads in the queue
            executor.shutdown();

            try {
                // Wait until all threads are finish
                while (!isExceptionRaised()
                    && !executor.awaitTermination(15, TimeUnit.SECONDS)) {
                    logProgress();
                }
            } catch (InterruptedException ie) {

            }
            long endTime = System.currentTimeMillis();
            LOG.info(
                "Uploaded:  [{}/{}] files, [{}/{}] size data, time taken = [{}] sec",
                new Object[] { currentCount, files.size(), currentSize,
                    totalSize, ((endTime - startTime) / 1000) });
            if (isExceptionRaised()) {
                executor.shutdownNow(); // Cancel currently executing tasks
                throw exception;
            }
        }

    }

    /**
     * This class implements {@link Runnable} interface and uploads list of
     * files from startIndex to endIndex to {@link Backend}
     */
    private class FileUploaderThread implements Runnable {
        final List<File> files;

        final FilesUploader filesUploader;

        final int startIndex;

        final int endIndex;

        final boolean updateAsyncCache;

        FileUploaderThread(List<File> files, int startIndex, int endIndex,
                FilesUploader controller, boolean updateAsyncCache) {
            super();
            this.files = files;
            this.filesUploader = controller;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.updateAsyncCache = updateAsyncCache;
        }

        public void run() {
            long time = System.currentTimeMillis();
            LOG.debug(
                "Thread [{}] : Uploading files from startIndex [{}] to endIndex [{}] both inclusive.",
                new Object[] { Thread.currentThread().getName(), startIndex,
                    (endIndex - 1) });
            int uploadCount = 0;
            long uploadSize = 0;
            try {
                for (File f : files) {

                    if (filesUploader.isExceptionRaised()) {
                        break;
                    }
                    String name = f.getName();
                    LOG.debug("upload file [{}] ",name);
                    if (!name.startsWith(TMP) && !name.endsWith(DS_STORE)
                        && f.length() > 0) {
                        uploadFileToBackEnd(f, updateAsyncCache);
                    }
                    uploadCount++;
                    uploadSize += f.length();
                    // update upload status at every 15 seconds.
                    long now = System.currentTimeMillis();
                    if (now > time + 15000) {
                        filesUploader.addCurrentCount(uploadCount);
                        filesUploader.addCurrentSize(uploadSize);
                        uploadCount = 0;
                        uploadSize = 0;
                        time = now;
                    }
                }
                // update final state.
                filesUploader.addCurrentCount(uploadCount);
                filesUploader.addCurrentSize(uploadSize);
            } catch (DataStoreException e) {
                if (!filesUploader.isExceptionRaised()) {
                    filesUploader.setException(e);
                }
            }

        }
    }

}
