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

package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.AsyncTouchCallback;
import org.apache.jackrabbit.core.data.AsyncTouchResult;
import org.apache.jackrabbit.core.data.AsyncUploadCallback;
import org.apache.jackrabbit.core.data.AsyncUploadResult;
import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.StringUtils;

import static com.google.common.collect.Iterables.filter;
import static java.lang.Thread.currentThread;

/**
 * A data store backend that stores data on Amazon S3.
 */
public class S3Backend implements SharedS3Backend {

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(S3Backend.class);

    private static final String KEY_PREFIX = "dataStore_";

    private static final String META_KEY_PREFIX = "META/";

    private AmazonS3Client s3service;

    private String bucket;

    private TransferManager tmx;

    private CachingDataStore store;

    private Properties properties;

    private Date startTime;

    private ThreadPoolExecutor asyncWriteExecuter;
    private S3RequestDecorator s3ReqDecorator;

    /**
     * Initialize S3Backend. It creates AmazonS3Client and TransferManager from
     * aws.properties. It creates S3 bucket if it doesn't pre-exist in S3.
     */
    @Override
    public void init(CachingDataStore store, String homeDir, String config)
            throws DataStoreException {
        Properties initProps = null;
        //Check is configuration is already provided. That takes precedence
        //over config provided via file based config
        if(this.properties != null){
            initProps = this.properties;
        } else {
            if(config == null){
                config = Utils.DEFAULT_CONFIG_FILE;
            }
            try{
                initProps = Utils.readConfig(config);
            }catch(IOException e){
                throw new DataStoreException("Could not initialize S3 from "
                        + config, e);
            }
            this.properties = initProps;
        }
        init(store, homeDir, initProps);
    }

    public void init(CachingDataStore store, String homeDir, Properties prop)
            throws DataStoreException {

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            startTime = new Date();
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            LOG.debug("init");
            this.store = store;
            s3ReqDecorator = new S3RequestDecorator(prop);
            s3service = Utils.openService(prop);
            if (bucket == null || "".equals(bucket.trim())) {
                bucket = prop.getProperty(S3Constants.S3_BUCKET);
            }
            String region = prop.getProperty(S3Constants.S3_REGION);
            Region s3Region = null;
            if (StringUtils.isNullOrEmpty(region)) {
                com.amazonaws.regions.Region ec2Region = Regions.getCurrentRegion();
                if (ec2Region != null) {
                    s3Region = Region.fromValue(ec2Region.getName());
                } else {
                    throw new AmazonClientException(
                            "parameter ["
                                    + S3Constants.S3_REGION
                                    + "] not configured and cannot be derived from environment");
                }
            } else {
                if (Utils.DEFAULT_AWS_BUCKET_REGION.equals(region)) {
                    s3Region = Region.US_Standard;
                } else if (Region.EU_Ireland.toString().equals(region)) {
                    s3Region = Region.EU_Ireland;
                } else {
                    s3Region = Region.fromValue(region);
                }
            }

            if (!s3service.doesBucketExist(bucket)) {
                s3service.createBucket(bucket, s3Region);
                LOG.info("Created bucket [{}] in [{}] ", bucket, region);
            } else {
                LOG.info("Using bucket [{}] in [{}] ", bucket, region);
            }

            int writeThreads = 10;
            String writeThreadsStr = prop.getProperty(S3Constants.S3_WRITE_THREADS);
            if (writeThreadsStr != null) {
                writeThreads = Integer.parseInt(writeThreadsStr);
            }
            LOG.info("Using thread pool of [{}] threads in S3 transfer manager.", writeThreads);
            tmx = new TransferManager(s3service,
                (ThreadPoolExecutor) Executors.newFixedThreadPool(writeThreads,
                    new NamedThreadFactory("s3-transfer-manager-worker")));

            int asyncWritePoolSize = 10;
            String maxConnsStr = prop.getProperty(S3Constants.S3_MAX_CONNS);
            if (maxConnsStr != null) {
                asyncWritePoolSize = Integer.parseInt(maxConnsStr)
                    - writeThreads;
            }

            asyncWriteExecuter = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                asyncWritePoolSize, new NamedThreadFactory("s3-write-worker"));
            String renameKeyProp = prop.getProperty(S3Constants.S3_RENAME_KEYS);
            boolean renameKeyBool = (renameKeyProp == null || "".equals(renameKeyProp))
                    ? false
                    : Boolean.parseBoolean(renameKeyProp);
            LOG.info("Rename keys [{}]", renameKeyBool);
            if (renameKeyBool) {
                renameKeys();
            }
            LOG.debug("S3 Backend initialized in [{}] ms",
                +(System.currentTimeMillis() - startTime.getTime()));
        } catch (Exception e) {
            LOG.debug("  error ", e);
            Map<String, String> filteredMap = Maps.newHashMap();
            if (prop != null) {
                filteredMap = Maps.filterKeys(Maps.fromProperties(prop), new Predicate<String>() {
                    @Override public boolean apply(String input) {
                        return !input.equals(S3Constants.ACCESS_KEY) && !input.equals(S3Constants
                            .SECRET_KEY);
                    }
                });
            }
            throw new DataStoreException("Could not initialize S3 from "
                + filteredMap, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * It uploads file to Amazon S3. If file size is greater than 5MB, this
     * method uses parallel concurrent connections to upload.
     */
    @Override
    public void write(DataIdentifier identifier, File file)
            throws DataStoreException {
        this.write(identifier, file, false, null);

    }

    @Override
    public void writeAsync(DataIdentifier identifier, File file,
            AsyncUploadCallback callback) throws DataStoreException {
        if (callback == null) {
            throw new IllegalArgumentException(
                "callback parameter cannot be null in asyncUpload");
        }
        asyncWriteExecuter.execute(new AsyncUploadJob(identifier, file,
            callback));
    }

    /**
     * Check if record identified by identifier exists in Amazon S3.
     */
    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ObjectMetadata objectMetaData = s3service.getObjectMetadata(bucket,
                key);
            if (objectMetaData != null) {
                LOG.trace("exists [{}]: [true] took [{}] ms.",
                    identifier, (System.currentTimeMillis() - start) );
                return true;
            }
            return false;
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404 || e.getStatusCode() == 403) {
                LOG.debug("exists [{}]: [false] took [{}] ms.",
                    identifier, (System.currentTimeMillis() - start) );
                return false;
            }
            throw new DataStoreException(
                "Error occured to getObjectMetadata for key ["
                    + identifier.toString() + "]", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public boolean exists(DataIdentifier identifier, boolean touch)
            throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ObjectMetadata objectMetaData = null;
        boolean retVal = false;
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            objectMetaData = s3service.getObjectMetadata(bucket, key);
            if (objectMetaData != null) {
                retVal = true;
                if (touch) {
                    CopyObjectRequest copReq = new CopyObjectRequest(bucket,
                        key, bucket, key);
                    copReq.setNewObjectMetadata(objectMetaData);
                    Copy copy = tmx.copy(s3ReqDecorator.decorate(copReq));
                    copy.waitForCopyResult();
                    LOG.debug("[{}] touched took [{}] ms. ", identifier,
                        (System.currentTimeMillis() - start));
                }
            } else {
                retVal = false;
            }

        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404 || e.getStatusCode() == 403) {
                retVal = false;
            } else {
                throw new DataStoreException(
                    "Error occured to find exists for key ["
                        + identifier.toString() + "]", e);
            }
        } catch (Exception e) {
            throw new DataStoreException(
                "Error occured to find exists for key  "
                    + identifier.toString(), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        LOG.debug("exists [{}]: [{}] took [{}] ms.", new Object[] { identifier,
            retVal, (System.currentTimeMillis() - start) });
        return retVal;
    }

    @Override
    public void touchAsync(final DataIdentifier identifier,
            final long minModifiedDate, final AsyncTouchCallback callback)
            throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            if (callback == null) {
                throw new IllegalArgumentException(
                    "callback parameter cannot be null in touchAsync");
            }
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());

            asyncWriteExecuter.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        touch(identifier, minModifiedDate);
                        callback.onSuccess(new AsyncTouchResult(identifier));
                    } catch (DataStoreException e) {
                        AsyncTouchResult result = new AsyncTouchResult(
                            identifier);
                        result.setException(e);
                        callback.onFailure(result);
                    }
                }
            });
        } catch (Exception e) {
            if (callback != null) {
                callback.onAbort(new AsyncTouchResult(identifier));
            }
            throw new DataStoreException("Cannot touch the record "
                + identifier.toString(), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }

    }

    @Override
    public void touch(DataIdentifier identifier, long minModifiedDate)
            throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final long start = System.currentTimeMillis();
            final String key = getKeyName(identifier);
            if (minModifiedDate > 0
                && minModifiedDate > getLastModified(identifier)) {
                CopyObjectRequest copReq = new CopyObjectRequest(bucket, key,
                    bucket, key);
                copReq.setNewObjectMetadata(new ObjectMetadata());
                Copy copy = tmx.copy(s3ReqDecorator.decorate(copReq));
                copy.waitForCompletion();
                LOG.debug("[{}] touched. time taken [{}] ms ", new Object[] {
                    identifier, (System.currentTimeMillis() - start) });
            } else {
                LOG.trace("[{}] touch not required. time taken [{}] ms ",
                    new Object[] { identifier,
                        (System.currentTimeMillis() - start) });
            }

        } catch (Exception e) {
            throw new DataStoreException("Error occured in touching key ["
                + identifier.toString() + "]", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public InputStream read(DataIdentifier identifier)
            throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            S3Object object = s3service.getObject(bucket, key);
            InputStream in = object.getObjectContent();
            LOG.debug("[{}] read took [{}]ms", identifier,
                (System.currentTimeMillis() - start));
            return in;
        } catch (AmazonServiceException e) {
            throw new DataStoreException("Object not found: " + key, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers()
            throws DataStoreException {
        return new RecordsIterator<DataIdentifier>(
            new Function<S3ObjectSummary, DataIdentifier>() {
                @Override
                public DataIdentifier apply(S3ObjectSummary input) {
                    return new DataIdentifier(getIdentifierName(input.getKey()));
                }
        });
    }

    @Override
    public long getLastModified(DataIdentifier identifier)
            throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ObjectMetadata object = s3service.getObjectMetadata(bucket, key);
            long lastModified = object.getLastModified().getTime();
            LOG.debug(
                "Identifier [{}]'s lastModified = [{}] took [{}]ms.",
                new Object[] { identifier, lastModified,
                    (System.currentTimeMillis() - start) });
            return lastModified;
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404 || e.getStatusCode() == 403) {
                LOG.info(
                    "getLastModified:Identifier [{}] not found. Took [{}] ms.",
                    identifier, (System.currentTimeMillis() - start));
            }
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public long getLength(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ObjectMetadata object = s3service.getObjectMetadata(bucket, key);
            long length = object.getContentLength();
            LOG.debug("Identifier [{}]'s length = [{}] took [{}]ms.",
                new Object[] { identifier, length,
                    (System.currentTimeMillis() - start) });
            return length;
        } catch (AmazonServiceException e) {
            throw new DataStoreException("Could not length of dataIdentifier "
                + identifier, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void deleteRecord(DataIdentifier identifier)
            throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            s3service.deleteObject(bucket, key);
            LOG.debug("Identifier [{}] deleted. It took [{}]ms.", new Object[] {
                identifier, (System.currentTimeMillis() - start) });
        } catch (AmazonServiceException e) {
            throw new DataStoreException(
                "Could not getLastModified of dataIdentifier " + identifier, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Set<DataIdentifier> deleteAllOlderThan(long min)
            throws DataStoreException {
        long start = System.currentTimeMillis();
        // S3 stores lastModified to lower boundary of timestamp in ms.
        // and hence min is reduced by 1000ms.
        min = min - 1000;
        Set<DataIdentifier> deleteIdSet = new HashSet<DataIdentifier>(30);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ObjectListing prevObjectListing = s3service.listObjects(bucket);
            while (true) {
                List<DeleteObjectsRequest.KeyVersion> deleteList = new ArrayList<DeleteObjectsRequest.KeyVersion>();
                for (S3ObjectSummary s3ObjSumm : prevObjectListing.getObjectSummaries()) {
                    if (!s3ObjSumm.getKey().startsWith(META_KEY_PREFIX)) {
                        DataIdentifier identifier = new DataIdentifier(getIdentifierName(s3ObjSumm.getKey()));
                        long lastModified = s3ObjSumm.getLastModified().getTime();
                        LOG.debug("Identifier [{}]'s lastModified = [{}]", identifier, lastModified);
                        if (lastModified < min && store.confirmDelete(identifier)
                            // confirm once more that record's lastModified < min
                            //  order is important here
                            && s3service.getObjectMetadata(bucket, s3ObjSumm.getKey()).getLastModified().getTime() <
                            min) {

                            store.deleteFromCache(identifier);
                            LOG.debug("add id [{}] to delete lists", s3ObjSumm.getKey());
                            deleteList.add(new DeleteObjectsRequest.KeyVersion(s3ObjSumm.getKey()));
                            deleteIdSet.add(identifier);
                        }
                    }
                }
                if (deleteList.size() > 0) {
                    DeleteObjectsRequest delObjsReq = new DeleteObjectsRequest(
                        bucket);
                    delObjsReq.setKeys(deleteList);
                    DeleteObjectsResult dobjs = s3service.deleteObjects(delObjsReq);
                    if (dobjs.getDeletedObjects().size() != deleteList.size()) {
                        throw new DataStoreException(
                            "Incomplete delete object request. only  "
                                + dobjs.getDeletedObjects().size() + " out of "
                                + deleteList.size() + " are deleted");
                    } else {
                        LOG.debug("[{}] records deleted from datastore",
                            deleteList);
                    }
                }
                if (!prevObjectListing.isTruncated()) {
                    break;
                }
                prevObjectListing = s3service.listNextBatchOfObjects(prevObjectListing);
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        LOG.info(
            "deleteAllOlderThan: min=[{}] exit. Deleted[{}] records. Number of records deleted [{}] took [{}]ms",
            new Object[] { min, deleteIdSet, deleteIdSet.size(),
                (System.currentTimeMillis() - start) });
        return deleteIdSet;
    }

    @Override
    public void close() {
        // backend is closing. abort all mulitpart uploads from start.
        asyncWriteExecuter.shutdownNow();
        if(s3service.doesBucketExist(bucket)) {
            tmx.abortMultipartUploads(bucket, startTime);
        }
        tmx.shutdownNow();
        s3service.shutdown();
        LOG.info("S3Backend closed.");
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    /**
     * Properties used to configure the backend. If provided explicitly
     * before init is invoked then these take precedence
     *
     * @param properties  to configure S3Backend
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void addMetadataRecord(final InputStream input, final String name) throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            Upload upload = tmx.upload(s3ReqDecorator
                .decorate(new PutObjectRequest(bucket, addMetaKeyPrefix(name), input, new ObjectMetadata())));
            upload.waitForUploadResult();
        } catch (InterruptedException e) {
            LOG.error("Error in uploading", e);
            throw new DataStoreException("Error in uploading", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    public DataRecord getMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ObjectMetadata meta = s3service.getObjectMetadata(bucket, addMetaKeyPrefix(name));
            return new S3DataRecord(s3service, bucket, name,
                meta.getLastModified().getTime(), meta.getContentLength());
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    public List<DataRecord> getAllMetadataRecords(String prefix) {
        List<DataRecord> metadataList = new ArrayList<DataRecord>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest().withBucketName(bucket).withPrefix(addMetaKeyPrefix(prefix));
            ObjectListing prevObjectListing = s3service.listObjects(listObjectsRequest);
            for (final S3ObjectSummary s3ObjSumm : prevObjectListing.getObjectSummaries()) {
                metadataList.add(new S3DataRecord(s3service, bucket, stripMetaKeyPrefix(s3ObjSumm.getKey()),
                    s3ObjSumm.getLastModified().getTime(), s3ObjSumm.getSize()));
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return metadataList;
    }

    public boolean deleteMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            s3service.deleteObject(bucket, addMetaKeyPrefix(name));
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return true;
    }

    public void deleteAllMetadataRecords(String prefix) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());

            ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest().withBucketName(bucket).withPrefix(addMetaKeyPrefix(prefix));
            ObjectListing metaList = s3service.listObjects(listObjectsRequest);
            List<DeleteObjectsRequest.KeyVersion> deleteList = new ArrayList<DeleteObjectsRequest.KeyVersion>();
            for (S3ObjectSummary s3ObjSumm : metaList.getObjectSummaries()) {
                deleteList.add(new DeleteObjectsRequest.KeyVersion(s3ObjSumm.getKey()));
            }
            if (deleteList.size() > 0) {
                DeleteObjectsRequest delObjsReq = new DeleteObjectsRequest(bucket);
                delObjsReq.setKeys(deleteList);
                DeleteObjectsResult dobjs = s3service.deleteObjects(delObjsReq);
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        return new RecordsIterator<DataRecord>(
            new Function<S3ObjectSummary, DataRecord>() {
                @Override
                public DataRecord apply(S3ObjectSummary input) {
                    return new S3DataRecord(s3service, bucket, getIdentifierName(input.getKey()),
                        input.getLastModified().getTime(), input.getSize());
                }
            });
    }

    /**
     * Returns an iterator over the S3 objects
     * @param <T>
     */
    class RecordsIterator<T> extends AbstractIterator<T> {
        ObjectListing prevObjectListing;
        Queue<S3ObjectSummary> queue;
        long size;
        Function<S3ObjectSummary, T> transformer;

        public RecordsIterator (Function<S3ObjectSummary, T> transformer) {
            queue = Lists.newLinkedList();
            this.transformer = transformer;
        }

        @Override
        protected T computeNext() {
            if (queue.isEmpty()) {
                loadBatch();
            }

            if (!queue.isEmpty()) {
                return transformer.apply(queue.remove());
            }

            return endOfData();
        }

        private boolean loadBatch() {
            ClassLoader contextClassLoader = currentThread().getContextClassLoader();
            long start = System.currentTimeMillis();
            try {
                currentThread().setContextClassLoader(getClass().getClassLoader());

                // initialize the listing the first time
                if (prevObjectListing == null) {
                    prevObjectListing = s3service.listObjects(bucket);
                } else if (prevObjectListing.isTruncated()) { //already initialized more objects available
                    prevObjectListing = s3service.listNextBatchOfObjects(prevObjectListing);
                } else { // no more available
                    return false;
                }

                List<S3ObjectSummary> listing = Lists.newArrayList(
                    filter(prevObjectListing.getObjectSummaries(),
                        new Predicate<S3ObjectSummary>() {
                            @Override
                            public boolean apply(S3ObjectSummary input) {
                                return !input.getKey().startsWith(META_KEY_PREFIX);
                            }
                        }));

                // After filtering no elements
                if (listing.isEmpty()) {
                    return false;
                }

                size += listing.size();
                queue.addAll(listing);

                LOG.info("Loaded batch of size [{}] in [{}] ms.",
                    listing.size(), (System.currentTimeMillis() - start));

                return true;
            } catch (AmazonServiceException e) {
                LOG.warn("Could not list objects", e);
            } finally {
                if (contextClassLoader != null) {
                    currentThread().setContextClassLoader(contextClassLoader);
                }
            }
            return false;
        }
    }

    private static String addMetaKeyPrefix(String key) {
        return META_KEY_PREFIX + key;
    }

    private static String stripMetaKeyPrefix(String name) {
        if (name.startsWith(META_KEY_PREFIX)) {
            return name.substring(META_KEY_PREFIX.length());
        }
        return name;
    }

    /**
     * S3DataRecord which lazily retrieves the input stream of the record.
     */
    static class S3DataRecord implements DataRecord {
        private AmazonS3Client s3service;
        private DataIdentifier identifier;
        private long length;
        private long lastModified;
        private String bucket;

        public S3DataRecord(AmazonS3Client s3service, String bucket, String key, long lastModified, long length) {
            this.s3service = s3service;
            this.identifier = new DataIdentifier(key);
            this.lastModified = lastModified;
            this.length = length;
            this.bucket = bucket;
        }

        @Override
        public DataIdentifier getIdentifier() {
            return identifier;
        }

        @Override
        public String getReference() {
            return identifier.toString();
        }

        @Override
        public long getLength() throws DataStoreException {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            return s3service.getObject(bucket, addMetaKeyPrefix(identifier.toString())).getObjectContent();
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }
    }

    private void write(DataIdentifier identifier, File file,
            boolean asyncUpload, AsyncUploadCallback callback)
            throws DataStoreException {
        String key = getKeyName(identifier);
        ObjectMetadata objectMetaData = null;
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            // check if the same record already exists
            try {
                objectMetaData = s3service.getObjectMetadata(bucket, key);
            } catch (AmazonServiceException ase) {
                if (!(ase.getStatusCode() == 404 || ase.getStatusCode() == 403)) {
                    throw ase;
                }
            }
            if (objectMetaData != null) {
                long l = objectMetaData.getContentLength();
                if (l != file.length()) {
                    throw new DataStoreException("Collision: " + key
                        + " new length: " + file.length() + " old length: " + l);
                }
                LOG.debug("[{}]'s exists, lastmodified = [{}]", key,
                    objectMetaData.getLastModified().getTime());
                CopyObjectRequest copReq = new CopyObjectRequest(bucket, key,
                    bucket, key);
                copReq.setNewObjectMetadata(objectMetaData);
                Copy copy = tmx.copy(s3ReqDecorator.decorate(copReq));
                try {
                    copy.waitForCopyResult();
                    LOG.debug("lastModified of [{}] updated successfully.", identifier);
                    if (callback != null) {
                        callback.onSuccess(new AsyncUploadResult(identifier, file));
                    }
                }catch (Exception e2) {
                    AsyncUploadResult asyncUpRes= new AsyncUploadResult(identifier, file);
                    asyncUpRes.setException(e2);
                    if (callback != null) {
                        callback.onAbort(asyncUpRes);
                    }
                    throw new DataStoreException("Could not upload " + key, e2);
                }
            }

            if (objectMetaData == null) {
                try {
                    // start multipart parallel upload using amazon sdk
                    Upload up = tmx.upload(s3ReqDecorator.decorate(new PutObjectRequest(
                        bucket, key, file)));
                    // wait for upload to finish
                    if (asyncUpload) {
                        up.addProgressListener(new S3UploadProgressListener(up,
                            identifier, file, callback));
                        LOG.debug(
                            "added upload progress listener to identifier [{}]",
                            identifier);
                    } else {
                        up.waitForUploadResult();
                        LOG.debug("synchronous upload to identifier [{}] completed.", identifier);
                        if (callback != null) {
                            callback.onSuccess(new AsyncUploadResult(
                                identifier, file));
                        }
                    }
                } catch (Exception e2 ) {
                    AsyncUploadResult asyncUpRes= new AsyncUploadResult(identifier, file);
                    asyncUpRes.setException(e2);
                    if (callback != null) {
                        callback.onAbort(asyncUpRes);
                    }
                    throw new DataStoreException("Could not upload " + key, e2);
                }
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        LOG.debug(
            "write of [{}], length=[{}], in async mode [{}], in [{}]ms",
            new Object[] { identifier, file.length(), asyncUpload,
                (System.currentTimeMillis() - start) });
    }

    /**
     * This method rename object keys in S3 concurrently. The number of
     * concurrent threads is defined by 'maxConnections' property in
     * aws.properties. As S3 doesn't have "move" command, this method simulate
     * move as copy object object to new key and then delete older key.
     */
    private void renameKeys() throws DataStoreException {
        long startTime = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long count = 0;
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ObjectListing prevObjectListing = s3service.listObjects(bucket);
            List<DeleteObjectsRequest.KeyVersion> deleteList = new ArrayList<DeleteObjectsRequest.KeyVersion>();
            int nThreads = Integer.parseInt(properties.getProperty("maxConnections"));
            ExecutorService executor = Executors.newFixedThreadPool(nThreads,
                new NamedThreadFactory("s3-object-rename-worker"));
            boolean taskAdded = false;
            while (true) {
                for (S3ObjectSummary s3ObjSumm : prevObjectListing.getObjectSummaries()) {
                    executor.execute(new KeyRenameThread(s3ObjSumm.getKey()));
                    taskAdded = true;
                    count++;
                    // delete the object if it follows old key name format
                    if( s3ObjSumm.getKey().startsWith(KEY_PREFIX)) {
                        deleteList.add(new DeleteObjectsRequest.KeyVersion(
                            s3ObjSumm.getKey()));
                    }
                }
                if (!prevObjectListing.isTruncated()) break;
                prevObjectListing = s3service.listNextBatchOfObjects(prevObjectListing);
            }
            // This will make the executor accept no new threads
            // and finish all existing threads in the queue
            executor.shutdown();

            try {
                // Wait until all threads are finish
                while (taskAdded
                    && !executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.info("Rename S3 keys tasks timedout. Waiting again");
                }
            } catch (InterruptedException ie) {

            }
            LOG.info("Renamed [{}] keys, time taken [{}]sec", count,
                ((System.currentTimeMillis() - startTime) / 1000));
            // Delete older keys.
            if (deleteList.size() > 0) {
                DeleteObjectsRequest delObjsReq = new DeleteObjectsRequest(
                    bucket);
                int batchSize = 500, startIndex = 0, size = deleteList.size();
                int endIndex = batchSize < size ? batchSize : size;
                while (endIndex <= size) {
                    delObjsReq.setKeys(Collections.unmodifiableList(deleteList.subList(
                        startIndex, endIndex)));
                    DeleteObjectsResult dobjs = s3service.deleteObjects(delObjsReq);
                    LOG.info(
                        "Records[{}] deleted in datastore from index [{}] to [{}]",
                        new Object[] { dobjs.getDeletedObjects().size(),
                            startIndex, (endIndex - 1) });
                    if (endIndex == size) {
                        break;
                    } else {
                        startIndex = endIndex;
                        endIndex = (startIndex + batchSize) < size
                                ? (startIndex + batchSize)
                                : size;
                    }
                }
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * The method convert old key format to new format. For e.g. this method
     * converts old key dataStore_004cb70c8f87d78f04da41e7547cb434094089ea to
     * 004c-b70c8f87d78f04da41e7547cb434094089ea.
     */
    private static String convertKey(String oldKey)
            throws IllegalArgumentException {
        if (!oldKey.startsWith(KEY_PREFIX)) {
            return oldKey;
        }
        String key = oldKey.substring(KEY_PREFIX.length());
        return key.substring(0, 4) + Utils.DASH + key.substring(4);
    }

    /**
     * Get key from data identifier. Object is stored with key in S3.
     */
    private static String getKeyName(DataIdentifier identifier) {
        String key = identifier.toString();
        return key.substring(0, 4) + Utils.DASH + key.substring(4);
    }

    /**
     * Get data identifier from key.
     */
    private static String getIdentifierName(String key) {
        if (!key.contains(Utils.DASH)) {
            return null;
        } else if (key.contains(META_KEY_PREFIX)) {
            return key;
        }
        return key.substring(0, 4) + key.substring(5);
    }


    /**
     * The class renames object key in S3 in a thread.
     */
    private class KeyRenameThread implements Runnable {

        private String oldKey;

        public void run() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());
                String newS3Key = convertKey(oldKey);
                CopyObjectRequest copReq = new CopyObjectRequest(bucket,
                    oldKey, bucket, newS3Key);
                Copy copy = tmx.copy(s3ReqDecorator.decorate(copReq));
                try {
                    copy.waitForCopyResult();
                    LOG.debug("[{}] renamed to [{}] ", oldKey, newS3Key);
                } catch (InterruptedException ie) {
                    LOG.error(" Exception in renaming [{}] to [{}] ",
                        new Object[] { ie, oldKey, newS3Key });
                }

            } finally {
                if (contextClassLoader != null) {
                    Thread.currentThread().setContextClassLoader(
                        contextClassLoader);
                }
            }
        }

        public KeyRenameThread(String oldKey) {
            this.oldKey = oldKey;
        }
    }

    /**
     * Listener which receives callback on status of S3 upload.
     */
    private class S3UploadProgressListener implements ProgressListener {

        private File file;

        private DataIdentifier identifier;

        private AsyncUploadCallback callback;

        private Upload upload;

        public S3UploadProgressListener(Upload upload, DataIdentifier identifier, File file,
                AsyncUploadCallback callback) {
            super();
            this.identifier = identifier;
            this.file = file;
            this.callback = callback;
            this.upload = upload;
        }

        public void progressChanged(ProgressEvent progressEvent) {
            switch (progressEvent.getEventCode()) {
                case ProgressEvent.COMPLETED_EVENT_CODE:
                    callback.onSuccess(new AsyncUploadResult(identifier, file));
                    break;
                case ProgressEvent.FAILED_EVENT_CODE:
                    AsyncUploadResult result = new AsyncUploadResult(
                        identifier, file);
                    try {
                        AmazonClientException e = upload.waitForException();
                        if (e != null) {
                            result.setException(e);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    callback.onFailure(result);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * This class implements {@link Runnable} interface to upload {@link java.io.File}
     * to S3 asynchronously.
     */
    private class AsyncUploadJob implements Runnable {

        private DataIdentifier identifier;

        private File file;

        private AsyncUploadCallback callback;

        public AsyncUploadJob(DataIdentifier identifier, File file,
                AsyncUploadCallback callback) {
            super();
            this.identifier = identifier;
            this.file = file;
            this.callback = callback;
        }

        public void run() {
            try {
                write(identifier, file, true, callback);
            } catch (DataStoreException e) {
                LOG.error("Could not upload [" + identifier + "], file[" + file
                    + "]", e);
            }

        }
    }


}
