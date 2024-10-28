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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.StringUtils;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.guava.common.cache.CacheBuilder;
import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.Maps;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static java.lang.Thread.currentThread;

/**
 * A data store backend that stores data on Amazon S3.
 */
public class S3Backend extends AbstractSharedBackend {

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(S3Backend.class);
    private static final Logger LOG_STREAMS_DOWNLOAD = LoggerFactory.getLogger("oak.datastore.download.streams");
    private static final Logger LOG_STREAMS_UPLOAD = LoggerFactory.getLogger("oak.datastore.upload.streams");

    private static final String KEY_PREFIX = "dataStore_";

    private static final String META_KEY_PREFIX = "META/";

    private static final String REF_KEY = "reference.key";

    private static final int MAX_UNIQUE_RECORD_TRIES = 10;

    static final String PART_NUMBER = "partNumber";
    static final String UPLOAD_ID = "uploadId";

    private static final int ONE_MB = 1024*1024;
    static final long MIN_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 10; // 10MB
    static final long MAX_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 256; // 256MB
    static final long MAX_SINGLE_PUT_UPLOAD_SIZE = 1024L * 1024L * 1024L * 5L; // 5GB, AWS limitation
    static final long MAX_BINARY_UPLOAD_SIZE = 1024L * 1024L * 1024L * 1024L * 5L; // 5TB, AWS limitation
    private static final int MAX_ALLOWABLE_UPLOAD_URIS = 10000; // AWS limitation

    private AmazonS3Client s3service;

    // needed only in case of transfer acceleration is enabled for presigned URIs
    private AmazonS3Client s3PresignService;

    private String bucket;

    private byte[] secret;

    private TransferManager tmx;

    private Properties properties;

    private Date startTime;

    private S3RequestDecorator s3ReqDecorator;

    private Cache<DataIdentifier, URI> httpDownloadURICache;

    // 0 = off by default
    private int httpUploadURIExpirySeconds = 0;
    private int httpDownloadURIExpirySeconds = 0;

    private boolean presignedDownloadURIVerifyExists = true;

    public void init() throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            startTime = new Date();
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            LOG.debug("init");

            s3ReqDecorator = new S3RequestDecorator(properties);
            s3service = Utils.openService(properties);
            s3PresignService = s3service;

            if (bucket == null || "".equals(bucket.trim())) {
                bucket = properties.getProperty(S3Constants.S3_BUCKET);
                // Alternately check if the 'container' property is set
                if (Strings.isNullOrEmpty(bucket)) {
                    bucket = properties.getProperty(S3Constants.S3_CONTAINER);
                }
            }
            String region = properties.getProperty(S3Constants.S3_REGION);
            
            if (StringUtils.isNullOrEmpty(region)) {
                com.amazonaws.regions.Region ec2Region = Regions.getCurrentRegion();
                if (ec2Region != null) {
                    region = ec2Region.getName();
                } else {
                    throw new AmazonClientException(
                            "parameter ["
                                    + S3Constants.S3_REGION
                                    + "] not configured and cannot be derived from environment");
                }
            } else if (Utils.DEFAULT_AWS_BUCKET_REGION.equals(region)) {
                region = Region.US_Standard.toString();
            }

            createBucketIfNeeded(region);

            int writeThreads = 10;
            String writeThreadsStr = properties.getProperty(S3Constants.S3_WRITE_THREADS);
            if (writeThreadsStr != null) {
                writeThreads = Integer.parseInt(writeThreadsStr);
            }
            LOG.info("Using thread pool of [{}] threads in S3 transfer manager.", writeThreads);
            tmx = new TransferManager(s3service, Executors.newFixedThreadPool(writeThreads,
                new NamedThreadFactory("s3-transfer-manager-worker")));

            String renameKeyProp = properties.getProperty(S3Constants.S3_RENAME_KEYS);
            boolean renameKeyBool = (renameKeyProp == null || "".equals(renameKeyProp))
                    ? false
                    : Boolean.parseBoolean(renameKeyProp);
            LOG.info("Rename keys [{}]", renameKeyBool);
            if (renameKeyBool) {
                renameKeys();
            }

            // settings around pre-signing

            String putExpiry = properties.getProperty(S3Constants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS);
            if (putExpiry != null) {
                setHttpUploadURIExpirySeconds(Integer.parseInt(putExpiry));
            }

            String getExpiry = properties.getProperty(S3Constants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS);
            if (getExpiry != null) {
                final int getExpirySeconds = Integer.parseInt(getExpiry);
                setHttpDownloadURIExpirySeconds(getExpirySeconds);

                int cacheMaxSize = 0; // off by default
                String cacheMaxSizeStr = properties.getProperty(S3Constants.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE);
                if (cacheMaxSizeStr != null) {
                    cacheMaxSize = Integer.parseInt(cacheMaxSizeStr);
                }

                setHttpDownloadURICacheSize(cacheMaxSize);
            }

            String enablePresignedAccelerationStr = properties.getProperty(S3Constants.PRESIGNED_URI_ENABLE_ACCELERATION);
            setBinaryTransferAccelerationEnabled(enablePresignedAccelerationStr != null && "true".equals(enablePresignedAccelerationStr));

            presignedDownloadURIVerifyExists =
                    PropertiesUtil.toBoolean(properties.get(S3Constants.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS), true);

            // Initialize reference key secret
            getOrCreateReferenceKey();

            LOG.debug("S3 Backend initialized in [{}] ms",
                +(System.currentTimeMillis() - startTime.getTime()));
        } catch (Exception e) {
            LOG.error("Error ", e);
            Map<String, Object> filteredMap = Maps.newHashMap();
            if (properties != null) {
                filteredMap = Maps.filterKeys(Utils.asMap(properties),
                        input -> !input.equals(S3Constants.ACCESS_KEY) &&!input.equals(S3Constants.SECRET_KEY));
            }
            throw new DataStoreException("Could not initialize S3 from " + filteredMap, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private void createBucketIfNeeded(final String region) {
        try {
            if (!s3service.doesBucketExist(bucket)) {
                String bucketRegion = region;
                if (Utils.US_EAST_1_AWS_BUCKET_REGION.equals(region)) {
                    // The SDK has changed such that if the region is us-east-1
                    // the region value should not be provided in the
                    // request to create the bucket.
                    // See https://stackoverflow.com/questions/51912072/invalidlocationconstraint-error-while-creating-s3-bucket-when-the-used-command-i
                    bucketRegion = null;
                }
                CreateBucketRequest req = new CreateBucketRequest(bucket, bucketRegion);
                s3service.createBucket(req);
                if (Utils.waitForBucket(s3service, bucket)) {
                    LOG.error("Bucket [{}] does not exist in [{}] and was not automatically created",
                            bucket, region);
                    return;
                }
                LOG.info("Created bucket [{}] in [{}] ", bucket, region);
            } else {
                LOG.info("Using bucket [{}] in [{}] ", bucket, region);
            }
        }
        catch (SdkClientException awsException) {
            LOG.error("Attempt to create S3 bucket [{}] in [{}] failed",
                    bucket, region, awsException);
        }
    }

    void setBinaryTransferAccelerationEnabled(boolean enabled) {
        if (enabled) {
            // verify acceleration is enabled on the bucket
            BucketAccelerateConfiguration accelerateConfig = s3service.getBucketAccelerateConfiguration(new GetBucketAccelerateConfigurationRequest(bucket));
            if (accelerateConfig.isAccelerateEnabled()) {
                // If transfer acceleration is enabled for presigned URIs, we need a separate AmazonS3Client
                // instance with the acceleration mode enabled, because we don't want the requests from the
                // data store itself to S3 to use acceleration
                s3PresignService = Utils.openService(properties);
                s3PresignService.setS3ClientOptions(S3ClientOptions.builder().setAccelerateModeEnabled(true).build());
                LOG.info("S3 Transfer Acceleration enabled for presigned URIs.");

            } else {
                LOG.warn("S3 Transfer Acceleration is not enabled on the bucket {}. Will create normal, non-accelerated presigned URIs.",
                    bucket, S3Constants.PRESIGNED_URI_ENABLE_ACCELERATION);
            }
        } else {
            s3PresignService = s3service;
        }
    }

    /**
     * It uploads file to Amazon S3. If file size is greater than 5MB, this
     * method uses parallel concurrent connections to upload.
     */
    @Override
    public void write(DataIdentifier identifier, File file)
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
                objectMetaData = s3service.getObjectMetadata(s3ReqDecorator.decorate(new GetObjectMetadataRequest(bucket, key)));
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
                }catch (Exception e2) {
                    throw new DataStoreException("Could not upload " + key, e2);
                }
            }

            if (objectMetaData == null) {
                try {
                    // start multipart parallel upload using amazon sdk
                    Upload up = tmx.upload(s3ReqDecorator.decorate(new PutObjectRequest(
                        bucket, key, file)));
                    if (LOG_STREAMS_UPLOAD.isDebugEnabled()) {
                        // Log message, with exception so we can get a trace to see where the call came from
                        LOG_STREAMS_UPLOAD.debug("Binary uploaded to S3 - identifier={}", key, new Exception());
                    }
                    // wait for upload to finish
                    up.waitForUploadResult();
                    LOG.debug("synchronous upload to identifier [{}] completed.", identifier);
                } catch (Exception e2 ) {
                    throw new DataStoreException("Could not upload " + key, e2);
                }
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        LOG.debug("write of [{}], length=[{}], in [{}]ms",
            identifier, file.length(), (System.currentTimeMillis() - start));
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
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            ObjectMetadata objectMetaData = s3service.getObjectMetadata(s3ReqDecorator.decorate(new GetObjectMetadataRequest(bucket, key)));
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
                "Error occured to getObjectMetadata for key [" + identifier.toString() + "]", e);
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
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            S3Object object = s3service.getObject(bucket, key);
            InputStream in = object.getObjectContent();
            LOG.debug("[{}] read took [{}]ms", identifier, (System.currentTimeMillis() - start));
            if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                // Log message, with exception so we can get a trace to see where the call came from
                LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from S3 - identifier={}", key, new Exception());
            }
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
        return new RecordsIterator<DataIdentifier>(input -> new DataIdentifier(getIdentifierName(input.getKey())));
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
                "Could not delete dataIdentifier " + identifier, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void close() {
        // backend is closing. abort all mulitpart uploads from start.
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

    @Override
    public void addMetadataRecord(final InputStream input, final String name) throws DataStoreException {
        checkArgument(input != null, "input should not be null");
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

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

    @Override
    public void addMetadataRecord(File input, String name) throws DataStoreException {
        checkArgument(input != null, "input should not be null");
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            Upload upload = tmx.upload(s3ReqDecorator
                .decorate(new PutObjectRequest(bucket, addMetaKeyPrefix(name), input)));
            upload.waitForUploadResult();
        } catch (InterruptedException e) {
            LOG.error("Exception in uploading metadata file {}", new Object[] {input, e});
            throw new DataStoreException("Error in uploading metadata file", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ObjectMetadata meta = s3service.getObjectMetadata(bucket, addMetaKeyPrefix(name));
            return new S3DataRecord(this, s3service, bucket, new DataIdentifier(name),
                meta.getLastModified().getTime(), meta.getContentLength(), true, s3ReqDecorator);
        } catch(Exception e) {
            LOG.error("Error getting metadata record for {}", name, e);
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return null;
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        checkArgument(null != prefix, "prefix should not be null");

        List<DataRecord> metadataList = new ArrayList<DataRecord>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest().withBucketName(bucket).withPrefix(addMetaKeyPrefix(prefix));
            ObjectListing prevObjectListing = s3service.listObjects(listObjectsRequest);
            for (final S3ObjectSummary s3ObjSumm : prevObjectListing.getObjectSummaries()) {
                metadataList.add(new S3DataRecord(this, s3service, bucket,
                    new DataIdentifier(stripMetaKeyPrefix(s3ObjSumm.getKey())),
                    s3ObjSumm.getLastModified().getTime(), s3ObjSumm.getSize(), true, s3ReqDecorator));
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return metadataList;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        checkArgument(!Strings.isNullOrEmpty(name), "name should not be empty");

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

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        checkArgument(null != prefix, "prefix should not be empty");

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
                s3service.deleteObjects(delObjsReq);
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        final AbstractSharedBackend backend = this;
        return new RecordsIterator<DataRecord>(
                input -> new S3DataRecord(backend, s3service, bucket, new DataIdentifier(getIdentifierName(input.getKey())),
                        input.getLastModified().getTime(), input.getSize(), s3ReqDecorator));
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            ObjectMetadata object = s3service.getObjectMetadata(s3ReqDecorator.decorate(new GetObjectMetadataRequest(bucket, key)));
            S3DataRecord record = new S3DataRecord(this, s3service, bucket, identifier,
                object.getLastModified().getTime(), object.getContentLength(), s3ReqDecorator);
            LOG.debug("Identifier [{}]'s getRecord = [{}] took [{}]ms.",
                identifier, record, (System.currentTimeMillis() - start));

            return record;
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404 || e.getStatusCode() == 403) {
                LOG.debug(
                        "getRecord:Identifier [{}] not found. Took [{}] ms.",
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
    public byte[] getOrCreateReferenceKey() throws DataStoreException {
        try {
            if (secret != null && secret.length != 0) {
                return secret;
            } else {
                byte[] key;
                // Try reading from the metadata folder if it exists
                if (metadataRecordExists(REF_KEY)) {
                    key = readMetadataBytes(REF_KEY);
                } else {
                    // Create a new key and then retrieve it to use it
                    key = super.getOrCreateReferenceKey();
                    addMetadataRecord(new ByteArrayInputStream(key), REF_KEY);
                    key = readMetadataBytes(REF_KEY);
                }
                secret = key;
                return secret;
            }
        } catch (IOException e) {
            throw new DataStoreException("Unable to get or create key " + e);
        }
    }

    private byte[] readMetadataBytes(String name) throws IOException, DataStoreException {
        DataRecord rec = getMetadataRecord(name);
        InputStream stream = null;
        try {
            stream = rec.getStream();
            return IOUtils.toByteArray(stream);
        } finally {
            IOUtils.closeQuietly(stream);
        }
    }

    @Override
    public boolean metadataRecordExists(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            return s3service.doesObjectExist(bucket, addMetaKeyPrefix(name));
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    void setHttpUploadURIExpirySeconds(int seconds) {
        this.httpUploadURIExpirySeconds = seconds;
    }

    private DataIdentifier generateSafeRandomIdentifier() {
        return new DataIdentifier(
                String.format("%s-%d",
                        UUID.randomUUID().toString(),
                        Instant.now().toEpochMilli()
                )
        );
    }

    private URI createPresignedPutURI(DataIdentifier identifier) {
        if (httpUploadURIExpirySeconds <= 0) {
            // feature disabled
            return null;
        }

        return createPresignedURI(identifier, HttpMethod.PUT, httpUploadURIExpirySeconds);
    }

    void setHttpDownloadURIExpirySeconds(int seconds) {
        this.httpDownloadURIExpirySeconds = seconds;
    }

    void setHttpDownloadURICacheSize(int maxSize) {
        // max size 0 or smaller is used to turn off the cache
        if (maxSize > 0) {
            LOG.info("presigned GET URI cache enabled, maxSize = {} items, expiry = {} seconds", maxSize, httpDownloadURIExpirySeconds / 2);
            httpDownloadURICache = CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    // cache for half the expiry time of the URIs before giving out new ones
                    .expireAfterWrite(httpDownloadURIExpirySeconds / 2, TimeUnit.SECONDS)
                    .build();
        } else {
            LOG.info("presigned GET URI cache disabled");
            httpDownloadURICache = null;
        }
    }

    URI createHttpDownloadURI(@NotNull DataIdentifier identifier,
                              @NotNull DataRecordDownloadOptions downloadOptions) {
        if (httpDownloadURIExpirySeconds <= 0) {
            // feature disabled
            return null;
        }

        // When running unit test from Maven, it doesn't always honor the @NotNull decorators
        if (null == identifier) throw new NullPointerException("identifier");
        if (null == downloadOptions) throw new NullPointerException("downloadOptions");

        URI uri = null;
        // if cache is enabled, check the cache
        if (httpDownloadURICache != null) {
            uri = httpDownloadURICache.getIfPresent(identifier);
        }
        if (null == uri) {
            if (presignedDownloadURIVerifyExists) {
                try {
                    if (!exists(identifier)) {
                        LOG.warn("Cannot create download URI for nonexistent blob {}; returning null", getKeyName(identifier));
                        return null;
                    }
                } catch (DataStoreException e) {
                    LOG.warn("Cannot create download URI for blob {} (caught DataStoreException); returning null", getKeyName(identifier), e);
                    return null;
                }
            }

            Map<String, String> requestParams = Maps.newHashMap();
            requestParams.put("response-cache-control",
                    String.format("private, max-age=%d, immutable",
                            httpDownloadURIExpirySeconds)
            );

            String contentType = downloadOptions.getContentTypeHeader();
            if (! Strings.isNullOrEmpty(contentType)) {
                requestParams.put("response-content-type", contentType);
            }
            String contentDisposition =
                    downloadOptions.getContentDispositionHeader();

            if (! Strings.isNullOrEmpty(contentDisposition)) {
                requestParams.put("response-content-disposition",
                        contentDisposition);
            }

            uri = createPresignedURI(identifier,
                    HttpMethod.GET,
                    httpDownloadURIExpirySeconds,
                    requestParams);
            if (uri != null && httpDownloadURICache != null) {
                httpDownloadURICache.put(identifier, uri);
            }
        }
        return uri;
    }

    DataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURIs) {
        List<URI> uploadPartURIs = new ArrayList<>();
        long minPartSize = MIN_MULTIPART_UPLOAD_PART_SIZE;
        long maxPartSize = MAX_MULTIPART_UPLOAD_PART_SIZE;

        if (0L >= maxUploadSizeInBytes) {
            throw new IllegalArgumentException("maxUploadSizeInBytes must be > 0");
        }
        else if (0 == maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        }
        else if (-1 > maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        }
        else if (maxUploadSizeInBytes > MAX_SINGLE_PUT_UPLOAD_SIZE &&
                maxNumberOfURIs == 1) {
            throw new IllegalArgumentException(
                    String.format("Cannot do single-put upload with file size %d - exceeds max single-put upload size of %d",
                            maxUploadSizeInBytes,
                            MAX_SINGLE_PUT_UPLOAD_SIZE)
            );
        }
        else if (maxUploadSizeInBytes > MAX_BINARY_UPLOAD_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Cannot do upload with file size %d - exceeds max upload size of %d",
                            maxUploadSizeInBytes,
                            MAX_BINARY_UPLOAD_SIZE)
            );
        }

        DataIdentifier newIdentifier = generateSafeRandomIdentifier();
        String blobId = getKeyName(newIdentifier);
        String uploadId = null;

        if (httpUploadURIExpirySeconds > 0) {
            if (maxNumberOfURIs == 1 ||
                    maxUploadSizeInBytes <= minPartSize) {
                // single put
                uploadPartURIs.add(createPresignedPutURI(newIdentifier));
            }
            else {
                // multi-part
                InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(bucket, blobId);
                InitiateMultipartUploadResult res = s3service.initiateMultipartUpload(s3ReqDecorator.decorate(req));
                uploadId = res.getUploadId();

                long numParts;
                if (maxNumberOfURIs > 1) {
                    long requestedPartSize = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) maxNumberOfURIs));
                    if (requestedPartSize <= maxPartSize) {
                        numParts = Math.min(
                                maxNumberOfURIs,
                                Math.min(
                                        (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) minPartSize)),
                                        MAX_ALLOWABLE_UPLOAD_URIS
                                )
                        );
                    } else {
                        throw new IllegalArgumentException(
                                String.format("Cannot do multi-part upload with requested part size %d", requestedPartSize)
                        );
                    }
                }
                else {
                    long maximalNumParts = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) MIN_MULTIPART_UPLOAD_PART_SIZE));
                    numParts = Math.min(maximalNumParts, MAX_ALLOWABLE_UPLOAD_URIS);
                }

                Map<String, String> presignedURIRequestParams = Maps.newHashMap();
                for (long blockId = 1; blockId <= numParts; ++blockId) {
                    presignedURIRequestParams.put("partNumber", String.valueOf(blockId));
                    presignedURIRequestParams.put("uploadId", uploadId);
                    uploadPartURIs.add(createPresignedURI(newIdentifier,
                            HttpMethod.PUT,
                            httpUploadURIExpirySeconds,
                            presignedURIRequestParams));
                }
            }

            try {
                byte[] secret = getOrCreateReferenceKey();
                String uploadToken = new DataRecordUploadToken(blobId, uploadId).getEncodedToken(secret);

                return new DataRecordUpload() {
                    @Override
                    @NotNull
                    public String getUploadToken() {
                        return uploadToken;
                    }

                    @Override
                    public long getMinPartSize() {
                        return minPartSize;
                    }

                    @Override
                    public long getMaxPartSize() {
                        return maxPartSize;
                    }

                    @Override
                    @NotNull
                    public Collection<URI> getUploadURIs() {
                        return uploadPartURIs;
                    }
                };
            } catch (DataStoreException e) {
                LOG.warn("Unable to obtain data store key");
            }
        }

        return null;
    }

    DataRecord completeHttpUpload(@NotNull String uploadTokenStr)
            throws DataRecordUploadException, DataStoreException {

        if (Strings.isNullOrEmpty(uploadTokenStr)) {
            throw new IllegalArgumentException("uploadToken required");
        }

        DataRecordUploadToken uploadToken = DataRecordUploadToken.fromEncodedToken(uploadTokenStr, getOrCreateReferenceKey());
        String key = uploadToken.getBlobId();
        DataIdentifier blobId = new DataIdentifier(getIdentifierName(key));

        DataRecord record = null;
        try {
            record = getRecord(blobId);
            // If this succeeds this means either it was a "single put" upload
            // (we don't need to do anything in this case - blob is already uploaded)
            // or it was completed before with the same token.
        }
        catch (DataStoreException e) {
            // record doesn't exist - so this means we are safe to do the complete request
            if (uploadToken.getUploadId().isPresent()) {
                // An existing upload ID means this is a multi-part upload
                String uploadId = uploadToken.getUploadId().get();
                ListPartsRequest listPartsRequest = new ListPartsRequest(bucket, key, uploadId);
                PartListing listing = s3service.listParts(listPartsRequest);
                List<PartETag> eTags = new ArrayList<>();
                long size = 0L;
                Date lastModified = null;
                for (PartSummary partSummary : listing.getParts()) {
                    PartETag eTag = new PartETag(partSummary.getPartNumber(), partSummary.getETag());
                    eTags.add(eTag);
                    size += partSummary.getSize();
                    if (null == lastModified || partSummary.getLastModified().after(lastModified)) {
                        lastModified = partSummary.getLastModified();
                    }
                }

                CompleteMultipartUploadRequest completeReq = new CompleteMultipartUploadRequest(
                        bucket,
                        key,
                        uploadId,
                        eTags
                );

                s3service.completeMultipartUpload(completeReq);

                record = new S3DataRecord(
                        this,
                        s3service,
                        bucket,
                        blobId,
                        lastModified.getTime(),
                        size,
                        s3ReqDecorator
                );
            }
            else {
                // Something is wrong - upload ID missing from upload token
                // but record doesn't exist already, so this is invalid
                throw new DataRecordUploadException(
                        String.format("Unable to finalize direct write of binary %s - upload ID missing from upload token",
                                blobId)
                );
            }
        }

        return record;
    }

    private URI createPresignedURI(DataIdentifier identifier,
                                   HttpMethod method,
                                   int expirySeconds) {
        return createPresignedURI(identifier, method, expirySeconds, Maps.newHashMap());
    }

    private URI createPresignedURI(DataIdentifier identifier,
                                   HttpMethod method,
                                   int expirySeconds,
                                   Map<String, String> reqParams) {
        final String key = getKeyName(identifier);

        try {
            final Date expiration = new Date();
            expiration.setTime(expiration.getTime() + expirySeconds * 1000);

            GeneratePresignedUrlRequest request = s3ReqDecorator.decorate(new GeneratePresignedUrlRequest(bucket, key)
                    .withMethod(method)
                    .withExpiration(expiration));

            for (Map.Entry<String, String> e : reqParams.entrySet()) {
                request.addRequestParameter(e.getKey(), e.getValue());
            }

            URI uri = null;
            URL presignedURL = null;
            try {
                presignedURL = s3PresignService.generatePresignedUrl(request);
                uri = presignedURL.toURI();

                LOG.debug("Presigned {} URI for key {}: {}", method.name(), key, uri.toString());
            }
            catch (URISyntaxException e) {
                LOG.error("AWS request to create presigned S3 URI failed - could not convert '{}' to URI",
                        (null != presignedURL ? presignedURL.toString() : "")
                );
            }

            return uri;

        } catch (AmazonServiceException e) {
            LOG.error("AWS request to create presigned S3 {} URI failed. " +
                            "Key: {}, Error: {}, HTTP Code: {}, AWS Error Code: {}, Error Type: {}, Request ID: {}",
                    method.name(), key, e.getMessage(), e.getStatusCode(), e.getErrorCode(), e.getErrorType(), e.getRequestId());

            return null;
        }
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
            queue = new LinkedList<>();
            this.transformer = transformer;
        }

        @Override
        protected T computeNext() {
            if (queue.isEmpty()) {
                loadBatch();
            }

            while (queue.isEmpty() && prevObjectListing.getNextMarker() != null) {
                LOG.debug("Queue is empty, but there is more data in the S3 bucket");
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
                    ListObjectsRequest listReq = new ListObjectsRequest();
                    listReq.withBucketName(bucket);
                    if (properties.containsKey(S3Constants.MAX_KEYS)) {
                        listReq.setMaxKeys(Integer.valueOf(properties.getProperty(S3Constants.MAX_KEYS)));
                    }

                    prevObjectListing = s3service.listObjects(listReq);
                } else if (prevObjectListing.isTruncated()) { //already initialized more objects available
                    prevObjectListing = s3service.listNextBatchOfObjects(prevObjectListing);
                } else { // no more available
                    return false;
                }

                List<S3ObjectSummary> listing = CollectionUtils.toList(
                    filter(prevObjectListing.getObjectSummaries(),
                            input -> !input.getKey().startsWith(META_KEY_PREFIX)));

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
    static class S3DataRecord extends AbstractDataRecord {
        private AmazonS3Client s3service;
        private long length;
        private long lastModified;
        private String bucket;
        private boolean isMeta;
        private final S3RequestDecorator s3ReqDecorator;

        public S3DataRecord(AbstractSharedBackend backend, AmazonS3Client s3service, String bucket,
            DataIdentifier key, long lastModified, long length, final S3RequestDecorator s3ReqDecorator) {
            this(backend, s3service, bucket, key, lastModified, length, false, s3ReqDecorator);
        }

        public S3DataRecord(AbstractSharedBackend backend, AmazonS3Client s3service, String bucket,
            DataIdentifier key, long lastModified, long length, boolean isMeta, final S3RequestDecorator s3ReqDecorator) {
            super(backend, key);
            this.s3service = s3service;
            this.lastModified = lastModified;
            this.length = length;
            this.bucket = bucket;
            this.isMeta = isMeta;
            this.s3ReqDecorator = s3ReqDecorator;
        }

        @Override
        public long getLength() throws DataStoreException {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            String id = getKeyName(getIdentifier());
            if (isMeta) {
                id = addMetaKeyPrefix(getIdentifier().toString());
                return s3service.getObject(bucket, id).getObjectContent();
            }
            else {
                // Don't worry about stream logging for metadata records
                if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                    // Log message, with exception so we can get a trace to see where the call came from
                    LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from S3 - identifier={}", id, new Exception());
                }
            }
            return s3service.getObject(s3ReqDecorator.decorate(new GetObjectRequest(bucket, id))).getObjectContent();
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }

        @Override
        public String toString() {
            return "S3DataRecord{" +
                "identifier=" + getIdentifier() +
                ", length=" + length +
                ", lastModified=" + lastModified +
                ", bucket='" + bucket + '\'' +
                '}';
        }
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
                    LOG.info("Records[{}] deleted in datastore from index [{}] to [{}]",
                        dobjs.getDeletedObjects().size(), startIndex, (endIndex - 1));
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
                    LOG.error(" Exception in renaming [{}] to [{}] ", ie, oldKey, newS3Key);
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
}
