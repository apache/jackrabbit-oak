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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.collections.IteratorUtils;
import org.apache.jackrabbit.oak.commons.collections.MapUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.guava.common.cache.CacheBuilder;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAccelerateStatus;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAccelerateConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Copy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

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

    static final String PART_NUMBER = "partNumber";
    static final String UPLOAD_ID = "uploadId";

    static final long MIN_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 10L; // 10MB
    static final long MAX_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 256L; // 256MB
    static final long MAX_SINGLE_PUT_UPLOAD_SIZE = 1024L * 1024L * 1024L * 5L; // 5GB, AWS limitation
    static final long MAX_BINARY_UPLOAD_SIZE = 1024L * 1024L * 1024L * 1024L * 5L; // 5TB, AWS limitation
    private static final int MAX_ALLOWABLE_UPLOAD_URIS = 10000; // AWS limitation

    private S3Client s3Client;
    private S3AsyncClient s3AsyncClient;

    // needed only in case of transfer acceleration is enabled for presigned URIs
    private S3Presigner s3PresignService;

    private String bucket;

    private byte[] secret;

    private S3TransferManager tmx;

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

            final String region = Utils.getRegion(properties);

            s3Client = Utils.openService(properties, false);

            setBinaryTransferAccelerationEnabled(
                    Boolean.parseBoolean(
                            properties.getProperty(S3Constants.PRESIGNED_URI_ENABLE_ACCELERATION, "false")));

            s3AsyncClient = Utils.openAsyncService(properties);
            s3ReqDecorator = new S3RequestDecorator(properties);

            if (bucket == null || bucket.trim().isEmpty()) {
                bucket = properties.getProperty(S3Constants.S3_BUCKET);
                // Alternately check if the 'container' property is set
                if (StringUtils.isEmpty(bucket)) {
                    bucket = properties.getProperty(S3Constants.S3_CONTAINER);
                }
            }

            createBucketIfNeeded(region);

            int writeThreads = 10;
            String writeThreadsStr = properties.getProperty(S3Constants.S3_WRITE_THREADS);
            if (writeThreadsStr != null) {
                writeThreads = Integer.parseInt(writeThreadsStr);
            }
            LOG.info("Using thread pool of [{}] threads in S3 transfer manager.", writeThreads);
            tmx = S3TransferManager.builder()
                    .s3Client(s3AsyncClient)
                    .executor(Executors.
                            newFixedThreadPool(writeThreads,
                                    BasicThreadFactory.builder().namingPattern("s3-transfer-manager-worker-%d").build()))
                    .build();

            String renameKeyProp = properties.getProperty(S3Constants.S3_RENAME_KEYS);
            boolean renameKeyBool = renameKeyProp != null && !renameKeyProp.isEmpty() && Boolean.parseBoolean(renameKeyProp);
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

            presignedDownloadURIVerifyExists =
                    PropertiesUtil.toBoolean(properties.get(S3Constants.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS), true);

            // Initialize reference key secret
            getOrCreateReferenceKey();

            LOG.debug("S3 Backend initialized in [{}] ms", (System.currentTimeMillis() - startTime.getTime()));
        } catch (Exception e) {
            LOG.error("Error while initializing S3 Backend", e);
            Map<String, Object> filteredMap = new HashMap<>();
            if (properties != null) {
                filteredMap = MapUtils.filterKeys(Utils.asMap(properties),
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
            if (!S3BackendHelper.bucketExists(s3Client, bucket)) {
                String bucketRegion;
                if (Utils.US_EAST_1_AWS_BUCKET_REGION.equals(region)) {
                    // The SDK has changed such that if the region is us-east-1
                    // the region value should not be provided in the
                    // request to create the bucket.
                    // See https://stackoverflow.com/questions/51912072/invalidlocationconstraint-error-while-creating-s3-bucket-when-the-used-command-i
                    bucketRegion = null;
                } else {
                    bucketRegion = region;
                }
                s3Client.createBucket(createBucket ->
                        createBucket.bucket(bucket).
                                createBucketConfiguration(config ->
                                        config.locationConstraint(bucketRegion)
                                                .build())
                                .build());
                if (S3BackendHelper.waitForBucket(s3Client, bucket, 20, 100L)) {
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
            LOG.error("Attempt to create S3 bucket [{}] in [{}] failed", bucket, region, awsException);
        }
    }

    void setBinaryTransferAccelerationEnabled(final boolean enabled) {
        if (!enabled) {
            s3PresignService = Utils.createPresigner(s3Client, properties);
            return;
        }

        GetBucketAccelerateConfigurationResponse accelerateConfig = s3Client.getBucketAccelerateConfiguration(
                b -> b.bucket(bucket).build());

        if (Objects.equals(BucketAccelerateStatus.ENABLED, accelerateConfig.status())) {
            s3PresignService = Utils.createPresigner(Utils.openService(properties, true), properties);
            LOG.info("S3 Transfer Acceleration enabled for presigned URIs.");
        } else {
            LOG.warn("S3 Transfer Acceleration is not enabled on the bucket {}. Will create normal, non-accelerated presigned URIs. To enable set {}",
                    bucket, S3Constants.PRESIGNED_URI_ENABLE_ACCELERATION);
            s3PresignService = Utils.createPresigner(s3Client, properties);
        }
    }

    /**
     * It uploads file to Amazon S3. If file size is greater than 5MB, this
     * method uses parallel concurrent connections to upload.
     */
    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            // check if the same record already exists
            HeadObjectResponse headMeta = null;
            try {
                headMeta = s3Client.headObject(s3ReqDecorator.decorate(
                        HeadObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .build()));
            } catch (S3Exception se) {
                if (!(se instanceof NoSuchKeyException || se.statusCode() == 404 || se.statusCode() == 403)) {
                    throw se;
                }
            }
            if (headMeta != null) {
                long l = headMeta.contentLength();
                if (l != file.length()) {
                    throw new DataStoreException("Collision: " + key + " new length: " + file.length() + " old length: " + l);
                }
                LOG.debug("[{}]'s exists, lastmodified = [{}]", key, headMeta.lastModified().toEpochMilli());
                CopyObjectRequest copyReq = 
                        CopyObjectRequest.builder()
                                .sourceBucket(bucket)
                                .sourceKey(key)
                                .destinationBucket(bucket)
                                .destinationKey(key)
                                .metadataDirective(MetadataDirective.REPLACE) // Required to update metadata and reset creation time
                                .build();
                LOG.warn("Object MetaData before copy: {}", headMeta.metadata());
                if (Objects.equals(RemoteStorageMode.S3, properties.get(S3Constants.MODE))) {
                    copyReq = copyReq.toBuilder().metadata(headMeta.metadata()).build();
                }
                try {
                    Copy copy = tmx.copy(CopyRequest.builder()
                            .copyObjectRequest(
                                    s3ReqDecorator.decorate(copyReq))
                            .build());
                    copy.completionFuture().join();
                    LOG.debug("lastModified of [{}] updated successfully.", identifier);
                } catch (Exception e2) {
                    throw new DataStoreException("Could not upload " + key, e2.getCause());
                }
            }

            if (headMeta == null) {
                try {
                    // start multipart parallel upload using amazon sdk
                    FileUpload up = tmx.uploadFile(uploadReq ->
                            uploadReq.source(file).
                                    putObjectRequest(
                                            s3ReqDecorator.decorate(
                                                    PutObjectRequest.builder().bucket(bucket).key(key).contentLength(file.length())
                                                            .build()))
                                    .build());

                    if (LOG_STREAMS_UPLOAD.isDebugEnabled()) {
                        // Log message, with exception so we can get a trace to see where the call came from
                        LOG_STREAMS_UPLOAD.debug("Binary uploaded to S3 - identifier={}", key, new Exception());
                    }
                    // wait for upload to finish
                    up.completionFuture().join();
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
        LOG.debug("write of [{}], length=[{}], in [{}]ms", identifier, file.length(), System.currentTimeMillis() - start);
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
            return S3BackendHelper.objectExists(s3Client, bucket, key, s3ReqDecorator);
        } catch (S3Exception e) {
            LOG.debug("exists [{}]: [false] took [{}] ms.", identifier, System.currentTimeMillis() - start);
            throw new DataStoreException("Error occurred to getObjectMetadata for key [" + identifier + "]", e);
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
            InputStream in = s3Client.getObject(s3ReqDecorator.decorate(
                    GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build()),
                    ResponseTransformer.toInputStream());

            LOG.debug("[{}] read took [{}]ms", identifier, System.currentTimeMillis() - start);
            if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                // Log message, with exception so we can get a trace to see where the call came from
                LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from S3 - identifier={}", key, new Exception());
            }
            return in;
        } catch (S3Exception e) {
            throw new DataStoreException("Object not found: " + key, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() {
        return getAllRecords(s3Object -> new DataIdentifier(getIdentifierName(s3Object.key())));
    }

    @Override
    public void deleteRecord(DataIdentifier identifier)
            throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            s3Client.deleteObject(delReq -> delReq.bucket(bucket).key(key).build());
            LOG.debug("Identifier [{}] deleted. It took [{}]ms.", identifier, (System.currentTimeMillis() - start));
        } catch (S3Exception e) {
            throw new DataStoreException("Could not delete dataIdentifier " + identifier, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void close() {
        // backend is closing. abort all mulitpart uploads from start.
        try {
            if(S3BackendHelper.bucketExists(s3Client, bucket)) {
                // List and abort multipart uploads initiated before startTime
                S3BackendHelper.abortMultipartUpload(bucket, startTime, s3Client);
            }
        } finally {
            if (tmx != null) {
                tmx.close();
            }
            if (s3AsyncClient != null) {
                s3AsyncClient.close();
            }
            if (s3PresignService != null) {
                s3PresignService.close();
            }
            s3Client.close();
        }
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
        Utils.setRemoteStorageMode(this.properties);
    }

    @Override
    public void addMetadataRecord(final InputStream input, final String name) throws DataStoreException {
        checkArgument(input != null, "input should not be null");
        checkArgument(!StringUtils.isEmpty(name), "name should not be empty");

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        // Executor required to handle reading from the InputStream on a separate thread so the main upload is not blocked.
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            final PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(bucket)
                    .contentType("application/octet-stream")
                    .key(addMetaKeyPrefix(name));

            // Specify `null` for the content length when you don't know the content length.
            final AsyncRequestBody body = getRequestBody(input, executor, builder);
            final Upload upload = tmx.upload(uploadReq ->
                    uploadReq.requestBody(body).
                            putObjectRequest(
                                    s3ReqDecorator.decorate(builder.build()))
                            .build());
            upload.completionFuture().join();
        } catch (Exception e) {
            LOG.error("Exception in uploading {}", e.getMessage());
            throw new DataStoreException("Error in uploading metadata file", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void addMetadataRecord(File input, String name) throws DataStoreException {
        checkArgument(input != null, "input should not be null");
        checkArgument(!StringUtils.isEmpty(name), "name should not be empty");

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            FileUpload upload = tmx.uploadFile(uploadReq ->
                    uploadReq.source(input).
                            putObjectRequest(
                                    s3ReqDecorator.decorate(
                                            PutObjectRequest.builder().bucket(bucket).contentLength(input.length()).key(addMetaKeyPrefix(name)).build()))
                            .build());

            upload.completionFuture().join();
        } catch (Exception e) {
            LOG.error("Exception in uploading metadata file {}", input);
            throw new DataStoreException("Error in uploading metadata file", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        checkArgument(!StringUtils.isEmpty(name), "name should not be empty");

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            HeadObjectResponse meta = s3Client.headObject(s3ReqDecorator.decorate(HeadObjectRequest.builder().bucket(bucket).key(addMetaKeyPrefix(name)).build()));
            return new S3DataRecord(this, s3Client, bucket, new DataIdentifier(name),
                meta.lastModified().toEpochMilli(), meta.contentLength(), true, s3ReqDecorator);
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

        List<DataRecord> metadataList = new ArrayList<>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            final ListObjectsV2Iterable prevObjectListing = s3Client.listObjectsV2Paginator(listReq ->
                    listReq.bucket(bucket).prefix(addMetaKeyPrefix(prefix)).build());

            for (final S3Object s3ObjSumm : prevObjectListing.contents()) {
                metadataList.add(new S3DataRecord(this, s3Client, bucket,
                    new DataIdentifier(stripMetaKeyPrefix(s3ObjSumm.key())),
                    s3ObjSumm.lastModified().toEpochMilli(), s3ObjSumm.size(), true, s3ReqDecorator));
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
        checkArgument(!StringUtils.isEmpty(name), "name should not be empty");

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
            s3Client.deleteObject(delReq ->  delReq.bucket(bucket).key(addMetaKeyPrefix(name)).build());
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
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            ListObjectsV2Iterable metaList = s3Client.listObjectsV2Paginator(builder -> builder
                    .bucket(bucket)
                    .prefix(addMetaKeyPrefix(prefix))
                    .build());
            S3BackendHelper.deleteBucketObjects(bucket, properties, s3Client, metaList);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        final AbstractSharedBackend backend = this;
        return getAllRecords(
                s3Object -> new S3DataRecord(backend, s3Client, bucket, new DataIdentifier(getIdentifierName(s3Object.key())),
                        s3Object.lastModified().toEpochMilli(), s3Object.size(), s3ReqDecorator));
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            HeadObjectResponse object = s3Client.headObject(s3ReqDecorator.decorate(
                    HeadObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build()));
            S3DataRecord s3DataRecord = new S3DataRecord(this, s3Client, bucket, identifier,
                object.lastModified().toEpochMilli(), object.contentLength(), s3ReqDecorator);
            LOG.debug("Identifier [{}]'s getRecord = [{}] took [{}]ms.",
                identifier, s3DataRecord, (System.currentTimeMillis() - start));

            return s3DataRecord;
        } catch (AwsServiceException e) {
            int statusCode = e.awsErrorDetails().sdkHttpResponse().statusCode();
            if (statusCode == 404 || statusCode == 403) {
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
            return S3BackendHelper.objectExists(s3Client, bucket, addMetaKeyPrefix(name), s3ReqDecorator);
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

        return createPresignedURI(identifier, httpUploadURIExpirySeconds);
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

            Map<String, String> requestParams = new HashMap<>();
            requestParams.put("response-cache-control",
                    String.format("private, max-age=%d, immutable",
                            httpDownloadURIExpirySeconds)
            );

            String contentType = downloadOptions.getContentTypeHeader();
            if (! StringUtils.isEmpty(contentType)) {
                requestParams.put("response-content-type", contentType);
            }
            String contentDisposition =
                    downloadOptions.getContentDispositionHeader();

            if (! StringUtils.isEmpty(contentDisposition)) {
                requestParams.put("response-content-disposition",
                        contentDisposition);
            }

            uri = createPresignedURI(identifier,
                    SdkHttpMethod.GET,
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
                final CreateMultipartUploadResponse res = s3Client.createMultipartUpload(
                        s3ReqDecorator.decorate(
                                CreateMultipartUploadRequest.builder()
                                        .bucket(bucket)
                                        .key(blobId)
                                        .build()));
                uploadId = res.uploadId();

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

                Map<String, String> presignedURIRequestParams = new HashMap<>();
                for (long blockId = 1; blockId <= numParts; ++blockId) {
                    presignedURIRequestParams.put("partNumber", String.valueOf(blockId));
                    presignedURIRequestParams.put("uploadId", uploadId);
                    uploadPartURIs.add(createPresignedURI(newIdentifier,
                            SdkHttpMethod.PUT,
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

        if (StringUtils.isEmpty(uploadTokenStr)) {
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
                ListPartsResponse listing = s3Client.listParts(builder -> builder.bucket(bucket).key(key).uploadId(uploadId).build());
                final List<CompletedPart> completedParts = new ArrayList<>();
                long size = 0L;
                Instant lastModified = null;
                for (Part part : listing.parts()) {
                    completedParts.add(CompletedPart.builder().partNumber(part.partNumber()).eTag(part.eTag()).build());
                    size += part.size();
                    if (null == lastModified || part.lastModified().isAfter(lastModified)) {
                        lastModified = part.lastModified();
                    }
                }

                s3Client.completeMultipartUpload(s3ReqDecorator.decorate(
                        CompleteMultipartUploadRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(uploadId)
                                .multipartUpload(CompletedMultipartUpload.builder()
                                        .parts(completedParts)
                                        .build())
                                .build()));

                record = new S3DataRecord(
                        this,
                        s3Client,
                        bucket,
                        blobId,
                        lastModified.toEpochMilli(),
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

    private URI createPresignedURI(DataIdentifier identifier, int expirySeconds) {
        return createPresignedURI(identifier, SdkHttpMethod.PUT, expirySeconds, new HashMap<>());
    }

    private URI createPresignedURI(DataIdentifier identifier,
                                   SdkHttpMethod method,
                                   int expirySeconds,
                                   Map<String, String> reqParams) {
        final String key = getKeyName(identifier);

        // SSE-C is supported with presigned URLs when encryption headers are included in both
        // the signature calculation (handled by S3RequestDecorator) and the actual HTTP request

        try {
            final Date expiration = new Date();
            URL presignedURL = null;
            expiration.setTime(expiration.getTime() + expirySeconds * 1000L);

            // Add raw query parameters via override configuration
            AwsRequestOverrideConfiguration.Builder overrideBuilder = AwsRequestOverrideConfiguration.builder();

            for (Map.Entry<String, String> e : reqParams.entrySet()) {
                overrideBuilder.putRawQueryParameter(e.getKey(), e.getValue());
            }

            switch (method) {
                case GET:
                    presignedURL = s3PresignService.presignGetObject(builder -> builder
                            .signatureDuration(Duration.between(
                                    Instant.now(),           // current time
                                    expiration.toInstant()             // expiration time from your Date object
                            ))
                            .getObjectRequest(s3ReqDecorator.decorate(
                                    GetObjectRequest.builder()
                                            .bucket(bucket)
                                            .key(key)
                                            .overrideConfiguration(overrideBuilder.build())
                                            .build()))
                            .build()).url();
                    break;
                case PUT:

                    presignedURL = s3PresignService.presignPutObject(builder -> builder
                            .signatureDuration(Duration.between(
                                    Instant.now(),           // current time
                                    expiration.toInstant()             // expiration time from your Date object
                            ))
                            .putObjectRequest(s3ReqDecorator.decorate(
                                    PutObjectRequest.builder()
                                            .bucket(bucket)
                                            .key(key)
                                            .overrideConfiguration(overrideBuilder.build())
                                            .build()))
                            .build()).url();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported method: " + method);
            }


            URI uri = null;
            try {
                uri = presignedURL.toURI();
                LOG.debug("Presigned {} URI for key {}: {}", method.name(), key, uri);
            }
            catch (URISyntaxException e) {
                LOG.error("AWS request to create presigned S3 URI failed - could not convert '{}' to URI", presignedURL);
            }
            return uri;

        } catch (S3Exception e) {
            LOG.error("AWS request to create presigned S3 {} URI failed. " +
                            "Key: {}, Error: {}, HTTP Code: {}, AWS Error Code: {}, Request ID: {}",
                    method.name(), key,
                    e.getMessage(),
                    e.statusCode(),
                    e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "N/A",
                    e.requestId());

            return null;
        }
    }

    private <T> Iterator<T> getAllRecords(Function<S3Object, T> transformer) {

        final ListObjectsV2Iterable prevObjectListing = s3Client.listObjectsV2Paginator(builder -> {
            builder.bucket(bucket);
            if (properties.containsKey(S3Constants.MAX_KEYS)) {
                builder.maxKeys(Integer.valueOf(properties.getProperty(S3Constants.MAX_KEYS)));
            }
            builder.build();
        });

        return IteratorUtils.transform(IteratorUtils.filter(
                prevObjectListing.contents().iterator(),
                s3Object -> !s3Object.key().startsWith(META_KEY_PREFIX)), transformer);
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
        private final S3Client s3Client;
        private final long length;
        private final long lastModified;
        private final String bucket;
        private final boolean isMeta;
        private final S3RequestDecorator s3ReqDecorator;

        public S3DataRecord(AbstractSharedBackend backend, S3Client s3Client, String bucket,
                            DataIdentifier key, long lastModified, long length, final S3RequestDecorator s3ReqDecorator) {
            this(backend, s3Client, bucket, key, lastModified, length, false, s3ReqDecorator);
        }

        public S3DataRecord(AbstractSharedBackend backend, S3Client s3Client, String bucket,
                            DataIdentifier key, long lastModified, long length, boolean isMeta, final S3RequestDecorator s3ReqDecorator) {
            super(backend, key);
            this.s3Client = s3Client;
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
                return s3Client.getObject(s3ReqDecorator.decorate(GetObjectRequest.builder()
                                .bucket(bucket)
                                .key(id)
                                .build()),
                        ResponseTransformer.toInputStream());
            }
            else {
                // Don't worry about stream logging for metadata records
                if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                    // Log message, with exception so we can get a trace to see where the call came from
                    LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from S3 - identifier={}", id, new Exception());
                }
            }
            return s3Client.getObject(s3ReqDecorator.decorate(
                    GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(id)
                            .build()),
                    ResponseTransformer.toInputStream());
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
    private void renameKeys() {
        long startTime = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long count = 0;
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            List<ObjectIdentifier> deleteList = new ArrayList<>();
            List<String> keysToDelete = new ArrayList<>();

            ListObjectsV2Iterable prevObjectListing = s3Client.listObjectsV2Paginator(getObj -> getObj.bucket(bucket).build());
            int nThreads = Integer.parseInt(properties.getProperty("maxConnections"));
            ExecutorService executor = Executors.newFixedThreadPool(nThreads,
                    BasicThreadFactory.builder().namingPattern("s3-object-rename-worker-%d").build());
            boolean taskAdded = false;
            for (S3Object s3Obj : prevObjectListing.contents()) {
                executor.execute(new KeyRenameThread(s3Obj.key()));
                taskAdded = true;
                count++;
                // delete the object if it follows old key name format
                if( s3Obj.key().startsWith(KEY_PREFIX)) {
                    deleteList.add(ObjectIdentifier.builder().key(s3Obj.key()).build());
                    keysToDelete.add(s3Obj.key());
                }
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
            LOG.info("Renamed [{}] keys, time taken [{}]sec", count, ((System.currentTimeMillis() - startTime) / 1000));
            // Delete older keys.
            if (!deleteList.isEmpty()) {
                RemoteStorageMode mode = (RemoteStorageMode) properties.getOrDefault(S3Constants.MODE, RemoteStorageMode.S3);
                if (mode == RemoteStorageMode.S3) {
                    int batchSize = 500, startIndex = 0, size = deleteList.size();
                    int endIndex = Math.min(batchSize, size);
                    while (endIndex <= size) {
                        DeleteObjectsResponse dobjs = s3Client.deleteObjects(DeleteObjectsRequest.builder()
                                .bucket(bucket)
                                .delete(Delete.builder()
                                        .objects(Collections.unmodifiableList(
                                                deleteList.subList(startIndex, startIndex))).build())
                                        .build());
                        LOG.info("Records[{}] deleted in datastore from index [{}] to [{}]",
                                dobjs.deleted().size(), startIndex, (endIndex - 1));
                        if (endIndex == size) {
                            break;
                        } else {
                            startIndex = endIndex;
                            endIndex = Math.min((startIndex + batchSize), size);
                        }
                    }
                } else {
                    long keysDeleteStartTime = System.currentTimeMillis();
                    keysToDelete.forEach(key -> s3Client.deleteObject(delObj -> delObj.bucket(bucket).key(key).build()));
                    LOG.debug("Delete operation for rename keys from gcp took: {} seconds", ((System.currentTimeMillis() - keysDeleteStartTime) / 1000));
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

    @NotNull
    private AsyncRequestBody getRequestBody(final InputStream input, final ExecutorService executor,
                                            final PutObjectRequest.Builder builder) throws IOException {
        final AsyncRequestBody body;
        if (Objects.equals(RemoteStorageMode.S3, properties.get(S3Constants.MODE))) {
            body = AsyncRequestBody.fromInputStream(input, null, executor);
        } else {
            // for GCP we need to know the length in advance, else it won't work.
            final long length;
            if (input instanceof FileInputStream) {
                final FileInputStream fis = (FileInputStream) input;
                // if the file is modified after opening, the size may not reflect the latest changes
                length = fis.getChannel().size();
                body = AsyncRequestBody.fromInputStream(input, length, executor);
            } else if (input instanceof ByteArrayInputStream) {
                length = input.available();
                body = AsyncRequestBody.fromInputStream(input, length, executor);
            } else if (input.markSupported()) {
                // in case the inputStream supports mark & reset
                input.mark(Integer.MAX_VALUE);
                length = IOUtils.consume(input);
                input.reset();
                body = AsyncRequestBody.fromInputStream(input, length, executor);
            } else {
                // we have to read all the stream to get the actual length
                // last else block: store to temp file and re-read
                final File tempFile = File.createTempFile("inputstream-", ".tmp");
                tempFile.deleteOnExit(); // Clean up after JVM exits

                try (OutputStream out = new FileOutputStream(tempFile)) {
                    IOUtils.copy(input, out); // Copy all bytes to file
                }
                // Get length from file
                length = tempFile.length();
                // Re-create InputStream from temp file
                body = AsyncRequestBody.fromInputStream(new FileInputStream(tempFile), length, executor);
            }
            builder.contentLength(length);
        }
        return body;
    }

    /**
     * The class renames object key in S3 in a thread.
     */
    private class KeyRenameThread implements Runnable {

        private final String oldKey;

        public void run() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());
                String newS3Key = convertKey(oldKey);
                Copy copy = tmx.copy(copyReq ->
                        copyReq.copyObjectRequest(s3ReqDecorator.decorate(
                                CopyObjectRequest.builder()
                                        .sourceBucket(bucket)
                                        .sourceKey(oldKey)
                                        .destinationBucket(bucket)
                                        .destinationKey(newS3Key)
                                        .build()))
                                .build());
                try {
                    copy.completionFuture().join();
                    LOG.debug("[{}] renamed to [{}] ", oldKey, newS3Key);
                } catch (Exception ie) {
                    LOG.error(" Exception {} in renaming [{}] to [{}] ", ie, oldKey, newS3Key);
                }

            } finally {
                if (contextClassLoader != null) {
                    Thread.currentThread().setContextClassLoader(contextClassLoader);
                }
            }
        }

        public KeyRenameThread(String oldKey) {
            this.oldKey = oldKey;
        }
    }

    /**
     * Enum to indicate remote storage mode
     */
    enum RemoteStorageMode {
        S3,
        GCP
    }
}
