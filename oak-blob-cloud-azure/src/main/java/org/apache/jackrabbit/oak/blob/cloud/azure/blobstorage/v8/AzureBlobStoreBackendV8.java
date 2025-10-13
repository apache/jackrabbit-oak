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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8;

import static java.lang.Thread.currentThread;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_BUFFERED_STREAM_THRESHOLD;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_LAST_MODIFIED_KEY;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BlOB_META_DIR_NAME;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_META_KEY_PREFIX;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_REF_KEY;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.guava.common.cache.CacheBuilder;
import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.SharedAccessBlobHeaders;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AbstractAzureBlobStoreBackend;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.util.Base64;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobStoreBackendV8 extends AbstractAzureBlobStoreBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackendV8.class);
    private static final Logger LOG_STREAMS_DOWNLOAD = LoggerFactory.getLogger("oak.datastore.download.streams");
    private static final Logger LOG_STREAMS_UPLOAD = LoggerFactory.getLogger("oak.datastore.upload.streams");

    private Properties properties;
    private AzureBlobContainerProviderV8 azureBlobContainerProvider;
    private int concurrentRequestCount = AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT;
    private RetryPolicy retryPolicy;
    private Integer requestTimeout;
    private int httpDownloadURIExpirySeconds = 0; // disabled by default
    private int httpUploadURIExpirySeconds = 0; // disabled by default
    private String uploadDomainOverride = null;
    private String downloadDomainOverride = null;
    private boolean createBlobContainer = true;
    private boolean presignedDownloadURIVerifyExists = true;
    private boolean enableSecondaryLocation = AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_DEFAULT;

    private Cache<String, URI> httpDownloadURICache;

    private byte[] secret;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    private final AtomicReference<CloudBlobContainer> azureContainerReference = new AtomicReference<>();

    public CloudBlobContainer getAzureContainer() throws DataStoreException {
        azureContainerReference.compareAndSet(null, azureBlobContainerProvider.getBlobContainer(getBlobRequestOptions()));
        return azureContainerReference.get();
    }

    @NotNull
    protected BlobRequestOptions getBlobRequestOptions() {
        BlobRequestOptions requestOptions = new BlobRequestOptions();
        if (null != retryPolicy) {
            requestOptions.setRetryPolicyFactory(retryPolicy);
        }
        if (null != requestTimeout) {
            requestOptions.setTimeoutIntervalInMs(requestTimeout);
        }
        requestOptions.setConcurrentRequestCount(concurrentRequestCount);
        if (enableSecondaryLocation) {
            requestOptions.setLocationMode(LocationMode.PRIMARY_THEN_SECONDARY);
        }
        return requestOptions;
    }

    @Override
    public void init() throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Stopwatch watch = Stopwatch.createStarted();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            LOG.debug("Started backend initialization");

            if (properties == null) {
                try {
                    properties = Utils.readConfig(UtilsV8.DEFAULT_CONFIG_FILE);
                }
                catch (IOException e) {
                    throw new DataStoreException("Unable to initialize Azure Data Store from " + UtilsV8.DEFAULT_CONFIG_FILE, e);
                }
            }

            try {
                UtilsV8.setProxyIfNeeded(properties);
                createBlobContainer = PropertiesUtil.toBoolean(
                    Strings.emptyToNull(properties.getProperty(AzureConstants.AZURE_CREATE_CONTAINER)), true);
                initAzureDSConfig();

                concurrentRequestCount = PropertiesUtil.toInteger(
                        properties.getProperty(AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION),
                        AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT);
                if (concurrentRequestCount < AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT) {
                    LOG.warn("Invalid setting [{}] for concurrentRequestsPerOperation (too low); resetting to {}",
                            concurrentRequestCount,
                             AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT);
                    concurrentRequestCount = AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT;
                } else if (concurrentRequestCount > AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT) {
                    LOG.warn("Invalid setting [{}] for concurrentRequestsPerOperation (too high); resetting to {}",
                            concurrentRequestCount,
                             AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT);
                    concurrentRequestCount = AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT;
                }
                LOG.info("Using concurrentRequestsPerOperation={}", concurrentRequestCount);

                retryPolicy = UtilsV8.getRetryPolicy(properties.getProperty(AzureConstants.AZURE_BLOB_MAX_REQUEST_RETRY));
                if (properties.getProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT) != null) {
                    requestTimeout = PropertiesUtil.toInteger(properties.getProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT), RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT);
                }
                presignedDownloadURIVerifyExists = PropertiesUtil.toBoolean(
                        Strings.emptyToNull(properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS)), true);

                enableSecondaryLocation = PropertiesUtil.toBoolean(
                        properties.getProperty(AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME),
                        AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_DEFAULT
                );

                CloudBlobContainer azureContainer = getAzureContainer();

                if (createBlobContainer && !azureContainer.exists()) {
                    azureContainer.create();
                    LOG.info("New container created. containerName={}", getContainerName());
                } else {
                    LOG.info("Reusing existing container. containerName={}", getContainerName());
                }
                LOG.debug("Backend initialized. duration={}", watch.elapsed(TimeUnit.MILLISECONDS));

                // settings pertaining to DataRecordAccessProvider functionality
                String putExpiry = properties.getProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS);
                if (null != putExpiry) {
                    this.setHttpUploadURIExpirySeconds(Integer.parseInt(putExpiry));
                }
                String getExpiry = properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS);
                if (null != getExpiry) {
                    this.setHttpDownloadURIExpirySeconds(Integer.parseInt(getExpiry));
                    String cacheMaxSize = properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE);
                    if (null != cacheMaxSize) {
                        this.setHttpDownloadURICacheSize(Integer.parseInt(cacheMaxSize));
                    }
                    else {
                        this.setHttpDownloadURICacheSize(0); // default
                    }
                }
                uploadDomainOverride = properties.getProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE, null);
                downloadDomainOverride = properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE, null);

                // Initialize reference key secret
                boolean createRefSecretOnInit = PropertiesUtil.toBoolean(
                        Strings.emptyToNull(properties.getProperty(AzureConstants.AZURE_REF_ON_INIT)), true);

                if (createRefSecretOnInit) {
                    getOrCreateReferenceKey();
                }
            }
            catch (StorageException e) {
                LOG.error("Error performing blob storage creation: {}", e.getMessage());
                throw new DataStoreException("Error performing backend initialization", e);
            }
        }
        finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    private void initAzureDSConfig() {
        AzureBlobContainerProviderV8.Builder builder = AzureBlobContainerProviderV8.Builder.builder(properties.getProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME))
                .withAzureConnectionString(properties.getProperty(AzureConstants.AZURE_CONNECTION_STRING, ""))
                .withAccountName(properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, ""))
                .withBlobEndpoint(properties.getProperty(AzureConstants.AZURE_BLOB_ENDPOINT, ""))
                .withSasToken(properties.getProperty(AzureConstants.AZURE_SAS, ""))
                .withAccountKey(properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, ""))
                .withTenantId(properties.getProperty(AzureConstants.AZURE_TENANT_ID, ""))
                .withClientId(properties.getProperty(AzureConstants.AZURE_CLIENT_ID, ""))
                .withClientSecret(properties.getProperty(AzureConstants.AZURE_CLIENT_SECRET, ""));
        azureBlobContainerProvider = builder.build();
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        Objects.requireNonNull(identifier, "identifier must not be null");

        String key = getKeyName(identifier);
        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (!blob.exists()) {
                throw new DataStoreException(String.format("Trying to read missing blob. identifier=%s", key));
            }

            InputStream is = blob.openInputStream();
            LOG.debug("Got input stream for blob. identifier={} duration={}", key, watch.elapsed(TimeUnit.MILLISECONDS));
            if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                // Log message, with exception so we can get a trace to see where the call came from
                LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from Azure Blob Storage - identifier={}", key, new Exception());
            }
            return is;
        }
        catch (StorageException e) {
            LOG.error("Error reading blob. identifier={}", key);
            throw new DataStoreException(String.format("Cannot read blob. identifier=%s", key), e);
        }
        catch (URISyntaxException e) {
            LOG.debug("Error reading blob. identifier={}", key);
            throw new DataStoreException(String.format("Cannot read blob. identifier=%s", key), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        Objects.requireNonNull(identifier, "identifier must not be null");
        Objects.requireNonNull(file, "file must not be null");

        String key = getKeyName(identifier);
        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            long len = file.length();
            LOG.debug("Blob write started. identifier={} length={}", key, len);
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (!blob.exists()) {
                addLastModified(blob);

                BlobRequestOptions options = new BlobRequestOptions();
                options.setConcurrentRequestCount(concurrentRequestCount);
                boolean useBufferedStream = len < AZURE_BLOB_BUFFERED_STREAM_THRESHOLD;
              try (InputStream in = useBufferedStream ? new BufferedInputStream(new FileInputStream(file)) : new FileInputStream(file)) {
                blob.upload(in, len, null, options, null);
                LOG.debug("Blob created. identifier={} length={} duration={} buffered={}", key, len, watch.elapsed(TimeUnit.MILLISECONDS), useBufferedStream);
                if (LOG_STREAMS_UPLOAD.isDebugEnabled()) {
                  // Log message, with exception so we can get a trace to see where the call came from
                  LOG_STREAMS_UPLOAD.debug("Binary uploaded to Azure Blob Storage - identifier={}", key, new Exception());
                }
              }
                return;
            }

            blob.downloadAttributes();
            if (blob.getProperties().getLength() != len) {
                throw new DataStoreException("Length Collision. identifier=" + key +
                        " new length=" + len +
                        " old length=" + blob.getProperties().getLength());
            }

            LOG.trace("Blob already exists. identifier={} lastModified={}", key, getLastModified(blob));
            addLastModified(blob);
            blob.uploadMetadata();

            LOG.debug("Blob updated. identifier={} lastModified={} duration={}", key,
                    getLastModified(blob), watch.elapsed(TimeUnit.MILLISECONDS));
        }
        catch (StorageException e) {
            LOG.info("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot write blob. identifier=%s", key), e);
        }
        catch (URISyntaxException | IOException e) {
            LOG.debug("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot write blob. identifier=%s", key), e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private static boolean waitForCopy(CloudBlob blob) throws StorageException, InterruptedException {
        boolean continueLoop = true;
        CopyStatus status = CopyStatus.PENDING;
        while (continueLoop) {
            blob.downloadAttributes();
            status = blob.getCopyState().getStatus();
            continueLoop = status == CopyStatus.PENDING;
            // Sleep if retry is needed
            if (continueLoop) {
                Thread.sleep(500);
            }
        }
        return status == CopyStatus.SUCCESS;
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        Objects.requireNonNull(identifier, "identifier must not be null");

        String key = getKeyName(identifier);
        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            blob.downloadAttributes();
            AzureBlobStoreDataRecord record = new AzureBlobStoreDataRecord(
                    this,
                    azureBlobContainerProvider,
                    new DataIdentifier(getIdentifierName(blob.getName())),
                    getLastModified(blob),
                    blob.getProperties().getLength());
            LOG.debug("Data record read for blob. identifier={} duration={} record={}",
                    key, watch.elapsed(TimeUnit.MILLISECONDS), record);
            return record;
        }
        catch (StorageException e) {
            if (404 == e.getHttpStatusCode()) {
                LOG.debug("Unable to get record for blob; blob does not exist. identifier={}", key);
            }
            else {
                LOG.info("Error getting data record for blob. identifier={}", key, e);
            }
            throw new DataStoreException(String.format("Cannot retrieve blob. identifier=%s", key), e);
        }
        catch (URISyntaxException e) {
            LOG.debug("Error getting data record for blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot retrieve blob. identifier=%s", key), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() {
        return new RecordsIterator<>(
                input -> new DataIdentifier(getIdentifierName(input.getName())));
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        final AbstractSharedBackend backend = this;
        return new RecordsIterator<>(
                input -> new AzureBlobStoreDataRecord(
                        backend,
                        azureBlobContainerProvider,
                        new DataIdentifier(getIdentifierName(input.getName())),
                        input.getLastModified(),
                        input.getLength())
        );
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        Stopwatch watch = Stopwatch.createStarted();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            boolean exists =getAzureContainer().getBlockBlobReference(key).exists();
            LOG.debug("Blob exists={} identifier={} duration={}", exists, key, watch.elapsed(TimeUnit.MILLISECONDS));
            return exists;
        }
        catch (Exception e) {
            throw new DataStoreException(e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void close() {
        azureBlobContainerProvider.close();
        LOG.info("AzureBlobBackend closed.");
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        Objects.requireNonNull(identifier, "identifier must not be null");

        String key = getKeyName(identifier);
        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            boolean result = getAzureContainer().getBlockBlobReference(key).deleteIfExists();
            LOG.debug("Blob {}. identifier={} duration={}",
                    result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                    key, watch.elapsed(TimeUnit.MILLISECONDS));
        }
        catch (StorageException e) {
            LOG.info("Error deleting blob. identifier={}", key, e);
            throw new DataStoreException(e);
        }
        catch (URISyntaxException e) {
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(InputStream input, String name) throws DataStoreException {
        Objects.requireNonNull(input, "input must not be null");
        Validate.checkArgument(StringUtils.isNotEmpty(name), "name should not be empty");

        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(input, name, -1L);
            LOG.debug("Metadata record added. metadataName={} duration={}", name, watch.elapsed(TimeUnit.MILLISECONDS));
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(File input, String name) throws DataStoreException {
        Objects.requireNonNull(input, "input must not be null");
        Validate.checkArgument(StringUtils.isNotEmpty(name), "name should not be empty");

        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(new FileInputStream(input), name, input.length());
            LOG.debug("Metadata record added. metadataName={} duration={}", name, watch.elapsed(TimeUnit.MILLISECONDS));
        }
        catch (FileNotFoundException e) {
            LOG.error("Error adding metadata record. metadataName={}", name, e);
            throw new DataStoreException(e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private void addMetadataRecordImpl(final InputStream input, String name, long recordLength) throws DataStoreException {
        try {
            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(AZURE_BlOB_META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            addLastModified(blob);
            blob.upload(input, recordLength);
        }
        catch (StorageException e) {
            LOG.error("Error adding metadata record. metadataName={} length={}", name, recordLength, e);
            throw new DataStoreException(e);
        }
        catch (URISyntaxException |  IOException e) {
            LOG.debug("Error adding metadata record. metadataName={} length={}", name, recordLength, e);
            throw new DataStoreException(e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Stopwatch watch = Stopwatch.createStarted();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(AZURE_BlOB_META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            if (!blob.exists()) {
                LOG.warn("Trying to read missing metadata. metadataName={}", name);
                return null;
            }
            blob.downloadAttributes();
            long lastModified = getLastModified(blob);
            long length = blob.getProperties().getLength();
            AzureBlobStoreDataRecord record = new AzureBlobStoreDataRecord(this,
                    azureBlobContainerProvider,
                    new DataIdentifier(name),
                    lastModified,
                    length,
                    true);
            LOG.debug("Metadata record read. metadataName={} duration={} record={}", name, watch.elapsed(TimeUnit.MILLISECONDS), record);
            return record;

        } catch (StorageException e) {
            LOG.info("Error reading metadata record. metadataName={}", name, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.debug("Error reading metadata record. metadataName={}", name, e);
            throw new RuntimeException(e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");

        Stopwatch watch = Stopwatch.createStarted();
        final List<DataRecord> records = Lists.newArrayList();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(AZURE_BlOB_META_DIR_NAME);
            for (ListBlobItem item : metaDir.listBlobs(prefix)) {
                if (item instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) item;
                    blob.downloadAttributes();
                    records.add(new AzureBlobStoreDataRecord(
                            this,
                            azureBlobContainerProvider,
                            new DataIdentifier(stripMetaKeyPrefix(blob.getName())),
                            getLastModified(blob),
                            blob.getProperties().getLength(),
                            true));
                }
            }
            LOG.debug("Metadata records read. recordsRead={} metadataFolder={} duration={}", records.size(), prefix, watch.elapsed(TimeUnit.MILLISECONDS));
        }
        catch (StorageException e) {
            LOG.info("Error reading all metadata records. metadataFolder={}", prefix, e);
        }
        catch (DataStoreException | URISyntaxException e) {
            LOG.debug("Error reading all metadata records. metadataFolder={}", prefix, e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return records;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(addMetaKeyPrefix(name));
            boolean result = blob.deleteIfExists();
            LOG.debug("Metadata record {}. metadataName={} duration={}",
                    result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                    name, watch.elapsed(TimeUnit.MILLISECONDS));
            return result;

        }
        catch (StorageException e) {
            LOG.info("Error deleting metadata record. metadataName={}", name, e);
        }
        catch (DataStoreException | URISyntaxException e) {
            LOG.debug("Error deleting metadata record. metadataName={}", name, e);
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");

        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(AZURE_BlOB_META_DIR_NAME);
            int total = 0;
            for (ListBlobItem item : metaDir.listBlobs(prefix)) {
                if (item instanceof CloudBlob) {
                    if (((CloudBlob)item).deleteIfExists()) {
                        total++;
                    }
                }
            }
            LOG.debug("Metadata records deleted. recordsDeleted={} metadataFolder={} duration={}",
                    total, prefix, watch.elapsed(TimeUnit.MILLISECONDS));

        }
        catch (StorageException e) {
            LOG.info("Error deleting all metadata records. metadataFolder={}", prefix, e);
        }
        catch (DataStoreException | URISyntaxException e) {
            LOG.debug("Error deleting all metadata records. metadataFolder={}", prefix, e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public boolean metadataRecordExists(String name) {
        Stopwatch watch = Stopwatch.createStarted();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(addMetaKeyPrefix(name));
            boolean exists = blob.exists();
            LOG.debug("Metadata record {} exists {}. duration={}", name, exists, watch.elapsed(TimeUnit.MILLISECONDS));
            return exists;
        }
        catch (DataStoreException | StorageException | URISyntaxException e) {
            LOG.debug("Error checking existence of metadata record = {}", name, e);
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return false;
    }

    /**
     * Get key from data identifier. Object is stored with key in ADS.
     */
    private static String getKeyName(DataIdentifier identifier) {
        String key = identifier.toString();
        return key.substring(0, 4) + UtilsV8.DASH + key.substring(4);
    }

    /**
     * Get data identifier from key.
     */
    private static String getIdentifierName(String key) {
        if (!key.contains(UtilsV8.DASH)) {
            return null;
        } else if (key.contains(AZURE_BLOB_META_KEY_PREFIX)) {
            return key;
        }
        return key.substring(0, 4) + key.substring(5);
    }

    private static String addMetaKeyPrefix(final String key) {
        return AZURE_BLOB_META_KEY_PREFIX + key;
    }

    private static String stripMetaKeyPrefix(String name) {
        if (name.startsWith(AZURE_BLOB_META_KEY_PREFIX)) {
            return name.substring(AZURE_BLOB_META_KEY_PREFIX.length());
        }
        return name;
    }

    private static void addLastModified(CloudBlockBlob blob) {
        blob.getMetadata().put(AZURE_BLOB_LAST_MODIFIED_KEY, String.valueOf(System.currentTimeMillis()));
    }

    private static long getLastModified(CloudBlob blob) {
        if (blob.getMetadata().containsKey(AZURE_BLOB_LAST_MODIFIED_KEY)) {
            return Long.parseLong(blob.getMetadata().get(AZURE_BLOB_LAST_MODIFIED_KEY));
        }
        return blob.getProperties().getLastModified().getTime();
    }

    protected void setHttpDownloadURIExpirySeconds(int seconds) {
        httpDownloadURIExpirySeconds = seconds;
    }

    protected void setHttpDownloadURICacheSize(int maxSize) {
        // max size 0 or smaller is used to turn off the cache
        if (maxSize > 0) {
            LOG.info("presigned GET URI cache enabled, maxSize = {} items, expiry = {} seconds", maxSize, httpDownloadURIExpirySeconds / 2);
            httpDownloadURICache = CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    .expireAfterWrite(httpDownloadURIExpirySeconds / 2, TimeUnit.SECONDS)
                    .build();
        } else {
            LOG.info("presigned GET URI cache disabled");
            httpDownloadURICache = null;
        }
    }

    @Override
    protected URI createHttpDownloadURI(@NotNull DataIdentifier identifier,
                              @NotNull DataRecordDownloadOptions downloadOptions) {
        URI uri = null;

        Objects.requireNonNull(identifier, "identifier must not be null");
        Objects.requireNonNull(downloadOptions, "downloadOptions must not be null");

        if (httpDownloadURIExpirySeconds > 0) {

            String domain = getDirectDownloadBlobStorageDomain(downloadOptions.isDomainOverrideIgnored());
            Objects.requireNonNull(domain, "Could not determine domain for direct download");

            String cacheKey = identifier
                    + domain
                    + Objects.toString(downloadOptions.getContentTypeHeader(), "")
                    + Objects.toString(downloadOptions.getContentDispositionHeader(), "");
            if (null != httpDownloadURICache) {
                uri = httpDownloadURICache.getIfPresent(cacheKey);
            }
            if (uri == null) {
                if (presignedDownloadURIVerifyExists) {
                    // Check if this identifier exists.  If not, we want to return null
                    // even if the identifier is in the download URI cache.
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

                String key = getKeyName(identifier);
                SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
                headers.setCacheControl(String.format("private, max-age=%d, immutable", httpDownloadURIExpirySeconds));

                String contentType = downloadOptions.getContentTypeHeader();
                if (!Strings.isNullOrEmpty(contentType)) {
                    headers.setContentType(contentType);
                }

                String contentDisposition =
                        downloadOptions.getContentDispositionHeader();
                if (!Strings.isNullOrEmpty(contentDisposition)) {
                    headers.setContentDisposition(contentDisposition);
                }

                uri = createPresignedURI(key,
                        EnumSet.of(SharedAccessBlobPermissions.READ),
                        httpDownloadURIExpirySeconds,
                        headers,
                        domain);
                if (uri != null && httpDownloadURICache != null) {
                    httpDownloadURICache.put(cacheKey, uri);
                }
            }
        }
        return uri;
    }

    protected void setHttpUploadURIExpirySeconds(int seconds) { httpUploadURIExpirySeconds = seconds; }

    private DataIdentifier generateSafeRandomIdentifier() {
        return new DataIdentifier(
                String.format("%s-%d",
                        UUID.randomUUID().toString(),
                        Instant.now().toEpochMilli()
                )
        );
    }

    protected DataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURIs, @NotNull final DataRecordUploadOptions options) {
        List<URI> uploadPartURIs = Lists.newArrayList();
        long minPartSize = AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
        long maxPartSize = AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE;

        Validate.checkArgument(maxUploadSizeInBytes > 0L, "maxUploadSizeInBytes must be > 0");
        Validate.checkArgument(maxNumberOfURIs > 0 || maxNumberOfURIs == -1, "maxNumberOfURIs must either be > 0 or -1");
        Validate.checkArgument(!(maxUploadSizeInBytes > AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE && maxNumberOfURIs == 1), "Cannot do single-put upload with file size %d - exceeds max single-put upload size of %d", maxUploadSizeInBytes, AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE);
        Validate.checkArgument(maxUploadSizeInBytes <= AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE, "Cannot do upload with file size %d - exceeds max upload size of %d", maxUploadSizeInBytes, AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE);

        DataIdentifier newIdentifier = generateSafeRandomIdentifier();
        String blobId = getKeyName(newIdentifier);
        String uploadId = null;

        if (httpUploadURIExpirySeconds > 0) {
            // Always do multi-part uploads for Azure, even for small binaries.
            //
            // This is because Azure requires a unique header, "x-ms-blob-type=BlockBlob", to be
            // set but only for single-put uploads, not multi-part.
            // This would require clients to know not only the type of service provider being used
            // but also the type of upload (single-put vs multi-part), which breaks abstraction.
            // Instead we can insist that clients always do multi-part uploads to Azure, even
            // if the multi-part upload consists of only one upload part.  This doesn't require
            // additional work on the part of the client since the "complete" request must always
            // be sent regardless, but it helps us avoid the client having to know what type
            // of provider is being used, or us having to instruct the client to use specific
            // types of headers, etc.

            // Azure doesn't use upload IDs like AWS does
            // Generate a fake one for compatibility - we use them to determine whether we are
            // doing multi-part or single-put upload
            uploadId = Base64.encode(UUID.randomUUID().toString());

            long numParts = 0L;
            if (maxNumberOfURIs > 0) {
                long requestedPartSize = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) maxNumberOfURIs));
                if (requestedPartSize <= maxPartSize) {
                    numParts = Math.min(
                            maxNumberOfURIs,
                            Math.min(
                                    (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) minPartSize)),
                                    AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS
                            )
                    );
                } else {
                    throw new IllegalArgumentException(
                            String.format("Cannot do multi-part upload with requested part size %d", requestedPartSize)
                    );
                }
            }
            else {
                long maximalNumParts = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE));
                numParts = Math.min(maximalNumParts, AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS);
            }

            String key = getKeyName(newIdentifier);
            String domain = getDirectUploadBlobStorageDomain(options.isDomainOverrideIgnored());
            Objects.requireNonNull(domain, "Could not determine domain for direct upload");

            EnumSet<SharedAccessBlobPermissions> perms = EnumSet.of(SharedAccessBlobPermissions.WRITE);
            Map<String, String> presignedURIRequestParams = Maps.newHashMap();
            // see https://docs.microsoft.com/en-us/rest/api/storageservices/put-block#uri-parameters
            presignedURIRequestParams.put("comp", "block");
            for (long blockId = 1; blockId <= numParts; ++blockId) {
                presignedURIRequestParams.put("blockid",
                        Base64.encode(String.format("%06d", blockId)));
                uploadPartURIs.add(
                        createPresignedURI(key,
                                perms,
                                httpUploadURIExpirySeconds,
                                presignedURIRequestParams,
                                domain)
                );
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

    protected DataRecord completeHttpUpload(@NotNull String uploadTokenStr)
            throws DataRecordUploadException, DataStoreException {

        Validate.checkArgument(StringUtils.isNotEmpty(uploadTokenStr), "uploadToken required");

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
        catch (DataStoreException e1) {
            // record doesn't exist - so this means we are safe to do the complete request
            try {
                if (uploadToken.getUploadId().isPresent()) {
                    CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
                    // An existing upload ID means this is a multi-part upload
                    List<BlockEntry> blocks = blob.downloadBlockList(
                            BlockListingFilter.UNCOMMITTED,
                            AccessCondition.generateEmptyCondition(),
                            null,
                            null);
                    addLastModified(blob);
                    blob.commitBlockList(blocks);
                    long size = 0L;
                    for (BlockEntry block : blocks) {
                        size += block.getSize();
                    }
                    record = new AzureBlobStoreDataRecord(
                            this,
                            azureBlobContainerProvider,
                            blobId,
                            getLastModified(blob),
                            size);
                }
                else {
                    // Something is wrong - upload ID missing from upload token
                    // but record doesn't exist already, so this is invalid
                    throw new DataRecordUploadException(
                            String.format("Unable to finalize direct write of binary %s - upload ID missing from upload token",
                                    blobId)
                    );
                }
            } catch (URISyntaxException | StorageException e2) {
                throw new DataRecordUploadException(
                        String.format("Unable to finalize direct write of binary %s", blobId),
                        e2
                );
            }
        }

        return record;
    }

    private String getDefaultBlobStorageDomain() {
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");
        if (Strings.isNullOrEmpty(accountName)) {
            LOG.warn("Can't generate presigned URI - Azure account name not found in properties");
            return null;
        }
        return String.format("%s.blob.core.windows.net", accountName);
    }

    private String getDirectDownloadBlobStorageDomain(boolean ignoreDomainOverride) {
        String domain = ignoreDomainOverride
                ? getDefaultBlobStorageDomain()
                : downloadDomainOverride;
        if (Strings.isNullOrEmpty(domain)) {
            domain = getDefaultBlobStorageDomain();
        }
        return domain;
    }

    private String getDirectUploadBlobStorageDomain(boolean ignoreDomainOverride) {
        String domain = ignoreDomainOverride
                ? getDefaultBlobStorageDomain()
                : uploadDomainOverride;
        if (Strings.isNullOrEmpty(domain)) {
            domain = getDefaultBlobStorageDomain();
        }
        return domain;
    }

    private URI createPresignedURI(String key,
                                   EnumSet<SharedAccessBlobPermissions> permissions,
                                   int expirySeconds,
                                   SharedAccessBlobHeaders optionalHeaders,
                                   String domain) {
        return createPresignedURI(key, permissions, expirySeconds, Maps.newHashMap(), optionalHeaders, domain);
    }

    private URI createPresignedURI(String key,
                                   EnumSet<SharedAccessBlobPermissions> permissions,
                                   int expirySeconds,
                                   Map<String, String> additionalQueryParams,
                                   String domain) {
        return createPresignedURI(key, permissions, expirySeconds, additionalQueryParams, null, domain);
    }

    private URI createPresignedURI(String key,
                                   EnumSet<SharedAccessBlobPermissions> permissions,
                                   int expirySeconds,
                                   Map<String, String> additionalQueryParams,
                                   SharedAccessBlobHeaders optionalHeaders,
                                   String domain) {
        if (Strings.isNullOrEmpty(domain)) {
            LOG.warn("Can't generate presigned URI - no Azure domain provided (is Azure account name configured?)");
            return null;
        }

        URI presignedURI = null;
        try {
            String sharedAccessSignature = azureBlobContainerProvider.generateSharedAccessSignature(getBlobRequestOptions(), key,
                    permissions, expirySeconds, optionalHeaders);

            // Shared access signature is returned encoded already.
            String uriString = String.format("https://%s/%s/%s?%s",
                    domain,
                    getContainerName(),
                    key,
                    sharedAccessSignature);

            if (! additionalQueryParams.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                for (Map.Entry<String, String> e : additionalQueryParams.entrySet()) {
                    builder.append("&");
                    builder.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8));
                    builder.append("=");
                    builder.append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
                }
                uriString += builder.toString();
            }

            presignedURI = new URI(uriString);
        }
        catch (DataStoreException e) {
            LOG.error("No connection to Azure Blob Storage", e);
        }
        catch (URISyntaxException | InvalidKeyException e) {
            LOG.error("Can't generate a presigned URI for key {}", key, e);
        }
        catch (StorageException e) {
            LOG.error("Azure request to create presigned Azure Blob Storage {} URI failed. " +
                            "Key: {}, Error: {}, HTTP Code: {}, Azure Error Code: {}",
                    permissions.contains(SharedAccessBlobPermissions.READ) ? "GET" :
                            (permissions.contains(SharedAccessBlobPermissions.WRITE) ? "PUT" : ""),
                    key,
                    e.getMessage(),
                    e.getHttpStatusCode(),
                    e.getErrorCode());
        }

        return presignedURI;
    }

    private static class AzureBlobInfo {
        private final String name;
        private final long lastModified;
        private final long length;

        public AzureBlobInfo(String name, long lastModified, long length) {
            this.name = name;
            this.lastModified = lastModified;
            this.length = length;
        }

        public String getName() {
            return name;
        }

        public long getLastModified() {
            return lastModified;
        }

        public long getLength() {
            return length;
        }

        public static AzureBlobInfo fromCloudBlob(CloudBlob cloudBlob) throws StorageException {
            cloudBlob.downloadAttributes();
            return new AzureBlobInfo(cloudBlob.getName(),
                    AzureBlobStoreBackendV8.getLastModified(cloudBlob),
                    cloudBlob.getProperties().getLength());
        }
    }

    private class RecordsIterator<T> extends AbstractIterator<T> {
        // Seems to be thread-safe (in 5.0.0)
        ResultContinuation resultContinuation;
        boolean firstCall = true;
        final Function<AzureBlobInfo, T> transformer;
        final Queue<AzureBlobInfo> items = Lists.newLinkedList();

        public RecordsIterator (Function<AzureBlobInfo, T> transformer) {
            this.transformer = transformer;
        }

        @Override
        protected T computeNext() {
            if (items.isEmpty()) {
                loadItems();
            }
            if (!items.isEmpty()) {
                return transformer.apply(items.remove());
            }
            return endOfData();
        }

        private boolean loadItems() {
            Stopwatch watch = Stopwatch.createStarted();
            ClassLoader contextClassLoader = currentThread().getContextClassLoader();
            try {
                currentThread().setContextClassLoader(getClass().getClassLoader());

                CloudBlobContainer container = azureBlobContainerProvider.getBlobContainer();
                if (!firstCall && (resultContinuation == null || !resultContinuation.hasContinuation())) {
                    LOG.trace("No more records in container. containerName={}", container);
                    return false;
                }
                firstCall = false;
                ResultSegment<ListBlobItem> results = container.listBlobsSegmented(null, false, EnumSet.noneOf(BlobListingDetails.class), null, resultContinuation, null, null);
                resultContinuation = results.getContinuationToken();
                for (ListBlobItem item : results.getResults()) {
                    if (item instanceof CloudBlob) {
                        items.add(AzureBlobInfo.fromCloudBlob((CloudBlob)item));
                    }
                }
                LOG.debug("Container records batch read. batchSize={} containerName={} duration={}",
                        results.getLength(), getContainerName(),  watch.elapsed(TimeUnit.MILLISECONDS));
                return results.getLength() > 0;
            }
            catch (StorageException e) {
                LOG.info("Error listing blobs. containerName={}", getContainerName(), e);
            }
            catch (DataStoreException e) {
                LOG.debug("Cannot list blobs. containerName={}", getContainerName(), e);
            } finally {
                if (contextClassLoader != null) {
                    currentThread().setContextClassLoader(contextClassLoader);
                }
            }
            return false;
        }
    }

    static class AzureBlobStoreDataRecord extends AbstractDataRecord {
        final AzureBlobContainerProviderV8 azureBlobContainerProvider;
        final long lastModified;
        final long length;
        final boolean isMeta;

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, AzureBlobContainerProviderV8 azureBlobContainerProvider,
                                        DataIdentifier key, long lastModified, long length) {
            this(backend, azureBlobContainerProvider, key, lastModified, length, false);
        }

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, AzureBlobContainerProviderV8 azureBlobContainerProvider,
                                        DataIdentifier key, long lastModified, long length, boolean isMeta) {
            super(backend, key);
            this.azureBlobContainerProvider = azureBlobContainerProvider;
            this.lastModified = lastModified;
            this.length = length;
            this.isMeta = isMeta;
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            String id = getKeyName(getIdentifier());
            CloudBlobContainer container = azureBlobContainerProvider.getBlobContainer();
            if (isMeta) {
                id = addMetaKeyPrefix(getIdentifier().toString());
            }
            else {
                // Don't worry about stream logging for metadata records
                if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                    // Log message, with exception so we can get a trace to see where the call came from
                    LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from Azure Blob Storage - identifier={} ", id, new Exception());
                }
            }
            try {
                return container.getBlockBlobReference(id).openInputStream();
            } catch (StorageException | URISyntaxException e) {
                throw new DataStoreException(e);
            }
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }

        @Override
        public String toString() {
            return "AzureBlobStoreDataRecord{" +
                    "identifier=" + getIdentifier() +
                    ", length=" + length +
                    ", lastModified=" + lastModified +
                    ", containerName='" + Optional.ofNullable(azureBlobContainerProvider).map(AzureBlobContainerProviderV8::getContainerName).orElse(null) + '\'' +
                    '}';
        }
    }

    private String getContainerName() {
        return Optional.ofNullable(this.azureBlobContainerProvider)
                .map(AzureBlobContainerProviderV8::getContainerName)
                .orElse(null);
    }

    @Override
    public byte[] getOrCreateReferenceKey() throws DataStoreException {
        try {
            if (secret != null && secret.length != 0) {
                return secret;
            } else {
                byte[] key;
                // Try reading from the metadata folder if it exists
                key = readMetadataBytes(AZURE_BLOB_REF_KEY);
                if (key == null) {
                    key = super.getOrCreateReferenceKey();
                    addMetadataRecord(new ByteArrayInputStream(key), AZURE_BLOB_REF_KEY);
                    key = readMetadataBytes(AZURE_BLOB_REF_KEY);
                }
                secret = key;
                return secret;
            }
        } catch (IOException e) {
            throw new DataStoreException("Unable to get or create key " + e);
        }
    }

    protected byte[] readMetadataBytes(String name) throws IOException, DataStoreException {
        DataRecord rec = getMetadataRecord(name);
        byte[] key = null;
        if (rec != null) {
            InputStream stream = null;
            try {
                stream = rec.getStream();
                return IOUtils.toByteArray(stream);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        }
        return key;
    }

}
