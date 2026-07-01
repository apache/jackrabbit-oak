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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.*;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.data.DataRecord;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.apache.jackrabbit.util.Base64;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12.AzureConstantsV12.*;
import static org.apache.jackrabbit.oak.commons.StringUtils.emptyToNull;


/**
 * Azure Blob Storage backend using the Azure SDK v12 (com.azure). Implements direct-upload (block-blob
 * staging + commit) and presigned GET URI generation. Counterpart to AzureBlobStoreBackend (v8, legacy SDK).
 * Selected at runtime by AzureDataStoreWrapper based on the blobstoreAzureV12Enabled flag.
 */
class AzureBlobStoreBackendV12 extends AbstractSharedBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackendV12.class);
    private static final Logger LOG_STREAMS_DOWNLOAD = LoggerFactory.getLogger("oak.datastore.download.streams");
    private static final Logger LOG_STREAMS_UPLOAD = LoggerFactory.getLogger("oak.datastore.upload.streams");

    private static final String ERR_ID_NULL = "identifier must not be null";

    private final AtomicReference<BlobContainerClient> azureContainerReference = new AtomicReference<>();

    private Properties properties;
    private AzureBlobContainerProviderV12 azureBlobContainerProvider;
    private int concurrentRequestCount = AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT;
    private RequestRetryOptions retryOptions;
    private Integer requestTimeout;
    private int httpDownloadURIExpirySeconds = 0; // disabled by default
    private int httpUploadURIExpirySeconds = 0; // disabled by default
    private String uploadDomainOverride = null;
    private String downloadDomainOverride = null;
    private boolean presignedDownloadURIVerifyExists = true;
    private Cache<String, URI> httpDownloadURICache;
    // HMAC-SHA1 key used to sign and verify upload tokens. Written once on cold start, then cached in memory.
    private byte[] secret;

    /**
     * Get key from data identifier. Object is stored with key in ADS.
     */
    private static String getKeyName(DataIdentifier identifier) {
        String key = identifier.toString();
        return key.substring(0, 4) + UtilsV12.DASH + key.substring(4);
    }

    /**
     * Get data identifier from key.
     */
    private static String getIdentifierName(String key) {
        if (key.startsWith(AZURE_BLOB_META_KEY_PREFIX)) {
            return null;
        }
        if (!key.contains(UtilsV12.DASH)) {
            return null;
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

    private static void updateLastModifiedMetadata(BlockBlobClient blockBlobClient) {
        blockBlobClient.setMetadata(Map.of(AZURE_BLOB_LAST_MODIFIED_KEY, String.valueOf(System.currentTimeMillis())));
    }

    private static long getLastModified(BlockBlobClient blockBlobClient) {
        return getLastModified(blockBlobClient.getProperties());
    }

    private static long getLastModified(BlobProperties props) {
        Map<String, String> metadata = props.getMetadata();
        if (metadata == null || !metadata.containsKey(AZURE_BLOB_LAST_MODIFIED_KEY)) {
            return props.getLastModified().toInstant().toEpochMilli();
        }
        return Long.parseLong(metadata.get(AZURE_BLOB_LAST_MODIFIED_KEY));
    }

    // Use BlobItem.getProperties() from the list response — no extra getProperties() HTTP call.
    // The custom lastModified metadata key is not available on BlobItem, so we fall back to the
    // Azure server LastModified (same fallback as getLastModified() for blobs without the key).
    private static long getLastModifiedFromBlobItem(BlobItem blobItem) {
        return blobItem.getProperties().getLastModified().toInstant().toEpochMilli();
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    // Lazy: retryOptions and azureBlobContainerProvider aren't set until initContainerConnection() runs.
    protected BlobContainerClient getAzureContainer() throws DataStoreException {
        BlobContainerClient existing = azureContainerReference.get();
        if (existing != null) {
            return existing;
        }
        // Synchronize so getBlobContainer() (which allocates a Netty event loop) is called
        // at most once — the previous non-synchronized compareAndSet could lose a race and
        // silently discard a fully initialised client including its event loop group.
        synchronized (this) {
            existing = azureContainerReference.get();
            if (existing == null) {
                existing = azureBlobContainerProvider.getBlobContainer();
                azureContainerReference.set(existing);
            }
            return existing;
        }
    }

    // Swaps Thread Class Context Loader to this bundle's classloader so Azure SDK's ServiceLoader-based SPI discovery works in OSGi.
    // RuntimeExceptions (including BlobStorageException) propagate as-is; other checked exceptions are wrapped.
    private <T> T withBundleContextClassLoader(AzureSDKCall<T> call) throws DataStoreException {
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            return call.execute();
        } catch (DataStoreException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new DataStoreException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(saved);
        }
    }

    private void withBundleContextClassLoaderVoid(AzureSDKCallVoid call) throws DataStoreException {
        withBundleContextClassLoader(() -> {
            call.execute();
            return null;
        });
    }

    // Not idempotent — calling twice reinitializes the container connection and re-reads the reference key.
    // OSGi activation calls this exactly once; tests that need a fresh state must construct a new instance.
    @Override
    public void init() throws DataStoreException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        withBundleContextClassLoaderVoid(() -> {
            LOG.debug("Started backend initialization");
            loadPropertiesIfAbsent();
            initAzureDSConfig();
            initContainerConnection();
            initPresignedURIConfig();
            initReferenceKey();
            LOG.debug("Backend initialized. duration={}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        });
    }

    private void loadPropertiesIfAbsent() throws DataStoreException {
        if (properties == null) {
            try {
                properties = UtilsV12.readConfig(UtilsV12.DEFAULT_CONFIG_FILE);
            } catch (IOException e) {
                throw new DataStoreException("Unable to initialize Azure Data Store from " + UtilsV12.DEFAULT_CONFIG_FILE, e);
            }
        }
    }

    private void initContainerConnection() throws DataStoreException {
        boolean createBlobContainer = PropertiesUtil.toBoolean(
                emptyToNull(properties.getProperty(AzureConstantsV12.AZURE_CREATE_CONTAINER)), true);

        concurrentRequestCount = PropertiesUtil.toInteger(
                properties.getProperty(AzureConstantsV12.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION),
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

        presignedDownloadURIVerifyExists = PropertiesUtil.toBoolean(
                emptyToNull(properties.getProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS)), true);

        BlobContainerClient azureContainer = getAzureContainer();

        try {
            if (createBlobContainer && Boolean.FALSE.equals(azureContainer.exists())) {
                azureContainer.create();
                LOG.info("New container created. containerName={}", getContainerName());
            } else {
                LOG.info("Reusing existing container. containerName={}", getContainerName());
            }
        } catch (BlobStorageException e) {
            LOG.error("Error setting up Azure Blob store backend: {}", e.getMessage());
            throw new DataStoreException(e);
        }
    }

    private void initPresignedURIConfig() {
        String putExpiry = properties.getProperty(AzureConstantsV12.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS);
        if (putExpiry != null) {
            this.setHttpUploadURIExpirySeconds(capToDelegationKeyLifetime(Integer.parseInt(putExpiry)));
        }
        String getExpiry = properties.getProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS);
        if (getExpiry != null) {
            this.setHttpDownloadURIExpirySeconds(capToDelegationKeyLifetime(Integer.parseInt(getExpiry)));
            String cacheMaxSize = properties.getProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE);
            if (cacheMaxSize != null) {
                this.setHttpDownloadURICacheSize(Integer.parseInt(cacheMaxSize));
            } else {
                this.setHttpDownloadURICacheSize(0);
            }
        }
        uploadDomainOverride = properties.getProperty(AzureConstantsV12.PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE, null);
        downloadDomainOverride = properties.getProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE, null);
    }

    /**
     * When presigned URIs are signed with a user delegation key (service-principal auth), the SAS
     * expiry can never exceed the key's lifetime — Azure caps that at 7 days
     * ({@link AzureBlobContainerProviderV12#DELEGATION_KEY_LIFETIME}). A configured expiry beyond
     * that would defeat the delegation-key cache (never hits) and the SAS would silently stop
     * working before its stated expiry. Cap it here and warn instead of failing silently later.
     */
    private int capToDelegationKeyLifetime(int configuredExpirySeconds) {
        long maxSeconds = AzureBlobContainerProviderV12.DELEGATION_KEY_LIFETIME.getSeconds();
        if (azureBlobContainerProvider.authenticateViaServicePrincipal() && configuredExpirySeconds > maxSeconds) {
            LOG.warn("Configured presigned URI expiry of {}s exceeds the {}s maximum lifetime of an Azure " +
                            "user delegation key; capping to {}s to avoid a SAS URI that silently stops working " +
                            "before its stated expiry.",
                    configuredExpirySeconds, maxSeconds, maxSeconds);
            return (int) maxSeconds;
        }
        return configuredExpirySeconds;
    }

    private void initReferenceKey() throws DataStoreException {
        // Set to false to defer key creation until the first upload — useful in read-only or cold-standby nodes
        // that should never write to blob storage during startup.
        boolean createRefSecretOnInit = PropertiesUtil.toBoolean(
                emptyToNull(properties.getProperty(AzureConstantsV12.AZURE_REF_ON_INIT)), true);
        if (createRefSecretOnInit) {
            getOrCreateReferenceKey();
        }
    }

    private void initAzureDSConfig() {
        if (properties.getProperty(AzureConstantsV12.AZURE_BLOB_REQUEST_TIMEOUT) != null) {
            requestTimeout = PropertiesUtil.toInteger(properties.getProperty(AzureConstantsV12.AZURE_BLOB_REQUEST_TIMEOUT), AZURE_BLOB_DEFAULT_REQUEST_TIMEOUT);
        }
        retryOptions = UtilsV12.getRetryOptions(properties.getProperty(AzureConstantsV12.AZURE_BLOB_MAX_REQUEST_RETRY), requestTimeout, computeSecondaryLocationEndpoint());

        azureBlobContainerProvider = AzureBlobContainerProviderV12.Builder
                .builder(properties.getProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME))
                .initializeWithProperties(properties)
                .withRetryOptions(retryOptions)
                .build();
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        Objects.requireNonNull(identifier, ERR_ID_NULL);

        String key = getKeyName(identifier);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return withBundleContextClassLoader(() -> {
                BlockBlobClient blob = getAzureContainer().getBlobClient(key).getBlockBlobClient();
                if (Boolean.FALSE.equals(blob.exists())) {
                    throw new DataStoreException("Trying to read missing blob. identifier=" + key);
                }
                InputStream is = blob.openInputStream();
                LOG.debug("Got input stream for blob. identifier={} duration={}", key, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                    // Log message, with exception, so we can get a trace to see where the call came from
                    LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from Azure Blob Storage - identifier={}", key, new Exception());
                }
                return is;
            });
        } catch (BlobStorageException e) {
            LOG.error("Error reading blob. identifier={}", key);
            throw new DataStoreException("Cannot read blob. identifier=" + key, e);
        }
    }

    private void uploadBlob(BlockBlobClient client, File file, long len, Stopwatch stopwatch, String key) throws IOException {
        // Files <= MAX_SINGLE_PUT_UPLOAD_SIZE are uploaded in a single PUT (no blocks needed).
        // Larger files use block upload with a fixed block size to bound memory usage.
        // Memory overhead = AZURE_BLOB_UPLOAD_BLOCK_SIZE × concurrentRequestCount.
        // Previously used min(len, MAX_MULTIPART) which could stage 4 GB blocks concurrently → OOM.
        // Reference: CSO Release 24893 (ASSETS-65164).
        long blockSize = len <= AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE
                ? len
                : AZURE_BLOB_UPLOAD_BLOCK_SIZE;
        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                .setBlockSizeLong(blockSize)
                .setMaxConcurrency(concurrentRequestCount)
                .setMaxSingleUploadSizeLong(AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE);
        BlobUploadFromFileOptions options = new BlobUploadFromFileOptions(file.getPath());
        options.setParallelTransferOptions(parallelTransferOptions);
        options.setMetadata(Map.of(AZURE_BLOB_LAST_MODIFIED_KEY, String.valueOf(System.currentTimeMillis())));
        try {
            BlobClient blobClient = client.getContainerClient().getBlobClient(key);
            Response<BlockBlobItem> blockBlob = blobClient.uploadFromFileWithResponse(options, null, null);
            LOG.debug("Upload status is {} for blob {}", blockBlob.getStatusCode(), key);
        } catch (UncheckedIOException ex) {
            LOG.debug("Failed to upload from file:{}}", ex.getMessage());
            throw new IOException("Failed to upload blob: " + key, ex);
        } catch (IllegalArgumentException ex) {
            // Azure SDK validation failure (e.g. invalid options) — surface as checked IOException
            // so write()'s catch block can wrap it as DataStoreException.
            throw new IOException("Invalid upload parameters for blob: " + key, ex);
        }
        LOG.debug("Blob created. identifier={} length={} duration={}", key, len, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        if (LOG_STREAMS_UPLOAD.isDebugEnabled()) {
            // Log message, with exception, so we can get a trace to see where the call came from
            LOG_STREAMS_UPLOAD.debug("Binary uploaded to Azure Blob Storage - identifier={}", key, new Exception());
        }
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        Objects.requireNonNull(identifier, ERR_ID_NULL);

        String key = getKeyName(identifier);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return withBundleContextClassLoader(() -> {
                BlockBlobClient blob = getAzureContainer().getBlobClient(key).getBlockBlobClient();
                BlobProperties props = blob.getProperties();
                AzureBlobStoreDataRecord dataRecord = new AzureBlobStoreDataRecord(
                        this,
                        azureBlobContainerProvider,
                        new DataIdentifier(getIdentifierName(blob.getBlobName())),
                        getLastModified(props),
                        props.getBlobSize());
                LOG.debug("Data record read for blob. identifier={} duration={} record={}",
                        key, stopwatch.elapsed(TimeUnit.MILLISECONDS), dataRecord);
                return dataRecord;
            });
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                LOG.debug("Unable to get record for blob; blob does not exist. identifier={}", key);
            } else {
                LOG.info("Error getting data record for blob. identifier={}", key, e);
            }
            throw new DataStoreException("Cannot retrieve blob. identifier=" + key, e);
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        // Preserve Azure SDK v12's lazy pagination — do not collect() to a List.
        return withBundleContextClassLoader(() ->
                getAzureContainer().listBlobs().stream()
                        .map(blobItem -> getIdentifierName(blobItem.getName()))
                        .filter(Objects::nonNull)
                        .map(DataIdentifier::new)
                        .iterator());
    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        // Preserve Azure SDK v12's lazy pagination — do not collect() to a List.
        return withBundleContextClassLoader(() ->
                getAzureContainer().listBlobs().stream()
                        .map(blobItem -> {
                            String identifierName = getIdentifierName(blobItem.getName());
                            if (identifierName == null) {
                                return null;
                            }
                            return (DataRecord) new AzureBlobStoreDataRecord(
                                    this,
                                    azureBlobContainerProvider,
                                    new DataIdentifier(identifierName),
                                    getLastModifiedFromBlobItem(blobItem),
                                    blobItem.getProperties().getContentLength());
                        })
                        .filter(Objects::nonNull)
                        .iterator());
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String key = getKeyName(identifier);
        return withBundleContextClassLoader(() -> {
            boolean exists = getAzureContainer().getBlobClient(key).getBlockBlobClient().exists();
            LOG.debug("Blob exists={} identifier={} duration={}", exists, key, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            return exists;
        });
    }

    @Override
    public void close() {
        azureContainerReference.set(null);
        if (azureBlobContainerProvider != null) {
            azureBlobContainerProvider.close();
        }
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        Objects.requireNonNull(identifier, ERR_ID_NULL);

        String key = getKeyName(identifier);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            withBundleContextClassLoaderVoid(() -> {
                boolean result = getAzureContainer().getBlobClient(key).getBlockBlobClient().deleteIfExists();
                LOG.debug("Blob {}. identifier={} duration={}",
                        result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                        key, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            });
        } catch (BlobStorageException e) {
            LOG.info("Error deleting blob. identifier={}", key, e);
            throw new DataStoreException(e);
        }
    }

    @Override
    public void addMetadataRecord(InputStream input, String name) throws DataStoreException {
        Objects.requireNonNull(input, "input must not be null");
        Validate.checkArgument(StringUtils.isNotEmpty(name), "name should not be empty");
        Stopwatch stopwatch = Stopwatch.createStarted();
        withBundleContextClassLoaderVoid(() -> {
            addMetadataRecordImpl(input, name, -1);
            LOG.debug("Metadata record added. metadataName={} duration={}", name, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        });
    }

    @Override
    public void addMetadataRecord(File inputFile, String name) throws DataStoreException {
        Objects.requireNonNull(inputFile, "input must not be null");
        Validate.checkArgument(StringUtils.isNotEmpty(name), "name should not be empty");
        Stopwatch stopwatch = Stopwatch.createStarted();
        withBundleContextClassLoaderVoid(() -> {
            try (InputStream input = new FileInputStream(inputFile)) {
                addMetadataRecordImpl(input, name, inputFile.length());
            }
            LOG.debug("Metadata record added. metadataName={} duration={}", name, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        });
    }

    private BlockBlobClient getMetaBlobClient(String name) throws DataStoreException {
        return getAzureContainer().getBlobClient(AzureConstantsV12.AZURE_BLOB_META_DIR_NAME + "/" + name).getBlockBlobClient();
    }

    private void addMetadataRecordImpl(final InputStream input, String name, long recordLength) throws DataStoreException {
        try {
            BlockBlobClient blockBlobClient = getMetaBlobClient(name);
            ParallelTransferOptions transferOptions = new ParallelTransferOptions()
                    .setBlockSizeLong(AZURE_BLOB_PARALLEL_UPLOAD_BLOCK_SIZE)
                    .setMaxConcurrency(AZURE_BLOB_PARALLEL_UPLOAD_MAX_CONCURRENCY);
            try (BufferedInputStream bufferedIn = new BufferedInputStream(input);
                 BlobOutputStream out = blockBlobClient.getBlobOutputStream(
                         transferOptions, null, null, null, null)) {
                bufferedIn.transferTo(out);
            }
            updateLastModifiedMetadata(blockBlobClient);
        } catch (BlobStorageException | IOException e) {
            LOG.info("Error adding metadata record. metadataName={} length={}", name, recordLength, e);
            throw new DataStoreException(e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return withBundleContextClassLoader(() -> {
                BlockBlobClient blockBlobClient = getMetaBlobClient(name);
                if (Boolean.FALSE.equals(blockBlobClient.exists())) {
                    LOG.warn("Trying to read missing metadata. metadataName={}", name);
                    return null;
                }
                BlobProperties metaProps = blockBlobClient.getProperties();
                long lastModified = getLastModified(metaProps);
                long length = metaProps.getBlobSize();
                AzureBlobStoreDataRecord dataRecord = new AzureBlobStoreDataRecord(this,
                        azureBlobContainerProvider,
                        new DataIdentifier(name),
                        lastModified,
                        length,
                        true);
                LOG.debug("Metadata record read. metadataName={} duration={} record={}", name, stopwatch.elapsed(TimeUnit.MILLISECONDS), dataRecord);
                return dataRecord;
            });
        } catch (BlobStorageException | DataStoreException e) {
            LOG.info("Error reading metadata record. metadataName={}", name, e);
            throw new IllegalStateException("Cannot read metadata record. metadataName=" + name, e);
        }
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return withBundleContextClassLoader(() -> {
                List<DataRecord> records = new ArrayList<>();
                ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
                listBlobsOptions.setPrefix(AzureConstantsV12.AZURE_BLOB_META_DIR_NAME + "/" + prefix);

                for (BlobItem blobItem : getAzureContainer().listBlobs(listBlobsOptions, null)) {
                    records.add(new AzureBlobStoreDataRecord(this,
                            azureBlobContainerProvider,
                            new DataIdentifier(stripMetaKeyPrefix(blobItem.getName())),
                            blobItem.getProperties().getLastModified().toInstant().toEpochMilli(),
                            blobItem.getProperties().getContentLength(),
                            true));
                }
                LOG.debug("Metadata records read. recordsRead={} metadataFolder={} duration={}", records.size(), prefix, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                return records;
            });
        } catch (BlobStorageException | DataStoreException e) {
            // Must not return empty — callers (GC) treat empty as "no records" and may delete all live blobs.
            throw new IllegalStateException("Failed to list metadata records for prefix: " + prefix, e);
        }
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return withBundleContextClassLoader(() -> {
                BlobClient blob = getAzureContainer().getBlobClient(addMetaKeyPrefix(name));
                boolean result = blob.deleteIfExists();
                LOG.debug("Metadata record {}. metadataName={} duration={}",
                        result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                        name, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                return result;
            });
        } catch (BlobStorageException | DataStoreException e) {
            LOG.info("Error deleting metadata record. metadataName={}", name, e);
        }
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            withBundleContextClassLoaderVoid(() -> {
                int total = 0;
                ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
                listBlobsOptions.setPrefix(AzureConstantsV12.AZURE_BLOB_META_DIR_NAME + "/" + prefix);

                for (BlobItem blobItem : getAzureContainer().listBlobs(listBlobsOptions, null)) {
                    BlobClient blobClient = getAzureContainer().getBlobClient(blobItem.getName());
                    if (blobClient.deleteIfExists()) {
                        total++;
                    }
                }
                LOG.debug("Metadata records deleted. recordsDeleted={} metadataFolder={} duration={}",
                        total, prefix, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            });
        } catch (BlobStorageException | DataStoreException e) {
            throw new IllegalStateException("Failed to delete metadata records for prefix: " + prefix, e);
        }
    }

    @Override
    public boolean metadataRecordExists(String name) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return withBundleContextClassLoader(() -> {
                BlobClient blob = getAzureContainer().getBlobClient(addMetaKeyPrefix(name));
                boolean exists = blob.exists();
                LOG.debug("Metadata record {} exists {}. duration={}", name, exists, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                return exists;
            });
        } catch (DataStoreException | BlobStorageException e) {
            LOG.info("Error checking existence of metadata record = {}", name, e);
        }
        return false;
    }

    protected void setHttpDownloadURIExpirySeconds(int seconds) {
        httpDownloadURIExpirySeconds = seconds;
    }

    // Package-private for test assertions on capped/parsed config values.
    int getHttpDownloadURIExpirySeconds() {
        return httpDownloadURIExpirySeconds;
    }

    protected void setHttpDownloadURICacheSize(int maxSize) {
        // max size 0 or smaller is used to turn off the cache
        if (maxSize > 0) {
            LOG.info("presigned GET URI cache enabled, maxSize = {} items, expiry = {} seconds", maxSize, httpDownloadURIExpirySeconds / 2);
            httpDownloadURICache = CacheBuilder.<String, URI>newBuilder()
                    .maximumSize(maxSize)
                    .expireAfterWrite(Duration.ofSeconds(httpDownloadURIExpirySeconds / 2))
                    .build();
        } else {
            LOG.info("presigned GET URI cache disabled");
            httpDownloadURICache = null;
        }
    }

    protected URI createHttpDownloadURI(@NotNull DataIdentifier identifier,
                                        @NotNull DataRecordDownloadOptions downloadOptions) {
        Objects.requireNonNull(identifier, ERR_ID_NULL);
        Objects.requireNonNull(downloadOptions, "downloadOptions must not be null");

        if (httpDownloadURIExpirySeconds <= 0) {
            return null;
        }

        String domain = getDirectDownloadBlobStorageDomain(downloadOptions.isDomainOverrideIgnored());
        Objects.requireNonNull(domain, "Could not determine domain for direct download");

        String cacheKey = identifier
                + domain
                + Objects.toString(downloadOptions.getContentTypeHeader(), "")
                + Objects.toString(downloadOptions.getContentDispositionHeader(), "");

        URI uri = (httpDownloadURICache != null) ? httpDownloadURICache.getIfPresent(cacheKey) : null;
        if (uri == null) {
            uri = buildPresignedDownloadURI(identifier, cacheKey, domain, downloadOptions);
        }
        return uri;
    }

    @Nullable
    private URI buildPresignedDownloadURI(DataIdentifier identifier, String cacheKey, String domain, DataRecordDownloadOptions downloadOptions) {
        if (presignedDownloadURIVerifyExists) {
            try {
                if (!exists(identifier)) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Cannot create download URI for nonexistent blob {}; returning null", getKeyName(identifier));
                    }
                    return null;
                }
            } catch (DataStoreException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Cannot create download URI for blob {} (caught DataStoreException); returning null", getKeyName(identifier), e);
                }
                return null;
            }
        }

        String key = getKeyName(identifier);
        BlobSasHeadersV12 headers = new BlobSasHeadersV12()
                .setCacheControl(String.format("private, max-age=%d, immutable", httpDownloadURIExpirySeconds))
                .setContentType(downloadOptions.getContentTypeHeader())
                .setContentDisposition(downloadOptions.getContentDispositionHeader());

        URI uri = createPresignedURI(key,
                new BlobSasPermission().setReadPermission(true),
                httpDownloadURIExpirySeconds,
                Map.of(),
                domain,
                headers);
        if (uri != null && httpDownloadURICache != null) {
            httpDownloadURICache.put(cacheKey, uri);
        }
        return uri;
    }

    protected void setHttpUploadURIExpirySeconds(int seconds) {
        httpUploadURIExpirySeconds = seconds;
    }

    // Package-private for test assertions on capped/parsed config values.
    int getHttpUploadURIExpirySeconds() {
        return httpUploadURIExpirySeconds;
    }

    private DataIdentifier generateSafeRandomIdentifier() {
        return new DataIdentifier(
                String.format("%s-%d",
                        UUID.randomUUID(),
                        Instant.now().toEpochMilli()
                )
        );
    }

    protected DataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURIs, @NotNull final DataRecordUploadOptions options) throws DataRecordUploadException {
        List<URI> uploadPartURIs = new ArrayList<>();
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

            long numParts;
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
            } else {
                long maximalNumParts = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE));
                numParts = Math.min(maximalNumParts, AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS);
            }

            String key = getKeyName(newIdentifier);
            String domain = getDirectUploadBlobStorageDomain(options.isDomainOverrideIgnored());
            Objects.requireNonNull(domain, "Could not determine domain for direct upload");

            BlobSasPermission perms = new BlobSasPermission()
                    .setWritePermission(true);
            Map<String, String> presignedURIRequestParams = new HashMap<>();
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
                byte[] refKey = getOrCreateReferenceKey();
                String uploadToken = new DataRecordUploadToken(blobId, uploadId).getEncodedToken(refKey);
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
                throw new DataRecordUploadException("Unable to obtain data store key", e);
            }
        }

        return null;
    }

    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        Objects.requireNonNull(identifier, ERR_ID_NULL);
        Objects.requireNonNull(file, "file must not be null");

        String key = getKeyName(identifier);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            withBundleContextClassLoaderVoid(() -> {
                long len = file.length();
                LOG.debug("Blob write started. identifier={} length={}", key, len);
                BlockBlobClient blob = getAzureContainer().getBlobClient(key).getBlockBlobClient();
                if (Boolean.FALSE.equals(blob.exists())) {
                    uploadBlob(blob, file, len, stopwatch, key);
                    return;
                }

                BlobProperties existingProps;
                try {
                    existingProps = blob.getProperties();
                } catch (BlobStorageException e) {
                    if (e.getStatusCode() == 404) {
                        // deleted between exists() and getProperties() — re-upload
                        uploadBlob(blob, file, len, stopwatch, key);
                        return;
                    }
                    throw e;
                }

                if (existingProps.getBlobSize() != len) {
                    throw new DataStoreException("Length Collision. identifier=" + key +
                            " new length=" + len +
                            " old length=" + existingProps.getBlobSize());
                }

                updateLastModifiedMetadata(blob);
                long lm = getLastModified(blob);

                LOG.trace("Blob already exists. identifier={} lastModified={}", key, lm);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Blob updated. identifier={} lastModified={} duration={}", key,
                            lm, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                }
            });
        } catch (BlobStorageException e) {
            throw new DataStoreException("Cannot write blob. identifier=" + key, e);
        } catch (DataStoreException e) {
            throw new DataStoreException("Cannot write blob. identifier=" + key, e);
        }
    }

    private Long commitBlocksAndGetSize(BlockBlobClient client) throws DataStoreException {
        List<Block> uncommittedBlocks = client.listBlocks(BlockListType.UNCOMMITTED).getUncommittedBlocks();
        if (uncommittedBlocks.isEmpty()) {
            // A concurrent completeDataRecordUpload already committed these blocks.
            // Calling commitBlockList([]) here would truncate the blob to 0 bytes.
            List<Block> committedBlocks = client.listBlocks(BlockListType.COMMITTED).getCommittedBlocks();
            long size = committedBlocks.stream().mapToLong(Block::getSizeLong).sum();
            if (committedBlocks.isEmpty()) {
                throw new DataStoreException("No committed or uncommitted blocks found — upload may not have completed");
            }
            return size;
        }
        // Include lastModified in the same commit RPC so the blob is never committed without it.
        // A separate setMetadata call after commit would leave a window where transient failure
        // produces a committed blob with no lastModified key, causing premature GC.
        Map<String, String> metadata = new HashMap<>();
        metadata.put(AZURE_BLOB_LAST_MODIFIED_KEY, String.valueOf(System.currentTimeMillis()));
        BlockBlobCommitBlockListOptions options = new BlockBlobCommitBlockListOptions(
                uncommittedBlocks.stream().map(Block::getName).toList())
                .setMetadata(metadata);
        client.commitBlockListWithResponse(options, null, Context.NONE);
        return uncommittedBlocks.stream().mapToLong(Block::getSizeLong).sum();
    }

    protected DataRecord completeHttpUpload(@NotNull String uploadTokenStr)
            throws DataRecordUploadException, DataStoreException {

        Validate.checkArgument(StringUtils.isNotEmpty(uploadTokenStr), "uploadToken required");

        DataRecordUploadToken uploadToken = DataRecordUploadToken.fromEncodedToken(uploadTokenStr, getOrCreateReferenceKey());
        String key = uploadToken.getBlobId();
        DataIdentifier blobId = new DataIdentifier(getIdentifierName(key));

        DataRecord dataRecord = null;
        try {
            dataRecord = getRecord(blobId);
            // If this succeeds this means either it was a "single put" upload
            // (we don't need to do anything in this case - blob is already uploaded)
            // or it was completed before with the same token.
        } catch (DataStoreException e1) {
            // Only treat as "record not found" when the cause is a 404 from Azure.
            // Transient errors (auth, network, throttle) must propagate, not silently
            // trigger a commit that may overwrite or corrupt an in-flight upload.
            Throwable cause = e1.getCause();
            if (!(cause instanceof BlobStorageException bse) || bse.getStatusCode() != 404) {
                throw e1;
            }
            // dataRecord doesn't exist - so this means we are safe to do the complete request
            try {
                if (uploadToken.getUploadId().isPresent()) {
                    BlockBlobClient blockBlobClient = getAzureContainer().getBlobClient(key).getBlockBlobClient();
                    long size = commitBlocksAndGetSize(blockBlobClient);
                    dataRecord = new AzureBlobStoreDataRecord(
                            this,
                            azureBlobContainerProvider,
                            blobId,
                            getLastModified(blockBlobClient),
                            size);
                } else {
                    // Something is wrong - upload ID missing from upload token
                    // but dataRecord doesn't exist already, so this is invalid
                    throw new DataRecordUploadException(
                            String.format("Unable to finalize direct write of binary %s - upload ID missing from upload token",
                                    blobId)
                    );
                }
            } catch (BlobStorageException e2) {
                throw new DataRecordUploadException(
                        String.format("Unable to finalize direct write of binary %s", blobId),
                        e2
                );
            }
        }

        return dataRecord;
    }

    String getDefaultBlobStorageDomain() {
        String customEndpoint = properties.getProperty(AzureConstantsV12.AZURE_BLOB_ENDPOINT);
        if (StringUtils.isNotBlank(customEndpoint)) {
            try {
                return new URI(customEndpoint).getHost();
            } catch (URISyntaxException e) {
                LOG.warn("Invalid blobEndpoint URI: {}, falling back to default", customEndpoint, e);
            }
        }
        String accountName = properties.getProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, "");
        if (StringUtils.isEmpty(accountName)) {
            LOG.warn("Can't generate presigned URI - Azure account name not found in properties");
            return null;
        }
        return String.format("%s.blob.core.windows.net", accountName);
    }

    private String getBlobStorageDomain(boolean ignoreDomainOverride, String domainOverride) {
        String domain = ignoreDomainOverride ? getDefaultBlobStorageDomain() : domainOverride;
        if (StringUtils.isEmpty(domain)) {
            domain = getDefaultBlobStorageDomain();
        }
        return domain;
    }

    private String getDirectDownloadBlobStorageDomain(boolean ignoreDomainOverride) {
        return getBlobStorageDomain(ignoreDomainOverride, downloadDomainOverride);
    }

    private String getDirectUploadBlobStorageDomain(boolean ignoreDomainOverride) {
        return getBlobStorageDomain(ignoreDomainOverride, uploadDomainOverride);
    }

    private URI createPresignedURI(String key,
                                   BlobSasPermission blobSasPermissions,
                                   int expirySeconds,
                                   Map<String, String> additionalQueryParams,
                                   String domain) {
        return createPresignedURI(key, blobSasPermissions, expirySeconds, additionalQueryParams, domain, null);
    }

    private URI createPresignedURI(String key,
                                   BlobSasPermission blobSasPermissions,
                                   int expirySeconds,
                                   Map<String, String> additionalQueryParams,
                                   String domain,
                                   BlobSasHeadersV12 optionalHeaders) {
        if (Objects.toString(domain, "").isEmpty()) {
            LOG.warn("Can't generate presigned URI - no Azure domain provided (is Azure account name configured?)");
            return null;
        }

        URI presignedURI = null;
        try {
            String sharedAccessSignature = azureBlobContainerProvider.generateSharedAccessSignature(retryOptions, key,
                    blobSasPermissions, expirySeconds, properties, optionalHeaders);

            // Shared access signature is returned encoded already.
            String uriString = String.format("https://%s/%s/%s?%s",
                    domain,
                    getContainerName(),
                    key,
                    sharedAccessSignature);

            if (!additionalQueryParams.isEmpty()) {
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
        } catch (DataStoreException e) {
            LOG.error("No connection to Azure Blob Storage", e);
        } catch (URISyntaxException | InvalidKeyException e) {
            LOG.error("Can't generate a presigned URI for key {}", key, e);
        } catch (BlobStorageException e) {
            String operation;
            if (blobSasPermissions.hasReadPermission()) {
                operation = "GET";
            } else if (blobSasPermissions.hasWritePermission()) {
                operation = "PUT";
            } else {
                operation = "";
            }
            LOG.error("Azure request to create presigned Azure Blob Storage {} URI failed. " +
                            "Key: {}, Error: {}, HTTP Code: {}, Azure Error Code: {}",
                    operation,
                    key,
                    e.getMessage(),
                    e.getStatusCode(),
                    e.getErrorCode());
        }

        return presignedURI;
    }

    // Package-private so the inner AzureBlobStoreDataRecord can call it with TCCL set correctly.
    InputStream openBlobInputStream(BlobContainerClient container, String blobKey) throws DataStoreException {
        return withBundleContextClassLoader(() -> container.getBlobClient(blobKey).openInputStream());
    }

    private String getContainerName() {
        return Optional.ofNullable(this.azureBlobContainerProvider)
                .map(AzureBlobContainerProviderV12::getContainerName)
                .orElse(null);
    }

    // synchronized: two concurrent cold-start calls must not each write a different key —
    // the second key would invalidate all upload tokens signed with the first.
    @Override
    public synchronized byte[] getOrCreateReferenceKey() throws DataStoreException {
        try {
            if (secret != null && secret.length != 0) {
                return secret;
            } else {
                byte[] key;
                // Read from Azure first: another cluster node may have already written the shared secret.
                // All nodes must use the same HMAC key so that upload tokens are valid cluster-wide.
                key = readMetadataBytes(AZURE_BLOB_REF_KEY);
                // readMetadataBytes returns an empty array for a missing record; a subclass override
                // (e.g. in tests) may still return null, so guard both.
                if (key == null || key.length == 0) {
                    key = super.getOrCreateReferenceKey();
                    addMetadataRecord(new ByteArrayInputStream(key), AZURE_BLOB_REF_KEY);
                }
                secret = key;
                return secret;
            }
        } catch (IOException e) {
            throw new DataStoreException("Unable to get or create key", e);
        }
    }

    protected byte[] readMetadataBytes(String name) throws IOException, DataStoreException {
        DataRecord rec = getMetadataRecord(name);
        if (rec == null) {
            return new byte[0];
        }
        try (InputStream stream = rec.getStream()) {
            return IOUtils.toByteArray(stream);
        }
    }

    private String computeSecondaryLocationEndpoint() {
        String accountName = properties.getProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, "");

        boolean enableSecondaryLocation = PropertiesUtil.toBoolean(properties.getProperty(AzureConstantsV12.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME),
                AzureConstantsV12.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_DEFAULT);

        if (enableSecondaryLocation) {
            return String.format("https://%s-secondary.blob.core.windows.net", accountName);
        }

        return null;
    }

    /**
     * This interface together with {@link #withBundleContextClassLoader(AzureSDKCall)} enables calls to AzureSDK within the Class Loader of the current bundle
     * @param <T>
     */
    @FunctionalInterface
    private interface AzureSDKCall<T> {
        T execute() throws DataStoreException, IOException;
    }

    /**
     * Same as {@link AzureSDKCall} but without return value
     * @see AzureSDKCall
     */
    @FunctionalInterface
    private interface AzureSDKCallVoid {
        void execute() throws DataStoreException, IOException;
    }

    static class AzureBlobStoreDataRecord extends AbstractDataRecord {
        final AzureBlobContainerProviderV12 azureBlobContainerProvider;
        final long lastModified;
        final long length;
        final boolean isMeta; // true for metadata blobs (stored under AZURE_BLOB_META_DIR_NAME/); affects key construction in getStream()

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, AzureBlobContainerProviderV12 azureBlobContainerProvider,
                                        DataIdentifier key, long lastModified, long length) {
            this(backend, azureBlobContainerProvider, key, lastModified, length, false);
        }

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, AzureBlobContainerProviderV12 azureBlobContainerProvider,
                                        DataIdentifier key, long lastModified, long length, boolean isMeta) {
            super(backend, key);
            this.azureBlobContainerProvider = azureBlobContainerProvider;
            this.lastModified = lastModified;
            this.length = length;
            this.isMeta = isMeta;
        }

        @Override
        public long getLength() throws DataStoreException {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            String id = getKeyName(getIdentifier());
            // Use the backend's cached container so retry and proxy options are applied.
            BlobContainerClient container = ((AzureBlobStoreBackendV12) backend).getAzureContainer();
            if (isMeta) {
                id = addMetaKeyPrefix(getIdentifier().toString());
            } else {
                // Don't worry about stream logging for metadata records
                if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                    // Log message, with exception, so we can get a trace to see where the call came from
                    LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from Azure Blob Storage - identifier={} ", id, new Exception());
                }
            }
            return ((AzureBlobStoreBackendV12) backend).openBlobInputStream(container, id);
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public String toString() {
            return "AzureBlobStoreDataRecord{" +
                    "identifier=" + getIdentifier() +
                    ", length=" + length +
                    ", lastModified=" + lastModified +
                    ", containerName='" + Optional.ofNullable(azureBlobContainerProvider).map(AzureBlobContainerProviderV12::getContainerName).orElse(null) + '\'' +
                    '}';
        }
    }
}
