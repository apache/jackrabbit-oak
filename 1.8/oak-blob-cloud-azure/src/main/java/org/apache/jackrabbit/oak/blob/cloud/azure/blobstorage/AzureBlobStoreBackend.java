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

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.RequestOptions;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Queue;

import static java.lang.Thread.currentThread;

public class AzureBlobStoreBackend extends AbstractSharedBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackend.class);

    private static final String META_DIR_NAME = "META";
    private static final String META_KEY_PREFIX = META_DIR_NAME + "/";

    private static final String REF_KEY = "reference.key";

    private static final long BUFFERED_STREAM_THRESHHOLD = 1024 * 1024;

    private Properties properties;
    private String containerName;
    private String connectionString;
    private int concurrentRequestCount = 1;
    private RetryPolicy retryPolicy;
    private Integer requestTimeout;

    private byte[] secret;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    protected CloudBlobContainer getAzureContainer() throws DataStoreException {
        CloudBlobContainer container = Utils.getBlobContainer(connectionString, containerName);
        RequestOptions requestOptions = container.getServiceClient().getDefaultRequestOptions();
        if (retryPolicy != null) {
            requestOptions.setRetryPolicyFactory(retryPolicy);
        }
        if (requestTimeout != null) {
            requestOptions.setTimeoutIntervalInMs(requestTimeout);
        }
        return container;
    }

    @Override
    public void init() throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long start = System.currentTimeMillis();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            LOG.debug("Started backend initialization");

            if (null == properties) {
                try {
                    properties = Utils.readConfig(Utils.DEFAULT_CONFIG_FILE);
                }
                catch (IOException e) {
                    throw new DataStoreException("Unable to initialize Azure Data Store from " + Utils.DEFAULT_CONFIG_FILE, e);
                }
            }

            try {
                Utils.setProxyIfNeeded(properties);
                containerName = (String) properties.get(AzureConstants.AZURE_BLOB_CONTAINER_NAME);
                connectionString = Utils.getConnectionStringFromProperties(properties);
                concurrentRequestCount = PropertiesUtil.toInteger(properties.get(AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION), 1);
                LOG.info("Using concurrentRequestsPerOperation={}", concurrentRequestCount);
                retryPolicy = Utils.getRetryPolicy((String)properties.get(AzureConstants.AZURE_BLOB_MAX_REQUEST_RETRY));
                if (properties.getProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT) != null) {
                    requestTimeout = PropertiesUtil.toInteger(properties.getProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT), RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT);
                }

                CloudBlobContainer azureContainer = getAzureContainer();

                if (azureContainer.createIfNotExists()) {
                    LOG.info("New container created. containerName={}", containerName);
                } else {
                    LOG.info("Reusing existing container. containerName={}", containerName);
                }
                LOG.debug("Backend initialized. duration={}",
                          +(System.currentTimeMillis() - start));
            }
            catch (StorageException e) {
                throw new DataStoreException(e);
            }
        }
        finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (!blob.exists()) {
                throw new DataStoreException(String.format("Trying to read missing blob. identifier=%s", key));
            }

            InputStream is = blob.openInputStream();
            LOG.debug("Got input stream for blob. identifier={} duration={}", key, (System.currentTimeMillis() - start));
            return is;
        }
        catch (StorageException e) {
            LOG.info("Error reading blob. identifier=%s", key);
            throw new DataStoreException(String.format("Cannot read blob. identifier=%s", key), e);
        }
        catch (URISyntaxException e) {
            LOG.debug("Error reading blob. identifier=%s", key);
            throw new DataStoreException(String.format("Cannot read blob. identifier=%s", key), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        if (null == identifier) {
            throw new NullPointerException("identifier");
        }
        if (null == file) {
            throw new NullPointerException("file");
        }
        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            long len = file.length();
            LOG.debug("Blob write started. identifier={} length={}", key, len);
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (!blob.exists()) {
                BlobRequestOptions options = new BlobRequestOptions();
                options.setConcurrentRequestCount(concurrentRequestCount);
                boolean useBufferedStream = len < BUFFERED_STREAM_THRESHHOLD;
                final InputStream in = useBufferedStream  ? new BufferedInputStream(new FileInputStream(file)) : new FileInputStream(file);
                try {
                    blob.upload(in, len, null, options, null);
                    LOG.debug("Blob created. identifier={} length={} duration={} buffered={}", key, len, (System.currentTimeMillis() - start), useBufferedStream);
                } finally {
                    in.close();
                }
                return;
            }

            blob.downloadAttributes();
            if (blob.getProperties().getLength() != len) {
                throw new DataStoreException("Length Collision. identifier=" + key +
                                             " new length=" + len +
                                             " old length=" + blob.getProperties().getLength());
            }
            LOG.trace("Blob already exists. identifier={} lastModified={}", key, blob.getProperties().getLastModified().getTime());
            blob.startCopy(blob);
            //TODO: better way of updating lastModified (use custom metadata?)
            if (!waitForCopy(blob)) {
                throw new DataStoreException(
                    String.format("Cannot update lastModified for blob. identifier=%s status=%s",
                                  key, blob.getCopyState().getStatusDescription()));
            }
            LOG.debug("Blob updated. identifier={} lastModified={} duration={}", key,
                      blob.getProperties().getLastModified().getTime(), (System.currentTimeMillis() - start));
        }
        catch (StorageException e) {
            LOG.info("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot write blob. identifier=%s", key), e);
        }
        catch (URISyntaxException | IOException e) {
            LOG.debug("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot write blob. identifier=%s", key), e);
        } catch (InterruptedException e) {
            LOG.debug("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot copy blob. identifier=%s", key), e);
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
    public byte[] getOrCreateReferenceKey() throws DataStoreException {
        try {
            if (secret != null && secret.length != 0) {
                return secret;
            } else {
                byte[] key;
                // Try reading from the metadata folder if it exists
                key = readMetadataBytes(REF_KEY);
                if (key == null) {
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

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) {
            throw new NullPointerException("identifier");
        }
        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (blob.exists()) {
                blob.downloadAttributes();
                AzureBlobStoreDataRecord record = new AzureBlobStoreDataRecord(
                    this,
                    connectionString,
                    containerName,
                    new DataIdentifier(getIdentifierName(blob.getName())),
                    blob.getProperties().getLastModified().getTime(),
                    blob.getProperties().getLength());
                LOG.debug("Data record read for blob. identifier={} duration={} record={}",
                          key, (System.currentTimeMillis() - start), record);
                return record;
            } else {
                LOG.debug("Blob not found. identifier={} duration={}",
                          key, (System.currentTimeMillis() - start));
                throw new DataStoreException(String.format("Cannot find blob. identifier=%s", key));
            }
        }catch (StorageException e) {
            LOG.info("Error getting data record for blob. identifier={}", key, e);
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
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return new RecordsIterator<DataIdentifier>(
                new Function<AzureBlobInfo, DataIdentifier>() {
                    @Override
                    public DataIdentifier apply(AzureBlobInfo input) {
                        return new DataIdentifier(getIdentifierName(input.getName()));
                    }
                }
        );
    }



    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        final AbstractSharedBackend backend = this;
        return new RecordsIterator<DataRecord>(
                new Function<AzureBlobInfo, DataRecord>() {
                    @Override
                    public DataRecord apply(AzureBlobInfo input) {
                        return new AzureBlobStoreDataRecord(
                            backend,
                            connectionString,
                            containerName,
                            new DataIdentifier(getIdentifierName(input.getName())),
                            input.getLastModified(),
                            input.getLength());
                    }
                }
        );
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            boolean exists =getAzureContainer().getBlockBlobReference(key).exists();
            LOG.debug("Blob exists={} identifier={} duration={}", exists, key, (System.currentTimeMillis() - start));
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
    public void close() throws DataStoreException {
        LOG.info("AzureBlobBackend closed.");
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            boolean result = getAzureContainer().getBlockBlobReference(key).deleteIfExists();
            LOG.debug("Blob {}. identifier={} duration={}",
                    result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                    key, (System.currentTimeMillis() - start));
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
        if (null == input) {
            throw new NullPointerException("input");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name");
        }
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(input, name, -1L);
            LOG.debug("Metadata record added. metadataName={} duration={}", name, (System.currentTimeMillis() - start));
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(File input, String name) throws DataStoreException {
        if (null == input) {
            throw new NullPointerException("input");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name");
        }
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(new FileInputStream(input), name, input.length());
            LOG.debug("Metadata record added. metadataName={} duration={}", name, (System.currentTimeMillis() - start));
        }
        catch (FileNotFoundException e) {
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
            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            blob.upload(input, recordLength);
        }
        catch (StorageException e) {
            LOG.info("Error adding metadata record. metadataName={} length={}", name, recordLength, e);
            throw new DataStoreException(e);
        }
        catch (URISyntaxException |  IOException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long start = System.currentTimeMillis();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            if (!blob.exists()) {
                LOG.warn("Trying to read missing metadata. metadataName={}", name);
                return null;
            }
            blob.downloadAttributes();
            long lastModified = blob.getProperties().getLastModified().getTime();
            long length = blob.getProperties().getLength();
            AzureBlobStoreDataRecord record =  new AzureBlobStoreDataRecord(this,
                                                connectionString,
                                                containerName, new DataIdentifier(name),
                                                lastModified,
                                                length,
                                                true);
            LOG.debug("Metadata record read. metadataName={} duration={} record={}", name, (System.currentTimeMillis() - start), record);
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
        if (null == prefix) {
            throw new NullPointerException("prefix");
        }
        long start = System.currentTimeMillis();
        final List<DataRecord> records = Lists.newArrayList();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            for (ListBlobItem item : metaDir.listBlobs(prefix)) {
                if (item instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) item;
                    records.add(new AzureBlobStoreDataRecord(
                        this,
                        connectionString,
                        containerName,
                        new DataIdentifier(stripMetaKeyPrefix(blob.getName())),
                        blob.getProperties().getLastModified().getTime(),
                        blob.getProperties().getLength(),
                        true));
                }
            }
            LOG.debug("Metadata records read. recordsRead={} metadataFolder={} duration={}", records.size(), prefix, (System.currentTimeMillis() - start));
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
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(addMetaKeyPrefix(name));
            boolean result = blob.deleteIfExists();
            LOG.debug("Metadata record {}. metadataName={} duration={}",
                    result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                    name, (System.currentTimeMillis() - start));
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
        if (null == prefix) {
            throw new NullPointerException("prefix");
        }
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            int total = 0;
            for (ListBlobItem item : metaDir.listBlobs(prefix)) {
                if (item instanceof CloudBlob) {
                    if (((CloudBlob)item).deleteIfExists()) {
                        total++;
                    }
                }
            }
            LOG.debug("Metadata records deleted. recordsDeleted={} metadataFolder={} duration={}",
                    total, prefix, (System.currentTimeMillis() - start));

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


    /**
     * Get key from data identifier. Object is stored with key in ADS.
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

    private static String addMetaKeyPrefix(final String key) {
        return META_KEY_PREFIX + key;
    }

    private static String stripMetaKeyPrefix(String name) {
        if (name.startsWith(META_KEY_PREFIX)) {
            return name.substring(META_KEY_PREFIX.length());
        }
        return name;
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

        public static AzureBlobInfo fromCloudBlob(CloudBlob cloudBlob) {
            return new AzureBlobInfo(cloudBlob.getName(),
                                     cloudBlob.getProperties().getLastModified().getTime(),
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
            long start = System.currentTimeMillis();
            ClassLoader contextClassLoader = currentThread().getContextClassLoader();
            try {
                currentThread().setContextClassLoader(getClass().getClassLoader());

                CloudBlobContainer container = Utils.getBlobContainer(connectionString, containerName);
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
                          results.getLength(), containerName,  (System.currentTimeMillis() - start));
                return results.getLength() > 0;
            }
            catch (StorageException e) {
                LOG.info("Error listing blobs. containerName={}", containerName, e);
            }
            catch (DataStoreException e) {
                LOG.debug("Cannot list blobs. containerName={}", containerName, e);
            } finally {
                if (contextClassLoader != null) {
                    currentThread().setContextClassLoader(contextClassLoader);
                }
            }
            return false;
        }
    }

    static class AzureBlobStoreDataRecord extends AbstractDataRecord {
        final String connectionString;
        final String containerName;
        final long lastModified;
        final long length;
        final boolean isMeta;

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, String connectionString, String containerName,
                                        DataIdentifier key, long lastModified, long length) {
            this(backend, connectionString, containerName, key, lastModified, length, false);
        }

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, String connectionString, String containerName,
                                        DataIdentifier key, long lastModified, long length, boolean isMeta) {
            super(backend, key);
            this.connectionString = connectionString;
            this.containerName = containerName;
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
            CloudBlobContainer container = Utils.getBlobContainer(connectionString, containerName);
            if (isMeta) {
                id = addMetaKeyPrefix(getIdentifier().toString());
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
                   ", containerName='" + containerName + '\'' +
                   '}';
        }
    }
}
