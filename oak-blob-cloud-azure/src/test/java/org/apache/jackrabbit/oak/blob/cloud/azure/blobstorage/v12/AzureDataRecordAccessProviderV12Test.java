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

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.data.DataRecord;
import org.apache.jackrabbit.oak.spi.blob.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.collections.MapUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.AbstractDataRecordAccessProviderTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests direct access (presigned URL) functionality for Azure V12 backend.
 *
 * <p>Requires real Azure credentials because presigned download/upload URIs
 * use HTTPS which Azurite does not support. Provide credentials via
 * {@code -Dazure.config=<path>} or place {@code azure.properties} in the
 * user home directory.</p>
 */
public class AzureDataRecordAccessProviderV12Test extends AbstractDataRecordAccessProviderTest {

    private static final Logger log = LoggerFactory.getLogger(AzureDataRecordAccessProviderV12Test.class);

    private static final String DEFAULT_CONFIG_PATH = "./src/test/resources/azure.properties";
    private static final String DEFAULT_PROPERTY_FILE = "azure.properties";
    private static final String SYS_PROP_NAME = "azure.config";

    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static AzureDataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
        dataStore = setupDirectAccessDataStore(homeDir, expirySeconds, expirySeconds);
    }

    private static AzureDataStore createDataStore(@NotNull Properties properties) throws Exception {
        return setupDirectAccessDataStore(homeDir, expirySeconds, expirySeconds, properties);
    }

    @Override
    protected ConfigurableDataRecordAccessProvider getDataStore() {
        return dataStore;
    }

    @Override
    protected ConfigurableDataRecordAccessProvider getDataStore(@NotNull Properties overrideProperties) throws Exception {
        return createDataStore(getDirectAccessDataStoreProperties(overrideProperties));
    }

    @Override
    protected DataRecord doGetRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        return ds.getRecord(identifier);
    }

    @Override
    protected DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException {
        return ((AzureDataStore)ds).addRecord(in, new BlobOptions().setUpload(BlobOptions.UploadType.SYNCHRONOUS));
    }

    @Override
    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((AzureDataStore)ds).deleteRecord(identifier);
    }

    @Override
    protected long getProviderMinPartSize() {
        return Math.max(0L, AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE);
    }

    @Override
    protected long getProviderMaxPartSize() {
        return AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE;
    }

    @Override
    protected long getProviderMaxSinglePutSize() { return AzureConstantsV12.AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE; }

    @Override
    protected long getProviderMaxBinaryUploadSize() { return AzureConstantsV12.AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE; }

    @Override
    protected boolean isSinglePutURI(URI uri) {
        // Since strictly speaking we don't support single-put for Azure due to the odd
        // required header for single-put uploads, we don't care and just always return true
        // here to avoid failing tests for this.
        return true;
    }

    @Override
    protected HttpsURLConnection getHttpsConnection(long length, URI uri) throws IOException {
        return createHttpsConnection(length, uri);
    }

    @Test
    public void testInitDirectUploadURIHonorsExpiryTime() throws DataRecordUploadException {
        ConfigurableDataRecordAccessProvider ds = getDataStore();
        try {
            Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            ds.setDirectUploadURIExpirySeconds(60);
            DataRecordUpload uploadContext = ds.initiateDataRecordUpload(ONE_MB, 1);
            assertNotNull("The upload context should not be null", uploadContext);
            URI uploadURI = uploadContext.getUploadURIs().iterator().next();
            Map<String, String> params = parseQueryString(uploadURI);
            String expiryDateStr = params.get("se");
            Instant expiry = Instant.parse(expiryDateStr);
            assertEquals(now, expiry.minusSeconds(60));
        }
        finally {
            ds.setDirectUploadURIExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testInitiateDirectUploadUnlimitedURIs() throws DataRecordUploadException {
        ConfigurableDataRecordAccessProvider ds = getDataStore();
        long uploadSize = ONE_GB * 100;
        int expectedNumURIs = 10000;
        DataRecordUpload upload = ds.initiateDataRecordUpload(uploadSize, -1);
        assertNotNull("The upload context should not be null", upload);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 500;
        expectedNumURIs = 50000;
        upload = ds.initiateDataRecordUpload(uploadSize, -1);
        assertNotNull("The upload context should not be null", upload);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 1000;
        // expectedNumURIs still 50000, Azure limit
        upload = ds.initiateDataRecordUpload(uploadSize, -1);
        assertNotNull("The upload context should not be null", upload);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());
    }

    @Test
    public void downloadURIsWithVaryingOptions() throws Exception {
        ConfigurableDataRecordAccessProvider dataStore = this.getDataStore();

        DataRecord record = null;
        try {
            // use a cache for download URIs
            dataStore.setDirectDownloadURICacheSize(100);

            InputStream testStream = DataStoreUtils.randomStream(0, 256L);
            record = this.doSynchronousAddRecord((DataStore) dataStore, testStream);
            DataIdentifier id = record.getIdentifier();
            URI uri = dataStore.getDownloadURI(id, downloadOptionsWithMimeType(null));
            assertNotNull(uri);
            URI uriWithContentType = dataStore.getDownloadURI(id, downloadOptionsWithMimeType("application/octet-stream"));
            assertNotNull(uriWithContentType);
            // must generate different download URIs
            assertNotEquals(uri.toString(), uriWithContentType.toString());
        } finally {
            dataStore.setDirectDownloadURICacheSize(0);
            if (record != null) {
                this.doDeleteRecord((DataStore) dataStore, record.getIdentifier());
            }
        }
    }

    private static DataRecordDownloadOptions downloadOptionsWithMimeType(String mimeType) {
        return DataRecordDownloadOptions.fromBlobDownloadOptions(
                new BlobDownloadOptions(
                        mimeType,
                        BlobDownloadOptions.DEFAULT.getCharacterEncoding(),
                        BlobDownloadOptions.DEFAULT.getFileName(),
                        BlobDownloadOptions.DEFAULT.getDispositionType()
                )
        );
    }

    // --- Test utility methods (moved from AzureDataStoreUtilsV12) ---

    static boolean isAzureConfigured() {
        Properties props = getAzureConfig();
        if (!props.containsKey(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_KEY) || !props.containsKey(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME)
                || !(props.containsKey(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME))) {
            if (!props.containsKey(AzureConstantsV12.AZURE_SAS) || !props.containsKey(AzureConstantsV12.AZURE_BLOB_ENDPOINT)
                    || !(props.containsKey(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME))) {
                return props.containsKey(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME) && props.containsKey(AzureConstantsV12.AZURE_TENANT_ID) &&
                        props.containsKey(AzureConstantsV12.AZURE_CLIENT_ID) && props.containsKey(AzureConstantsV12.AZURE_CLIENT_SECRET) &&
                        props.containsKey(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME);
            }
        }
        return true;
    }

    static Properties getAzureConfig() {
        String config = System.getProperty(SYS_PROP_NAME);
        if (StringUtils.isEmpty(config)) {
            File cfgFile = new File(System.getProperty("user.home"), DEFAULT_PROPERTY_FILE);
            if (cfgFile.exists()) {
                config = cfgFile.getAbsolutePath();
            }
        }
        if (StringUtils.isEmpty(config)) {
            config = DEFAULT_CONFIG_PATH;
        }

        Properties props = new Properties();
        if (new File(config).exists()) {
            InputStream is = null;
            try {
                is = new FileInputStream(config);
                props.load(is);
            } catch (Exception e) {
                log.warn("Error loading azure config", e);
            } finally {
                IOUtils.closeQuietly(is);
            }
            props.putAll(DataStoreUtils.getConfig());
            Map<String, String> filtered = MapUtils.filterEntries(MapUtils.fromProperties(props),
                    input -> !StringUtils.isEmpty(input.getValue()));
            props = new Properties();
            props.putAll(filtered);
        }

        props.setProperty(org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_V12_ENABLED_PROPERTY, "true");
        return props;
    }

    static DataStore getAzureDataStore(Properties props, String homeDir) throws Exception {
        AzureDataStore ds = new AzureDataStore();
        PropertiesUtil.populate(ds, MapUtils.fromProperties(props), false);
        ds.setProperties(props);
        ds.init(homeDir);
        return ds;
    }

    @SuppressWarnings("unchecked")
    static <T extends DataStore> T setupDirectAccessDataStore(
            @NotNull final TemporaryFolder homeDir,
            int directDownloadExpirySeconds,
            int directUploadExpirySeconds) throws Exception {
        return setupDirectAccessDataStore(homeDir, directDownloadExpirySeconds, directUploadExpirySeconds, null);
    }

    @SuppressWarnings("unchecked")
    static <T extends DataStore> T setupDirectAccessDataStore(
            @NotNull final TemporaryFolder homeDir,
            int directDownloadExpirySeconds,
            int directUploadExpirySeconds,
            @Nullable final Properties overrideProperties) throws Exception {
        assumeTrue(isAzureConfigured());
        T ds = (T) getAzureDataStore(getDirectAccessDataStoreProperties(overrideProperties), homeDir.newFolder().getAbsolutePath());
        if (ds instanceof ConfigurableDataRecordAccessProvider) {
            ((ConfigurableDataRecordAccessProvider) ds).setDirectDownloadURIExpirySeconds(directDownloadExpirySeconds);
            ((ConfigurableDataRecordAccessProvider) ds).setDirectUploadURIExpirySeconds(directUploadExpirySeconds);
        }
        return ds;
    }

    static Properties getDirectAccessDataStoreProperties() {
        return getDirectAccessDataStoreProperties(null);
    }

    static Properties getDirectAccessDataStoreProperties(@Nullable final Properties overrideProperties) {
        Properties mergedProperties = new Properties();
        mergedProperties.putAll(getAzureConfig());
        if (overrideProperties != null) {
            mergedProperties.putAll(overrideProperties);
        }
        if (mergedProperties.getProperty("cacheSize", null) == null) {
            mergedProperties.put("cacheSize", "0");
        }
        return mergedProperties;
    }

    static HttpsURLConnection createHttpsConnection(long length, URI uri) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(length));
        conn.setRequestProperty("Date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now()));
        conn.setRequestProperty("x-ms-version", "2017-11-09");
        return conn;
    }
}
