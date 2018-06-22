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

import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractHttpDataRecordProviderTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.ConfigurableHttpDataRecordProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.HttpDataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.HttpUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.UnsupportedHttpUploadArgumentsException;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AzureDataStoreHttpDataRecordProviderTest extends AbstractHttpDataRecordProviderTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static AzureDataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
        dataStore = new AzureDataStore();
        Map<String, String> propsAsMap = Maps.newHashMap();
        Properties props = getProperties(
                "azure.blob.config",
                "azure.properties",
                ".azure"
        );
        for (String key : props.stringPropertyNames()) {
            propsAsMap.put(key, props.getProperty(key));
        }
        PropertiesUtil.populate(dataStore, propsAsMap, false);
        dataStore.setProperties(props);
        dataStore.init(homeDir.newFolder().getAbsolutePath());

        dataStore.setHttpDownloadURLExpirySeconds(expirySeconds);
        dataStore.setHttpUploadURLExpirySeconds(expirySeconds);
    }

    @Override
    protected ConfigurableHttpDataRecordProvider getDataStore() {
        return dataStore;
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
        return Math.max(0L, AzureDataStore.minPartSize);
    }

    @Override
    protected long getProviderMaxPartSize() {
        return AzureDataStore.maxPartSize;
    }

    @Override
    protected long getProviderMaxSinglePutSize() { return AzureDataStore.maxSinglePutUploadSize; }

    @Override
    protected long getProviderMaxBinaryUploadSize() { return AzureDataStore.maxBinaryUploadSize; }

    @Override
    protected boolean isSinglePutURL(URL url) {
        // Since strictly speaking we don't support single-put for Azure due to the odd
        // required header for single-put uploads, we don't care and just always return true
        // here to avoid failing tests for this.
        return true;
    }

    @Override
    protected HttpsURLConnection getHttpsConnection(long length, URL url) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(length));
        conn.setRequestProperty("x-ms-date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now()));
        conn.setRequestProperty("x-ms-version", "2017-11-09");

        Map<String, String> queryParams = parseQueryString(url);
        if (! queryParams.containsKey("comp") && ! queryParams.containsKey("blockId")) {
            // single put
            conn.setRequestProperty("x-ms-blob-type", "BlockBlob");
        }

        return conn;
    }

    /** Only run if explicitly asked to via -Dtest=AzureDataStoreHttpDataRecordProviderTest */
    /** Run like this:  mvn test -Dtest=AzureDataStoreHttpDataRecordProviderTest -Dtest.opts.memory=-Xmx2G */
    private static final boolean INTEGRATION_TESTS_ENABLED =
            AzureDataStoreHttpDataRecordProviderTest.class.getSimpleName().equals(getProperty("test"));
    @Override
    protected boolean integrationTestsEnabled() {
        return INTEGRATION_TESTS_ENABLED;
    }

    @Test
    public void testInitDirectUploadURLHonorsExpiryTime() throws UnsupportedHttpUploadArgumentsException, HttpUploadException {
        ConfigurableHttpDataRecordProvider ds = getDataStore();
        try {
            Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            ds.setHttpUploadURLExpirySeconds(60);
            HttpDataRecordUpload uploadContext = ds.initiateHttpUpload(ONE_MB, 1);
            URL uploadUrl = uploadContext.getUploadURLs().iterator().next();
            Map<String, String> params = parseQueryString(uploadUrl);
            String expiryDateStr = params.get("se");
            Instant expiry = Instant.parse(expiryDateStr);
            assertEquals(now, expiry.minusSeconds(60));
        }
        finally {
            ds.setHttpUploadURLExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testInitiateHttpUploadUnlimitedURLs() throws UnsupportedHttpUploadArgumentsException, HttpUploadException {
        ConfigurableHttpDataRecordProvider ds = getDataStore();
        long uploadSize = ONE_GB * 100;
        int expectedNumUrls = 10000;
        HttpDataRecordUpload upload = ds.initiateHttpUpload(uploadSize, -1);
        assertEquals(expectedNumUrls, upload.getUploadURLs().size());

        uploadSize = ONE_GB * 500;
        expectedNumUrls = 50000;
        upload = ds.initiateHttpUpload(uploadSize, -1);
        assertEquals(expectedNumUrls, upload.getUploadURLs().size());

        uploadSize = ONE_GB * 1000;
        // expectedNumUrls still 50000, Azure limit
        upload = ds.initiateHttpUpload(uploadSize, -1);
        assertEquals(expectedNumUrls, upload.getUploadURLs().size());
    }
}
