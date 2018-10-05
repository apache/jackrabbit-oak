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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.AbstractDataRecordAccessProviderTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class AzureDataRecordAccessProviderTest extends AbstractDataRecordAccessProviderTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static AzureDataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
        assumeTrue(AzureDataStoreUtils.isAzureConfigured());
        Properties props = AzureDataStoreUtils.getAzureConfig();
        props.setProperty("cacheSize", "0");
        dataStore = (AzureDataStore) AzureDataStoreUtils
            .getAzureDataStore(props, homeDir.newFolder().getAbsolutePath());
        dataStore.setDirectDownloadURIExpirySeconds(expirySeconds);
        dataStore.setDirectUploadURIExpirySeconds(expirySeconds);
    }

    @Override
    protected ConfigurableDataRecordAccessProvider getDataStore() {
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
        return Math.max(0L, AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE);
    }

    @Override
    protected long getProviderMaxPartSize() {
        return AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE;
    }

    @Override
    protected long getProviderMaxSinglePutSize() { return AzureBlobStoreBackend.MAX_SINGLE_PUT_UPLOAD_SIZE; }

    @Override
    protected long getProviderMaxBinaryUploadSize() { return AzureBlobStoreBackend.MAX_BINARY_UPLOAD_SIZE; }

    @Override
    protected boolean isSinglePutURI(URI uri) {
        // Since strictly speaking we don't support single-put for Azure due to the odd
        // required header for single-put uploads, we don't care and just always return true
        // here to avoid failing tests for this.
        return true;
    }

    @Override
    protected HttpsURLConnection getHttpsConnection(long length, URI uri) throws IOException {
        return AzureDataStoreUtils.getHttpsConnection(length, uri);
    }

    @Test
    public void testInitDirectUploadURIHonorsExpiryTime() throws DataRecordUploadException {
        ConfigurableDataRecordAccessProvider ds = getDataStore();
        try {
            Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            ds.setDirectUploadURIExpirySeconds(60);
            DataRecordUpload uploadContext = ds.initiateDataRecordUpload(ONE_MB, 1);
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
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 500;
        expectedNumURIs = 50000;
        upload = ds.initiateDataRecordUpload(uploadSize, -1);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 1000;
        // expectedNumURIs still 50000, Azure limit
        upload = ds.initiateDataRecordUpload(uploadSize, -1);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());
    }
}
