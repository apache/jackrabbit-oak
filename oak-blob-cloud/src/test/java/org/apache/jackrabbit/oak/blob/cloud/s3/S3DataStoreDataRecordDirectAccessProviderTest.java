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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import static java.lang.System.getProperty;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.AbstractDataRecordDirectAccessProviderTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordDirectAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDirectUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class S3DataStoreDataRecordDirectAccessProviderTest extends AbstractDataRecordDirectAccessProviderTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static S3DataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
        dataStore = (S3DataStore) S3DataStoreUtils.getS3DataStore(
                "org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore",
                getProperties("s3.config",
                        "aws.properties",
                        ".aws"),
                homeDir.newFolder().getAbsolutePath()
        );
        dataStore.setDirectDownloadURIExpirySeconds(expirySeconds);
        dataStore.setDirectUploadURIExpirySeconds(expirySeconds);
    }

    @Override
    protected ConfigurableDataRecordDirectAccessProvider getDataStore() {
        return dataStore;
    }

    @Override
    protected DataRecord doGetRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        return ds.getRecord(identifier);
    }

    @Override
    protected DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException {
        return ((S3DataStore)ds).addRecord(in, new BlobOptions().setUpload(BlobOptions.UploadType.SYNCHRONOUS));
    }

    @Override
    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((S3DataStore)ds).deleteRecord(identifier);
    }

    @Override
    protected long getProviderMinPartSize() {
        return Math.max(0L, S3Backend.MIN_MULTIPART_UPLOAD_PART_SIZE);
    }

    @Override
    protected long getProviderMaxPartSize() {
        return S3Backend.MAX_MULTIPART_UPLOAD_PART_SIZE;
    }

    @Override
    protected long getProviderMaxSinglePutSize() { return S3Backend.MAX_SINGLE_PUT_UPLOAD_SIZE; }

    @Override
    protected long getProviderMaxBinaryUploadSize() { return S3Backend.MAX_BINARY_UPLOAD_SIZE; }

    @Override
    protected boolean isSinglePutURI(URI uri) {
        Map<String, String> queryParams = parseQueryString(uri);
        return ! queryParams.containsKey(S3Backend.PART_NUMBER) && ! queryParams.containsKey(S3Backend.UPLOAD_ID);
    }

    @Override
    protected HttpsURLConnection getHttpsConnection(long length, URI uri) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(length));
        conn.setRequestProperty("Date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now()));
        conn.setRequestProperty("Host", uri.getHost());

        return conn;
    }

    /** Only run if explicitly asked to via -Dtest=S3DataStoreDataRecordDirectAccessProviderTest */
    /** Run like this:  mvn test -Dtest=S3DataStoreDataRecordDirectAccessProviderTest -Dtest.opts.memory=-Xmx2G */
    private static final boolean INTEGRATION_TESTS_ENABLED =
            S3DataStoreDataRecordDirectAccessProviderTest.class.getSimpleName().equals(getProperty("test"));
    @Override
    protected boolean integrationTestsEnabled() {
        return INTEGRATION_TESTS_ENABLED;
    }

    @Test
    public void testInitDirectUploadURIHonorsExpiryTime() throws DataRecordDirectUploadException {
        ConfigurableDataRecordDirectAccessProvider ds = getDataStore();
        try {
            ds.setDirectUploadURIExpirySeconds(60);
            DataRecordUpload uploadContext = ds.initiateDirectUpload(ONE_MB, 1);
            URI uploadURI = uploadContext.getUploadURIs().iterator().next();
            Map<String, String> params = parseQueryString(uploadURI);
            String expiresTime = params.get("X-Amz-Expires");
            assertTrue(60 >= Integer.parseInt(expiresTime));
        }
        finally {
            ds.setDirectUploadURIExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testInitiateDirectUploadUnlimitedURIs() throws DataRecordDirectUploadException {
        ConfigurableDataRecordDirectAccessProvider ds = getDataStore();
        long uploadSize = ONE_GB * 50;
        int expectedNumURIs = 5000;
        DataRecordUpload upload = ds.initiateDirectUpload(uploadSize, -1);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 100;
        expectedNumURIs = 10000;
        upload = ds.initiateDirectUpload(uploadSize, -1);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 200;
        // expectedNumURIs still 10000, AWS limit
        upload = ds.initiateDirectUpload(uploadSize, -1);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());
    }
}
