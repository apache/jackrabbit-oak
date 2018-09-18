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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

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

import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3Config;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class S3DataRecordAccessProviderTest extends AbstractDataRecordAccessProviderTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static S3DataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
        assumeTrue(isS3Configured());
        dataStore = (S3DataStore) getS3DataStore(getFixtures().get(0), getS3Config(),
            homeDir.newFolder().getAbsolutePath());

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
        return S3DataStoreUtils.getHttpsConnection(length, uri);
    }

    @Test
    public void testInitDirectUploadURIHonorsExpiryTime() throws DataRecordUploadException {
        ConfigurableDataRecordAccessProvider ds = getDataStore();
        try {
            ds.setDirectUploadURIExpirySeconds(60);
            DataRecordUpload uploadContext = ds.initiateDataRecordUpload(ONE_MB, 1);
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
    public void testInitiateDirectUploadUnlimitedURIs() throws DataRecordUploadException {
        ConfigurableDataRecordAccessProvider ds = getDataStore();
        long uploadSize = ONE_GB * 50;
        int expectedNumURIs = 5000;
        DataRecordUpload upload = ds.initiateDataRecordUpload(uploadSize, -1);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 100;
        expectedNumURIs = 10000;
        upload = ds.initiateDataRecordUpload(uploadSize, -1);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 200;
        // expectedNumURIs still 10000, AWS limit
        upload = ds.initiateDataRecordUpload(uploadSize, -1);
        assertEquals(expectedNumURIs, upload.getUploadURIs().size());
    }
}
