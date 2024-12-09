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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import javax.jcr.RepositoryException;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.amazonaws.services.s3.Headers.SERVER_SIDE_ENCRYPTION;
import static com.amazonaws.services.s3.Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID;
import static com.amazonaws.services.s3.Headers.SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM;
import static com.amazonaws.services.s3.Headers.SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY;
import static com.amazonaws.services.s3.Headers.SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5;
import static com.amazonaws.services.s3.model.SSEAlgorithm.AES256;
import static com.amazonaws.services.s3.model.SSEAlgorithm.KMS;
import static com.amazonaws.util.Base64.decode;
import static com.amazonaws.util.Md5Utils.md5AsBase64;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_C;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_KMS;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_C_KEY;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_KMS_KEYID;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3Config;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Test {@link S3DataStore} with S3Backend and local cache on.
 * It requires to pass aws config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties
 */
@RunWith(Parameterized.class)
public class TestS3Ds extends AbstractDataStoreTest {

    protected static final Logger LOG = LoggerFactory.getLogger(TestS3Ds.class);
    protected static long ONE_KB = 1024;
    protected static long ONE_MB = ONE_KB * ONE_KB;
    protected static long ONE_HUNDRED_MB = ONE_MB * 100;
    protected static long ONE_GB = ONE_HUNDRED_MB * 10;

    private static Date overallStartTime = getBackdatedDate();
    private Date thisTestStartTime = null;

    protected Properties props;

    protected String bucket;

    @Parameterized.Parameter
    public String s3Class;

    @Parameterized.Parameters(name = "{index}: ({0})")
    public static List<String> fixtures() {
        return getFixtures();
    }

    public static Date getBackdatedDate() {
        // Use a backdated date to accommodate time drift when deleting created resources.
        return DateUtils.addMinutes(new Date(), -1);
    }

    @BeforeClass
    public static void assumptions() {
        assumeTrue(isS3Configured());
    }

    private static List<String> createdBucketNames = new ArrayList<>();

    @Override
    @Before
    public void setUp() throws Exception {
        props = getS3Config();
        thisTestStartTime = getBackdatedDate();
        bucket = randomGen.nextInt(9999) + "-" +
                randomGen.nextInt(9999) + "-s3ds-unittest-autogenerated";
        createdBucketNames.add(bucket);
        props.setProperty(S3Constants.S3_BUCKET, bucket);
        props.setProperty("secret", "123456");
        props.setProperty(S3Constants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS,"60");
        props.setProperty(S3Constants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS, "60");
        props.setProperty(S3Constants.PRESIGNED_URI_ENABLE_ACCELERATION, "60");
        props.setProperty(S3Constants.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE, "60");
        props.setProperty(S3_ENCRYPTION, S3Constants.S3_ENCRYPTION_NONE);
        super.setUp();
    }

    @Test
    public void testInitiateDirectUploadUnlimitedURIs() throws DataRecordUploadException,
            RepositoryException {
        ConfigurableDataRecordAccessProvider ds
                  = (ConfigurableDataRecordAccessProvider) createDataStore();
        long uploadSize = ONE_GB * 50;
        int expectedNumURIs = 5000;
        DataRecordUpload upload = ds.initiateDataRecordUpload(uploadSize, -1);
        Assert.assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 100;
        expectedNumURIs = 10000;
        upload = ds.initiateDataRecordUpload(uploadSize, -1);
        Assert.assertEquals(expectedNumURIs, upload.getUploadURIs().size());

        uploadSize = ONE_GB * 200;
        upload = ds.initiateDataRecordUpload(uploadSize, -1);
        Assert.assertEquals(expectedNumURIs, upload.getUploadURIs().size());
    }

    @Test
    public void testGetDownloadURI() throws IOException, RepositoryException {
        DataStore ds = createDataStore();

        byte[] data = new byte[dataLength];
        randomGen.nextBytes(data);

        DataRecord record = doSynchronousAddRecord(ds, new ByteArrayInputStream(data));
        URI uri = ((DataRecordAccessProvider) ds).getDownloadURI(record.getIdentifier(),
                                 DataRecordDownloadOptions.DEFAULT);
        Assert.assertNotNull("uri is null", uri);

        // Download content from the URI directly and check
        HttpEntity entity = httpGet(uri);
        assertStream(new ByteArrayInputStream(data), entity.getContent());

        // Download with DataStore API and check
        DataRecord getrec = ds.getRecord(record.getIdentifier());
        Assert.assertNotNull(getrec);
        Assert.assertEquals(data.length, getrec.getLength());
        assertRecord(data, getrec);
    }

    @Test
    public void testDataMigration() {
        try {
            String encryption = props.getProperty(S3_ENCRYPTION);

            //manually close the setup ds and remove encryption
            ds.close();
            props.remove(S3_ENCRYPTION);
            ds = createDataStore();

            byte[] data = new byte[dataLength];
            randomGen.nextBytes(data);
            DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
            Assert.assertEquals(data.length, rec.getLength());
            assertRecord(data, rec);
            ds.close();

            // turn encryption now anc recreate datastore instance
            props.setProperty(S3_ENCRYPTION, encryption);
            props.setProperty(S3Constants.S3_RENAME_KEYS, "true");
            ds = createDataStore();

            Assert.assertNotEquals(null, ds);
            rec = ds.getRecord(rec.getIdentifier());
            Assert.assertNotEquals(null, rec);
            Assert.assertEquals(data.length, rec.getLength());
            assertRecord(data, rec);

            randomGen.nextBytes(data);
            rec = ds.addRecord(new ByteArrayInputStream(data));
            DataRecord rec1 = ds.getRecord(rec.getIdentifier());
            Assert.assertEquals(rec.getLength(), rec1.getLength());
            assertRecord(data, rec);

            ds.close();
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

    @Test
    public void testInitiateCompleteUpload() throws IOException, RepositoryException, IllegalArgumentException, DataRecordUploadException {

        S3DataStore ds = (S3DataStore) createDataStore();
        ds.setDirectUploadURIExpirySeconds(60*5);
        ds.setDirectDownloadURIExpirySeconds(60*5);
        ds.setDirectDownloadURICacheSize(60*5);

        DataRecordUpload uploadContext = ds.initiateDataRecordUpload(ONE_GB, 1);
        assertNotNull(uploadContext);

        String uploadToken = uploadContext.getUploadToken();

        byte[] data = new byte[dataLength];
        randomGen.nextBytes(data);

        // Upload directly using the URI and check
        CloseableHttpResponse response =  httpPut(uploadContext, new ByteArrayInputStream(data), data.length);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        DataRecord uploadedRecord = ds.completeDataRecordUpload(uploadToken);
        assertNotNull(uploadedRecord);
        Assert.assertEquals(data.length, uploadedRecord.getLength());
        assertRecord(data, uploadedRecord);

        // Retieve through DataStore API and check
        DataRecord getrec = ds.getRecord(uploadedRecord.getIdentifier());
        Assert.assertNotNull(getrec);
        Assert.assertEquals(data.length, getrec.getLength());
        assertRecord(data, getrec);
    }

    public CloseableHttpResponse httpPut(@Nullable DataRecordUpload uploadContext, InputStream inputstream, long length) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(uploadContext);

        URI puturl = uploadContext.getUploadURIs().iterator().next();
        HttpPut putreq = new HttpPut(puturl);

        String keyId = null;
        String encryptionType = props.getProperty(S3_ENCRYPTION);

        switch (encryptionType) {
            case S3_ENCRYPTION_SSE_KMS:
                keyId = props.getProperty(S3_SSE_KMS_KEYID);
                putreq.addHeader(new BasicHeader(SERVER_SIDE_ENCRYPTION, KMS.getAlgorithm()));
                if (keyId != null) {
                    putreq.addHeader(new BasicHeader(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, keyId));
                }
                break;
            case S3_ENCRYPTION_SSE_C:
                keyId = props.getProperty(S3_SSE_C_KEY);
                putreq.addHeader(new BasicHeader(SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AES256.getAlgorithm()));
                putreq.addHeader(new BasicHeader(SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, keyId));
                putreq.addHeader(new BasicHeader(SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, md5AsBase64(decode(keyId))));
                break;
            default:
                break;
        }

        putreq.setEntity(new InputStreamEntity(inputstream , length));
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response  = httpclient.execute(putreq);
        return response;
    }


    private HttpEntity httpGet(URI uri) throws IOException {
        HttpGet getreq = new HttpGet(uri);

        final String encryptionType = props.getProperty(S3_ENCRYPTION);

        if (Objects.equals(S3_ENCRYPTION_SSE_C, encryptionType)) {
            String keyId = props.getProperty(S3_SSE_C_KEY);
            getreq.addHeader(new BasicHeader(SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AES256.getAlgorithm()));
            getreq.addHeader(new BasicHeader(SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, keyId));
            getreq.addHeader(new BasicHeader(SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, md5AsBase64(decode(keyId))));
        }

        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse res = httpclient.execute(getreq);
        Assert.assertEquals(200, res.getStatusLine().getStatusCode());
        return res.getEntity();
    }

    protected DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException {
        return ((S3DataStore)ds).addRecord(in, new BlobOptions().setUpload(BlobOptions.UploadType.SYNCHRONOUS));
    }

    private static void assertStream(InputStream expected, InputStream actual) throws IOException {
        while (true) {
            int expectedByte = expected.read();
            int actualByte = actual.read();
            Assert.assertEquals(expectedByte, actualByte);
            if (expectedByte == -1) {
                break;
            }
        }
    }

    @Override
    @After
    public void tearDown() {
        try {
            super.tearDown();
        }
        catch (Exception ignore) { }

        try {
            S3DataStoreUtils.deleteBucket(bucket, thisTestStartTime);
        }
        catch (Exception ignore) { }
    }

    @AfterClass
    public static void verifyAllBucketsDeleted() {
        for (String bucket : createdBucketNames) {
            try {
                S3DataStoreUtils.deleteBucket(bucket, overallStartTime);
            }
            catch (Exception ignore) { }
        }
    }

    protected DataStore createDataStore() throws RepositoryException {
        DataStore s3ds = null;
        try {
            s3ds = getS3DataStore(s3Class, props, dataStoreDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
        sleep(1000);
        return s3ds;
    }

    /**----------Not supported-----------**/
    @Override
    public void testUpdateLastModifiedOnAccess() {
    }

    @Override
    public void testDeleteAllOlderThan() {
    }
}
