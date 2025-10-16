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
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_C;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_KMS;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_C_KEY;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_KMS_KEYID;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3Config;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isSseCustomerKeyEncrypted;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256;

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

    protected Properties props = new Properties();

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

    protected void setEncryptionData() {}

    @BeforeClass
    public static void assumptions() {
        Assume.assumeTrue(isS3Configured());
    }

    private static List<String> createdBucketNames = new ArrayList<>();

    @Override
    @Before
    public void setUp() throws Exception {
        props.putAll(getS3Config());
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
        setEncryptionData();
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
        Assume.assumeTrue("SSE-C doesn't support presigned GET URLs", !isSseCustomerKeyEncrypted());
        DataStore ds = createDataStore();

        byte[] data = new byte[dataLength];
        randomGen.nextBytes(data);

        DataRecord record = doSynchronousAddRecord(ds, new ByteArrayInputStream(data));
        URI uri = ((DataRecordAccessProvider) ds).getDownloadURI(record.getIdentifier(),
                                 DataRecordDownloadOptions.DEFAULT);
        assertNotNull("uri is null", uri);

        // Download content from the URI directly and check
        InputStream entity = httpGet(uri);
        assertStream(new ByteArrayInputStream(data), entity);

        // Download with DataStore API and check
        DataRecord getrec = ds.getRecord(record.getIdentifier());
        assertNotNull(getrec);
        Assert.assertEquals(data.length, getrec.getLength());
        assertRecord(data, getrec);
    }

    @Test
    public void testDataMigration() {
        Assume.assumeTrue("For SSE-C we can't change encryption without manual intervention", !isSseCustomerKeyEncrypted());
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
        PutObjectResponse response =  httpPut(uploadContext, new ByteArrayInputStream(data), data.length);
        Assert.assertEquals(200, response.sdkHttpResponse().statusCode());
        DataRecord uploadedRecord = ds.completeDataRecordUpload(uploadToken);
        assertNotNull(uploadedRecord);
        Assert.assertEquals(data.length, uploadedRecord.getLength());
        assertRecord(data, uploadedRecord);

        // Retieve through DataStore API and check
        DataRecord getrec = ds.getRecord(uploadedRecord.getIdentifier());
        assertNotNull(getrec);
        Assert.assertEquals(data.length, getrec.getLength());
        assertRecord(data, getrec);
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

    // helper methods

    private PutObjectResponse httpPut(@Nullable DataRecordUpload uploadContext, InputStream inputstream, long length) {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(uploadContext);

        URI putUri = uploadContext.getUploadURIs().iterator().next();
        try (S3Client s3Client = Utils.openService(props, false)) {
            String bucketName = extractBucketFromUri(s3Client, putUri);
            String key = extractKeyFromUri(s3Client, putUri);

            String encryptionType = props.getProperty(S3_ENCRYPTION);

            PutObjectRequest.Builder putReqBuilder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key);

            switch (encryptionType) {
                case S3_ENCRYPTION_SSE_KMS:
                    String keyId = props.getProperty(S3_SSE_KMS_KEYID);
                    putReqBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
                    if (keyId != null) {
                        putReqBuilder.ssekmsKeyId(keyId);
                    }
                    break;
                case S3_ENCRYPTION_SSE_C:
                    String sseCustomerKey = props.getProperty(S3_SSE_C_KEY);
                    putReqBuilder
                            .sseCustomerAlgorithm(AES256.toString())
                            .sseCustomerKey(sseCustomerKey)
                            .sseCustomerKeyMD5(Utils.calculateMD5(sseCustomerKey));  // implement helpers
                    break;
                default:
                    // No encryption
                    break;
            }
            return s3Client.putObject(putReqBuilder.build(), RequestBody.fromInputStream(inputstream, length));
        }
    }

    private InputStream httpGet(URI uri) {
        String encryptionType = props.getProperty(S3_ENCRYPTION);

        try (S3Client s3Client = Utils.openService(props, false)) {
            String bucketName = extractBucketFromUri(s3Client, uri);
            String key = extractKeyFromUri(s3Client, uri);

            GetObjectRequest.Builder req = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key);

            if (Objects.equals(S3_ENCRYPTION_SSE_C, encryptionType)) {
                String keyId = props.getProperty(S3_SSE_C_KEY);
                if (keyId != null) {
                    req.sseCustomerAlgorithm(AES256.toString())
                            .sseCustomerKey(keyId)
                            .sseCustomerKeyMD5(Utils.calculateMD5(keyId));
                }
            }

            return s3Client.getObject(req.build(), ResponseTransformer.toInputStream());
        }
    }

    private static String extractBucketFromUri(S3Client s3Client, URI uri) {
        LOG.info("Extracting bucket from URI {}", uri);
        S3Utilities s3Utilities = s3Client.utilities();

        S3Uri s3Uri = s3Utilities.parseUri(uri);

        return s3Uri.bucket().orElse(null);
    }

    private static String extractKeyFromUri(S3Client s3Client, URI uri) {
        LOG.info("Extracting key from URI {}", uri);
        S3Utilities s3Utilities = s3Client.utilities();

        S3Uri s3Uri = s3Utilities.parseUri(uri);

        return s3Uri.key().orElse(null);
    }
}
