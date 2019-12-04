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

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3Config;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.jcr.RepositoryException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.jackrabbit.core.data.DataIdentifier;
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

    private static List<String> createdBucketNames = Lists.newArrayList();

    @Override
    @Before
    public void setUp() throws Exception {
        props = getS3Config();
        thisTestStartTime = getBackdatedDate();
        bucket =
            String.valueOf(randomGen.nextInt(9999)) + "-" + String.valueOf(randomGen.nextInt(9999))
                + "-s3ds-unittest-autogenerated";
        createdBucketNames.add(bucket);
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
    public void testGetDownloadURIIT() throws IOException, RepositoryException {
        DataRecord record = null;
        DataRecordAccessProvider ds = (DataRecordAccessProvider) createDataStore();
        InputStream testStream = randomStream(0, 256);
        record = doSynchronousAddRecord(ds, testStream);
        URI uri = ((DataRecordAccessProvider) ds).getDownloadURI(record.getIdentifier(), DataRecordDownloadOptions.DEFAULT);
        Assert.assertNotNull("uri is null", uri);
    }

    @Test
    public void testGetDownloadURI() throws IOException, RepositoryException {
        DataRecord record = null;
        DataRecordAccessProvider ds = (DataRecordAccessProvider) createDataStore();
        InputStream testStream = randomStream(0, 256);
        record = doSynchronousAddRecord(ds, testStream);
        URI uri = ((DataRecordAccessProvider) ds).getDownloadURI(record.getIdentifier(), DataRecordDownloadOptions.DEFAULT);
        Assert.assertNotNull("uri is null", uri);
    }

    @Test
    public void testPresigned() throws IOException, RepositoryException, IllegalArgumentException, DataRecordUploadException {
        ConfigurableDataRecordAccessProvider ds = (ConfigurableDataRecordAccessProvider) createDataStore();
        ds.setDirectUploadURIExpirySeconds(60*5);
        ds.setDirectDownloadURIExpirySeconds(60*5);
        ds.setDirectDownloadURICacheSize(60*5);

        DataRecordUpload uploadContext = ds.initiateDataRecordUpload(ONE_GB, 1);
        assertNotNull(uploadContext);

        String uploadToken = uploadContext.getUploadToken();

        int res =  httpPut(uploadContext);
        Assert.assertEquals(200, res);
        DataRecord uploadedRecord = ds.completeDataRecordUpload(uploadToken);
        assertNotNull(uploadedRecord);
    }

    public int httpPut(@Nullable DataRecordUpload uploadContext) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(uploadContext);
        
        File fileToUpload = createFile();
        URI puturl = uploadContext.getUploadURIs().iterator().next();
        HttpPut putreq = new HttpPut(puturl);

        String keyId = null;
        String encryptionType = props.getProperty(S3Constants.S3_ENCRYPTION);

        if (encryptionType.equals(S3Constants.S3_ENCRYPTION_SSE_KMS)) {
             keyId = props.getProperty(S3Constants.S3_SSE_KMS_KEYID);
             putreq.addHeader(new BasicHeader(Headers.SERVER_SIDE_ENCRYPTION,
                     SSEAlgorithm.KMS.getAlgorithm()));
             if(keyId != null) {
                 putreq.addHeader(new BasicHeader(Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
                         keyId));
             }
        } else {
            putreq.addHeader(new BasicHeader(Headers.SERVER_SIDE_ENCRYPTION,
                    SSEAlgorithm.AES256.getAlgorithm()));
        }

        // Explicitly specifying your KMS customer master key id
        putreq.setEntity(new FileEntity(fileToUpload));
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response  = httpclient.execute(putreq);
        return response.getStatusLine().getStatusCode();
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

    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((S3DataStore)ds).deleteRecord(identifier);
    }

    protected DataRecord doSynchronousAddRecord(DataRecordAccessProvider ds, InputStream in) throws DataStoreException {
        return ((S3DataStore)ds).addRecord(in, new BlobOptions().setUpload(BlobOptions.UploadType.SYNCHRONOUS));
    }

    protected File createFile() {
        long count=0;
        File file = null;
        try{
            file = new File("bigfile.txt");
            PrintWriter writer = new PrintWriter(file, "UTF-8");

            Random random = new Random();
            for(int i = 0; i < 23695522; i++)
            {           
                char[] word = new char[random.nextInt(8)+3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
                count+=word.length;
                for(int j = 0; j < word.length; j++)
                {
                    word[j] = (char)('a' + random.nextInt(26));
                }
                writer.print(new String(word) + ' ');
                count+=1;
                if (i % 10 == 0){
                    writer.println();
                    count+=2;
                }
            }
            writer.close();
        } catch (IOException e) {
           // do something
        }
        return file;
    }
    /**----------Not supported-----------**/
    @Override
    public void testUpdateLastModifiedOnAccess() {
    }

    @Override
    public void testDeleteAllOlderThan() {
    }
}