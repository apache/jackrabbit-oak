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
package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.Backend;
import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.TestCaseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * Test {@link org.apache.jackrabbit.core.data.CachingDataStore} with S3Backend and local cache on. It requires
 * to pass aws config file via system property. For e.g.
 * -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties
 */
public class TestS3Ds extends TestCaseBase {

    protected static final Logger LOG = LoggerFactory.getLogger(TestS3Ds.class);

    private Date startTime = null;

    protected Properties props;

    protected String config;

    public TestS3Ds() throws IOException {
        System.setProperty(
            TestCaseBase.CONFIG,
            "./src/test/resources/aws.properties");
        config = System.getProperty(CONFIG);
        props = Utils.readConfig(System.getProperty(CONFIG));
    }

    @Override
    protected void setUp() throws Exception {
        startTime = new Date();
        super.setUp();
        String bucket = String.valueOf(randomGen.nextInt(9999)) + "-"
            + String.valueOf(randomGen.nextInt(9999)) + "-test";
        props.setProperty(S3Constants.S3_BUCKET, bucket);
        // delete bucket if exists
        deleteBucket(bucket);
    }

    @Override
    protected void tearDown() {
        try {
            deleteBucket();
            super.tearDown();
        } catch (Exception ignore) {

        }
    }

    @Override
    protected CachingDataStore createDataStore() throws RepositoryException {
        S3DataStore s3ds = new S3DataStore();
        s3ds.setProperties(props);
        s3ds.setSecret("123456");
        s3ds.init(dataStoreDir);
        sleep(1000);
        return s3ds;
    }

    /**
     * Cleaning of bucket after test run.
     */
    /**
     * Cleaning of bucket after test run.
     */
    public void deleteBucket() throws Exception {
        Backend backend = ((S3DataStore) ds).getBackend();
        String bucket = ((S3Backend) backend).getBucket();
        deleteBucket(bucket);
    }

    public void deleteBucket(String bucket) throws Exception {
        LOG.info("deleting bucket [" + bucket + "]");
        Properties props = Utils.readConfig(config);
        AmazonS3Client s3service = Utils.openService(props);
        TransferManager tmx = new TransferManager(s3service);

        if (s3service.doesBucketExist(bucket)) {
            for (int i = 0; i < 4; i++) {
                tmx.abortMultipartUploads(bucket, startTime);
                ObjectListing prevObjectListing = s3service.listObjects(bucket);
                while (prevObjectListing != null) {
                    List<DeleteObjectsRequest.KeyVersion> deleteList = new ArrayList<DeleteObjectsRequest.KeyVersion>();
                    for (S3ObjectSummary s3ObjSumm : prevObjectListing.getObjectSummaries()) {
                        deleteList.add(new DeleteObjectsRequest.KeyVersion(
                            s3ObjSumm.getKey()));
                    }
                    if (deleteList.size() > 0) {
                        DeleteObjectsRequest delObjsReq = new DeleteObjectsRequest(
                            bucket);
                        delObjsReq.setKeys(deleteList);
                        s3service.deleteObjects(delObjsReq);
                    }
                    if (!prevObjectListing.isTruncated()) break;
                    prevObjectListing = s3service.listNextBatchOfObjects(prevObjectListing);
                }
            }
            s3service.deleteBucket(bucket);
            LOG.info("bucket [ " + bucket + "] deleted");

        } else {
            LOG.info("bucket [" + bucket + "] doesn't exists");
        }
        tmx.shutdownNow();
        s3service.shutdown();
    }
}