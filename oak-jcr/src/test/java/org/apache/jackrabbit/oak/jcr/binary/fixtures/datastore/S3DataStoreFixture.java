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

package org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore;

import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.UUID;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Backend;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3CrudHelper;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.Utils;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.FixtureUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AccelerateConfiguration;
import software.amazon.awssdk.services.s3.model.BucketAccelerateStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAccelerateConfigurationRequest;

/**
 * Fixture for S3DataStore based on an aws.properties config file. It creates
 * a new temporary Azure Blob Container for each DataStore created.
 *
 * <p>
 * Note: when using this, it's highly recommended to reuse the NodeStores across multiple tests (using
 * {@link org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest#AbstractRepositoryTest(NodeStoreFixture, boolean) AbstractRepositoryTest(fixture, true)})
 * otherwise it will be slower and can lead to out of memory issues if there are many tests.
 *
 * <p>
 * Test buckets are named "direct-binary-test-...". If some did not get cleaned up, you can
 * list them using the aws cli with this command:
 * <pre>
 *     aws s3 ls | grep direct-binary-test-
 * </pre>
 *
 * And after checking, delete them all in one go with this command:
 * <pre>
 *     aws s3 ls | grep direct-binary-test- | cut -f 3 -d " " | xargs -n 1 -I {} sh -c 'aws s3 rb s3://{} || exit 1'
 * </pre>
 */
public class S3DataStoreFixture implements DataStoreFixture {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Nullable
    private final Properties s3Props;

    public S3DataStoreFixture() {
        s3Props = FixtureUtils.loadDataStoreProperties("s3.config", "aws.properties", ".aws");
    }

    @Override
    public boolean isAvailable() {
        if (s3Props == null) {
            log.warn("Skipping S3 DataStore fixture because no S3 properties file was found given by " +
                "'s3.config' system property or named 'aws.properties' or '~/.aws/aws.properties'.");
            return false;
        }
        return true;
    }

    @NotNull
    @Override
    public DataStore createDataStore() {
        if (s3Props == null) {
            throw new AssertionError("createDataStore() called but this fixture is not available");
        }

        String bucketName = null;

        try (S3Client s3Client = Utils.openService(s3Props, false)) {
            // Create a temporary bucket that will be removed at test completion
            bucketName = "direct-binary-test-" + UUID.randomUUID();

            log.info("Creating S3 test bucket {}", bucketName);
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucketName).build();
            s3Client.createBucket(createBucketRequest);
            assertTrue("Failed to create test bucket [" + bucketName + "]", S3CrudHelper.waitForBucket(s3Client, bucketName, 20, 100L));

            log.info("Enabling S3 acceleration for bucket {}", bucketName);
            s3Client.putBucketAccelerateConfiguration(
                    PutBucketAccelerateConfigurationRequest.builder()
                            .bucket(bucketName)
                            .accelerateConfiguration(
                                    AccelerateConfiguration.builder()
                                            .status(BucketAccelerateStatus.ENABLED)
                                            .build())
                            .build()
            );
        }

        // create new properties since azProps is shared for all created DataStores
        Properties clonedS3Props = new Properties(s3Props);
        clonedS3Props.setProperty(S3Constants.S3_BUCKET, bucketName);

        // setup Oak DS
        S3DataStore dataStore = new S3DataStore();
        dataStore.setProperties(clonedS3Props);
        dataStore.setStagingSplitPercentage(0);

        log.info("s3props: {}", s3Props);

        return dataStore;
    }

    @Override
    public void dispose(DataStore dataStore) {
        if (dataStore instanceof S3DataStore) {
            try {
                dataStore.close();
            } catch (DataStoreException e) {
                log.warn("Issue while disposing DataStore", e);
            } catch (IllegalStateException e) {
                log.warn("IllegalStateException trying to close S3 connection", e);
            }

            S3DataStore s3DataStore = (S3DataStore) dataStore;
            String bucketName = ((S3Backend) s3DataStore.getBackend()).getBucket();

            if (s3Props == null) {
                // should be impossible if we created the client successfully in createDataStore()
                log.warn("Could not cleanup and remove S3 bucket {}", bucketName);
                return;
            }
            
            try (S3Client s3Client = Utils.openService(s3Props, false)) {
                S3CrudHelper.deleteBucketObjects(bucketName, s3Props, s3Client);
                s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
            }
        }
    }
}
