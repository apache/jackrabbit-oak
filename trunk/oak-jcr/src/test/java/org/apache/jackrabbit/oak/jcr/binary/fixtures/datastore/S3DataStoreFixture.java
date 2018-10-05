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

import java.util.Properties;
import java.util.UUID;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Backend;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.Utils;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.FixtureUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketAccelerateStatus;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;

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

        AmazonS3 s3Client = Utils.openService(s3Props);

        // Create a temporary bucket that will be removed at test completion
        String bucketName = "direct-binary-test-" + UUID.randomUUID().toString();

        log.info("Creating S3 test bucket {}", bucketName);
        CreateBucketRequest createBucket = new CreateBucketRequest(bucketName);
        s3Client.createBucket(createBucket);

        log.info("Enabling S3 acceleration for bucket {}", bucketName);
        s3Client.setBucketAccelerateConfiguration(
            new SetBucketAccelerateConfigurationRequest(bucketName,
                new BucketAccelerateConfiguration(
                    BucketAccelerateStatus.Enabled
                )
            )
        );

        s3Client.shutdown();

        // create new properties since azProps is shared for all created DataStores
        Properties clonedS3Props = new Properties(s3Props);
        clonedS3Props.setProperty(S3Constants.S3_BUCKET, createBucket.getBucketName());

        // setup Oak DS
        S3DataStore dataStore = new S3DataStore();
        dataStore.setProperties(clonedS3Props);
        dataStore.setStagingSplitPercentage(0);

        log.info("s3props: " + s3Props.toString());

        return dataStore;
    }

    @Override
    public void dispose(DataStore dataStore) {
        if (dataStore != null && dataStore instanceof S3DataStore) {
            try {
                dataStore.close();
            } catch (DataStoreException e) {
                log.warn("Issue while disposing DataStore", e);
            }

            S3DataStore s3DataStore = (S3DataStore) dataStore;
            String bucketName = ((S3Backend) s3DataStore.getBackend()).getBucket();

            if (s3Props == null) {
                // should be impossible if we created the client successfully in createDataStore()
                log.warn("Could not cleanup and remove S3 bucket {}", bucketName);
                return;
            }
            
            AmazonS3 s3Client = Utils.openService(s3Props);

            // For S3, you have to empty the bucket before removing the bucket itself
            log.info("Emptying S3 test bucket {}", bucketName);
            
            ObjectListing listing = s3Client.listObjects(bucketName);
            while (true) {
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    s3Client.deleteObject(bucketName, summary.getKey());
                }
                if (! listing.isTruncated()) {
                    break;
                }
                listing = s3Client.listNextBatchOfObjects(listing);
            }
            
            log.info("Removing S3 test bucket {}", bucketName);
            s3Client.deleteBucket(bucketName);

            s3Client.shutdown();
        }
    }
}
