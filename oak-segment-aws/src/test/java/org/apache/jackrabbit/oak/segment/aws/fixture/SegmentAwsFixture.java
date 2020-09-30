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

package org.apache.jackrabbit.oak.segment.aws.fixture;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.io.Files;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.aws.AwsContext;
import org.apache.jackrabbit.oak.segment.aws.AwsPersistence;
import org.apache.jackrabbit.oak.segment.aws.S3MockRule;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SegmentAwsFixture extends NodeStoreFixture {

    private static final String AWS_BUCKET_NAME = System.getProperty("oak.segment.aws.bucketName", "oak");

    private static final String AWS_ROOT_PATH = System.getProperty("oak.segment.aws.rootPath", "/oak");

    private static final String AWS_JOURNAL_TABLE_NAME = System.getProperty("oak.segment.aws.tableName",
            "journaltable");

    private static final String AWS_LOCK_TABLE_NAME = System.getProperty("oak.segment.aws.lockTableName", "locktable");

    private Map<NodeStore, FileStore> fileStoreMap = new HashMap<>();

    private Map<NodeStore, String> bucketNameMap = new HashMap<>();

    private S3MockRule s3Mock;
    private AmazonS3 s3;
    private AmazonDynamoDB ddb;

    @Override
    public NodeStore createNodeStore() {
        if (s3Mock == null) {
            s3Mock = new S3MockRule();
            try {
                s3 = s3Mock.createClient();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        }

        AwsPersistence persistence;
        AwsContext awsContext;
        String bucketName;
        try {
            while (true) {
                bucketName = AWS_BUCKET_NAME + "-" + UUID.randomUUID().toString();
                if (!s3.doesBucketExistV2(bucketName)) {
                    s3.createBucket(bucketName);
                    break;
                }
            }
            awsContext = AwsContext.create(s3, bucketName, AWS_ROOT_PATH, ddb, AWS_JOURNAL_TABLE_NAME,
                    AWS_LOCK_TABLE_NAME);
            persistence = new AwsPersistence(awsContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            FileStore fileStore = FileStoreBuilder.fileStoreBuilder(Files.createTempDir())
                    .withCustomPersistence(persistence).build();
            NodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            fileStoreMap.put(nodeStore, fileStore);
            bucketNameMap.put(nodeStore, bucketName);
            return nodeStore;
        } catch (IOException | InvalidFileStoreVersionException e) {
            throw new RuntimeException(e);
        }
    }

    public void dispose(NodeStore nodeStore) {
        FileStore fs = fileStoreMap.remove(nodeStore);
        if (fs != null) {
            fs.close();
        }
        try {
            String bucketName = bucketNameMap.remove(nodeStore);
            if (s3.doesBucketExistV2(bucketName)) {
                s3.deleteBucket(bucketName);
            }
        } catch (AmazonServiceException e) {
            throw new RuntimeException(e);
        }

        try {
            if (s3Mock != null) {
                s3Mock.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "SegmentAws";
    }
}
