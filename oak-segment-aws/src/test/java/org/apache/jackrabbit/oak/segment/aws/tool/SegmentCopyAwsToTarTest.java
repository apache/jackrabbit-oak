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
package org.apache.jackrabbit.oak.segment.aws.tool;

import java.io.IOException;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.jackrabbit.oak.segment.aws.AwsContext;
import org.apache.jackrabbit.oak.segment.aws.S3MockRule;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.junit.Before;
import org.junit.ClassRule;

public class SegmentCopyAwsToTarTest extends SegmentCopyTestBase {

    @ClassRule
    public static final S3MockRule s3Mock = new S3MockRule();

    @Before
    public void setUp() throws IOException {
        AmazonS3 s3 = s3Mock.createClient();
        AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        long time = new Date().getTime();
        awsContext = AwsContext.create(s3, bucketName = "bucket-" + time, rootDirectory = "repository", ddb,
                journalTable = "journaltable-" + time, lockTable = "locktable-" + time);
    }

    @Override
    protected SegmentNodeStorePersistence getSrcPersistence() throws IOException {
        return getAwsPersistence();
    }

    @Override
    protected SegmentNodeStorePersistence getDestPersistence() throws IOException {
        return getTarPersistence();
    }

    @Override
    protected String getSrcPathOrUri() {
        return awsContext.getConfig();
    }

    @Override
    protected String getDestPathOrUri() {
        return folder.getRoot().getAbsolutePath();
    }
}
