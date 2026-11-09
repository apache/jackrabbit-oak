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
package org.apache.jackrabbit.oak.segment.aws.tool;

import java.io.File;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.jackrabbit.oak.segment.aws.AwsContext;
import org.apache.jackrabbit.oak.segment.aws.AwsPersistence;
import org.apache.jackrabbit.oak.segment.aws.S3MockRule;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.SegmentStoreMigratorTestBase;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.junit.ClassRule;

public class AwsSegmentStoreMigratorTest extends SegmentStoreMigratorTestBase {

    private static final String ROOT_DIRECTORY = "repository";

    @ClassRule
    public static final S3MockRule s3Mock = new S3MockRule();

    private AmazonS3 s3;

    private String bucketName;

    @Override
    protected SegmentNodeStorePersistence createSourcePersistence() throws Exception {
        s3 = s3Mock.createClient();
        long time = new Date().getTime();
        bucketName = "bucket-" + time;
        AwsContext awsContext = AwsContext.create(s3, bucketName, ROOT_DIRECTORY, DynamoDBEmbedded.create().amazonDynamoDB(),
                "journaltable-" + time, "locktable-" + time);
        return new AwsPersistence(awsContext);
    }

    @Override
    protected void migrate(SegmentNodeStorePersistence source, File tarDir) throws Exception {
        try (AwsSegmentStoreMigrator migrator = new AwsSegmentStoreMigrator.Builder()
                .withSourcePersistence(source, "aws")
                .withTargetPersistence(new TarPersistence(tarDir), "tar")
                .build()) {
            migrator.migrate();
        }
    }

    @Override
    protected void deleteArchiveFile(String archive, String name) {
        s3.deleteObject(bucketName, archiveKey(archive, name));
    }

    @Override
    protected boolean archiveFileExists(String archive, String name) {
        return s3.doesObjectExist(bucketName, archiveKey(archive, name));
    }

    private static String archiveKey(String archive, String name) {
        return ROOT_DIRECTORY + "/" + archive + "/" + name;
    }
}
