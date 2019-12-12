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
package org.apache.jackrabbit.oak.segment.aws;

import java.io.IOException;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jackrabbit.oak.segment.file.GcJournalTest;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class AwsGCJournalFileTest extends GcJournalTest {

    @ClassRule
    public static final S3MockRule s3Mock = new S3MockRule();

    private AwsContext awsContext;

    @Before
    public void setup() throws IOException {
        AmazonS3 s3 = s3Mock.createClient();
        AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        long time = new Date().getTime();
        awsContext = AwsContext.create(s3, "bucket-" + time, "oak", ddb, "journaltable-" + time);
    }

    @Override
    protected SegmentNodeStorePersistence getPersistence() throws Exception {
        return new MockPersistence(awsContext);
    }

    @Test
    @Override
    public void tarGcJournal() throws Exception {
        super.tarGcJournal();
    }

    @Test
    @Ignore
    @Override
    public void testReadOak16GCLog() throws Exception {
        super.testReadOak16GCLog();
    }

    @Test
    @Ignore
    @Override
    public void testUpdateOak16GCLog() throws Exception {
        super.testUpdateOak16GCLog();
    }

    private static class MockPersistence implements SegmentNodeStorePersistence {

        private final AwsContext awsContext;

        public MockPersistence(AwsContext awsContext) {
            this.awsContext = awsContext;
        }

        @Override
        public SegmentArchiveManager createArchiveManager(boolean arg0, boolean arg1, IOMonitor arg2,
                FileStoreMonitor arg3, RemoteStoreMonitor arg4) throws IOException {
            throw new IOException();
        }

        @Override
        public GCJournalFile getGCJournalFile() throws IOException {
            return new AwsGCJournalFile(awsContext, "gc.log");
        }

        @Override
        public JournalFile getJournalFile() {
            return null;
        }

        @Override
        public ManifestFile getManifestFile() throws IOException {
            throw new IOException();
        }

        @Override
        public RepositoryLock lockRepository() throws IOException {
            throw new IOException();
        }

        @Override
        public boolean segmentFilesExist() {
            throw new NotImplementedException("");
        }

    }
}
