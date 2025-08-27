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

import java.io.File;
import java.io.IOException;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class AwsReadSegmentTest {

    @ClassRule
    public static final S3MockRule s3Mock = new S3MockRule();

    private AwsContext awsContext;

    @Before
    public void setup() throws IOException {
        AmazonS3 s3 = s3Mock.createClient();
        AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        long time = new Date().getTime();
        awsContext = AwsContext.create(s3, "bucket-" + time, "oak", ddb, "journaltable-" + time, "locktable-" + time);
    }

    @Test(expected = SegmentNotFoundException.class)
    public void testReadNonExistentSegmentRepositoryReachable() throws InvalidFileStoreVersionException, IOException {
        AwsPersistence p = new AwsPersistence(awsContext);
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentId id = new SegmentId(fs, 0, 0);

        try {
            fs.readSegment(id);
        } finally {
            fs.close();
        }
    }

    @Test(expected = RepositoryNotReachableException.class)
    public void testReadExistentSegmentRepositoryNotReachable() throws InvalidFileStoreVersionException, IOException {
        AwsPersistence p = new ReadFailingAwsPersistence(awsContext);
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();

        SegmentId id = new SegmentId(fs, 0, 0);
        byte[] buffer = new byte[2];

        try {
            fs.writeSegment(id, buffer, 0, 2);
            fs.readSegment(id);
        } finally {
            fs.close();
        }
    }

    static class ReadFailingAwsPersistence extends AwsPersistence {
        public ReadFailingAwsPersistence(AwsContext awsContext) {
            super(awsContext);
        }

        @Override
        public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor,
                FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
            return new AwsArchiveManager(awsContext.directory, ioMonitor, fileStoreMonitor) {
                @Override
                public SegmentArchiveReader open(String archiveName) throws IOException {
                    S3Directory archiveDirectory = awsContext.directory.withDirectory(archiveName);
                    return new AwsSegmentArchiveReader(archiveDirectory, archiveName, ioMonitor) {
                        @Override
                        public Buffer readSegment(long msb, long lsb) throws IOException {
                            throw new RepositoryNotReachableException(new RuntimeException("Cannot access AWS S3"));
                        }
                    };
                }

                @Override
                public SegmentArchiveWriter create(String archiveName) throws IOException {
                    S3Directory archiveDirectory = awsContext.directory.withDirectory(archiveName);
                    return new AwsSegmentArchiveWriter(archiveDirectory, archiveName, ioMonitor, fileStoreMonitor) {
                        @Override
                        public Buffer readSegment(long msb, long lsb) throws IOException {
                            throw new RepositoryNotReachableException(new RuntimeException("Cannot access AWS S3"));
                        }
                    };
                }
            };
        }
    }
}
