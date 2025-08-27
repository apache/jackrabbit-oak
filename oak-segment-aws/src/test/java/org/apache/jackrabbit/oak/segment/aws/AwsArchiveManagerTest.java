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
package org.apache.jackrabbit.oak.segment.aws;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class AwsArchiveManagerTest {

    @ClassRule
    public static final S3MockRule s3Mock = new S3MockRule();

    private AwsContext awsContext;

    private AmazonS3 s3;
    private String bucketName;
    private String rootDirectory = "oak";

    @Before
    public void setup() throws IOException {
        s3 = s3Mock.createClient();
        AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        long time = new Date().getTime();
        bucketName = "bucket-" + time;
        awsContext = AwsContext.create(s3, bucketName, "oak", ddb, "journaltable-" + time, "locktable-" + time);
    }

    @Test
    public void testRecovery() throws IOException {
        SegmentArchiveManager manager = new AwsPersistence(awsContext).createArchiveManager(false, false,
                new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0,
                    false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        s3.deleteObject(bucketName, rootDirectory + "/data00000a.tar/0005." + uuids.get(5).toString());

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);
        assertEquals(uuids.subList(0, 5), new ArrayList<>(recovered.keySet()));
    }

    @Test
    public void testUncleanStop() throws InvalidFileStoreVersionException, IOException, CommitFailedException {
        SegmentNodeStorePersistence p = new AwsPersistence(awsContext);
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        s3.deleteObject(bucketName, rootDirectory + "/data00000a.tar/closed");
        s3.deleteObject(bucketName, rootDirectory + "/data00000a.tar/data00000a.tar.brf");
        s3.deleteObject(bucketName, rootDirectory + "/data00000a.tar/data00000a.tar.gph");

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }

    @Test
    // see OAK-8566
    public void testUncleanStopWithEmptyArchive()
            throws IOException, CommitFailedException, InvalidFileStoreVersionException {
        AwsPersistence p = new AwsPersistence(awsContext);
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        // make sure there are 2 archives
        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo2", "bar2");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        // remove the segment 0000 from the second archive
        S3ObjectSummary segment0000 = awsContext.directory.listObjects("data00001a.tar/0000.").iterator().next();
        s3.deleteObject(segment0000.getBucketName(), segment0000.getKey());
        s3.deleteObject(segment0000.getBucketName(), "oak/data00001a.tar/closed");

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }

    @Test
    public void testExists() throws IOException {
        SegmentArchiveManager manager = new AwsPersistence(awsContext).createArchiveManager(false, false,
                new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0,
                    false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        Assert.assertTrue(manager.exists("data00000a.tar"));
        Assert.assertFalse(manager.exists("data00001a.tar"));
    }

    @Test
    public void testArchiveExistsAfterFlush() throws IOException {
        SegmentArchiveManager manager = new AwsPersistence(awsContext).createArchiveManager(false, false,
                new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        Assert.assertTrue(manager.exists("data00000a.tar"));
    }
}
