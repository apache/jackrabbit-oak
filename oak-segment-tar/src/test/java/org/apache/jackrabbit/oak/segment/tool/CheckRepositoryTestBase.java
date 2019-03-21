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

package org.apache.jackrabbit.oak.segment.tool;

import static com.google.common.base.Charsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckRepositoryTestBase {

    private static final int HEADER_SIZE = 512;

    private static final int MAX_SEGMENT_SIZE = 262144;

    private static final Logger log = LoggerFactory.getLogger(CheckRepositoryTestBase.class);

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    protected Set<String> checkpoints = new LinkedHashSet<>();

    @Before
    public void setup() throws Exception {
        addValidRevision();
    }

    protected void addValidRevision() throws InvalidFileStoreVersionException, IOException, CommitFailedException {
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(temporaryFolder.getRoot()).withMaxFileSize(256)
            .withSegmentCacheSize(64).build();

        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        NodeBuilder builder = nodeStore.getRoot().builder();

        addChildWithBlobProperties(nodeStore, builder, "a", 1);
        addChildWithBlobProperties(nodeStore, builder, "b", 2);
        addChildWithBlobProperties(nodeStore, builder, "c", 3);

        addChildWithProperties(nodeStore, builder, "d", 4);
        addChildWithProperties(nodeStore, builder, "e", 5);
        addChildWithProperties(nodeStore, builder, "f", 6);

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // add checkpoints
        String cp1 = nodeStore.checkpoint(10_000);
        String cp2 = nodeStore.checkpoint(10_000);
        checkpoints.add(cp1);
        checkpoints.add(cp2);

        fileStore.close();
    }

    protected void addInvalidRevision() throws InvalidFileStoreVersionException, IOException, CommitFailedException {
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(temporaryFolder.getRoot()).withMaxFileSize(256)
            .withSegmentCacheSize(64).build();

        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        NodeBuilder builder = nodeStore.getRoot().builder();

        // add a new child "z"
        addChildWithBlobProperties(nodeStore, builder, "z", 5);

        // add a new property value to existing child "a"
        addChildWithBlobProperties(nodeStore, builder, "a", 1);

        NodeState after = nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // get record number to corrupt (NODE record for "z")
        SegmentNodeState child = (SegmentNodeState) after.getChildNode("z");
        RecordId zRecordId = child.getRecordId();

        // get record number to corrupt (NODE record for "a")
        child = (SegmentNodeState) after.getChildNode("a");
        RecordId aRecordId = child.getRecordId();

        fileStore.close();

        corruptRecord(zRecordId, "data00001a.tar");
        corruptRecord(aRecordId, "data00001a.tar");
    }

    protected void corruptPathFromCheckpoint() throws InvalidFileStoreVersionException, IOException {
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(temporaryFolder.getRoot()).withMaxFileSize(256)
            .withSegmentCacheSize(64).build();

        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        SegmentNodeState cp1 = (SegmentNodeState) nodeStore.retrieve(checkpoints.iterator().next());
        RecordId bRecordId = ((SegmentNodeState) cp1.getChildNode("b")).getRecordId();
        fileStore.close();

        corruptRecord(bRecordId, "data00000a.tar");
    }

    private void corruptRecord(RecordId recordId, String tarFileName) throws FileNotFoundException, IOException {
        RandomAccessFile file = new RandomAccessFile(new File(temporaryFolder.getRoot(), tarFileName), "rw");

        String segmentName = recordId.getSegmentId().toString();
        String crtEntryName = "";
        int entrySize = 0;
        long filePointer = 0;

        while (!crtEntryName.equals(segmentName)) {
            filePointer = file.getFilePointer();
            // read entry header
            ByteBuffer entryHeader = ByteBuffer.allocate(HEADER_SIZE);
            file.readFully(entryHeader.array());

            // read entry size from header
            byte[] crtEntryNameBytes = new byte[100];
            System.arraycopy(entryHeader.array(), 0, crtEntryNameBytes, 0, 100);
            crtEntryName = new String(crtEntryNameBytes, 0, 100, UTF_8);
            crtEntryName = crtEntryName.substring(0, crtEntryName.indexOf('.'));

            byte[] entrySizeBytes = new byte[11];
            System.arraycopy(entryHeader.array(), 124, entrySizeBytes, 0, 11);
            entrySize = Integer.parseInt(new String(entrySizeBytes, Charset.forName("UTF-8")), 8);

            if (!crtEntryName.equals(segmentName)) {
                file.skipBytes(entrySize);
                file.skipBytes(HEADER_SIZE - (entrySize % HEADER_SIZE));
            }
        }
        ;

        // read actual segment
        ByteBuffer segmentBytes = ByteBuffer.allocate(entrySize);
        file.readFully(segmentBytes.array());

        int segmentRefs = segmentBytes.getInt(14);

        // read the header for our record 
        int skip = 32 + segmentRefs * 16 + recordId.getRecordNumber() * 9;
        int number = segmentBytes.getInt(skip);
        byte type = segmentBytes.get(skip + 4);
        int offset = segmentBytes.getInt(skip + 4 + 1);

        Assert.assertEquals(recordId.getRecordNumber(), number);
        Assert.assertEquals(RecordType.NODE.ordinal(), type);

        // read the offset of previous record to derive length of our record
        int prevSkip = 32 + segmentRefs * 16 + (recordId.getRecordNumber() - 1) * 9;
        int prevOffset = segmentBytes.getInt(prevSkip + 4 + 1);

        int length = prevOffset - offset;

        int realOffset = entrySize - (MAX_SEGMENT_SIZE - offset);

        // write random bytes inside the NODE record to corrupt it
        Random r = new Random(10);
        byte[] bogusData = new byte[length];
        r.nextBytes(bogusData);
        file.seek(filePointer + HEADER_SIZE + realOffset);
        file.write(bogusData);

        file.close();
    }

    protected static void assertExpectedOutput(String message, List<String> assertMessages) {
        log.info("Assert message: {}", assertMessages);
        log.info("Message logged: {}", message);

        for (String msg : assertMessages) {
            Assert.assertTrue(message.contains(msg));
        }
    }

    protected static void addChildWithBlobProperties(SegmentNodeStore nodeStore, NodeBuilder builder, String childName,
        int propCount) throws IOException {
        NodeBuilder child = builder.child(childName);
        for (int i = 0; i < propCount; i++) {
            child.setProperty(childName + i, nodeStore.createBlob(randomStream(i, 2000)));
        }
    }

    protected static void addChildWithProperties(SegmentNodeStore nodeStore, NodeBuilder builder, String childName,
        int propCount) throws IOException {
        NodeBuilder child = builder.child(childName);
        for (int i = 0; i < propCount; i++) {
            child.setProperty(childName + i, childName + i);
        }
    }

    protected static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }
}
