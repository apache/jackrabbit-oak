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

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.data.PartialSegmentState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentBufferWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore openFileStore() throws Exception {
        return fileStoreBuilder(folder.getRoot()).build();
    }

    private ReadOnlyFileStore openReadOnlyFileStore() throws Exception {
        return fileStoreBuilder(folder.getRoot()).buildReadOnly();
    }

    @Test
    public void nonDirtyBuffersShouldNotBeFlushed() throws Exception {
        List<SegmentId> before;

        try (FileStore store = openFileStore()) {
            // init
        }
        try (ReadOnlyFileStore store = openReadOnlyFileStore()) {
            before = newArrayList(store.getSegmentIds());
        }

        try (FileStore store = openFileStore()) {
            defaultSegmentWriterBuilder("t").build(store).flush();
        }

        List<SegmentId> after;

        try (ReadOnlyFileStore store = openReadOnlyFileStore()) {
            after = newArrayList(store.getSegmentIds());
        }

        assertEquals(before, after);
    }

    @Test
    public void dirtyBuffersShouldBeFlushed() throws Exception {
        List<SegmentId> before;

        try (FileStore store = openFileStore()) {
            // init
        }
        try (ReadOnlyFileStore store = openReadOnlyFileStore()) {
            before = newArrayList(store.getSegmentIds());
        }

        try (FileStore store = openFileStore()) {
            SegmentWriter writer = defaultSegmentWriterBuilder("t").build(store);
            writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();
        }

        List<SegmentId> after;

        try (ReadOnlyFileStore store = openReadOnlyFileStore()) {
            after = newArrayList(store.getSegmentIds());
        }

        assertNotEquals(before, after);
    }

    @Test
    public void tooBigRecord() throws Exception {

        // See OAK-7721 to understand why this test exists.

        try (FileStore store = openFileStore()) {

            // Please don't change anything from the following statement yet.
            // Read the next comment to understand why.

            SegmentBufferWriter writer = new SegmentBufferWriter(
                store.getSegmentIdProvider(),
                "t",
                store.getRevisions().getHead().getSegment().getGcGeneration()
            );

            // The size of the record is chosen with the precise intention to
            // fool `writer` into having enough space to write the record. In
            // particular, at the end of `prepare()`, `writer` will have
            // `this.length = 262144`, which is `MAX_SEGMENT_SIZE`, and
            // `this.position = 0`. This result is particularly sensitive to the
            // initial content of the segment, which in turn is influenced by
            // the segment info. Try to change the writer ID in the constructor
            // of `SegmentBufferWriter` to a longer string, and you will have
            // `prepare()` throw ISEs because the writer ID is embedded in the
            // segment info.

            Optional<IllegalArgumentException> error = Optional.empty();
            try {
                writer.prepare(RecordType.BLOCK, 262101, Collections.emptyList(), store);
            } catch (IllegalArgumentException e) {
                error = Optional.of(e);
            }
            assertEquals("Record too big: type=BLOCK, size=262101, recordIds=0, total=262104", error.map(Exception::getMessage).orElse(null));
        }
    }

    @Test
    public void readPartialSegmentState() throws Exception {
        try (FileStore store = openFileStore()) {
            GCGeneration gcGeneration = store.getRevisions().getHead().getSegment().getGcGeneration();
            SegmentBufferWriter writer = new SegmentBufferWriter(store.getSegmentIdProvider(), "t", gcGeneration);

            var referencedRecordId = store.getHead().getRecordId();
            writer.flush(store);

            // Write a record completely
            RecordId completeRecordId = writer.prepare(RecordType.BLOCK, Long.BYTES, Collections.emptyList(), store);
            writer.writeLong(0b00001010_11010000_10111110_10101101_00001011_11101010_11010000_10111110L);
            assert completeRecordId.getSegmentId() != referencedRecordId.getSegmentId();

            // Reference another segment
            final int RECORD_ID_BYTES = Short.BYTES + Integer.BYTES;
            RecordId referenceRecordId = writer.prepare(RecordType.VALUE, RECORD_ID_BYTES, Collections.emptyList(), store);
            writer.writeRecordId(referencedRecordId);

            // Write a record partially
            RecordId partialRecordId = writer.prepare(RecordType.BLOCK, Integer.BYTES * 2, Collections.emptyList(), store);
            writer.writeByte((byte)42);

            // Read the segment partially
            PartialSegmentState partialSegmentState = writer.readPartialSegmentState(completeRecordId.getSegmentId());
            assertNotNull(partialSegmentState);

            assertEquals(partialSegmentState.generation(), gcGeneration.getGeneration());
            assertEquals(partialSegmentState.fullGeneration(), gcGeneration.getFullGeneration());
            assertEquals(partialSegmentState.isCompacted(), gcGeneration.isCompacted());
            assertEquals(partialSegmentState.version(), (byte) 13);

            assertEquals(1, partialSegmentState.segmentReferences().size());
            assertEquals(referencedRecordId.getSegmentId().getMostSignificantBits(), partialSegmentState.segmentReferences().get(0).msb());
            assertEquals(referencedRecordId.getSegmentId().getLeastSignificantBits(), partialSegmentState.segmentReferences().get(0).lsb());

            assertEquals(4, partialSegmentState.records().size());

            writer.flush(store);

            int offset = partialSegmentState.records().get(1).offset();
            assertEquals(completeRecordId.getRecordNumber(), partialSegmentState.records().get(1).refNumber());
            assertEquals(RecordType.BLOCK, partialSegmentState.records().get(1).recordType());
            assertArrayEquals(new byte[] { (byte)0b00001010, (byte)0b11010000, (byte)0b10111110, (byte)0b10101101, (byte)0b00001011, (byte)0b11101010, (byte)0b11010000, (byte)0b10111110 }, partialSegmentState.records().get(1).contents());
            offset -= Long.BYTES;

            assertEquals(referenceRecordId.getRecordNumber(), partialSegmentState.records().get(2).refNumber());
            assertEquals(RecordType.VALUE, partialSegmentState.records().get(2).recordType());
            assertEquals(offset, partialSegmentState.records().get(2).offset());
            offset -= 8; // align(RECORD_ID_BYTES, 4)

            assertEquals(partialRecordId.getRecordNumber(), partialSegmentState.records().get(3).refNumber());
            assertEquals(RecordType.BLOCK, partialSegmentState.records().get(3).recordType());
            assertEquals(offset, partialSegmentState.records().get(3).offset());
            assertArrayEquals(new byte[] { 42 }, partialSegmentState.records().get(3).contents());
        }
    }

    @Test
    public void readPartialSegmentStateOfOtherSegmentReturnsNull() throws Exception {
        try (FileStore store = openFileStore()) {
            SegmentBufferWriter writer = new SegmentBufferWriter(store.getSegmentIdProvider(), "t", store.getRevisions().getHead().getSegment().getGcGeneration());

            RecordId recordId = writer.prepare(RecordType.BLOCK, Long.BYTES, Collections.emptyList(), store);
            writer.writeLong(0b101011010000101111101010110100001011111010101101000010111110L);
            writer.flush(store);

            assertNull(writer.readPartialSegmentState(recordId.getSegmentId()));
        }
    }
}
