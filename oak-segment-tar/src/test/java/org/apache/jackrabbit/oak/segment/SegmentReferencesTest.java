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

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentReferencesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(folder.getRoot()).build();
    }

    @Test
    public void segmentShouldNotReferenceItself() throws Exception {
        try (FileStore store = newFileStore()) {

            // Write two records, one referencing the other.

            SegmentWriter writer = defaultSegmentWriterBuilder("test").build(store);
            RecordId a = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
            builder.setChildNode("referred", store.getReader().readNode(a));
            RecordId b = writer.writeNode(builder.getNodeState());
            writer.flush();

            // The two records should be living in the same segment.

            assertEquals(b.getSegmentId(), a.getSegmentId());

            // This inter-segment reference shouldn't generate a reference from
            // this segment to itself.

            assertEquals(0, b.getSegment().getReferencedSegmentIdCount());
        }
    }

    @Test
    public void segmentShouldExposeReferencedSegments() throws Exception {
        try (FileStore store = newFileStore()) {

            // Write two records, one referencing the other.

            SegmentWriter writer = defaultSegmentWriterBuilder("test").build(store);

            RecordId a = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();

            NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
            builder.setChildNode("referred", store.getReader().readNode(a));
            RecordId b = writer.writeNode(builder.getNodeState());
            writer.flush();

            // The two records should be living in two different segments.

            assertNotEquals(a.getSegmentId(), b.getSegmentId());

            // This intra-segment reference should generate a reference from the
            // segment containing the list to the segment containing the string.

            assertEquals(1, b.getSegment().getReferencedSegmentIdCount());
            assertEquals(a.getSegmentId().asUUID(), b.getSegment().getReferencedSegmentId(0));
        }
    }

    @Test
    public void shouldReadSegmentReferencesFromSegmentData() throws IOException {
        SegmentData sampleSegmentData = SegmentData.newSegmentData(Buffer.wrap(new byte[] {
                48, 97, 75, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                -116, -71, -35, -50, 87, -7, 64, 125, -96, -24, -82, 112, 44, -36, 63, 67, 0, 0, 0, 0, 4, 0, 3, -1, -40,
                0, 0, 0, 1, 5, 0, 3, -1, -48, 0, 0, 0, 2, 4, 0, 3, -1, -56, 0, 0, 0, 3, 5, 0, 3, -1, -64, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 10, -48, -66, -83, 11, -22, -48,
                -66, 37, 123, 34, 119, 105, 100, 34, 58, 34, 116, 34, 44, 34, 115, 110, 111, 34, 58, 50, 44, 34, 116,
                34, 58, 49, 55, 50, 51, 55, 49, 51, 51, 53, 57, 54, 54, 54, 125, 0, 0
        }).asReadOnlyBuffer());

        SegmentReferences segmentReferences = SegmentReferences.fromSegmentData(sampleSegmentData, new MemoryStore().getSegmentIdProvider());

        SegmentId segmentId = segmentReferences.getSegmentId(1);
        assertEquals(segmentId.getMostSignificantBits(), -8306364159399214979L);
        assertEquals(segmentId.getLeastSignificantBits(), -6852035036232007869L);
    }
}
