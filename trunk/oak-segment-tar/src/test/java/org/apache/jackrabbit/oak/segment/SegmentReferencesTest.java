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

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
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

}
