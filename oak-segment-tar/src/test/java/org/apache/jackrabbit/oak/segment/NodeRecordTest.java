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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NodeRecordTest {

    @Rule
    public TemporaryFolder root = new TemporaryFolder();

    private FileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(root.getRoot()).build();
    }

    @Test
    public void unreferencedNodeRecordShouldBeRoot() throws Exception {
        try (FileStore store = newFileStore()) {
            SegmentWriter writer = SegmentWriterBuilder.segmentWriterBuilder("test").build(store);
            SegmentNodeState state = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();
            assertTrue(isRootRecord(state));
        }
    }

    @Test
    public void stableIdShouldPersistAcrossGenerations() throws Exception {
        try (FileStore store = newFileStore()) {
            SegmentWriter writer;

            writer = SegmentWriterBuilder.segmentWriterBuilder("1").withGeneration(1).build(store);
            SegmentNodeState one = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();

            writer = SegmentWriterBuilder.segmentWriterBuilder("2").withGeneration(2).build(store);
            SegmentNodeState two = writer.writeNode(one);
            writer.flush();

            writer = SegmentWriterBuilder.segmentWriterBuilder("3").withGeneration(3).build(store);
            SegmentNodeState three = writer.writeNode(two);
            writer.flush();

            assertArrayEquals(three.getStableIdBytes(), two.getStableIdBytes());
            assertArrayEquals(two.getStableIdBytes(), one.getStableIdBytes());
        }
    }

    private boolean isRootRecord(SegmentNodeState sns) {
        Segment segment = sns.getRecordId().getSegment();

        for (int i = 0; i < segment.getRootCount(); i++) {
            if (segment.getRootType(i) != RecordType.NODE) {
                continue;
            }

            if (segment.getRootOffset(i) != sns.getRecordId().getOffset()) {
                continue;
            }

            return true;
        }

        return false;
    }

}
