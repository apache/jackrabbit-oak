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

package org.apache.jackrabbit.oak.segment.file;

import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.SegmentWriterBuilder;
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
