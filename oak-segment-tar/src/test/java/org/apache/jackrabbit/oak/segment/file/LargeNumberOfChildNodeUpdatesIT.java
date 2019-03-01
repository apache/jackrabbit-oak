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
 *
 */

package org.apache.jackrabbit.oak.segment.file;

import static java.lang.System.getProperty;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.segment.DefaultSegmentWriter;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This test asserts that a large number of child nodes can be added in a single
 * transaction. Due to its long running time the test needs to be explicitly enabled
 * via {@code -Dtest=LargeNumberOfChildNodeUpdatesIT}.
 * Used {@code -DLargeNumberOfChildNodeUpdatesIT.child-count=<int>} to control the number
 * of child nodes used by this test. Default is 5000000.
 */
public class LargeNumberOfChildNodeUpdatesIT {

    /** Only run if explicitly asked to via -Dtest=LargeNumberOfChildNodeUpdatesIT */
    private static final boolean ENABLED =
            LargeNumberOfChildNodeUpdatesIT.class.getSimpleName().equals(getProperty("test"));

    private static final int NODE_COUNT = Integer
            .getInteger("LargeNumberOfChildNodeUpdatesIT.child-count", 5000000);

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        assumeTrue(ENABLED);
    }

    @Test
    public void testNode() throws IOException, InvalidFileStoreVersionException {
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(folder.getRoot()).build()) {
            DefaultSegmentWriter writer = defaultSegmentWriterBuilder("test")
                    .withGeneration(GCGeneration.newGCGeneration(1, 1, false))
                    .build(fileStore);

            SegmentNodeState root = fileStore.getHead();
            SegmentNodeBuilder builder = root.builder();
            for (int k = 0; k < NODE_COUNT; k++) {
                builder.setChildNode("n-" + k);
            }

            SegmentNodeState node1 = builder.getNodeState();
            RecordId nodeId = writer.writeNode(node1);
            SegmentNodeState node2 = fileStore.getReader().readNode(nodeId);

            assertNotEquals(node1.getRecordId(), node2.getRecordId());
            assertEquals(node1, node2);
        }
    }

}
