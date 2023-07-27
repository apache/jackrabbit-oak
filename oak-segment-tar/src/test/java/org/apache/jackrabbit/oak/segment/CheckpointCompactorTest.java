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

package org.apache.jackrabbit.oak.segment;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.addTestContent;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.assertSameRecord;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.assertSameStableId;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.checkGeneration;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.createCompactor;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.getCheckpoint;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CheckpointCompactorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore fileStore;

    private SegmentNodeStore nodeStore;

    private CheckpointCompactor compactor;

    private GCGeneration compactedGeneration;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        fileStore = fileStoreBuilder(folder.getRoot()).build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        compactedGeneration = newGCGeneration(1,1, true);
        compactor = createCompactor(fileStore, compactedGeneration);
    }

    @After
    public void tearDown() {
        fileStore.close();
    }

    @Test
    public void testCompact() throws Exception {
        addTestContent("cp1", nodeStore, 42);
        String cp1 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp2", nodeStore, 42);
        String cp2 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted1 = fileStore.getHead();
        SegmentNodeState compacted1 = compactor.compact(EMPTY_NODE, uncompacted1, EMPTY_NODE, Canceller.newCanceller());
        assertNotNull(compacted1);
        assertFalse(uncompacted1 == compacted1);
        checkGeneration(compacted1, compactedGeneration);

        assertSameStableId(uncompacted1, compacted1);
        assertSameStableId(getCheckpoint(uncompacted1, cp1), getCheckpoint(compacted1, cp1));
        assertSameStableId(getCheckpoint(uncompacted1, cp2), getCheckpoint(compacted1, cp2));
        assertSameRecord(getCheckpoint(compacted1, cp2), compacted1.getChildNode("root"));

        // Simulate a 2nd compaction cycle
        addTestContent("cp3", nodeStore, 42);
        String cp3 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp4", nodeStore, 42);
        String cp4 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted2 = fileStore.getHead();
        SegmentNodeState compacted2 = compactor.compact(uncompacted1, uncompacted2, compacted1, Canceller.newCanceller());
        assertNotNull(compacted2);
        assertFalse(uncompacted2 == compacted2);
        checkGeneration(compacted2, compactedGeneration);

        assertTrue(fileStore.getRevisions().setHead(uncompacted2.getRecordId(), compacted2.getRecordId()));

        assertEquals(uncompacted2, compacted2);
        assertSameStableId(uncompacted2, compacted2);
        assertSameStableId(getCheckpoint(uncompacted2, cp1), getCheckpoint(compacted2, cp1));
        assertSameStableId(getCheckpoint(uncompacted2, cp2), getCheckpoint(compacted2, cp2));
        assertSameStableId(getCheckpoint(uncompacted2, cp3), getCheckpoint(compacted2, cp3));
        assertSameStableId(getCheckpoint(uncompacted2, cp4), getCheckpoint(compacted2, cp4));
        assertSameRecord(getCheckpoint(compacted1, cp1), getCheckpoint(compacted2, cp1));
        assertSameRecord(getCheckpoint(compacted1, cp2), getCheckpoint(compacted2, cp2));
        assertSameRecord(getCheckpoint(compacted2, cp4), compacted2.getChildNode("root"));
    }
}
