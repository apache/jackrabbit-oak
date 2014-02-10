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
package org.apache.jackrabbit.oak.plugins.segment;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class CheckpointTest {

    @Test
    public void testCheckpoint() throws CommitFailedException {
        SegmentNodeStore store = new SegmentNodeStore(new MemoryStore());
        addTestNode(store, "test-checkpoint");
        verifyNS(store, true);
        rmTestNode(store, "test-checkpoint");
        verifyNS(store, false);

        // gc?
        store.retrieve(new SegmentIdFactory().newDataSegmentId().toString());
    }

    private static void verifyNS(SegmentNodeStore store, boolean exists) {
        String cp = store.checkpoint(TimeUnit.HOURS.toMillis(1));
        assertNotNull("Checkpoint must not be null", cp);

        NodeState cpns = store.retrieve(cp);
        assertNotNull(cpns);
        if (exists) {
            assertTrue("Node doesn't exist in checkpoint",
                    cpns.getChildNode("test-checkpoint").exists());
        } else {
            assertFalse("Node shouldn't exist in checkpoint", cpns
                    .getChildNode("test-checkpoint").exists());
        }
    }

    private static void addTestNode(NodeStore store, String name)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(name);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void rmTestNode(NodeStore store, String name)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(name).remove();
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
