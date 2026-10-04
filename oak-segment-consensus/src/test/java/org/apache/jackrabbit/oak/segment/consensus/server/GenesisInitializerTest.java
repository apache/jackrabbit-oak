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
package org.apache.jackrabbit.oak.segment.consensus.server;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.genesis.CanonicalGenesisContent;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

public class GenesisInitializerTest {
    @Test
    public void existingGenesisIsVerifiedAndLeftUnchanged() throws Exception {
        MemoryNodeStore nodeStore = storeWithGenesis("DO IT LIVE!");
        NodeState before = nodeStore.getRoot();

        new GenesisInitializer(nodeStore, mock(FileStore.class, RETURNS_DEEP_STUBS), null).initializeGenesisContent();

        assertEquals(before, nodeStore.getRoot());
    }

    @Test
    public void tamperedGenesisFailsVerification() throws Exception {
        MemoryNodeStore nodeStore = storeWithGenesis("tampered");

        try {
            new GenesisInitializer(nodeStore, mock(FileStore.class, RETURNS_DEEP_STUBS), null).initializeGenesisContent();
            fail("tampered genesis was accepted");
        } catch (RuntimeException e) {
            assertTrue(e.getCause().getMessage().contains("message"));
        }
    }

    @Test
    public void missingGenesisIsNeverCreatedOutsideTheConsensusLog() {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        NodeState before = nodeStore.getRoot();

        new GenesisInitializer(nodeStore, mock(FileStore.class, RETURNS_DEEP_STUBS), null).initializeGenesisContent();

        assertFalse(getGenesisNode(nodeStore.getRoot()).exists());
        assertEquals(before, nodeStore.getRoot());
    }

    private static MemoryNodeStore storeWithGenesis(String message) throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder root = nodeStore.getRoot().builder();
        new CanonicalGenesisContent(nodeStore, null).populate(root, 42L, "http://leader:8090");
        NodeBuilder genesis = root;
        for (String name : CanonicalGenesisContent.getGenesisPath().substring(1).split("/")) {
            genesis = genesis.getChildNode(name);
        }
        genesis.setProperty("message", message);
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return nodeStore;
    }

    private static NodeState getGenesisNode(NodeState root) {
        return CanonicalGenesisContent.getGenesisNode(root);
    }
}
