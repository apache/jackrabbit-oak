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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.genesis.CanonicalGenesisContent;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AeronGenesisInitializerTest {

    @Test
    public void initializeGenesisContentUsesReplicatedTimestampAndValidator() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn("head-1");

        AeronGenesisInitializer initializer = new AeronGenesisInitializer(fileStore, nodeStore, null);
        String proposalJson = AeronGenesisInitializer.GenesisProposal
            .create(123456789L, "http://leader:8090")
            .toJson();

        initializer.initializeGenesisContent(proposalJson);

        NodeState genesis = getGenesisNode(nodeStore.getRoot());
        NodeState imageContent = genesis.getChildNode("do-it-live.jpeg").getChildNode("jcr:content");
        NodeState ipfs = genesis.getChildNode("ipfs");
        NodeState boldBets = genesis.getChildNode("bold-bets");
        NodeState apiDiscovery = genesis.getChildNode("api").getChildNode("discovery");
        NodeState troubleshooting = genesis.getChildNode("troubleshooting").getChildNode("common-issues");

        assertTrue(genesis.exists());
        assertEquals(Long.valueOf(123456789L), genesis.getProperty("genesisTimestamp").getValue(Type.LONG));
        assertEquals("http://leader:8090", genesis.getProperty("genesisValidator").getValue(Type.STRING));
        assertEquals(CanonicalGenesisContent.getGenesisPath(), genesis.getProperty("canonicalGenesisPath").getValue(Type.STRING));
        assertEquals("Live validator surface manifest", apiDiscovery.getProperty("GET_v1_index").getValue(Type.STRING));
        assertEquals("NOT_LEADER: Resolve the leader via /v1/consensus/leader and retry against that validator.",
            troubleshooting.getProperty("issue-not-leader").getValue(Type.STRING));
        assertNotNull(imageContent.getProperty("jcr:data").getValue(Type.BINARY));
        assertEquals(Boolean.FALSE, ipfs.getProperty("enabled").getValue(Type.BOOLEAN));
        assertEquals("Developers will choose systems with stronger guarantees over familiar platforms.",
            boldBets.getProperty("bet-5").getValue(Type.STRING));
    }

    @Test
    public void genesisRecordsTheAppliedLogPositionInTheSameMerge() {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        AeronGenesisInitializer initializer =
            new AeronGenesisInitializer(mock(FileStore.class, RETURNS_DEEP_STUBS), nodeStore, null);

        initializer.initializeGenesisContent(
            AeronGenesisInitializer.GenesisProposal.create(42L, "http://leader:8090").toJson(),
            new AppliedLogPosition(256L, 0, 0L));

        assertTrue(getGenesisNode(nodeStore.getRoot()).exists());
        assertEquals(new AppliedLogPosition(256L, 0, 0L), AppliedLogPosition.read(nodeStore.getRoot()));
    }

    @Test
    public void initializeGenesisContentStoresBlobMetadataWhenBlobStoreIsConfigured() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        BlobStore blobStore = mock(BlobStore.class);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn("head-2");
        when(blobStore.writeBlob(any())).thenReturn("QmDeterministicGenesis#1024");

        AeronGenesisInitializer initializer = new AeronGenesisInitializer(fileStore, nodeStore, blobStore);
        initializer.initializeGenesisContent(
            AeronGenesisInitializer.GenesisProposal.create(42L, "https://validator.example:8090").toJson()
        );

        NodeState genesis = getGenesisNode(nodeStore.getRoot());
        NodeState imageContent = genesis.getChildNode("do-it-live.jpeg").getChildNode("jcr:content");
        NodeState ipfs = genesis.getChildNode("ipfs");

        assertEquals("QmDeterministicGenesis#1024", imageContent.getProperty("jcr:blobId").getValue(Type.STRING));
        assertEquals("QmDeterministicGenesis", ipfs.getProperty("genesisImageCid").getValue(Type.STRING));
        assertEquals(Boolean.TRUE, ipfs.getProperty("enabled").getValue(Type.BOOLEAN));
        verify(blobStore, times(1)).writeBlob(any());
    }

    @Test
    public void initializeGenesisContentIsIdempotentAfterFirstCommit() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        BlobStore blobStore = mock(BlobStore.class);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn("head-3");
        when(blobStore.writeBlob(any())).thenReturn("QmGenesis#512");

        AeronGenesisInitializer initializer = new AeronGenesisInitializer(fileStore, nodeStore, blobStore);
        initializer.initializeGenesisContent(
            AeronGenesisInitializer.GenesisProposal.create(10L, "http://leader-0:8090").toJson()
        );
        initializer.initializeGenesisContent(
            AeronGenesisInitializer.GenesisProposal.create(20L, "http://leader-1:8090").toJson()
        );

        NodeState genesis = getGenesisNode(nodeStore.getRoot());

        assertEquals(Long.valueOf(10L), genesis.getProperty("genesisTimestamp").getValue(Type.LONG));
        assertEquals("http://leader-0:8090", genesis.getProperty("genesisValidator").getValue(Type.STRING));
        verify(blobStore, times(1)).writeBlob(any());
    }

    private static NodeState getGenesisNode(NodeState root) {
        return CanonicalGenesisContent.getGenesisNode(root);
    }
}
