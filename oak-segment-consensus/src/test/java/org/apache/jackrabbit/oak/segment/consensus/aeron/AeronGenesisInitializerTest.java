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

import java.io.IOException;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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

    @Test
    public void genesisCreatesShardedContentWithDefaultNetworkInfo() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn("head-10");

        new AeronGenesisInitializer(fileStore, nodeStore, null)
            .initializeGenesisContent(AeronGenesisInitializer.GenesisProposal.create(42L, null).toJson());

        NodeState genesis = getGenesisNode(nodeStore.getRoot());
        NodeState protocol = genesis.getChildNode("protocol");
        NodeState contract = genesis.getChildNode("content-contract");
        NodeState gettingStarted = genesis.getChildNode("getting-started");
        NodeState apiConsensus = genesis.getChildNode("api").getChildNode("consensus");
        NodeState imageContent = genesis.getChildNode("do-it-live.jpeg").getChildNode("jcr:content");
        NodeState ipfs = genesis.getChildNode("ipfs");

        assertTrue(genesis.exists());
        assertEquals("DO IT LIVE!", genesis.getProperty("message").getValue(Type.STRING));
        assertEquals("oak-blockchain-aem", genesis.getProperty("chainId").getValue(Type.STRING));
        assertEquals(CanonicalGenesisContent.getGenesisPath(), genesis.getProperty("canonicalGenesisPath").getValue(Type.STRING));
        assertEquals("http://localhost:8090", protocol.getProperty("genesisValidator").getValue(Type.STRING));
        assertEquals("localhost", protocol.getProperty("genesisHost").getValue(Type.STRING));
        assertEquals("Below /content the shape is intentionally open and may evolve.",
            contract.getProperty("contentShapeStatus").getValue(Type.STRING));
        assertEquals("GET /v1/explorer/content/nav and pick a clusterId.",
            gettingStarted.getChildNode("3-browse-genesis").getProperty("step-1").getValue(Type.STRING));
        assertEquals("Consensus status and cluster health",
            apiConsensus.getProperty("GET_v1_consensus_status").getValue(Type.STRING));
        assertTrue(imageContent.getProperty("jcr:data").getValue(Type.BINARY) instanceof org.apache.jackrabbit.oak.api.Blob);
        assertEquals(false, ipfs.getProperty("enabled").getValue(Type.BOOLEAN));
    }

    @Test
    public void genesisUsesBlobStoreAndSkipsRecreatingExistingGenesis() throws Exception {
        String genesisBlobId = "QmYwAPJzv5CZsnAzt8auVZRnGi2C4gYQqbiZ9erjRzCQXD#1024";
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        BlobStore blobStore = mock(BlobStore.class);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn("head-10");
        when(blobStore.writeBlob(any())).thenReturn(genesisBlobId);
        AeronGenesisInitializer initializer = new AeronGenesisInitializer(fileStore, nodeStore, blobStore);
        String proposal = AeronGenesisInitializer.GenesisProposal.create(42L, "https://validator.example:8090").toJson();

        initializer.initializeGenesisContent(proposal);
        initializer.initializeGenesisContent(proposal);

        NodeState genesis = getGenesisNode(nodeStore.getRoot());
        NodeState protocol = genesis.getChildNode("protocol");
        NodeState imageContent = genesis.getChildNode("do-it-live.jpeg").getChildNode("jcr:content");
        NodeState ipfs = genesis.getChildNode("ipfs");

        assertEquals("https://validator.example:8090", protocol.getProperty("genesisValidator").getValue(Type.STRING));
        assertEquals("validator.example", protocol.getProperty("genesisHost").getValue(Type.STRING));
        assertEquals(genesisBlobId, imageContent.getProperty("jcr:blobId").getValue(Type.STRING));
        assertEquals("QmYwAPJzv5CZsnAzt8auVZRnGi2C4gYQqbiZ9erjRzCQXD", ipfs.getProperty("genesisImageCid").getValue(Type.STRING));
        assertTrue(ipfs.getProperty("enabled").getValue(Type.BOOLEAN));
        verify(blobStore, times(1)).writeBlob(any());
    }

    @Test
    public void genesisContentDoesNotDependOnTheJvmTimeZone() {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            NodeState utc = applyGenesis(1_700_000_000_000L);
            TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
            NodeState losAngeles = applyGenesis(1_700_000_000_000L);

            assertEquals(utc, losAngeles);
            NodeState genesis = getGenesisNode(losAngeles);
            assertEquals("Tue Nov 14 22:13:20 UTC 2023", genesis.getProperty("genesisDate").getValue(Type.STRING));
            assertEquals("Tue Nov 14 22:13:20 UTC 2023",
                genesis.getChildNode("protocol").getProperty("genesisDate").getValue(Type.STRING));
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void genesisBlobFailureIsNotSwallowed() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.writeBlob(any())).thenThrow(new IOException("IPFS unreachable"));
        AeronGenesisInitializer initializer =
            new AeronGenesisInitializer(mock(FileStore.class, RETURNS_DEEP_STUBS), nodeStore, blobStore);

        try {
            initializer.initializeGenesisContent(
                AeronGenesisInitializer.GenesisProposal.create(42L, "http://leader:8090").toJson(),
                new AppliedLogPosition(256L, 0, 0L));
            fail("this member carried on without genesis");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IOException);
        }
        assertFalse(getGenesisNode(nodeStore.getRoot()).exists());
        assertEquals(AppliedLogPosition.NONE, AppliedLogPosition.read(nodeStore.getRoot()));
    }

    private static NodeState applyGenesis(long timestamp) {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        new AeronGenesisInitializer(mock(FileStore.class, RETURNS_DEEP_STUBS), nodeStore, null)
            .initializeGenesisContent(AeronGenesisInitializer.GenesisProposal.create(timestamp, "http://leader:8090").toJson(),
                new AppliedLogPosition(256L, 0, 0L));
        return nodeStore.getRoot();
    }

    private static NodeState getGenesisNode(NodeState root) {
        return CanonicalGenesisContent.getGenesisNode(root);
    }
}
