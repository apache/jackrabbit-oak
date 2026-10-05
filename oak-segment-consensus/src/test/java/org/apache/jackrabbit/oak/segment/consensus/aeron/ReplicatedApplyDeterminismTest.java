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
import org.apache.jackrabbit.oak.segment.consensus.queue.QueuedProposal;
import org.apache.jackrabbit.oak.segment.consensus.service.DeleteApplicationService;
import org.apache.jackrabbit.oak.segment.consensus.service.FileStoreFlushService;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.apache.jackrabbit.oak.segment.consensus.service.WriteApplicationService;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

/**
 * The same replicated log applied on two members, at different wall-clock times,
 * must produce identical repository content.
 */
public class ReplicatedApplyDeterminismTest {

    private static final String WALLET = "0x1234567890abcdef1234567890abcdef12345678";
    private static final String BASE = "/oak-chain/12/34/56/" + WALLET + "/Acme/content/";
    private static final long T1 = 1_000_000L;
    private static final long T2 = 2_000_000L;
    private static final long T3 = 3_000_000L;
    private static final long T4 = 4_000_000L;

    @Test
    public void sameLogProducesIdenticalStateUnderDifferentWallClocks() throws Exception {
        MemoryNodeStore nodeA = applyLog();
        Thread.sleep(5);
        MemoryNodeStore nodeB = applyLog();

        assertEquals(nodeA.getRoot(), nodeB.getRoot());

        NodeState wallet = nodeAt(nodeA, "/oak-chain/12/34/56/" + WALLET);
        assertEquals(T1, (long) wallet.getProperty("walletCreated").getValue(Type.LONG));
        assertEquals(T3, (long) wallet.getProperty("lastWrite").getValue(Type.LONG));
        assertEquals(T1, (long) nodeAt(nodeA, BASE + "doc-1").getProperty("timestamp").getValue(Type.LONG));
        assertEquals(T3, (long) nodeAt(nodeA, BASE + "doc-2").getProperty("timestamp").getValue(Type.LONG));
        assertEquals(T3, (long) nodeAt(nodeA, BASE + "doc-3").getProperty("timestamp").getValue(Type.LONG));
        assertTrue(nodeAt(nodeA, BASE + "doc-1").exists());
    }

    private static MemoryNodeStore applyLog() {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        MessageDispatcher dispatcher = dispatcherFor(nodeStore);
        AeronIngressWritePayloadBuilder builder = new AeronIngressWritePayloadBuilder();

        dispatch(dispatcher, T1, builder.buildWriteProposal(WALLET, BASE + "doc-1", "page", "one", "sig-1", 7, null, "p1"));
        dispatch(dispatcher, T2, builder.buildWriteProposal(WALLET, BASE + "doc-x", "page", "x", "sig-x", 7, null, "px"));
        List<QueuedProposal> batch = Arrays.asList(
            ReplicatedCommandRoundTripTest.batchItem("p2", BASE + "doc-2", QueuedProposal.ProposalType.WRITE),
            ReplicatedCommandRoundTripTest.batchItem("p3", BASE + "doc-3", QueuedProposal.ProposalType.WRITE));
        batch.forEach(p -> p.setWalletAddress(WALLET));
        dispatch(dispatcher, T3, builder.buildWriteBatch(batch, 7));
        dispatch(dispatcher, T4, builder.buildDeleteProposal(WALLET, BASE + "doc-x", "sig-del", 7, "pdel"));
        return nodeStore;
    }

    private static void dispatch(MessageDispatcher dispatcher, long clusterTimestamp, AeronEncodedMessage encoded) {
        assertTrue(dispatcher.dispatch(clusterTimestamp, encoded.buffer, 0, encoded.totalLength));
    }

    private static MessageDispatcher dispatcherFor(MemoryNodeStore nodeStore) {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        FileStoreFlushService flush = mock(FileStoreFlushService.class);
        WriteApplicationService writes = new WriteApplicationService(fileStore, nodeStore, null, flush);
        DeleteApplicationService deletes = new DeleteApplicationService(fileStore, nodeStore, flush);
        return new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message,
                                   String signature, String intentToken, String blobId, String mimeType,
                                   String ipfsCid, MutationAuditMetadata auditMetadata) {
                writes.applyWriteWithAuditMetadata(walletAddress, path, contentType, message, signature,
                    intentToken, blobId, mimeType, ipfsCid, auditMetadata);
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature,
                                    MutationAuditMetadata auditMetadata) {
                deletes.applyDeleteWithAuditMetadata(walletAddress, path, signature, auditMetadata);
            }
        });
    }

    private static NodeState nodeAt(MemoryNodeStore nodeStore, String path) {
        NodeState current = nodeStore.getRoot();
        for (String part : path.substring(1).split("/")) {
            current = current.getChildNode(part);
        }
        return current;
    }
}
