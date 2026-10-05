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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

/**
 * A DELETE packed into a write batch must leave the same head state as a DELETE sent alone.
 */
public class MixedWriteDeleteBatchApplyTest {

    private static final String WALLET = "0x1234567890abcdef1234567890abcdef12345678";
    private static final String BASE = "/oak-chain/12/34/56/" + WALLET + "/Acme/content/";

    @Test
    public void deleteInBatchRemovesNodeFromHead() {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        FileStoreFlushService flush = mock(FileStoreFlushService.class);
        WriteApplicationService writes = new WriteApplicationService(fileStore, nodeStore, null, flush);
        DeleteApplicationService deletes = new DeleteApplicationService(fileStore, nodeStore, flush);
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
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

        AeronEncodedMessage batch = new AeronIngressWritePayloadBuilder().buildWriteBatch(Arrays.asList(
            item("pid-a", "doc-a", QueuedProposal.ProposalType.WRITE),
            item("pid-p", "doc-p", QueuedProposal.ProposalType.WRITE),
            item("pid-del-p", "doc-p", QueuedProposal.ProposalType.DELETE)), 7);
        assertTrue(dispatcher.dispatch(1L, batch.buffer, 0, batch.totalLength));

        assertTrue(nodeAt(nodeStore, BASE + "doc-a").exists());
        assertFalse(nodeAt(nodeStore, BASE + "doc-p").exists());
    }

    private static QueuedProposal item(String proposalId, String name, QueuedProposal.ProposalType type) {
        QueuedProposal p = ReplicatedCommandRoundTripTest.batchItem(proposalId, BASE + name, type);
        p.setWalletAddress(WALLET);
        return p;
    }

    private static NodeState nodeAt(MemoryNodeStore nodeStore, String path) {
        NodeState current = nodeStore.getRoot();
        for (String part : path.substring(1).split("/")) {
            current = current.getChildNode(part);
        }
        return current;
    }
}
