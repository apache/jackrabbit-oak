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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.queue.QueuedProposal;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.segment.consensus.service.DeleteApplicationService;
import org.apache.jackrabbit.oak.segment.consensus.service.FileStoreFlushService;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.apache.jackrabbit.oak.segment.consensus.service.WriteApplicationService;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

/**
 * Invalid input is rejected the same way on every member; a member whose own environment fails must not go on
 * applying the log as if the entry had been applied.
 */
public class ApplyFailureClassificationTest {

    private static final String WALLET = "0x1234567890abcdef1234567890abcdef12345678";
    private static final String BASE = "/oak-chain/12/34/56/" + WALLET + "/Acme/content/";
    private static final int TERM = 3;
    private static final long POSITION = 640L;

    @Test
    public void invalidBatchItemIsRejectedOnEveryMemberAndTheRestOfTheBatchApplies() {
        AeronEncodedMessage batch = new AeronIngressWritePayloadBuilder().buildWriteBatch(Arrays.asList(
            item("p-0", BASE + "a"), item("p-1", "/oak-chain/bad"), item("p-2", BASE + "c")), TERM);
        MemoryNodeStore memberA = new MemoryNodeStore();
        MemoryNodeStore memberB = new MemoryNodeStore();

        dispatcher(memberA).dispatch(1_000L, POSITION, batch.buffer, 0, batch.totalLength);
        dispatcher(memberB).dispatch(1_000L, POSITION, batch.buffer, 0, batch.totalLength);

        assertTrue(node(memberA, BASE + "a").exists());
        assertTrue("items after the rejected one still apply", node(memberA, BASE + "c").exists());
        assertEquals(new AppliedLogPosition(POSITION, 2, TERM), AppliedLogPosition.read(memberA.getRoot()));
        assertEquals(memberA.getRoot(), memberB.getRoot());
    }

    @Test
    public void nodeLocalMergeFailureIsNotSwallowedAndStopsTheBatch() {
        AeronEncodedMessage batch = new AeronIngressWritePayloadBuilder().buildWriteBatch(Arrays.asList(
            item("p-0", BASE + "a"), item("p-1", BASE + "b"), item("p-2", BASE + "c")), TERM);
        MemoryNodeStore diskFullOnSecondMerge = new MemoryNodeStore() {
            private int merges;

            @Override
            public synchronized NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info)
                    throws CommitFailedException {
                if (++merges == 2) {
                    throw new CommitFailedException(CommitFailedException.OAK, 1, "flush failed",
                        new IOException("No space left on device"));
                }
                return super.merge(builder, commitHook, info);
            }
        };

        try {
            dispatcher(diskFullOnSecondMerge).dispatch(1_000L, POSITION, batch.buffer, 0, batch.totalLength);
            fail("a node-local merge failure was swallowed");
        } catch (RuntimeException e) {
            assertTrue(hasCause(e, IOException.class));
        }

        assertTrue(node(diskFullOnSecondMerge, BASE + "a").exists());
        assertFalse(node(diskFullOnSecondMerge, BASE + "b").exists());
        assertFalse("nothing after the failed item is applied", node(diskFullOnSecondMerge, BASE + "c").exists());
        assertEquals(new AppliedLogPosition(POSITION, 0, TERM), AppliedLogPosition.read(diskFullOnSecondMerge.getRoot()));
    }

    private static MessageDispatcher dispatcher(MemoryNodeStore store) {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        FileStoreFlushService flush = mock(FileStoreFlushService.class);
        WriteApplicationService writes = new WriteApplicationService(fileStore, store, null, flush);
        DeleteApplicationService deletes = new DeleteApplicationService(fileStore, store, flush);
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
        dispatcher.setTermProvider(() -> TERM);
        return dispatcher;
    }

    private static QueuedProposal item(String proposalId, String path) {
        QueuedProposal item = ReplicatedCommandRoundTripTest.batchItem(proposalId, path, QueuedProposal.ProposalType.WRITE);
        item.setWalletAddress(WALLET);
        return item;
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    private static NodeState node(MemoryNodeStore store, String path) {
        NodeState current = store.getRoot();
        for (String part : path.substring(1).split("/")) {
            current = current.getChildNode(part);
        }
        return current;
    }
}
