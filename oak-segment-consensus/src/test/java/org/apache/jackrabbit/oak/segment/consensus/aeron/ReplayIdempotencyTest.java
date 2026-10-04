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
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.segment.consensus.service.DeleteApplicationService;
import org.apache.jackrabbit.oak.segment.consensus.service.FileStoreFlushService;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.apache.jackrabbit.oak.segment.consensus.service.WriteApplicationService;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

/**
 * Restart replays the Aeron log from its start (or from a snapshot) into a store that already holds the
 * effects; entries at or below the store's applied watermark must not be applied again.
 */
public class ReplayIdempotencyTest {

    private static final String WALLET = "0x1234567890abcdef1234567890abcdef12345678";
    private static final String WALLET_PATH = "/oak-chain/12/34/56/" + WALLET;
    private static final String BASE = WALLET_PATH + "/Acme/content/";
    private static final int TERM = 7;
    private static final AeronIngressWritePayloadBuilder BUILDER = new AeronIngressWritePayloadBuilder();

    @Test
    public void writeThenDeleteThenReplayChangesNothing() {
        MemoryNodeStore store = new MemoryNodeStore();
        List<Entry> log = Arrays.asList(
            new Entry(100, 1_000L, BUILDER.buildWriteProposal(WALLET, BASE + "a", "page", "one", "sig", TERM, null, "p-a")),
            new Entry(200, 2_000L, BUILDER.buildDeleteProposal(WALLET, BASE + "a", "sig", TERM, "p-del")));
        new Member(store).apply(log);
        NodeState afterFirstRun = store.getRoot();

        Member restarted = new Member(store).restart();
        restarted.apply(log);

        assertEquals("nothing re-applied", 0, restarted.applies.size());
        assertFalse(node(store, BASE + "a").exists());
        assertEquals(1L, walletLong(store, "totalWrites"));
        assertEquals(1_000L, walletLong(store, "lastWrite"));
        assertEquals(afterFirstRun, store.getRoot());
    }

    @Test
    public void writeWithoutProposalIdIsNotReappliedOnReplay() {
        MemoryNodeStore store = new MemoryNodeStore();
        List<Entry> log = Arrays.asList(
            new Entry(100, 1_000L, BUILDER.buildWriteProposal(WALLET, BASE + "a", "page", "one", "sig", TERM, null, (String) null)));
        new Member(store).apply(log);

        Member restarted = new Member(store).restart();
        restarted.apply(log);

        assertEquals(0, restarted.applies.size());
        assertEquals(1L, walletLong(store, "totalWrites"));
    }

    @Test
    public void crashInsideBatchReappliesOnlyTheRemainingItems() {
        MemoryNodeStore store = new MemoryNodeStore();
        List<QueuedProposal> items = Arrays.asList(
            batchItem("p-0", BASE + "b0"), batchItem("p-1", BASE + "b1"), batchItem("p-2", BASE + "b2"));
        List<Entry> log = Arrays.asList(
            new Entry(100, 1_000L, BUILDER.buildWriteProposal(WALLET, BASE + "a", "page", "one", "sig", TERM, null, "p-a")),
            new Entry(300, 3_000L, BUILDER.buildWriteBatch(items, TERM)));
        Member crashing = new Member(store);
        crashing.crashAfterApplies = 3;
        crashing.apply(log);
        assertFalse("crash happened before the last batch item", node(store, BASE + "b2").exists());
        assertEquals(new AppliedLogPosition(300, 1, TERM), AppliedLogPosition.read(store.getRoot()));

        Member restarted = new Member(store).restart();
        restarted.apply(log);

        assertEquals(Arrays.asList(BASE + "b2"), restarted.applies);
        assertTrue(node(store, BASE + "b2").exists());
        assertEquals(4L, walletLong(store, "totalWrites"));
        assertEquals(new AppliedLogPosition(300, 2, TERM), AppliedLogPosition.read(store.getRoot()));
    }

    @Test
    public void sameLogLeavesIdenticalWatermarkOnEveryMember() {
        List<QueuedProposal> items = Arrays.asList(batchItem("p-0", BASE + "b0"), batchItem("p-1", BASE + "b1"));
        List<Entry> log = Arrays.asList(
            new Entry(100, 1_000L, BUILDER.buildWriteProposal(WALLET, BASE + "a", "page", "one", "sig", TERM, null, "p-a")),
            new Entry(250, 2_000L, BUILDER.buildWriteBatch(items, TERM)),
            new Entry(400, 3_000L, BUILDER.buildDeleteProposal(WALLET, BASE + "a", "sig", TERM, "p-del")));
        MemoryNodeStore memberA = new MemoryNodeStore();
        MemoryNodeStore memberB = new MemoryNodeStore();

        new Member(memberA).apply(log);
        new Member(memberB).apply(log);

        AppliedLogPosition watermark = AppliedLogPosition.read(memberA.getRoot());
        assertEquals(new AppliedLogPosition(400, 0, TERM), watermark);
        assertEquals(watermark, AppliedLogPosition.read(memberB.getRoot()));
        assertEquals(memberA.getRoot(), memberB.getRoot());
    }

    @Test
    public void entriesAfterTheWatermarkAreStillApplied() {
        MemoryNodeStore store = new MemoryNodeStore();
        Entry first = new Entry(100, 1_000L,
            BUILDER.buildWriteProposal(WALLET, BASE + "a", "page", "one", "sig", TERM, null, "p-a"));
        Entry second = new Entry(200, 2_000L,
            BUILDER.buildWriteProposal(WALLET, BASE + "b", "page", "two", "sig", TERM, null, "p-b"));
        new Member(store).apply(Arrays.asList(first));

        Member restarted = new Member(store).restart();
        restarted.apply(Arrays.asList(first, second));

        assertEquals(Arrays.asList(BASE + "b"), restarted.applies);
        assertEquals(2L, walletLong(store, "totalWrites"));
    }

    static QueuedProposal batchItem(String proposalId, String path) {
        QueuedProposal item = ReplicatedCommandRoundTripTest.batchItem(proposalId, path, QueuedProposal.ProposalType.WRITE);
        item.setWalletAddress(WALLET);
        return item;
    }

    private static long walletLong(MemoryNodeStore store, String property) {
        return node(store, WALLET_PATH).getProperty(property).getValue(Type.LONG);
    }

    private static NodeState node(MemoryNodeStore store, String path) {
        NodeState current = store.getRoot();
        for (String part : path.substring(1).split("/")) {
            current = current.getChildNode(part);
        }
        return current;
    }

    private static final class Entry {
        final long position;
        final long timestamp;
        final AeronEncodedMessage message;

        Entry(long position, long timestamp, AeronEncodedMessage message) {
            this.position = position;
            this.timestamp = timestamp;
            this.message = message;
        }
    }

    /** One cluster member: a dispatcher wired to real apply services over its own store. */
    private static final class Member {
        final MemoryNodeStore store;
        final MessageDispatcher dispatcher;
        final List<String> applies = new ArrayList<>();
        int crashAfterApplies = Integer.MAX_VALUE;

        Member(MemoryNodeStore store) {
            this.store = store;
            FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
            FileStoreFlushService flush = mock(FileStoreFlushService.class);
            WriteApplicationService writes = new WriteApplicationService(fileStore, store, null, flush);
            DeleteApplicationService deletes = new DeleteApplicationService(fileStore, store, flush);
            this.dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
                @Override
                public void applyWrite(String walletAddress, String path, String contentType, String message,
                                       String signature, String intentToken, String blobId, String mimeType,
                                       String ipfsCid, MutationAuditMetadata auditMetadata) {
                    record(path);
                    writes.applyWriteWithAuditMetadata(walletAddress, path, contentType, message, signature,
                        intentToken, blobId, mimeType, ipfsCid, auditMetadata);
                }

                @Override
                public void applyDelete(String walletAddress, String path, String signature,
                                        MutationAuditMetadata auditMetadata) {
                    record(path);
                    deletes.applyDeleteWithAuditMetadata(walletAddress, path, signature, auditMetadata);
                }
            });
            this.dispatcher.setTermProvider(() -> TERM);
        }

        Member restart() {
            dispatcher.setReplayFloor(AppliedLogPosition.read(store.getRoot()));
            return this;
        }

        /** Stops at the first failure, as a member does (the failure stops its service and process). */
        void apply(List<Entry> log) {
            try {
                for (Entry entry : log) {
                    dispatcher.dispatch(entry.timestamp, entry.position, entry.message.buffer, 0, entry.message.totalLength);
                }
            } catch (IllegalStateException crashed) {
                assertEquals("simulated crash", crashed.getMessage());
            }
        }

        private void record(String path) {
            if (applies.size() >= crashAfterApplies) {
                throw new IllegalStateException("simulated crash");
            }
            applies.add(path);
        }
    }
}
