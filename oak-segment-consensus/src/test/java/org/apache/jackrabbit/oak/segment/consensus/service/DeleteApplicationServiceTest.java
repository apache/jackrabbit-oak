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
package org.apache.jackrabbit.oak.segment.consensus.service;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

public class DeleteApplicationServiceTest {

    private static final String WALLET = "0x1234567890abcdef1234567890abcdef12345678";
    private static final String EXISTING_PATH = "/oak-chain/aa/bb/cc/" + WALLET + "/Acme/content/doc-1";
    private static final String MISSING_TARGET_PATH = "/oak-chain/aa/bb/cc/" + WALLET + "/Acme/content/doc-2";
    private static final String MISSING_BRANCH_PATH = "/oak-chain/aa/bb/cc/" + WALLET + "/Missing/content/doc-9";
    private static final String LARGE_DELETE_ROOT_PATH = "/oak-chain/aa/bb/cc/" + WALLET + "/Acme/content/wallet-root";
    private static final String LARGE_DELETE_DESCENDANT_PATH = LARGE_DELETE_ROOT_PATH + "/branch-99/leaf-99";
    private static final String LARGE_DELETE_SIBLING_PATH = "/oak-chain/aa/bb/cc/" + WALLET + "/Acme/content/keep-me";

    @Test
    public void testApplyDeleteRemovesNodeAndInvokesCallbacks() throws Exception {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = seededNodeStore(EXISTING_PATH);
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        doAnswer(invocation -> {
            Runnable callback = invocation.getArgument(0);
            if (callback != null) {
                callback.run();
            }
            return true;
        }).when(flushService).onChangeApplied(any());
        DeleteApplicationService service = new DeleteApplicationService(fileStore, nodeStore, flushService);

        AtomicReference<String> updatedHead = new AtomicReference<>();
        AtomicReference<String> durableProposal = new AtomicReference<>();
        AtomicReference<String> durableHead = new AtomicReference<>();
        AtomicReference<String> ssePayload = new AtomicReference<>();
        service.setHeadUpdateCallback(updatedHead::set);
        service.setDurabilityCallback(new DeleteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHeadValue) {
                durableProposal.set(proposalId);
                durableHead.set(durableHeadValue);
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });
        service.setSseEventCallback((path, wallet, org, signature) ->
            ssePayload.set(path + "|" + wallet + "|" + org + "|" + signature));

        String newHead = service.applyDelete(WALLET, EXISTING_PATH, "0xsig", "proposal-1");

        assertEquals("new-head", newHead);
        assertEquals("new-head", updatedHead.get());
        assertEquals("proposal-1", durableProposal.get());
        assertEquals("new-head", durableHead.get());
        assertEquals(EXISTING_PATH + "|" + WALLET + "|Acme|0xsig", ssePayload.get());
        verify(flushService).onChangeApplied(any());
        assertFalse(nodeAt(nodeStore, EXISTING_PATH).exists());
    }

    @Test
    public void testApplyDeleteWithAuditMetadataStillAppliesWhenEpochFieldsAbsent() throws Exception {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = seededNodeStore(EXISTING_PATH);
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        doAnswer(invocation -> {
            Runnable callback = invocation.getArgument(0);
            if (callback != null) {
                callback.run();
            }
            return true;
        }).when(flushService).onChangeApplied(any());
        DeleteApplicationService service = new DeleteApplicationService(fileStore, nodeStore, flushService);

        AtomicReference<String> durableProposal = new AtomicReference<>();
        service.setDurabilityCallback(new DeleteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHeadValue) {
                durableProposal.set(proposalId);
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });

        String newHead = service.applyDeleteWithAuditMetadata(
            WALLET,
            EXISTING_PATH,
            "0xsig",
            MutationAuditMetadata.delete("tx-1", "corr-1", "proposal-audit", "0xeth", null, null, null)
        );

        assertEquals("new-head", newHead);
        assertEquals("proposal-audit", durableProposal.get());
        assertFalse(nodeAt(nodeStore, EXISTING_PATH).exists());
        verify(flushService).onChangeApplied(any());
    }

    @Test
    public void testApplyDeleteDeliversDurabilityCallbackWhenDeferredFlushCompletes() throws Exception {
        FileStore fileStore = fileStoreWithHeads("prev-head", "current-head");
        MemoryNodeStore nodeStore = seededNodeStore(EXISTING_PATH);
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        AtomicReference<Runnable> deferredFlush = new AtomicReference<>();
        doAnswer(invocation -> {
            deferredFlush.set(invocation.getArgument(0));
            return false;
        }).when(flushService).onChangeApplied(any());
        DeleteApplicationService service = new DeleteApplicationService(fileStore, nodeStore, flushService);

        AtomicReference<String> durableProposal = new AtomicReference<>();
        AtomicReference<String> durableHead = new AtomicReference<>();
        service.setDurabilityCallback(new DeleteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHeadValue) {
                durableProposal.set(proposalId);
                durableHead.set(durableHeadValue);
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });

        String newHead = service.applyDelete(WALLET, EXISTING_PATH, "0xsig", "proposal-deferred");

        assertEquals("current-head", newHead);
        assertNull(durableProposal.get());
        assertNull(durableHead.get());
        assertTrue(deferredFlush.get() != null);
        deferredFlush.get().run();
        assertEquals("proposal-deferred", durableProposal.get());
        assertEquals("current-head", durableHead.get());
        verify(flushService).onChangeApplied(any());
    }

    @Test
    public void testApplyDeleteIsIdempotentWhenParentPathMissing() throws Exception {
        FileStore fileStore = fileStoreWithHeads("prev-head", "current-head");
        MemoryNodeStore nodeStore = seededNodeStore(EXISTING_PATH);
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        DeleteApplicationService service = new DeleteApplicationService(fileStore, nodeStore, flushService);

        AtomicReference<String> durableProposal = new AtomicReference<>();
        AtomicReference<String> durableHead = new AtomicReference<>();
        service.setDurabilityCallback(new DeleteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHeadValue) {
                durableProposal.set(proposalId);
                durableHead.set(durableHeadValue);
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });

        String newHead = service.applyDelete(WALLET, MISSING_BRANCH_PATH, "0xsig", "proposal-missing-branch");

        assertNull(newHead);
        assertEquals("proposal-missing-branch", durableProposal.get());
        assertEquals("current-head", durableHead.get());
    }

    @Test
    public void testApplyDeleteIsIdempotentWhenTargetNodeAlreadyMissing() throws Exception {
        FileStore fileStore = fileStoreWithHeads("prev-head", "current-head");
        MemoryNodeStore nodeStore = seededNodeStore("/oak-chain/aa/bb/cc/" + WALLET + "/Acme/content");
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        DeleteApplicationService service = new DeleteApplicationService(fileStore, nodeStore, flushService);

        AtomicReference<String> durableProposal = new AtomicReference<>();
        AtomicReference<String> durableHead = new AtomicReference<>();
        service.setDurabilityCallback(new DeleteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHeadValue) {
                durableProposal.set(proposalId);
                durableHead.set(durableHeadValue);
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });

        String newHead = service.applyDelete(WALLET, MISSING_TARGET_PATH, "0xsig", "proposal-missing-target");

        assertNull(newHead);
        assertEquals("proposal-missing-target", durableProposal.get());
        assertEquals("current-head", durableHead.get());
    }

    @Test
    public void testApplyDeleteReportsDurabilityFailureForInvalidPath() {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        DeleteApplicationService service = new DeleteApplicationService(fileStore, nodeStore, flushService);

        AtomicReference<String> failedProposal = new AtomicReference<>();
        AtomicReference<String> failureMessage = new AtomicReference<>();
        service.setDurabilityCallback(new DeleteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHead) {
            }

            @Override
            public void onFailure(String proposalId, String error) {
                failedProposal.set(proposalId);
                failureMessage.set(error);
            }
        });

        try {
            service.applyDelete(WALLET, "invalid", "0xsig", "proposal-fail");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Failed to apply replicated delete"));
        }

        assertEquals("proposal-fail", failedProposal.get());
        assertTrue(failureMessage.get().contains("Invalid path format"));
    }

    @Test
    public void testApplyDeleteRemovesLargeSubtreeInSingleOperation() throws Exception {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = seededLargeDeleteNodeStore();
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        doAnswer(invocation -> true).when(flushService).onChangeApplied(any());
        DeleteApplicationService service = new DeleteApplicationService(fileStore, nodeStore, flushService);

        assertTrue(nodeAt(nodeStore, LARGE_DELETE_DESCENDANT_PATH).exists());
        assertTrue(nodeAt(nodeStore, LARGE_DELETE_SIBLING_PATH).exists());

        String newHead = service.applyDelete(WALLET, LARGE_DELETE_ROOT_PATH, "0xsig", "proposal-large-delete");

        assertEquals("new-head", newHead);
        assertFalse(nodeAt(nodeStore, LARGE_DELETE_ROOT_PATH).exists());
        assertFalse(nodeAt(nodeStore, LARGE_DELETE_DESCENDANT_PATH).exists());
        assertTrue(nodeAt(nodeStore, LARGE_DELETE_SIBLING_PATH).exists());
        verify(flushService).onChangeApplied(any());
    }

    @Test
    public void deleteDecrementsWalletContentCountOnlyWhenANodeIsRemoved() {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        WriteApplicationService writes = new WriteApplicationService(fileStore, nodeStore, null, flushService);
        DeleteApplicationService deletes = new DeleteApplicationService(fileStore, nodeStore, flushService);
        String content = "/oak-chain/aa/bb/cc/" + WALLET + "/Acme/content/";
        for (int i = 1; i <= 3; i++) {
            writes.applyWrite(WALLET, content + "doc-" + i, "page", "m", "0xsig", null, null, null, null, "p-" + i);
        }

        deletes.applyDelete(WALLET, content + "doc-1", "0xsig", "d-1");
        assertEquals(2L, walletLong(nodeStore, "contentCount"));
        assertEquals("totalWrites counts writes ever applied", 3L, walletLong(nodeStore, "totalWrites"));

        deletes.applyDelete(WALLET, content + "doc-1", "0xsig", "d-2");
        assertEquals(2L, walletLong(nodeStore, "contentCount"));
    }

    @Test
    public void deleteNeverTakesContentCountBelowZero() throws Exception {
        MemoryNodeStore nodeStore = seededNodeStore(EXISTING_PATH);
        NodeBuilder root = nodeStore.getRoot().builder();
        root.getChildNode("oak-chain").getChildNode("aa").getChildNode("bb").getChildNode("cc").getChildNode(WALLET)
            .setProperty("contentCount", 0L);
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new DeleteApplicationService(fileStoreWithHeads("prev-head", "new-head"), nodeStore,
            mock(FileStoreFlushService.class)).applyDelete(WALLET, EXISTING_PATH, "0xsig", "d-1");

        assertFalse(nodeAt(nodeStore, EXISTING_PATH).exists());
        assertEquals(0L, walletLong(nodeStore, "contentCount"));
    }

    private static long walletLong(MemoryNodeStore nodeStore, String property) {
        return nodeAt(nodeStore, "/oak-chain/aa/bb/cc/" + WALLET).getProperty(property)
            .getValue(org.apache.jackrabbit.oak.api.Type.LONG);
    }

    private static FileStore fileStoreWithHeads(String previousHead, String currentHead) {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString()).thenReturn(previousHead);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn(currentHead);
        return fileStore;
    }

    private static MemoryNodeStore seededNodeStore(String path) throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder current = root;
        for (String part : path.substring(1).split("/")) {
            current = current.child(part);
        }
        current.setProperty("contentType", "page");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return nodeStore;
    }

    private static MemoryNodeStore seededLargeDeleteNodeStore() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder root = nodeStore.getRoot().builder();

        NodeBuilder deleteRoot = root;
        for (String part : LARGE_DELETE_ROOT_PATH.substring(1).split("/")) {
            deleteRoot = deleteRoot.child(part);
        }
        deleteRoot.setProperty("contentType", "wallet");

        for (int branch = 0; branch < 100; branch++) {
            NodeBuilder branchBuilder = deleteRoot.child("branch-" + branch);
            branchBuilder.setProperty("branchIndex", branch);
            for (int leaf = 0; leaf < 100; leaf++) {
                NodeBuilder leafBuilder = branchBuilder.child("leaf-" + leaf);
                leafBuilder.setProperty("leafIndex", leaf);
            }
        }

        NodeBuilder sibling = root;
        for (String part : LARGE_DELETE_SIBLING_PATH.substring(1).split("/")) {
            sibling = sibling.child(part);
        }
        sibling.setProperty("contentType", "page");

        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return nodeStore;
    }

    private static NodeState nodeAt(MemoryNodeStore nodeStore, String path) {
        NodeState current = nodeStore.getRoot();
        for (String part : path.substring(1).split("/")) {
            current = current.getChildNode(part);
        }
        return current;
    }
}
