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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.blob.cloud.ipfs.IPFSDataStore;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

public class WriteApplicationServiceTest {

    private static final String WALLET = "0x1234567890abcdef1234567890abcdef12345678";
    private static final String PATH = "/oak-chain/aa/bb/cc/" + WALLET + "/Acme/content/doc-1";

    @Test
    public void testApplyWriteCreatesWalletAndContentNodesAndInvokesCallbacks() {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        doAnswer(invocation -> {
            Runnable callback = invocation.getArgument(0);
            if (callback != null) {
                callback.run();
            }
            return true;
        }).when(flushService).onChangeApplied(any());
        WriteApplicationService service = new WriteApplicationService(fileStore, nodeStore, null, flushService);

        AtomicReference<String> updatedHead = new AtomicReference<>();
        AtomicReference<String> fragmentedWallet = new AtomicReference<>();
        AtomicReference<String> durableProposal = new AtomicReference<>();
        AtomicReference<String> durableHead = new AtomicReference<>();
        AtomicReference<String> sseEvent = new AtomicReference<>();
        service.setHeadUpdateCallback(updatedHead::set);
        service.setFragmentationCallback(fragmentedWallet::set);
        service.setDurabilityCallback(new WriteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String head) {
                durableProposal.set(proposalId);
                durableHead.set(head);
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });
        service.setSseEventCallback(new WriteApplicationService.SSEEventCallback() {
            @Override
            public void emitContentWrite(String path, String wallet, String org, String message, String signature, String contentType) {
                sseEvent.set("content:" + path + ":" + wallet + ":" + org + ":" + contentType);
            }

            @Override
            public void emitBinaryUpload(String path, String wallet, String org, String message, String ipfsCid, String mimeType) {
                sseEvent.set("binary");
            }
        });

        String newHead = service.applyWrite(
            WALLET,
            PATH,
            "page",
            "{\"title\":\"Hello\",\"body\":\"World\",\"tags\":[\"one\",\"two\"],\"meta\":{\"published\":true},\"payload\":{\"kind\":\"note\"}}",
            "0xsig",
            null,
            null,
            null,
            null,
            "proposal-1");

        assertEquals("new-head", newHead);
        assertEquals("new-head", updatedHead.get());
        assertEquals(WALLET, fragmentedWallet.get());
        assertEquals("proposal-1", durableProposal.get());
        assertEquals("new-head", durableHead.get());
        assertEquals("content:" + PATH + ":" + WALLET + ":Acme:page", sseEvent.get());
        verify(flushService).onChangeApplied(any());

        NodeState walletNode = contentNode(nodeStore, "/oak-chain/aa/bb/cc/" + WALLET);
        assertEquals(WALLET, stringProperty(walletNode, "wallet"));
        assertEquals("wallet-root", stringProperty(walletNode, "nodeType"));
        assertEquals(1L, longProperty(walletNode, "contentCount"));
        assertEquals(1L, longProperty(walletNode, "totalWrites"));

        NodeState contentNode = contentNode(nodeStore, PATH);
        assertEquals("page", stringProperty(contentNode, "contentType"));
        assertEquals("proposal-1", stringProperty(contentNode, "oak:proposalId"));
        assertEquals("0xsig", stringProperty(contentNode, "signature"));
        assertEquals("Acme", stringProperty(contentNode, "organization"));
        assertEquals("Hello", stringProperty(contentNode, "oak:title"));
        assertEquals("World", stringProperty(contentNode, "oak:body"));
        assertEquals("{\"published\":true}", stringProperty(contentNode, "oak:metaJson"));
        assertEquals("{\"kind\":\"note\"}", stringProperty(contentNode, "oak:payloadJson"));
        assertEquals(Arrays.asList("one", "two"), stringListProperty(contentNode, "oak:tags"));
    }

    @Test
    public void testApplyWriteWithAuditMetadataStillAppliesWhenEpochFieldsAbsent() {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        doAnswer(invocation -> {
            Runnable callback = invocation.getArgument(0);
            if (callback != null) {
                callback.run();
            }
            return true;
        }).when(flushService).onChangeApplied(any());
        WriteApplicationService service = new WriteApplicationService(fileStore, nodeStore, null, flushService);

        AtomicReference<String> durableProposal = new AtomicReference<>();
        service.setDurabilityCallback(new WriteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String head) {
                durableProposal.set(proposalId);
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });

        String newHead = service.applyWriteWithAuditMetadata(
            WALLET,
            PATH,
            "page",
            "body",
            "0xsig",
            null,
            null,
            null,
            null,
            MutationAuditMetadata.write("tx-1", "corr-1", "proposal-audit", "0xeth", null, null, null)
        );

        assertEquals("new-head", newHead);
        assertEquals("proposal-audit", durableProposal.get());
        assertEquals("proposal-audit", stringProperty(contentNode(nodeStore, PATH), "oak:proposalId"));
    }

    @Test
    public void testApplyWriteStoresClientIpfsReferenceAndIntentToken() {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        doAnswer(invocation -> true).when(flushService).onChangeApplied(any());
        WriteApplicationService service = new WriteApplicationService(fileStore, nodeStore, null, flushService);

        service.applyWrite(
            WALLET,
            PATH,
            "asset",
            "plain-text",
            "0xsig",
            "intent-1",
            null,
            null,
            "bafybeigdyrzt5",
            null);

        NodeState contentNode = contentNode(nodeStore, PATH);
        assertEquals("client", stringProperty(contentNode, "oak:binaryStorageMode"));
        assertEquals("bafybeigdyrzt5", stringProperty(contentNode, "ipfsCid"));
        assertEquals("https://ipfs.io/ipfs/bafybeigdyrzt5", stringProperty(contentNode, "ipfsGateway"));
        assertEquals("intent-1", stringProperty(contentNode, "jcr:intentToken"));
        assertTrue(booleanProperty(contentNode, "jcr:pendingBinary"));
    }

    @Test
    public void testApplyWriteUsesConfiguredGatewayBaseForStoredIpfsGateway() throws Exception {
        withProperty("ipfs.gateway.base", "http://127.0.0.1:8099/ipfs/", () -> {
            FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
            MemoryNodeStore nodeStore = new MemoryNodeStore();
            FileStoreFlushService flushService = mock(FileStoreFlushService.class);
            WriteApplicationService service = new WriteApplicationService(fileStore, nodeStore, null, flushService);

            service.applyWrite(
                WALLET,
                PATH,
                "asset",
                "plain-text",
                "0xsig",
                null,
                null,
                null,
                "bafybeigdyrzt5",
                null);

            NodeState contentNode = contentNode(nodeStore, PATH);
            assertEquals("http://127.0.0.1:8099/ipfs/bafybeigdyrzt5", stringProperty(contentNode, "ipfsGateway"));
        });
    }

    @Test
    public void testApplyWriteUsesLateBoundNodeStoreWhenResolvingBinaryEventCid() {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        AtomicReference<MemoryNodeStore> nodeStoreRef = new AtomicReference<>();
        doAnswer(invocation -> true).when(flushService).onChangeApplied(any());
        WriteApplicationService service = new WriteApplicationService(
            fileStore,
            nodeStoreRef::get,
            () -> (BlobStore) null,
            flushService);

        AtomicReference<String> binaryEvent = new AtomicReference<>();
        service.setSseEventCallback(new WriteApplicationService.SSEEventCallback() {
            @Override
            public void emitContentWrite(String path, String wallet, String org, String message, String signature, String contentType) {
                binaryEvent.set("unexpected-content-event");
            }

            @Override
            public void emitBinaryUpload(String path, String wallet, String org, String message, String ipfsCid, String mimeType) {
                binaryEvent.set(path + "|" + wallet + "|" + org + "|" + ipfsCid + "|" + mimeType);
            }
        });

        MemoryNodeStore authoritativeStore = new MemoryNodeStore();
        nodeStoreRef.set(authoritativeStore);

        String newHead = service.applyWrite(
            WALLET,
            PATH,
            "file",
            "binary-message",
            "0xsig",
            null,
            "deadbeef#123",
            "image/jpeg",
            null,
            "proposal-binary");

        assertEquals("new-head", newHead);
        assertEquals(PATH + "|" + WALLET + "|Acme|null|image/jpeg", binaryEvent.get());
        assertEquals("deadbeef#123", stringProperty(contentNode(authoritativeStore, PATH), "jcr:data"));
        verify(flushService).onChangeApplied(any());
    }

    @Test
    public void binaryWriteStoresTheSameContentOnMembersWithDifferentIpfsIndexesAndNeverAsksIpfs() {
        IPFSDataStore indexed = mock(IPFSDataStore.class);
        when(indexed.getCID(any())).thenReturn("bafyIndexedOnlyHere");
        IPFSDataStore notIndexed = mock(IPFSDataStore.class);

        NodeState memberA = applyBinaryWrite(indexed, null);
        NodeState memberB = applyBinaryWrite(notIndexed, null);

        assertEquals(nonBinaryProperties(memberA), nonBinaryProperties(memberB));
        assertFalse(memberA.hasProperty("ipfsCid"));
        verifyNoInteractions(indexed, notIndexed);
    }

    @Test
    public void binaryWriteStoresTheCidCarriedInTheProposal() {
        IPFSDataStore dataStore = mock(IPFSDataStore.class);
        when(dataStore.getCID(any())).thenReturn("bafyLocal");

        NodeState content = applyBinaryWrite(dataStore, "bafyFromProposal");

        assertEquals("bafyFromProposal", stringProperty(content, "ipfsCid"));
        verifyNoInteractions(dataStore);
    }

    @Test
    public void testApplyWriteIsIdempotentForDuplicateProposalReplay() {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        doAnswer(invocation -> {
            Runnable callback = invocation.getArgument(0);
            if (callback != null) {
                callback.run();
            }
            return true;
        }).when(flushService).onChangeApplied(any());
        WriteApplicationService service = new WriteApplicationService(fileStore, nodeStore, null, flushService);

        AtomicInteger durableCount = new AtomicInteger();
        service.setDurabilityCallback(new WriteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String durableHead) {
                durableCount.incrementAndGet();
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });

        service.applyWrite(
            WALLET,
            PATH,
            "page",
            "body",
            "0xsig",
            null,
            null,
            null,
            null,
            "proposal-replay");

        service.applyWrite(
            WALLET,
            PATH,
            "page",
            "body",
            "0xsig",
            null,
            null,
            null,
            null,
            "proposal-replay");

        NodeState walletNode = contentNode(nodeStore, "/oak-chain/aa/bb/cc/" + WALLET);
        NodeState contentNode = contentNode(nodeStore, PATH);
        assertEquals(1L, longProperty(walletNode, "contentCount"));
        assertEquals(1L, longProperty(walletNode, "totalWrites"));
        assertEquals("proposal-replay", stringProperty(contentNode, "oak:proposalId"));
        assertEquals("a duplicate replay persisted nothing new, so it must not ack again", 1, durableCount.get());
        verify(flushService).onChangeApplied(any());
    }

    @Test
    public void testApplyWriteDeliversDurabilityCallbackWhenDeferredFlushCompletes() {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        AtomicReference<Runnable> deferredFlush = new AtomicReference<>();
        doAnswer(invocation -> {
            deferredFlush.set(invocation.getArgument(0));
            return false;
        }).when(flushService).onChangeApplied(any());
        WriteApplicationService service = new WriteApplicationService(fileStore, nodeStore, null, flushService);

        AtomicReference<String> durableProposal = new AtomicReference<>();
        AtomicReference<String> durableHead = new AtomicReference<>();
        service.setDurabilityCallback(new WriteApplicationService.DurabilityCallback() {
            @Override
            public void onDurable(String proposalId, String head) {
                durableProposal.set(proposalId);
                durableHead.set(head);
            }

            @Override
            public void onFailure(String proposalId, String error) {
            }
        });

        String newHead = service.applyWrite(
            WALLET,
            PATH,
            "page",
            "body",
            "0xsig",
            null,
            null,
            null,
            null,
            "proposal-deferred");

        assertEquals("new-head", newHead);
        assertNull(durableProposal.get());
        assertNull(durableHead.get());
        assertNotNull(deferredFlush.get());
        deferredFlush.get().run();
        assertEquals("proposal-deferred", durableProposal.get());
        assertEquals("new-head", durableHead.get());
        verify(flushService).onChangeApplied(any());
    }

    @Test
    public void testApplyWriteReportsDurabilityFailureForInvalidPath() {
        FileStore fileStore = fileStoreWithHeads("prev-head", "new-head");
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        FileStoreFlushService flushService = mock(FileStoreFlushService.class);
        WriteApplicationService service = new WriteApplicationService(fileStore, nodeStore, null, flushService);

        AtomicReference<String> failedProposal = new AtomicReference<>();
        AtomicReference<String> failureMessage = new AtomicReference<>();
        service.setDurabilityCallback(new WriteApplicationService.DurabilityCallback() {
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
            service.applyWrite(
                WALLET,
                "/invalid/path",
                "page",
                "body",
                "0xsig",
                null,
                null,
                null,
                null,
                "proposal-fail");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Failed to apply replicated write"));
        }

        assertEquals("proposal-fail", failedProposal.get());
        assertNotNull(failureMessage.get());
        assertTrue(failureMessage.get().contains("Invalid path format"));
    }

    @Test
    public void testExtractOrganizationFromPath() {
        WriteApplicationService service = new WriteApplicationService(
            fileStoreWithHeads("prev-head", "new-head"),
            new MemoryNodeStore(),
            null,
            mock(FileStoreFlushService.class));

        assertEquals("Acme", service.extractOrganizationFromPath(PATH));
        assertEquals(null, service.extractOrganizationFromPath("/oak-chain/aa/bb/cc/" + WALLET + "/content/doc-1"));
        assertEquals(null, service.extractOrganizationFromPath(null));
    }

    @Test
    public void testDerivedCanonicalPropertiesDecodeEscapedJson() {
        String title = "He said \"hi\" \\ ok\nline \u00e9 \uD83D\uDE00";
        String body = "a } b { \"q\" \t tab \u0001 \u65e5\u672c";
        List<String> tags = Arrays.asList("a\"b", "c\\d", "e\nf", "\uD83D\uDE00", "");
        String meta = "{\"note\":" + JsopBuilder.encode("x \"y\" } \\") + ",\"n\":[1,-2,{\"k\":\"v\"}],"
            + "\"ok\":true,\"z\":null,\"f\":1.5}";
        String payload = "{\"kind\":" + JsopBuilder.encode("note \"q\"\n") + "}";
        StringBuilder tagsJson = new StringBuilder("[");
        for (String tag : tags) {
            tagsJson.append(tagsJson.length() > 1 ? "," : "").append(JsopBuilder.encode(tag));
        }
        String message = "{\"title\":" + JsopBuilder.encode(title) + ",\"body\":" + JsopBuilder.encode(body)
            + ",\"tags\":" + tagsJson + "]" + ",\"meta\":" + meta + ",\"payload\":" + payload + "}";

        NodeState contentNode = applyMessage(message);

        assertEquals(message, stringProperty(contentNode, "message"));
        assertEquals(title, stringProperty(contentNode, "oak:title"));
        assertEquals(body, stringProperty(contentNode, "oak:body"));
        assertEquals(tags, stringListProperty(contentNode, "oak:tags"));
        assertEquals(meta, stringProperty(contentNode, "oak:metaJson"));
        assertEquals(payload, stringProperty(contentNode, "oak:payloadJson"));
    }

    @Test
    public void testDerivedCanonicalPropertiesOnlyForTopLevelFieldsOfJsonObjects() {
        for (String message : new String[] {"plain \"title\":\"x\"", "{\"title\":\"unterminated}", "[\"title\"]"}) {
            NodeState contentNode = applyMessage(message);
            assertEquals(message, stringProperty(contentNode, "message"));
            assertNull(message, contentNode.getProperty("oak:title"));
        }
        NodeState nested = applyMessage("{\"meta\":{\"title\":\"nested\",\"tags\":[\"t\"]}}");
        assertNull(nested.getProperty("oak:title"));
        assertNull(nested.getProperty("oak:tags"));
    }

    private static NodeState applyMessage(String message) {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        WriteApplicationService service = new WriteApplicationService(
            fileStoreWithHeads("prev-head", "new-head"), nodeStore, null, mock(FileStoreFlushService.class));
        service.applyWrite(WALLET, PATH, "page", message, "0xsig", null, null, null, null, "proposal-1");
        return contentNode(nodeStore, PATH);
    }

    private static NodeState applyBinaryWrite(IPFSDataStore localIpfs, String proposalCid) {
        DataStoreBlobStore blobStore = mock(DataStoreBlobStore.class);
        when(blobStore.getDataStore()).thenReturn(localIpfs);
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        WriteApplicationService service = new WriteApplicationService(
            fileStoreWithHeads("prev-head", "new-head"), nodeStore, blobStore, mock(FileStoreFlushService.class));
        service.applyWriteWithAuditMetadata(WALLET, PATH, "file", "m", "0xsig", null, "blob-1#3", "image/png",
            proposalCid, MutationAuditMetadata.write(null, null, "p-1", null, null, null, null).withClusterTimestamp(1_000L));
        return contentNode(nodeStore, PATH);
    }

    private static Map<String, String> nonBinaryProperties(NodeState node) {
        Map<String, String> properties = new TreeMap<>();
        for (PropertyState property : node.getProperties()) {
            if (property.getType() != Type.BINARY) {
                properties.put(property.getName(), property.getValue(Type.STRING));
            }
        }
        return properties;
    }

    private static FileStore fileStoreWithHeads(String previousHead, String newHead) {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString()).thenReturn(previousHead);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn(newHead);
        return fileStore;
    }

    private static NodeState contentNode(MemoryNodeStore nodeStore, String path) {
        NodeState current = nodeStore.getRoot();
        for (String part : path.substring(1).split("/")) {
            current = current.getChildNode(part);
        }
        return current;
    }

    private static String stringProperty(NodeState node, String name) {
        return node.getProperty(name).getValue(Type.STRING);
    }

    private static long longProperty(NodeState node, String name) {
        return node.getProperty(name).getValue(Type.LONG);
    }

    private static boolean booleanProperty(NodeState node, String name) {
        return node.getProperty(name).getValue(Type.BOOLEAN);
    }

    private static List<String> stringListProperty(NodeState node, String name) {
        PropertyState property = node.getProperty(name);
        List<String> values = new ArrayList<>();
        for (String value : property.getValue(Type.STRINGS)) {
            values.add(value);
        }
        return values;
    }

    private static void withProperty(String key, String value, ThrowingRunnable runnable) throws Exception {
        String previous = System.getProperty(key);
        try {
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
            runnable.run();
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
