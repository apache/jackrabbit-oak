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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageDispatcherTest {

    @Test
    public void testDispatchRejectsShortMessage() {
        MessageDispatcher dispatcher = new MessageDispatcher();
        DirectBuffer buffer = bufferFor(buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, ""));

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0, 4);

        assertFalse(result);
    }

    @Test
    public void testWriteProposalDispatchCallsCallback() {
        List<String> calls = new ArrayList<>();
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String wallet, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
                calls.add(wallet + "|" + path + "|" + proposalId);
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            }
        });
        dispatcher.setTermProvider(() -> 7L);

        String payload = "{\"walletAddress\":\"0xabc\",\"path\":\"/oak-chain/test\",\"proposalId\":\"p1\",\"term\":7}";
        DirectBuffer buffer = bufferFor(buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, payload));

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length);

        assertTrue(result);
        assertEquals(1, calls.size());
        assertEquals("0xabc|/oak-chain/test|p1", calls.get(0));
    }

    @Test
    public void testWriteProposalDispatchMapsExplicitAuditMetadata() {
        AtomicReference<MutationAuditMetadata> captured = new AtomicReference<>();
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message,
                                   String signature, String intentToken, String blobId, String mimeType,
                                   String ipfsCid, MutationAuditMetadata auditMetadata) {
                captured.set(auditMetadata);
            }
        });
        dispatcher.setTermProvider(() -> 7L);

        String payload = "{"
            + "\"walletAddress\":\"0xabc\","
            + "\"path\":\"/oak-chain/test\","
            + "\"proposalId\":\"p1\","
            + "\"transactionId\":\"tx-1\","
            + "\"correlationId\":\"corr-1\","
            + "\"ethereumTxHash\":\"0xeth\","
            + "\"confirmedBlockNumber\":42,"
            + "\"ethereumObservedEpoch\":84,"
            + "\"ethereumFinalizedEpoch\":82,"
            + "\"operation\":\"WRITE\","
            + "\"term\":7"
            + "}";

        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, payload));
        assertEquals("tx-1", captured.get().getTransactionId());
        assertEquals("corr-1", captured.get().getCorrelationId());
        assertEquals("p1", captured.get().getProposalId());
        assertEquals("0xeth", captured.get().getEthereumTxHash());
        assertEquals(Long.valueOf(42L), captured.get().getConfirmedBlockNumber());
        assertEquals(Long.valueOf(84L), captured.get().getEthereumObservedEpoch());
        assertEquals(Long.valueOf(82L), captured.get().getEthereumFinalizedEpoch());
        assertEquals(MutationAuditMetadata.Operation.WRITE, captured.get().getOperation());
    }

    @Test
    public void testWriteProposalStaleTermRejected() {
        AtomicReference<String> called = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String wallet, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
                called.set(wallet);
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            }
        });
        dispatcher.setTermProvider(() -> 10L);

        String payload = "{\"walletAddress\":\"0xabc\",\"path\":\"/oak-chain/test\",\"proposalId\":\"p1\",\"term\":7}";
        DirectBuffer buffer = bufferFor(buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, payload));

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length);

        assertFalse(result);
        assertNull(called.get());
    }

    @Test
    public void testWriteProposalMissingTermAcceptedForCompatibility() {
        AtomicReference<String> called = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String wallet, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
                called.set(wallet + "|" + path);
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            }
        });
        dispatcher.setTermProvider(() -> 10L);

        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL,
            "{\"walletAddress\":\"0xabc\",\"path\":\"/oak-chain/test\"}"));
        assertEquals("0xabc|/oak-chain/test", called.get());
    }

    @Test
    public void testWriteProposalWithoutCallbackFails() {
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setTermProvider(() -> 1L);

        assertMissingCallbackFails(dispatcher, SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL,
            "{\"walletAddress\":\"0xabc\",\"path\":\"/oak-chain/test\",\"term\":1}");
    }

    /** A missing callback is this node's wiring, not the entry: skipping it would diverge, so it must fail. */
    private void assertMissingCallbackFails(MessageDispatcher dispatcher, int templateId, String payload) {
        try {
            dispatch(dispatcher, templateId, payload);
            fail("a member without a write callback skipped the entry");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("callback"));
        }
    }

    @Test
    public void testWriteBatchDispatchProcessesAll() {
        List<String> calls = new ArrayList<>();
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String wallet, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
                calls.add(path);
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            }
        });

        String payload = "{\"batch\":[{\"walletAddress\":\"0x1\",\"path\":\"/a\"},{\"walletAddress\":\"0x2\",\"path\":\"/b\"}]}";
        DirectBuffer buffer = bufferFor(buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_WRITE_BATCH, payload));

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length);

        assertTrue(result);
        assertEquals(2, calls.size());
        assertEquals(2, dispatcher.getLastBatchSize());
    }

    @Test
    public void testWriteBatchSkipsInvalidAndStaleEntries() {
        List<String> calls = new ArrayList<>();
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String wallet, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
                calls.add(wallet + "|" + path);
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            }
        });
        dispatcher.setTermProvider(() -> 9L);

        String payload = "{\"batch\":[" +
            "{\"walletAddress\":\"0x1\",\"path\":\"/ok\",\"term\":9}," +
            "{\"walletAddress\":\"0x2\",\"path\":\"/stale\",\"term\":8}," +
            "{\"walletAddress\":\"0x3\",\"term\":9}" +
            "]}";

        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_WRITE_BATCH, payload));
        assertEquals(1, calls.size());
        assertEquals("0x1|/ok", calls.get(0));
        assertEquals(1, dispatcher.getLastBatchSize());
    }

    @Test
    public void testWriteBatchRejectsInvalidFormatAndFailsWithoutCallback() {
        MessageDispatcher dispatcher = new MessageDispatcher();

        assertMissingCallbackFails(dispatcher, SimpleMessageHeader.TEMPLATE_ID_WRITE_BATCH,
            "{\"batch\":[{\"walletAddress\":\"0x1\",\"path\":\"/ok\"}]}");
        dispatcher.setCallbacks(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            }
        });

        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_WRITE_BATCH, "{\"oops\":true}"));
    }

    @Test
    public void testDeleteProposalDispatchCallsCallback() {
        AtomicReference<String> deleted = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
                deleted.set(walletAddress + "|" + path + "|" + proposalId);
            }
        });
        dispatcher.setTermProvider(() -> 5L);

        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_DELETE_PROPOSAL,
            "{\"walletAddress\":\"0xabc\",\"path\":\"/oak-chain/test\",\"proposalId\":\"p1\",\"term\":5}"));
        assertEquals("0xabc|/oak-chain/test|p1", deleted.get());
    }

    @Test
    public void testDeleteProposalDispatchMapsExplicitAuditMetadata() {
        AtomicReference<MutationAuditMetadata> captured = new AtomicReference<>();
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyDelete(String walletAddress, String path, String signature,
                                    MutationAuditMetadata auditMetadata) {
                captured.set(auditMetadata);
            }
        });
        dispatcher.setTermProvider(() -> 5L);

        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_DELETE_PROPOSAL,
            "{"
                + "\"walletAddress\":\"0xabc\","
                + "\"path\":\"/oak-chain/test\","
                + "\"proposalId\":\"p1\","
                + "\"transactionId\":\"tx-2\","
                + "\"correlationId\":\"corr-2\","
                + "\"ethereumObservedEpoch\":12,"
                + "\"operation\":\"DELETE\","
                + "\"term\":5"
                + "}"));
        assertEquals(MutationAuditMetadata.Operation.DELETE, captured.get().getOperation());
        assertEquals("tx-2", captured.get().getTransactionId());
        assertEquals("corr-2", captured.get().getCorrelationId());
        assertEquals("p1", captured.get().getProposalId());
        assertEquals(Long.valueOf(12L), captured.get().getEthereumObservedEpoch());
    }

    @Test
    public void testDeleteProposalRejectsMissingFieldsAndFailsWithoutCallback() {
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setTermProvider(() -> 5L);

        assertMissingCallbackFails(dispatcher, SimpleMessageHeader.TEMPLATE_ID_DELETE_PROPOSAL,
            "{\"walletAddress\":\"0xabc\",\"path\":\"/oak-chain/test\",\"term\":5}");

        dispatcher.setCallbacks(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            }
        });

        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_DELETE_PROPOSAL,
            "{\"walletAddress\":\"0xabc\",\"term\":5}"));
    }

    @Test
    public void testGcProposalDispatch() {
        AtomicReference<String> proposalId = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setGCCallback(new MessageDispatcher.GCCallback() {
            @Override
            public void applyGCProposal(String proposalIdValue, String proposerWallet, String targetRevision,
                                        long estimatedReclaimableSizeMB, String estimatedCostUSDC) {
                proposalId.set(proposalIdValue);
            }

            @Override
            public void applyGCVote(String proposalId, int validatorId, boolean approve, String reason) {
            }

            @Override
            public void applyGCExecute(String proposalId, int executorId) {
            }
        });

        String payload = "{\"proposalId\":\"gc-1\",\"proposerWallet\":\"0xabc\",\"targetRevision\":\"r1\",\"estimatedReclaimableSizeMB\":12,\"estimatedCostUSDC\":\"3.1\"}";
        DirectBuffer buffer = bufferFor(buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_GC_PROPOSAL, payload));

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length);

        assertTrue(result);
        assertEquals("gc-1", proposalId.get());
    }

    @Test
    public void testGcVoteAndExecuteDispatch() {
        AtomicReference<String> vote = new AtomicReference<>(null);
        AtomicReference<String> execute = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setGCCallback(new MessageDispatcher.GCCallback() {
            @Override
            public void applyGCProposal(String proposalId, String proposerWallet, String targetRevision,
                                        long estimatedReclaimableSizeMB, String estimatedCostUSDC) {
            }

            @Override
            public void applyGCVote(String proposalId, int validatorId, boolean approve, String reason) {
                vote.set(proposalId + "|" + validatorId + "|" + approve + "|" + reason);
            }

            @Override
            public void applyGCExecute(String proposalId, int executorId) {
                execute.set(proposalId + "|" + executorId);
            }
        });

        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_GC_VOTE,
            "{\"proposalId\":\"gc-2\",\"validatorId\":4,\"approve\":true,\"reason\":\"ok\"}"));
        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_GC_EXECUTE,
            "{\"proposalId\":\"gc-2\",\"executorId\":7}"));

        assertEquals("gc-2|4|true|ok", vote.get());
        assertEquals("gc-2|7", execute.get());
    }

    @Test
    public void testGcHandlersRejectMissingCallbackAndRequiredFields() {
        MessageDispatcher dispatcher = new MessageDispatcher();

        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_GC_VOTE,
            "{\"proposalId\":\"gc-3\",\"validatorId\":1,\"approve\":true}"));

        dispatcher.setGCCallback(new MessageDispatcher.GCCallback() {
            @Override
            public void applyGCProposal(String proposalId, String proposerWallet, String targetRevision,
                                        long estimatedReclaimableSizeMB, String estimatedCostUSDC) {
            }

            @Override
            public void applyGCVote(String proposalId, int validatorId, boolean approve, String reason) {
            }

            @Override
            public void applyGCExecute(String proposalId, int executorId) {
            }
        });

        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_GC_VOTE,
            "{\"proposalId\":\"gc-3\",\"validatorId\":1}"));
        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_GC_EXECUTE,
            "{\"proposalId\":\"gc-3\"}"));
    }

    @Test
    public void testDurabilitySegmentPersistedDispatchAndLegacyAckIgnored() {
        AtomicReference<String> persisted = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setDurabilityCallback(new MessageDispatcher.DurabilityCallback() {
            @Override
            public void onSegmentPersisted(String proposalId, int memberId, String durableHead, boolean success, String error) {
                persisted.set(proposalId + "|" + memberId + "|" + durableHead + "|" + success + "|" + error);
            }
        });

        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_SEGMENT_PERSISTED,
            "{\"proposalId\":\"p-2\",\"memberId\":4,\"durableHead\":\"dh1\",\"success\":false,\"error\":\"disk\"}"));
        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_ACK_SEGMENT_PERSISTED,
            "{\"proposalId\":\"p-2\",\"success\":true,\"durableHead\":\"dh2\",\"totalMembers\":5,\"requiredAcks\":3}"));

        assertEquals("p-2|4|dh1|false|disk", persisted.get());
    }

    @Test
    public void testDurabilityHandlersRejectMissingCallbackAndRequiredFields() {
        MessageDispatcher dispatcher = new MessageDispatcher();

        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_SEGMENT_PERSISTED,
            "{\"proposalId\":\"p-1\",\"memberId\":1,\"success\":true}"));

        dispatcher.setDurabilityCallback(new MessageDispatcher.DurabilityCallback() {
            @Override
            public void onSegmentPersisted(String proposalId, int memberId, String durableHead, boolean success, String error) {
            }
        });

        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_SEGMENT_PERSISTED,
            "{\"proposalId\":\"p-1\",\"memberId\":1}"));
    }

    @Test
    public void testStartTransactionDispatch() {
        AtomicReference<String> started = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setTransactionCallback(new MessageDispatcher.TransactionCallback() {
            @Override
            public void onStartTransaction(String transactionId, String correlationId, long timeoutMs, String initiatorWallet) {
                started.set(transactionId + "|" + correlationId + "|" + timeoutMs + "|" + initiatorWallet);
            }

            @Override
            public void onCommitTransaction(String transactionId, String correlationId) {
            }

            @Override
            public void onAbortTransaction(String transactionId, String correlationId, String reason) {
            }
        });
        dispatcher.setTermProvider(() -> 11L);

        String payload = "{\"transactionId\":\"tx-1\",\"correlationId\":\"corr-1\",\"timeoutMs\":15000,\"initiatorWallet\":\"0xabc\",\"term\":11}";
        DirectBuffer buffer = bufferFor(buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_START_TRANSACTION, payload));

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length);

        assertTrue(result);
        assertEquals("tx-1|corr-1|15000|0xabc", started.get());
    }

    @Test
    public void testStartTransactionUsesDefaultTimeoutWhenMissing() {
        AtomicReference<String> started = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setTransactionCallback(new MessageDispatcher.TransactionCallback() {
            @Override
            public void onStartTransaction(String transactionId, String correlationId, long timeoutMs, String initiatorWallet) {
                started.set(transactionId + "|" + correlationId + "|" + timeoutMs + "|" + initiatorWallet);
            }

            @Override
            public void onCommitTransaction(String transactionId, String correlationId) {
            }

            @Override
            public void onAbortTransaction(String transactionId, String correlationId, String reason) {
            }
        });

        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_START_TRANSACTION,
            "{\"transactionId\":\"tx-4\",\"correlationId\":\"corr-4\",\"initiatorWallet\":\"0xdef\"}"));
        assertEquals("tx-4|corr-4|30000|0xdef", started.get());
    }

    @Test
    public void testCommitTransactionDispatch() {
        AtomicReference<String> committed = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setTransactionCallback(new MessageDispatcher.TransactionCallback() {
            @Override
            public void onStartTransaction(String transactionId, String correlationId, long timeoutMs, String initiatorWallet) {
            }

            @Override
            public void onCommitTransaction(String transactionId, String correlationId) {
                committed.set(transactionId + "|" + correlationId);
            }

            @Override
            public void onAbortTransaction(String transactionId, String correlationId, String reason) {
            }
        });

        String payload = "{\"transactionId\":\"tx-2\",\"correlationId\":\"corr-2\"}";
        DirectBuffer buffer = bufferFor(buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_COMMIT_TRANSACTION, payload));

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length);

        assertTrue(result);
        assertEquals("tx-2|corr-2", committed.get());
    }

    @Test
    public void testTransactionHandlersRejectMissingCallbackOrTransactionId() {
        MessageDispatcher dispatcher = new MessageDispatcher();

        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_COMMIT_TRANSACTION,
            "{\"transactionId\":\"tx-5\",\"correlationId\":\"corr-5\"}"));

        dispatcher.setTransactionCallback(new MessageDispatcher.TransactionCallback() {
            @Override
            public void onStartTransaction(String transactionId, String correlationId, long timeoutMs, String initiatorWallet) {
            }

            @Override
            public void onCommitTransaction(String transactionId, String correlationId) {
            }

            @Override
            public void onAbortTransaction(String transactionId, String correlationId, String reason) {
            }
        });

        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_START_TRANSACTION,
            "{\"correlationId\":\"corr-6\"}"));
        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_COMMIT_TRANSACTION,
            "{\"correlationId\":\"corr-6\"}"));
        assertFalse(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_ABORT_TRANSACTION,
            "{\"reason\":\"timeout\"}"));
    }

    @Test
    public void testAbortTransactionStaleTermRejected() {
        AtomicReference<String> aborted = new AtomicReference<>(null);
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.setTransactionCallback(new MessageDispatcher.TransactionCallback() {
            @Override
            public void onStartTransaction(String transactionId, String correlationId, long timeoutMs, String initiatorWallet) {
            }

            @Override
            public void onCommitTransaction(String transactionId, String correlationId) {
            }

            @Override
            public void onAbortTransaction(String transactionId, String correlationId, String reason) {
                aborted.set(transactionId);
            }
        });
        dispatcher.setTermProvider(() -> 20L);

        String payload = "{\"transactionId\":\"tx-3\",\"correlationId\":\"corr-3\",\"reason\":\"timeout\",\"term\":19}";
        DirectBuffer buffer = bufferFor(buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_ABORT_TRANSACTION, payload));

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length);

        assertFalse(result);
        assertNull(aborted.get());
    }

    @Test
    public void testDispatchRejectsPayloadShorterThanHeaderBlockLength() {
        MessageDispatcher dispatcher = new MessageDispatcher();
        byte[] bytes = buildMessageBytes(SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, "{}");
        DirectBuffer buffer = bufferFor(bytes);

        boolean result = dispatcher.dispatch(System.currentTimeMillis(), buffer, 0, SimpleMessageHeader.ENCODED_LENGTH + 1);

        assertFalse(result);
    }

    @Test
    public void testDispatchRejectsUnknownTemplateAndAcceptsGenesisAndSnapshot() {
        MessageDispatcher dispatcher = new MessageDispatcher();

        assertFalse(dispatch(dispatcher, 999, "{}"));
        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_GENESIS_PROPOSAL, "{}"));
        assertTrue(dispatch(dispatcher, SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT, "{}"));
    }

    @Test
    public void testLifecycleAndSetterMethodsAreCallable() {
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.activate();
        dispatcher.setCallbacks(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message, String signature,
                                   String intentToken, String blobId, String mimeType, String ipfsCid, String proposalId) {
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            }
        });
        dispatcher.setGCCallback(new MessageDispatcher.GCCallback() {
            @Override
            public void applyGCProposal(String proposalId, String proposerWallet, String targetRevision,
                                        long estimatedReclaimableSizeMB, String estimatedCostUSDC) {
            }

            @Override
            public void applyGCVote(String proposalId, int validatorId, boolean approve, String reason) {
            }

            @Override
            public void applyGCExecute(String proposalId, int executorId) {
            }
        });
        dispatcher.setDurabilityCallback(new MessageDispatcher.DurabilityCallback() {
            @Override
            public void onSegmentPersisted(String proposalId, int memberId, String durableHead, boolean success, String error) {
            }
        });
        dispatcher.setTransactionCallback(new MessageDispatcher.TransactionCallback() {
            @Override
            public void onStartTransaction(String transactionId, String correlationId, long timeoutMs, String initiatorWallet) {
            }

            @Override
            public void onCommitTransaction(String transactionId, String correlationId) {
            }

            @Override
            public void onAbortTransaction(String transactionId, String correlationId, String reason) {
            }
        });
        dispatcher.setTermProvider(() -> 3L);
        dispatcher.deactivate();
    }

    @Test
    public void agentTerminationRaisedInsideApplyPropagatesOutOfDispatch() {
        AgentTerminationException termination = new AgentTerminationException("interrupted");
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message,
                                   String signature, String intentToken, String blobId, String mimeType,
                                   String ipfsCid, MutationAuditMetadata auditMetadata) {
                throw termination;
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature,
                                    MutationAuditMetadata auditMetadata) {
                throw termination;
            }
        });
        dispatcher.setGCCallback(new MessageDispatcher.GCCallback() {
            @Override
            public void applyGCProposal(String proposalId, String proposerWallet, String targetRevision,
                                        long estimatedReclaimableSizeMB, String estimatedCostUSDC) {
                throw termination;
            }

            @Override
            public void applyGCVote(String proposalId, int validatorId, boolean approve, String reason) {
                throw termination;
            }

            @Override
            public void applyGCExecute(String proposalId, int executorId) {
                throw termination;
            }
        });
        dispatcher.setDurabilityCallback(new MessageDispatcher.DurabilityCallback() {
            @Override
            public void onSegmentPersisted(String proposalId, int memberId, String durableHead, boolean success,
                                           String error) {
                throw termination;
            }
        });
        dispatcher.setTransactionCallback(new MessageDispatcher.TransactionCallback() {
            @Override
            public void onStartTransaction(String transactionId, String correlationId, long timeoutMs,
                                           String initiatorWallet) {
                throw termination;
            }

            @Override
            public void onCommitTransaction(String transactionId, String correlationId) {
                throw termination;
            }

            @Override
            public void onAbortTransaction(String transactionId, String correlationId, String reason) {
                throw termination;
            }
        });
        String write = "\"walletAddress\":\"0xabc\",\"path\":\"/oak-chain/test\"";
        String[][] messages = {
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL), "{" + write + "}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_DELETE_PROPOSAL), "{" + write + "}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_WRITE_BATCH), "{\"batch\":[{" + write + "}]}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_GC_PROPOSAL),
                "{\"proposalId\":\"gc\",\"proposerWallet\":\"0xabc\"}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_GC_VOTE),
                "{\"proposalId\":\"gc\",\"validatorId\":1,\"approve\":true}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_GC_EXECUTE), "{\"proposalId\":\"gc\",\"executorId\":1}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_SEGMENT_PERSISTED),
                "{\"proposalId\":\"p\",\"memberId\":1,\"success\":true}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_START_TRANSACTION), "{\"transactionId\":\"t\"}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_COMMIT_TRANSACTION), "{\"transactionId\":\"t\"}"},
            {String.valueOf(SimpleMessageHeader.TEMPLATE_ID_ABORT_TRANSACTION), "{\"transactionId\":\"t\"}"},
        };

        for (String[] message : messages) {
            try {
                dispatch(dispatcher, Integer.parseInt(message[0]), message[1]);
                fail("template " + message[0] + " swallowed the AgentTerminationException");
            } catch (AgentTerminationException e) {
                assertSame(termination, e);
            }
        }
    }

    private boolean dispatch(MessageDispatcher dispatcher, int templateId, String payload) {
        DirectBuffer buffer = bufferFor(buildMessageBytes(templateId, payload));
        return dispatcher.dispatch(System.currentTimeMillis(), buffer, 0,
            SimpleMessageHeader.ENCODED_LENGTH + payload.getBytes(StandardCharsets.UTF_8).length);
    }

    private byte[] buildMessageBytes(int templateId, String payload) {
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[SimpleMessageHeader.ENCODED_LENGTH + payloadBytes.length];
        putShortLE(bytes, 0, (short) payloadBytes.length);
        putShortLE(bytes, 2, (short) templateId);
        putShortLE(bytes, 4, (short) 1);
        putShortLE(bytes, 6, (short) 1);
        System.arraycopy(payloadBytes, 0, bytes, SimpleMessageHeader.ENCODED_LENGTH, payloadBytes.length);
        return bytes;
    }

    private DirectBuffer bufferFor(byte[] bytes) {
        DirectBuffer buffer = mock(DirectBuffer.class);
        Mockito.doAnswer(invocation -> {
            int offset = invocation.getArgument(0);
            byte[] dest = invocation.getArgument(1);
            int length = Math.min(dest.length, bytes.length - offset);
            if (length > 0) {
                System.arraycopy(bytes, offset, dest, 0, length);
            }
            return null;
        }).when(buffer).getBytes(Mockito.anyInt(), any(byte[].class));
        when(buffer.getShort(Mockito.anyInt(), Mockito.any(java.nio.ByteOrder.class))).thenAnswer(invocation -> {
            int offset = invocation.getArgument(0);
            java.nio.ByteOrder order = invocation.getArgument(1);
            if (order != java.nio.ByteOrder.LITTLE_ENDIAN) {
                throw new IllegalArgumentException("Unexpected byte order");
            }
            int lo = bytes[offset] & 0xFF;
            int hi = bytes[offset + 1] & 0xFF;
            return (short) ((hi << 8) | lo);
        });
        return buffer;
    }

    private void putShortLE(byte[] bytes, int offset, short value) {
        bytes[offset] = (byte) (value & 0xFF);
        bytes[offset + 1] = (byte) ((value >> 8) & 0xFF);
    }
}
